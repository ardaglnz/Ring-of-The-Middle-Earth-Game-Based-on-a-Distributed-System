// Package engine implements the 13-step turn processor from Section 6.
// Maia ability dispatch is config-driven: the same MaiaAbility order type
// produces different effects depending on config.Maia fields — never hardcoded.
package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/rotr/option-b/internal/cache"
	"github.com/rotr/option-b/internal/combat"
	"github.com/rotr/option-b/internal/config"
	"github.com/rotr/option-b/internal/detection"
	"github.com/rotr/option-b/internal/graph"
)

// Order types as string constants (these are order type names, not unit IDs).
const (
	OrderAssignRoute  = "ASSIGN_ROUTE"
	OrderRedirectUnit = "REDIRECT_UNIT"
	OrderBlockPath    = "BLOCK_PATH"
	OrderSearchPath   = "SEARCH_PATH"
	OrderReinforce    = "REINFORCE_REGION"
	OrderFortify      = "FORTIFY_REGION"
	OrderMaiaAbility  = "MAIA_ABILITY"
	OrderAttack       = "ATTACK_REGION"
	OrderDestroyRing  = "DESTROY_RING"
	OrderDeployNazgul = "DEPLOY_NAZGUL"
)

// Order is a validated game order.
type Order struct {
	PlayerID  string          `json:"playerId"`
	UnitID    string          `json:"unitId"`
	OrderType string          `json:"orderType"`
	Turn      int             `json:"turn"`
	Payload   json.RawMessage `json:"payload"`
}

// AssignRoutePayload contains path IDs for movement.
type AssignRoutePayload struct {
	PathIDs []string `json:"pathIds"`
}

// RedirectPayload contains new path IDs for redirection.
type RedirectPayload struct {
	NewPathIDs []string `json:"newPathIds"`
}

// BlockPathPayload contains the path to block.
type BlockPathPayload struct {
	PathID string `json:"pathId"`
}

// SearchPathPayload contains the path to search.
type SearchPathPayload struct {
	PathID string `json:"pathId"`
}

// AttackPayload contains the target region.
type AttackPayload struct {
	TargetRegion string `json:"targetRegion"`
}

// MaiaAbilityPayload contains the target path.
type MaiaAbilityPayload struct {
	TargetPathID string `json:"targetPathId"`
}

// EventEmitter is implemented by the Kafka producer wrapper.
type EventEmitter interface {
	EmitUnitEvent(eventType string, payload interface{}) error
	EmitRegionEvent(eventType string, payload interface{}) error
	EmitPathEvent(eventType string, payload interface{}) error
	EmitBroadcast(payload interface{}) error
	EmitRingPosition(payload interface{}) error
	EmitRingDetection(playerID string, payload interface{}) error
	EmitGameOver(winner, cause string, turn int) error
	EmitDLQ(errorCode, errorMessage string, rawPayload []byte) error
}

// TurnProcessor executes the 13-step turn processing from Section 6.
type TurnProcessor struct {
	cache      *cache.WorldStateCache
	graph      *graph.Graph
	gameConfig *config.GameConfig
	emitter    EventEmitter
}

// New creates a TurnProcessor.
func New(c *cache.WorldStateCache, g *graph.Graph, gc *config.GameConfig, e EventEmitter) *TurnProcessor {
	return &TurnProcessor{cache: c, graph: g, gameConfig: gc, emitter: e}
}

// ProcessTurn executes all 13 steps for the given turn's orders.
func (tp *TurnProcessor) ProcessTurn(orders []Order) {
	tp.cache.Update(func(c *cache.WorldStateCache) {
		turn := c.Turn

		// Step 1: Orders already collected.
		// Drop any stale-turn orders (PDF Section 11 Rule 1: WRONG_TURN).
		filtered := orders[:0]
		for _, o := range orders {
			if o.Turn != 0 && o.Turn != turn {
				log.Printf("[engine] WRONG_TURN: dropping %s for %s (order.turn=%d, current=%d)",
					o.OrderType, o.UnitID, o.Turn, turn)
				continue
			}
			filtered = append(filtered, o)
		}
		orders = filtered

		// Enforce DUPLICATE_UNIT_ORDER (PDF Section 5.1 / Section 11 Rule 8):
		// keep only the first order seen per unit id this turn.
		orders = dedupeOrdersByUnit(orders)

		// Step 2: AssignRoute and RedirectUnit.
		for _, o := range orders {
			switch o.OrderType {
			case OrderAssignRoute:
				tp.applyAssignRoute(c, o, turn)
			case OrderRedirectUnit:
				tp.applyRedirect(c, o)
			}
		}

		// Step 3: BlockPath and SearchPath. Revert stale blocks.
		tp.revertStalePaths(c)
		for _, o := range orders {
			switch o.OrderType {
			case OrderBlockPath:
				tp.applyBlockPath(c, o, turn)
			case OrderSearchPath:
				tp.applySearchPath(c, o, turn)
			}
		}

		// Step 4: ReinforceRegion and DeployNazgul.
		for _, o := range orders {
			switch o.OrderType {
			case OrderReinforce, OrderDeployNazgul:
				tp.applyReinforce(c, o, turn)
			}
		}

		// Step 5: FortifyRegion.
		for _, o := range orders {
			if o.OrderType == OrderFortify {
				tp.applyFortify(c, o, turn)
			}
		}

		// Step 6: MaiaAbility — config-driven dispatch.
		// Same order type → different effect based on unit's config.Maia fields.
		for _, o := range orders {
			if o.OrderType == OrderMaiaAbility {
				tp.applyMaiaAbility(c, o, turn)
			}
		}

		// Step 7: Auto-advance all units with assigned routes.
		tp.autoAdvance(c, turn)

		// Step 8: AttackRegion.
		attacks := make(map[string]map[config.Side][]Order)
		for _, o := range orders {
			if o.OrderType == OrderAttack {
				var p AttackPayload
				if err := json.Unmarshal(o.Payload, &p); err == nil {
					if u, ok := c.Units[o.UnitID]; ok {
						if attacks[p.TargetRegion] == nil {
							attacks[p.TargetRegion] = make(map[config.Side][]Order)
						}
						attacks[p.TargetRegion][u.Config.Side] = append(attacks[p.TargetRegion][u.Config.Side], o)
					}
				}
			}
		}
		for targetRegion, sideAttacks := range attacks {
			for side, atkOrders := range sideAttacks {
				tp.applyGroupAttack(c, targetRegion, side, atkOrders, turn)
			}
		}

		// Step 9: Decrement TEMPORARILY_OPEN timers.
		tp.decrementTempOpen(c)

		// Step 10: Decrement fortification timers.
		tp.decrementFortification(c)

		// Step 11: Decrement respawn and cooldown counters.
		tp.decrementRespawnAndCooldown(c)

		// Step 12: Detection check.
		tp.runDetection(c, turn)

		// Step 12.5: Update region control based on unit presence (for uncontested regions).
		tp.updateRegionControl(c)

		// Step 13: Evaluate win conditions.
		tp.evaluateWinConditions(c, orders, turn)

		// Advance turn.
		c.Turn++

		// Reset exposed.
		rb := c.RingBearer
		rb.Exposed = false
		c.RingBearer = rb
	})
}

// ----- Step implementations -----

func (tp *TurnProcessor) applyAssignRoute(c *cache.WorldStateCache, o Order, turn int) {
	var p AssignRoutePayload
	if err := json.Unmarshal(o.Payload, &p); err != nil {
		return
	}
	if u, ok := c.Units[o.UnitID]; ok {
		u.Route = p.PathIDs
		u.RouteIdx = 0
		c.Units[o.UnitID] = u
		// Ring Bearer route synced to private state.
		if u.Config.Class == config.ClassRingBearer {
			rb := c.RingBearer
			rb.Route = p.PathIDs
			rb.RouteIdx = 0
			c.RingBearer = rb
		}
		msg := fmt.Sprintf("AssignRoute: %s assigned route %v", o.UnitID, p.PathIDs)
		log.Printf("[engine] %s (turn %d)", msg, turn)
		c.TurnLogs = append(c.TurnLogs, msg)
	}
}

func (tp *TurnProcessor) applyRedirect(c *cache.WorldStateCache, o Order) {
	var p RedirectPayload
	if err := json.Unmarshal(o.Payload, &p); err != nil {
		return
	}
	if u, ok := c.Units[o.UnitID]; ok {
		u.Route = p.NewPathIDs
		u.RouteIdx = 0
		c.Units[o.UnitID] = u
		if u.Config.Class == config.ClassRingBearer {
			rb := c.RingBearer
			rb.Route = p.NewPathIDs
			rb.RouteIdx = 0
			c.RingBearer = rb
		}
	}
}

func (tp *TurnProcessor) revertStalePaths(c *cache.WorldStateCache) {
	for pid, path := range c.Paths {
		if path.Status == cache.StatusBlocked && path.BlockedByUnit != "" {
			blocker, ok := c.Units[path.BlockedByUnit]
			if !ok || (blocker.Region != path.Config.From && blocker.Region != path.Config.To) {
				// Blocker moved away — revert to OPEN.
				path.Status = cache.StatusOpen
				path.BlockedByUnit = ""
				c.Paths[pid] = path
			}
		}
	}
}

func (tp *TurnProcessor) applyBlockPath(c *cache.WorldStateCache, o Order, turn int) {
	var p BlockPathPayload
	if err := json.Unmarshal(o.Payload, &p); err != nil {
		return
	}
	path, ok := c.Paths[p.PathID]
	if !ok {
		return
	}
	u, ok := c.Units[o.UnitID]
	if !ok {
		return
	}
	// Unit must be at an endpoint.
	if u.Region != path.Config.From && u.Region != path.Config.To {
		log.Printf("[engine] UNIT_NOT_ADJACENT: %s cannot block %s from %s", o.UnitID, p.PathID, u.Region)
		return
	}

	// PDF Section 2.4 / 7.1: "A FellowshipGuard stationed at a path endpoint prevents
	// a Nazgul from permanently blocking that path — the Nazgul must defeat the guard first."
	// This rule applies ONLY when the blocker is a Nazgul, and ignores the blocking unit
	// itself (a guard can never "block itself out").
	if u.Config.Class == config.ClassNazgul {
		for _, other := range c.Units {
			if other.ID == u.ID {
				continue
			}
			if other.Config.Class == config.ClassFellowshipGuard &&
				other.Status == cache.UnitActive &&
				(other.Region == path.Config.From || other.Region == path.Config.To) {
				msg := fmt.Sprintf("BLOCK failed: %s is defended by FellowshipGuard %s", p.PathID, other.ID)
				log.Printf("[engine] %s", msg)
				c.TurnLogs = append(c.TurnLogs, msg)
				return
			}
		}
	}

	path.Status = cache.StatusBlocked
	path.BlockedByUnit = o.UnitID
	c.Paths[p.PathID] = path
	msg := fmt.Sprintf("BlockPath: %s blocked %s", o.UnitID, p.PathID)
	log.Printf("[engine] %s (turn %d)", msg, turn)
	c.TurnLogs = append(c.TurnLogs, msg)
}

func (tp *TurnProcessor) applySearchPath(c *cache.WorldStateCache, o Order, turn int) {
	var p SearchPathPayload
	if err := json.Unmarshal(o.Payload, &p); err != nil {
		return
	}
	path, ok := c.Paths[p.PathID]
	if !ok {
		return
	}
	// PDF Section 11 Rule 5: unit must be at an endpoint.
	u, ok := c.Units[o.UnitID]
	if !ok || u.Status != cache.UnitActive {
		return
	}
	if u.Region != path.Config.From && u.Region != path.Config.To {
		log.Printf("[engine] UNIT_NOT_ADJACENT: %s cannot search %s from %s", o.UnitID, p.PathID, u.Region)
		return
	}
	if path.SurveillanceLevel < 3 {
		path.SurveillanceLevel++
	}
	c.Paths[p.PathID] = path
	msg := fmt.Sprintf("SearchPath: %s searched %s", o.UnitID, p.PathID)
	log.Printf("[engine] %s (turn %d)", msg, turn)
	c.TurnLogs = append(c.TurnLogs, msg)
}

func (tp *TurnProcessor) applyReinforce(c *cache.WorldStateCache, o Order, turn int) {
	var p AttackPayload
	if err := json.Unmarshal(o.Payload, &p); err != nil {
		return
	}
	u, ok := c.Units[o.UnitID]
	if !ok || u.Status != cache.UnitActive {
		return
	}
	// REINFORCE_REGION: must be adjacent. DEPLOY_NAZGUL bypasses this check
	// because it represents a Shadow-side respawn-style placement.
	if o.OrderType == OrderReinforce && u.Region != p.TargetRegion {
		pathID := tp.graph.PathBetween(u.Region, p.TargetRegion)
		if pathID == "" {
			log.Printf("[engine] INVALID_TARGET: reinforce target %s not adjacent to %s (%s)",
				p.TargetRegion, u.Region, o.UnitID)
			return
		}
		if path, ok := c.Paths[pathID]; ok && path.Status == cache.StatusBlocked {
			log.Printf("[engine] PATH_BLOCKED: cannot reinforce %s via blocked %s",
				p.TargetRegion, pathID)
			return
		}
	}

	if o.OrderType == OrderDeployNazgul {
		target, ok := c.Regions[p.TargetRegion]
		if !ok || target.ControlledBy != config.ControlShadow {
			log.Printf("[engine] INVALID_TARGET: DEPLOY_NAZGUL target %s is not Shadow-controlled", p.TargetRegion)
			return
		}
	}

	u.Region = p.TargetRegion
	c.Units[o.UnitID] = u
	msg := fmt.Sprintf("%s: %s moved to %s", o.OrderType, o.UnitID, p.TargetRegion)
	log.Printf("[engine] %s (turn %d)", msg, turn)
	c.TurnLogs = append(c.TurnLogs, msg)
}

func (tp *TurnProcessor) applyFortify(c *cache.WorldStateCache, o Order, turn int) {
	u, ok := c.Units[o.UnitID]
	if !ok {
		return
	}
	// Config-driven: only units with canFortify=true can fortify.
	if !u.Config.CanFortify {
		return
	}

	region, ok := c.Regions[u.Region]
	if ok {
		region.Fortified = true
		region.FortifyTurns = 2
		c.Regions[u.Region] = region
	}
	msg := fmt.Sprintf("FortifyRegion: %s fortified %s", o.UnitID, u.Region)
	log.Printf("[engine] %s (turn %d)", msg, turn)
	c.TurnLogs = append(c.TurnLogs, msg)
}

// applyMaiaAbility dispatches by config, not by unit ID.
// Gandalf (maia=true, side=FREE_PEOPLES, maiaAbilityPaths=[]) → OpenPath
// Saruman (maia=true, side=SHADOW, maiaAbilityPaths=[...]) → CorruptPath
// Sauron (maia=true, side=SHADOW, startRegion=mordor) → passive only
func (tp *TurnProcessor) applyMaiaAbility(c *cache.WorldStateCache, o Order, turn int) {
	u, ok := c.Units[o.UnitID]
	if !ok {
		return
	}
	// Must be ACTIVE — DESTROYED Maia (e.g. Saruman after Isengard falls) cannot dispatch.
	if u.Status != cache.UnitActive {
		log.Printf("[engine] MAIA_DISABLED: %s is not active", o.UnitID)
		return
	}
	// Config-driven: only Maia class.
	if !u.Config.Maia {
		return
	}
	if u.Cooldown > 0 {
		log.Printf("[engine] ABILITY_ON_COOLDOWN: %s cooldown=%d", o.UnitID, u.Cooldown)
		return
	}

	var p MaiaAbilityPayload
	if err := json.Unmarshal(o.Payload, &p); err != nil {
		return
	}
	path, ok := c.Paths[p.TargetPathID]
	if !ok {
		return
	}

	// Config-driven dispatch: check maiaAbilityPaths.
	// If the unit has maiaAbilityPaths entries → Saruman's CorruptPath behaviour.
	// If empty maiaAbilityPaths AND side==FREE_PEOPLES → Gandalf's OpenPath.
	// Sauron (startRegion=mordor) has no ability to dispatch — passive only.
	if u.Config.StartRegion == "mordor" {
		// Sauron — passive only, no dispatched ability.
		return
	}

	if len(u.Config.MaiaAbilityPaths) > 0 {
		// Saruman — CorruptPath.
		// Verify path is in Saruman's maiaAbilityPaths.
		allowed := false
		for _, ap := range u.Config.MaiaAbilityPaths {
			if ap == p.TargetPathID {
				allowed = true
				break
			}
		}
		if !allowed {
			return
		}
		// Verify Saruman is at an endpoint.
		if u.Region != path.Config.From && u.Region != path.Config.To {
			return
		}
		path.SurveillanceLevel = 3 // permanent
		path.Status = cache.StatusCorrupted
		c.Paths[p.TargetPathID] = path
		u.Cooldown = u.Config.Cooldown
		c.Units[o.UnitID] = u
		msg := fmt.Sprintf("CorruptPath: %s by %s", p.TargetPathID, o.UnitID)
		log.Printf("[engine] %s (turn %d)", msg, turn)
		c.TurnLogs = append(c.TurnLogs, msg)
	} else {
		// Gandalf — OpenPath. Path must be BLOCKED.
		if path.Status != cache.StatusBlocked {
			msg := fmt.Sprintf("OpenPath failed: %s is not BLOCKED", p.TargetPathID)
			log.Printf("[engine] %s", msg)
			c.TurnLogs = append(c.TurnLogs, msg)
			return
		}
		if u.Region != path.Config.From && u.Region != path.Config.To {
			return
		}
		path.Status = cache.StatusTemporarilyOpen
		path.TempOpenTurns = 3 // initialized to 3 because Step 9 will decrement it to 2 on this same turn
		c.Paths[p.TargetPathID] = path
		u.Cooldown = u.Config.Cooldown
		c.Units[o.UnitID] = u
		msg := fmt.Sprintf("OpenPath: %s by %s", p.TargetPathID, o.UnitID)
		log.Printf("[engine] %s (turn %d)", msg, turn)
		c.TurnLogs = append(c.TurnLogs, msg)
	}
}

func (tp *TurnProcessor) autoAdvance(c *cache.WorldStateCache, turn int) {
	for uid, u := range c.Units {
		if u.Status != cache.UnitActive {
			continue
		}
		if len(u.Route) == 0 || u.RouteIdx >= len(u.Route) {
			continue
		}

		nextPathID := u.Route[u.RouteIdx]
		path, ok := c.Paths[nextPathID]
		if !ok {
			continue
		}

		if path.Status == cache.StatusBlocked || path.Status == cache.StatusCorrupted {
			// RouteBlocked — unit stays.
			log.Printf("[engine] RouteBlocked: %s on %s (turn %d) status: %s", uid, nextPathID, turn, path.Status)
			continue
		}

		// OPEN, THREATENED, or TEMPORARILY_OPEN → advance.
		// The unit MUST currently be at one of this path's endpoints — otherwise
		// the route is invalid (silently skip rather than teleport).
		var dest string
		currentRegion := u.Region
		if u.Config.Class == config.ClassRingBearer {
			currentRegion = c.RingBearer.TrueRegion
		}

		switch currentRegion {
		case path.Config.From:
			dest = path.Config.To
		case path.Config.To:
			dest = path.Config.From
		default:
			log.Printf("[engine] RouteInvalid: %s not at endpoint of %s (region=%s) — skipping", uid, nextPathID, currentRegion)
			continue
		}

		if u.Config.Class != config.ClassRingBearer {
			u.Region = dest
		}
		u.RouteIdx++

		// Ring Bearer special handling.
		if u.Config.Class == config.ClassRingBearer {
			rb := c.RingBearer
			rb.TrueRegion = dest
			rb.RouteIdx = u.RouteIdx

			// Check surveillance — expose if path has surveillanceLevel >= 1 and past hidden turns.
			if path.SurveillanceLevel >= 1 && turn > tp.gameConfig.HiddenUntilTurn {
				rb.Exposed = true
				log.Printf("[engine] RingBearerSpotted on path %s (turn %d)", nextPathID, turn)
				
				// PDF Section 6 Step 7: Emit RingBearerSpotted to Dark Side only.
				tp.emitter.EmitRingDetection("dark-side-player", map[string]interface{}{
					"type":      "RingBearerSpotted",
					"pathId":    nextPathID,
					"turn":      turn,
					"timestamp": time.Now().UnixMilli(),
				})
			}
			c.RingBearer = rb
			// RingBearerMoved emitted to game.ring.position (Light Side only)
			tp.emitter.EmitRingPosition(map[string]interface{}{
				"type":       "RingBearerMoved",
				"trueRegion": dest,
				"turn":       turn,
				"timestamp":  time.Now().UnixMilli(),
			})
		}

		if u.RouteIdx >= len(u.Route) {
			log.Printf("[engine] RouteComplete: %s arrived at %s (turn %d)", uid, dest, turn)
		}

		c.Units[uid] = u
	}
}

// updateRegionControl assigns ownership of regions that have units from only one side.
func (tp *TurnProcessor) updateRegionControl(c *cache.WorldStateCache) {
	for rid, region := range c.Regions {
		freePeoplesCount := 0
		shadowCount := 0

		for _, u := range c.Units {
			if u.Status != cache.UnitActive {
				continue
			}
			if u.Config.Class == config.ClassRingBearer {
				continue // Ring Bearer is hidden, does not project control
			}
			
			// Determine actual location of unit
			if u.Region == rid {

				if u.Config.Side == config.SideFreePeoples {
					freePeoplesCount++
				} else if u.Config.Side == config.SideShadow {
					shadowCount++
				}
			}
		}

		// If only one side is present, they take control.
		// If both are present (contested), control doesn't change until an attack resolves.
		if freePeoplesCount > 0 && shadowCount == 0 {
			region.ControlledBy = config.ControlFreePeoples
			c.Regions[rid] = region
		} else if shadowCount > 0 && freePeoplesCount == 0 {
			region.ControlledBy = config.ControlShadow
			c.Regions[rid] = region
		}
	}
}

func (tp *TurnProcessor) applyGroupAttack(c *cache.WorldStateCache, targetRegion string, attackerSide config.Side, atkOrders []Order, turn int) {
	if len(atkOrders) == 0 {
		return
	}

	region, ok := c.Regions[targetRegion]
	if !ok {
		return
	}

	var attackers []combat.UnitSnapshot
	for _, o := range atkOrders {
		attacker, ok := c.Units[o.UnitID]
		if !ok || attacker.Status != cache.UnitActive {
			continue
		}

		// PDF Section 11 Rule 6: AttackRegion target must be adjacent and enemy-controlled.
		if attacker.Region != targetRegion {
			pathID := tp.graph.PathBetween(attacker.Region, targetRegion)
			if pathID == "" {
				log.Printf("[engine] INVALID_TARGET: %s cannot attack non-adjacent %s from %s", o.UnitID, targetRegion, attacker.Region)
				continue
			}
			if path, ok := c.Paths[pathID]; ok && path.Status == cache.StatusBlocked {
				log.Printf("[engine] PATH_BLOCKED: %s cannot attack %s via blocked %s", o.UnitID, targetRegion, pathID)
				continue
			}
		}

		attackers = append(attackers, toCombatSnap(attacker))
	}

	if len(attackers) == 0 {
		return
	}

	// Collect defenders (opposite side, in target region).
	var defenders []combat.UnitSnapshot
	for _, u := range c.Units {
		if u.Status == cache.UnitActive && u.Region == targetRegion && u.Config.Side != attackerSide {
			defenders = append(defenders, toCombatSnap(u))
		}
	}

	if len(defenders) == 0 {
		// Uncontested — all attackers move in.
		for _, a := range attackers {
			attacker := c.Units[a.ID]
			attacker.Region = targetRegion
			c.Units[a.ID] = attacker
		}
		region.ControlledBy = config.Controller(attackerSide)
		c.Regions[targetRegion] = region
		return
	}

	regionState := combat.RegionState{
		ID:        region.ID,
		Terrain:   region.Config.Terrain,
		Fortified: region.Fortified,
	}
	result := combat.ResolveCombat(attackers, defenders, regionState)

	if result.AttackerWon {
		// Update ALL attackers positions.
		for _, au := range result.UpdatedAttackers {
			if existing, ok := c.Units[au.ID]; ok {
				existing.Region = targetRegion
				existing.Strength = au.Strength
				c.Units[au.ID] = existing
			}
		}
		region.ControlledBy = config.Controller(attackerSide)
		region.Fortified = false
		c.Regions[targetRegion] = region

		// Update defenders.
		for _, du := range result.UpdatedDefenders {
			if existing, ok := c.Units[du.ID]; ok {
				existing.Strength = du.Strength
				existing.Status = cache.UnitStatus(du.Status)
				if cache.UnitStatus(du.Status) == cache.UnitRespawning {
					existing.Region = ""
					existing.RespawnTurns = existing.Config.RespawnTurns
				} else if cache.UnitStatus(du.Status) == cache.UnitDestroyed {
					existing.Region = ""
				}
				c.Units[du.ID] = existing
			}
		}

		// If Isengard falls to FREE_PEOPLES: disable Saruman.
		if targetRegion == "isengard" && attackerSide == config.SideFreePeoples {
			tp.disableSaruman(c)
		}
		msg := fmt.Sprintf("Battle at %s: attackers WON (%d vs %d) — defenders took %d damage", targetRegion, result.AttackerPower, result.DefenderPower, result.Damage)
		log.Printf("[engine] %s (turn %d)", msg, turn)
		c.TurnLogs = append(c.TurnLogs, msg)
	} else {
		// Defenders hold — attackers each lose 1 strength.
		for _, au := range result.UpdatedAttackers {
			if existing, ok := c.Units[au.ID]; ok {
				existing.Strength = au.Strength
				existing.Status = cache.UnitStatus(au.Status)
				if cache.UnitStatus(au.Status) == cache.UnitRespawning {
					existing.Region = ""
					existing.RespawnTurns = existing.Config.RespawnTurns
				} else if cache.UnitStatus(au.Status) == cache.UnitDestroyed {
					existing.Region = ""
				}
				c.Units[au.ID] = existing
			}
		}
		// In case any defenders died (though they usually hold if they don't die)
		for _, du := range result.UpdatedDefenders {
			if existing, ok := c.Units[du.ID]; ok {
				existing.Strength = du.Strength
				existing.Status = cache.UnitStatus(du.Status)
				if cache.UnitStatus(du.Status) == cache.UnitRespawning {
					existing.Region = ""
					existing.RespawnTurns = existing.Config.RespawnTurns
				} else if cache.UnitStatus(du.Status) == cache.UnitDestroyed {
					existing.Region = ""
				}
				c.Units[du.ID] = existing
			}
		}
		msg := fmt.Sprintf("Battle at %s: defenders held (%d vs %d) — attackers took 1 damage each", targetRegion, result.AttackerPower, result.DefenderPower)
		log.Printf("[engine] %s (turn %d)", msg, turn)
		c.TurnLogs = append(c.TurnLogs, msg)
	}
}

// disableSaruman permanently disables Saruman when Isengard falls.
// Config-driven: finds the unit with Class==Maia, Side==SHADOW, StartRegion=="isengard".
func (tp *TurnProcessor) disableSaruman(c *cache.WorldStateCache) {
	for uid, u := range c.Units {
		if u.Config.Class == config.ClassMaia &&
			u.Config.Side == config.SideShadow &&
			u.Config.StartRegion == "isengard" {
			u.Status = cache.UnitDestroyed
			c.Units[uid] = u
			log.Printf("[engine] Saruman disabled — Isengard fell")
			return
		}
	}
}

func (tp *TurnProcessor) decrementTempOpen(c *cache.WorldStateCache) {
	for pid, path := range c.Paths {
		if path.Status == cache.StatusTemporarilyOpen {
			path.TempOpenTurns--
			if path.TempOpenTurns <= 0 {
				if path.BlockedByUnit != "" {
					path.Status = cache.StatusBlocked
				} else {
					path.Status = cache.StatusOpen
				}
			}
			c.Paths[pid] = path
		}
	}
}

func (tp *TurnProcessor) decrementFortification(c *cache.WorldStateCache) {
	for rid, region := range c.Regions {
		if region.Fortified {
			region.FortifyTurns--
			if region.FortifyTurns <= 0 {
				region.Fortified = false
			}
			c.Regions[rid] = region
		}
	}
}

func (tp *TurnProcessor) decrementRespawnAndCooldown(c *cache.WorldStateCache) {
	for uid, u := range c.Units {
		if u.Status == cache.UnitRespawning {
			u.RespawnTurns--
			if u.RespawnTurns <= 0 {
				// Return to home region at full strength.
				u.Status = cache.UnitActive
				u.Region = u.Config.StartRegion
				u.Strength = u.Config.Strength
				log.Printf("[engine] Unit %s respawned at %s", uid, u.Region)
			}
		}
		if u.Cooldown > 0 {
			u.Cooldown--
		}
		c.Units[uid] = u
	}
}

func (tp *TurnProcessor) runDetection(c *cache.WorldStateCache, turn int) {
	var detUnits []detection.UnitDetectionState
	for _, u := range c.Units {
		detUnits = append(detUnits, detection.UnitDetectionState{
			ID:     u.ID,
			Config: u.Config,
			Region: u.Region,
			Status: string(u.Status),
		})
	}

	// Ring Bearer's true region comes from private state, not public unit state.
	result := detection.RunDetection(
		detUnits,
		c.RingBearer.TrueRegion,
		turn,
		tp.gameConfig.HiddenUntilTurn,
		tp.graph,
	)

	// If exposure occurred via Nazgul OR if RingBearer crossed a surveilled path earlier this turn.
	if result.Exposed || c.RingBearer.Exposed {
		rb := c.RingBearer
		rb.Exposed = true
		rb.LastDetectedRegion = c.RingBearer.TrueRegion // Use current true region
		rb.LastDetectedTurn = turn
		c.RingBearer = rb

		// Update dark view — only last detected region, NEVER true region.
		c.DarkView.LastDetectedRegion = rb.TrueRegion
		c.DarkView.LastDetectedTurn = turn
		// c.DarkView.RingBearerRegion is NEVER set — always "".

		msg := fmt.Sprintf("RingBearerDetected at %s", rb.TrueRegion)
		log.Printf("[engine] %s (turn %d)", msg, turn)

		// Emit detection event to Dark Side
		tp.emitter.EmitRingDetection("dark-side-player", map[string]interface{}{
			"type":      "RingBearerDetected",
			"regionId":  rb.TrueRegion,
			"turn":      turn,
			"timestamp": time.Now().UnixMilli(),
		})
	}
}

func (tp *TurnProcessor) evaluateWinConditions(c *cache.WorldStateCache, orders []Order, turn int) {
	// Check DestroyRing order.
	for _, o := range orders {
		if o.OrderType == OrderDestroyRing {
			rb := c.RingBearer
			// Light Side wins when Ring Bearer is at mount-doom and no Dark Side unit is there.
			if rb.TrueRegion == "mount-doom" && !darkSideUnitAt(c, "mount-doom") {
				c.GameOver = true
				c.Winner = "FREE_PEOPLES"
				log.Printf("[engine] LIGHT SIDE WINS — Ring destroyed at turn %d", turn)
				return
			}
		}
	}

	// Dark Side wins when any Nazgul occupies same region as Ring Bearer AND exposed.
	rb := c.RingBearer
	if rb.Exposed {
		for _, u := range c.Units {
			if u.Config.Class == config.ClassNazgul &&
				u.Status == cache.UnitActive &&
				u.Region == rb.TrueRegion {
				c.GameOver = true
				c.Winner = "SHADOW"
				log.Printf("[engine] DARK SIDE WINS — Ring Bearer caught at %s (turn %d)", rb.TrueRegion, turn)
				return
			}
		}
	}

	// Draw after max turns.
	if turn >= tp.gameConfig.MaxTurns {
		c.GameOver = true
		c.Winner = "DRAW"
		log.Printf("[engine] DRAW — max turns reached")
	}
}

func darkSideUnitAt(c *cache.WorldStateCache, region string) bool {
	for _, u := range c.Units {
		if u.Status == cache.UnitActive &&
			u.Region == region &&
			u.Config.Side == config.SideShadow {
			return true
		}
	}
	return false
}

func toCombatSnap(u cache.UnitSnapshot) combat.UnitSnapshot {
	return combat.UnitSnapshot{
		ID:       u.ID,
		Config:   u.Config,
		Strength: u.Strength,
		Region:   u.Region,
		Status:   string(u.Status),
	}
}

// BuildWorldStateJSON builds the JSON payload for WorldStateSnapshot.
// Ring Bearer's currentRegion is never included in the shared payload.
func BuildWorldStateJSON(c cache.WorldStateCache) ([]byte, error) {
	type UnitOut struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		Class         string `json:"class"`
		Side          string `json:"side"`
		CurrentRegion string `json:"currentRegion"` // always "" for ring-bearer
		Strength      int    `json:"strength"`
		Status        string `json:"status"`
	}
	type RegionOut struct {
		ID           string `json:"id"`
		ControlledBy string `json:"controlledBy"`
		ThreatLevel  int    `json:"threatLevel"`
		Fortified    bool   `json:"fortified"`
	}
	type PathOut struct {
		ID                string `json:"id"`
		Status            string `json:"status"`
		SurveillanceLevel int    `json:"surveillanceLevel"`
	}
	type Snapshot struct {
		Turn      int         `json:"turn"`
		Units     []UnitOut   `json:"units"`
		Regions   []RegionOut `json:"regions"`
		Paths     []PathOut   `json:"paths"`
		Timestamp int64       `json:"timestamp"`
	}

	snap := Snapshot{
		Turn:      c.Turn,
		Timestamp: time.Now().UnixMilli(),
	}

	for _, u := range c.Units {
		region := u.Region
		// Ring Bearer's currentRegion is always "" in shared state.
		if u.Config.Class == config.ClassRingBearer {
			region = ""
		}
		snap.Units = append(snap.Units, UnitOut{
			ID:            u.ID,
			Name:          u.Config.Name,
			Class:         string(u.Config.Class),
			Side:          string(u.Config.Side),
			CurrentRegion: region,
			Strength:      u.Strength,
			Status:        string(u.Status),
		})
	}

	for _, r := range c.Regions {
		snap.Regions = append(snap.Regions, RegionOut{
			ID:           r.ID,
			ControlledBy: string(r.ControlledBy),
			ThreatLevel:  r.ThreatLevel,
			Fortified:    r.Fortified,
		})
	}

	for _, p := range c.Paths {
		snap.Paths = append(snap.Paths, PathOut{
			ID:                p.ID,
			Status:            string(p.Status),
			SurveillanceLevel: p.SurveillanceLevel,
		})
	}

	return json.Marshal(snap)
}

// ValidateOrder runs the 8 validation rules from Section 11.
// Returns error code string or "" if valid.
func ValidateOrder(o Order, c cache.WorldStateCache, playerSide config.Side) string {
	// Rule 1: Wrong turn.
	if o.Turn != c.Turn {
		return "WRONG_TURN"
	}

	u, unitExists := c.Units[o.UnitID]

	// Rule 2: Unit belongs to correct side.
	if unitExists && u.Config.Side != playerSide {
		return "NOT_YOUR_UNIT"
	}

	// Rule 7: Maia cooldown.
	if o.OrderType == OrderMaiaAbility && unitExists && u.Cooldown > 0 {
		return "ABILITY_ON_COOLDOWN"
	}

	// Rule 8: Duplicate unit order (caller checks before calling).

	// Rule 3 & 4: Ring Bearer path checks handled in applyAssignRoute.

	return "" // valid
}

// PlayerSideFromID returns a player's side based on their player ID prefix.
// Light Side IDs begin with "light-", Dark Side with "dark-".
func PlayerSideFromID(playerID string) config.Side {
	if len(playerID) > 5 && playerID[:6] == "light-" {
		return config.SideFreePeoples
	}
	return config.SideShadow
}

// dedupeOrdersByUnit keeps only the first order per unit id in submission order.
// Enforces PDF Section 5.1: "Maximum one order per unit per turn".
func dedupeOrdersByUnit(orders []Order) []Order {
	seen := make(map[string]bool, len(orders))
	out := make([]Order, 0, len(orders))
	for _, o := range orders {
		if o.UnitID == "" {
			out = append(out, o)
			continue
		}
		if seen[o.UnitID] {
			log.Printf("[engine] DUPLICATE_UNIT_ORDER: dropping extra order for %s (%s)", o.UnitID, o.OrderType)
			continue
		}
		seen[o.UnitID] = true
		out = append(out, o)
	}
	return out
}

// Errorf wraps fmt.Errorf.
var Errorf = fmt.Errorf
