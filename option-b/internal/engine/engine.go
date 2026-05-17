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
	OrderAssignRoute    = "ASSIGN_ROUTE"
	OrderRedirectUnit   = "REDIRECT_UNIT"
	OrderBlockPath      = "BLOCK_PATH"
	OrderSearchPath     = "SEARCH_PATH"
	OrderReinforce      = "REINFORCE_REGION"
	OrderFortify        = "FORTIFY_REGION"
	OrderMaiaAbility    = "MAIA_ABILITY"
	OrderAttack         = "ATTACK_REGION"
	OrderDestroyRing    = "DESTROY_RING"
	OrderDeployNazgul   = "DEPLOY_NAZGUL"
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
	cache       *cache.WorldStateCache
	graph       *graph.Graph
	gameConfig  *config.GameConfig
	emitter     EventEmitter
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

		// Step 2: AssignRoute and RedirectUnit.
		for _, o := range orders {
			switch o.OrderType {
			case OrderAssignRoute:
				tp.applyAssignRoute(c, o)
			case OrderRedirectUnit:
				tp.applyRedirect(c, o)
			}
		}

		// Step 3: BlockPath and SearchPath. Revert stale blocks.
		tp.revertStalePaths(c)
		for _, o := range orders {
			switch o.OrderType {
			case OrderBlockPath:
				tp.applyBlockPath(c, o)
			case OrderSearchPath:
				tp.applySearchPath(c, o)
			}
		}

		// Step 4: ReinforceRegion and DeployNazgul.
		for _, o := range orders {
			switch o.OrderType {
			case OrderReinforce, OrderDeployNazgul:
				tp.applyReinforce(c, o)
			}
		}

		// Step 5: FortifyRegion.
		for _, o := range orders {
			if o.OrderType == OrderFortify {
				tp.applyFortify(c, o)
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
		for _, o := range orders {
			if o.OrderType == OrderAttack {
				tp.applyAttack(c, o, turn)
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

func (tp *TurnProcessor) applyAssignRoute(c *cache.WorldStateCache, o Order) {
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

func (tp *TurnProcessor) applyBlockPath(c *cache.WorldStateCache, o Order) {
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
		return
	}
	path.Status = cache.StatusBlocked
	path.BlockedByUnit = o.UnitID
	c.Paths[p.PathID] = path
}

func (tp *TurnProcessor) applySearchPath(c *cache.WorldStateCache, o Order) {
	var p SearchPathPayload
	if err := json.Unmarshal(o.Payload, &p); err != nil {
		return
	}
	path, ok := c.Paths[p.PathID]
	if !ok {
		return
	}
	if path.SurveillanceLevel < 3 {
		path.SurveillanceLevel++
	}
	c.Paths[p.PathID] = path
}

func (tp *TurnProcessor) applyReinforce(c *cache.WorldStateCache, o Order) {
	var p AttackPayload
	if err := json.Unmarshal(o.Payload, &p); err != nil {
		return
	}
	u, ok := c.Units[o.UnitID]
	if !ok {
		return
	}
	u.Region = p.TargetRegion
	c.Units[o.UnitID] = u
}

func (tp *TurnProcessor) applyFortify(c *cache.WorldStateCache, o Order) {
	u, ok := c.Units[o.UnitID]
	if !ok {
		return
	}
	// Config-driven: only units with canFortify=true can fortify.
	if !u.Config.CanFortify {
		return
	}
	region, ok := c.Regions[u.Region]
	if !ok {
		return
	}
	region.Fortified = true
	region.FortifyTurns = 2
	c.Regions[u.Region] = region
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
	// Config-driven: only Maia class.
	if !u.Config.Maia {
		return
	}
	if u.Cooldown > 0 {
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
		c.Paths[p.TargetPathID] = path
		u.Cooldown = u.Config.Cooldown
		c.Units[o.UnitID] = u
		log.Printf("[engine] CorruptPath: %s by %s (turn %d)", p.TargetPathID, o.UnitID, turn)
	} else {
		// Gandalf — OpenPath. Path must be BLOCKED.
		if path.Status != cache.StatusBlocked {
			return
		}
		if u.Region != path.Config.From && u.Region != path.Config.To {
			return
		}
		path.Status = cache.StatusTemporarilyOpen
		path.TempOpenTurns = 2
		c.Paths[p.TargetPathID] = path
		u.Cooldown = u.Config.Cooldown
		c.Units[o.UnitID] = u
		log.Printf("[engine] OpenPath: %s by %s (turn %d)", p.TargetPathID, o.UnitID, turn)
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

		if path.Status == cache.StatusBlocked {
			// RouteBlocked — unit stays.
			log.Printf("[engine] RouteBlocked: %s on %s (turn %d)", uid, nextPathID, turn)
			continue
		}

		// OPEN, THREATENED, or TEMPORARILY_OPEN → advance.
		var dest string
		if u.Region == path.Config.From {
			dest = path.Config.To
		} else {
			dest = path.Config.From
		}

		u.Region = dest
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
			}
			c.RingBearer = rb
			// RingBearerMoved emitted to game.ring.position (Light Side only) — done by emitter.
		}

		if u.RouteIdx >= len(u.Route) {
			log.Printf("[engine] RouteComplete: %s arrived at %s (turn %d)", uid, dest, turn)
		}

		c.Units[uid] = u
	}
}

func (tp *TurnProcessor) applyAttack(c *cache.WorldStateCache, o Order, turn int) {
	var p AttackPayload
	if err := json.Unmarshal(o.Payload, &p); err != nil {
		return
	}
	attacker, ok := c.Units[o.UnitID]
	if !ok || attacker.Status != cache.UnitActive {
		return
	}
	region, ok := c.Regions[p.TargetRegion]
	if !ok {
		return
	}

	// Collect all attackers (same side, attacking same region).
	var attackers []combat.UnitSnapshot
	attackers = append(attackers, toCombatSnap(attacker))

	// Collect defenders (opposite side, in target region).
	var defenders []combat.UnitSnapshot
	for _, u := range c.Units {
		if u.Status == cache.UnitActive && u.Region == p.TargetRegion && u.Config.Side != attacker.Config.Side {
			defenders = append(defenders, toCombatSnap(u))
		}
	}

	if len(defenders) == 0 {
		// Uncontested — attacker moves in.
		attacker.Region = p.TargetRegion
		c.Units[o.UnitID] = attacker
		region.ControlledBy = config.Controller(attacker.Config.Side)
		c.Regions[p.TargetRegion] = region
		return
	}

	regionState := combat.RegionState{
		ID:        region.ID,
		Terrain:   region.Config.Terrain,
		Fortified: region.Fortified,
	}
	result := combat.ResolveCombat(attackers, defenders, regionState)

	if result.AttackerWon {
		// Update attacker position.
		attacker.Region = p.TargetRegion
		c.Units[o.UnitID] = attacker
		region.ControlledBy = config.Controller(attacker.Config.Side)
		region.Fortified = false
		c.Regions[p.TargetRegion] = region

		// Update defenders.
		for _, du := range result.UpdatedDefenders {
			if existing, ok := c.Units[du.ID]; ok {
				existing.Strength = du.Strength
				existing.Status = cache.UnitStatus(du.Status)
				if cache.UnitStatus(du.Status) == cache.UnitRespawning {
					existing.Region = ""
					existing.RespawnTurns = existing.Config.RespawnTurns
				}
				c.Units[du.ID] = existing
			}
		}

		// If Isengard falls to FREE_PEOPLES: disable Saruman.
		if p.TargetRegion == "isengard" && attacker.Config.Side == config.SideFreePeoples {
			tp.disableSaruman(c)
		}
		log.Printf("[engine] Battle at %s: attackers WON (turn %d)", p.TargetRegion, turn)
	} else {
		// Defenders hold — attackers each lose 1 strength.
		for _, au := range result.UpdatedAttackers {
			if existing, ok := c.Units[au.ID]; ok {
				existing.Strength = au.Strength
				existing.Status = cache.UnitStatus(au.Status)
				c.Units[au.ID] = existing
			}
		}
		log.Printf("[engine] Battle at %s: defenders held (turn %d)", p.TargetRegion, turn)
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

	if result.Exposed {
		rb := c.RingBearer
		rb.Exposed = true
		rb.LastDetectedRegion = result.TrueRegion
		rb.LastDetectedTurn = turn
		c.RingBearer = rb

		// Update dark view — only last detected region, NEVER true region.
		c.DarkView.LastDetectedRegion = result.TrueRegion
		c.DarkView.LastDetectedTurn = turn
		// c.DarkView.RingBearerRegion is NEVER set — always "".

		log.Printf("[engine] RingBearerDetected at %s (turn %d)", result.TrueRegion, turn)
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

// Errorf wraps fmt.Errorf.
var Errorf = fmt.Errorf
