// Package combat implements the combat resolution formula from Section 4.
// All behaviour is config-driven — no unit ID string literals in logic.
package combat

import (
	"github.com/rotr/option-b/internal/config"
)

// UnitSnapshot is the runtime state of a unit used during combat.
type UnitSnapshot struct {
	ID       string
	Config   config.UnitConfig
	Strength int
	Region   string
	Status   string // "ACTIVE" | "DESTROYED" | "RESPAWNING"
}

// RegionState holds the current region data needed for combat.
type RegionState struct {
	ID        string
	Terrain   config.Terrain
	Fortified bool
}

// CombatResult is the output of ResolveCombat.
type CombatResult struct {
	AttackerWon    bool
	Damage         int   // inflicted on losers
	AttackerPower  int
	DefenderPower  int
	// UpdatedAttackers and UpdatedDefenders have updated Strength values.
	UpdatedAttackers []UnitSnapshot
	UpdatedDefenders []UnitSnapshot
}

// ResolveCombat executes the combat formula from Section 4.1.
//
// Key rules (all config-driven):
//   - terrain_bonus: FORTRESS=+2, MOUNTAINS=+1, others=0
//   - fortification_bonus: region.Fortified=+2 (GondorArmy canFortify)
//   - ignoresFortress: attacker.config.IgnoresFortress → terrain_bonus skipped for defender
//     (fortification_bonus still applies)
//   - leadership_bonus: co-located leader gives +leadershipBonus to all allies in same region
//   - indestructible: strength floors at 1, never DESTROYED
//
// attacker_power = sum of effective attacker strengths
// defender_power = sum of effective defender strengths + terrain_bonus + fortification_bonus
// if attacker_power > defender_power: damage = difference, region control → attacker side
// else: each attacker loses 1 strength, region unchanged
func ResolveCombat(attackers, defenders []UnitSnapshot, region RegionState) CombatResult {
	// Apply leadership bonuses within each group.
	effAttackers := applyLeadership(attackers)
	effDefenders := applyLeadership(defenders)

	attackerPower := sumStrengths(effAttackers)

	// Terrain bonus — skipped if any attacker has IgnoresFortress config.
	terrainBonus := terrainBonus(region.Terrain)
	anyIgnoresFortress := false
	for _, a := range attackers {
		if a.Config.IgnoresFortress {
			anyIgnoresFortress = true
			break
		}
	}
	if anyIgnoresFortress {
		terrainBonus = 0
	}

	fortBonus := 0
	if region.Fortified {
		fortBonus = 2
	}

	defenderPower := sumStrengths(effDefenders) + terrainBonus + fortBonus

	result := CombatResult{
		AttackerPower: attackerPower,
		DefenderPower: defenderPower,
	}

	if attackerPower > defenderPower {
		// Attacker wins.
		result.AttackerWon = true
		result.Damage = attackerPower - defenderPower
		result.UpdatedAttackers = effAttackers // unchanged
		result.UpdatedDefenders = applyDamage(effDefenders, result.Damage)
	} else {
		// Defender holds — each attacker loses 1 strength.
		result.AttackerWon = false
		result.Damage = 1
		result.UpdatedAttackers = applyDamage(effAttackers, 1)
		result.UpdatedDefenders = effDefenders
	}

	return result
}

// applyLeadership returns new snapshots with leadership bonuses applied.
// Config-driven: reads config.Leadership and config.LeadershipBonus.
func applyLeadership(units []UnitSnapshot) []UnitSnapshot {
	// Find leaders in this group.
	leaderBonus := 0
	for _, u := range units {
		if u.Config.Leadership {
			leaderBonus += u.Config.LeadershipBonus
		}
	}

	result := make([]UnitSnapshot, len(units))
	for i, u := range units {
		updated := u
		if !u.Config.Leadership {
			// Non-leaders receive the bonus.
			updated.Strength += leaderBonus
		}
		result[i] = updated
	}
	return result
}

// applyDamage distributes damage among units. Config-driven:
// - config.Indestructible: strength floors at 1
// - config.Respawns: → status RESPAWNING
// - otherwise → DESTROYED when strength <= 0
func applyDamage(units []UnitSnapshot, totalDamage int) []UnitSnapshot {
	result := make([]UnitSnapshot, len(units))
	remaining := totalDamage

	for i, u := range units {
		updated := u
		if remaining <= 0 {
			result[i] = updated
			continue
		}
		dmg := 1 // each unit takes 1 damage from the pool
		if remaining < dmg {
			dmg = remaining
		}
		remaining -= dmg

		raw := updated.Strength - dmg
		if updated.Config.Indestructible {
			// Config-driven: indestructible units floor at 1.
			if raw < 1 {
				raw = 1
			}
			updated.Strength = raw
		} else if raw <= 0 {
			updated.Strength = 0
			if updated.Config.Respawns {
				updated.Status = "RESPAWNING"
			} else {
				updated.Status = "DESTROYED"
			}
		} else {
			updated.Strength = raw
		}
		result[i] = updated
	}
	return result
}

func sumStrengths(units []UnitSnapshot) int {
	total := 0
	for _, u := range units {
		if u.Status == "ACTIVE" {
			total += u.Strength
		}
	}
	return total
}

func terrainBonus(t config.Terrain) int {
	switch t {
	case config.TerrainFortress:
		return 2
	case config.TerrainMountains:
		return 1
	default:
		return 0
	}
}
