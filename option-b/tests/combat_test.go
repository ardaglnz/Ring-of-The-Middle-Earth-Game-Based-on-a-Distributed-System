// combat_test.go — 6 test cases from Section 35 (Option B).
// Run with: go test ./tests/... (no Docker or Kafka required)
package tests

import (
	"testing"

	"github.com/rotr/option-b/internal/combat"
	"github.com/rotr/option-b/internal/config"
)

// makeUnit creates a test unit snapshot with given strength, config.
func makeUnit(id string, strength int, cfg config.UnitConfig) combat.UnitSnapshot {
	cfg.Strength = strength
	return combat.UnitSnapshot{
		ID:       id,
		Config:   cfg,
		Strength: strength,
		Region:   "test-region",
		Status:   "ACTIVE",
	}
}

// Case 1: Attacker(5) vs Defender(5, PLAINS) → tie, attacker repelled.
func TestCombat_PlainsTie(t *testing.T) {
	attacker := makeUnit("a", 5, config.UnitConfig{})
	defender := makeUnit("d", 5, config.UnitConfig{})
	region := combat.RegionState{Terrain: config.TerrainPlains}

	result := combat.ResolveCombat([]combat.UnitSnapshot{attacker}, []combat.UnitSnapshot{defender}, region)

	if result.AttackerWon {
		t.Error("expected attacker to be repelled on tie")
	}
	// Attacker loses 1 strength.
	if result.UpdatedAttackers[0].Strength != 4 {
		t.Errorf("expected attacker strength=4, got %d", result.UpdatedAttackers[0].Strength)
	}
}

// Case 2: Attacker(5) vs Defender(5, FORTRESS) → defender wins (5 vs 7).
func TestCombat_FortressDefenderWins(t *testing.T) {
	attacker := makeUnit("a", 5, config.UnitConfig{})
	defender := makeUnit("d", 5, config.UnitConfig{})
	region := combat.RegionState{Terrain: config.TerrainFortress}

	result := combat.ResolveCombat([]combat.UnitSnapshot{attacker}, []combat.UnitSnapshot{defender}, region)

	if result.AttackerWon {
		t.Errorf("expected defender to win: attacker_power=%d defender_power=%d",
			result.AttackerPower, result.DefenderPower)
	}
	if result.DefenderPower != 7 {
		t.Errorf("expected defender_power=7 (5+2 fortress), got %d", result.DefenderPower)
	}
}

// Case 3: UrukHai(5, ignoresFortress) vs Defender(5, FORTRESS) → tie (5 vs 5).
func TestCombat_UrukHaiIgnoresFortress(t *testing.T) {
	urukCfg := config.UnitConfig{IgnoresFortress: true}
	attacker := makeUnit("uruk", 5, urukCfg)
	defender := makeUnit("d", 5, config.UnitConfig{})
	region := combat.RegionState{Terrain: config.TerrainFortress}

	result := combat.ResolveCombat([]combat.UnitSnapshot{attacker}, []combat.UnitSnapshot{defender}, region)

	// Terrain bonus skipped → 5 vs 5 → tie.
	if result.AttackerWon {
		t.Error("expected tie (UrukHai ignores fortress terrain but 5==5)")
	}
	if result.AttackerPower != 5 || result.DefenderPower != 5 {
		t.Errorf("expected 5 vs 5, got %d vs %d", result.AttackerPower, result.DefenderPower)
	}
}

// Case 4: UrukHai(5) vs Defender(5, FORTRESS, fortified) → defender wins (5 vs 7).
// IgnoresFortress skips terrain_bonus but NOT fortification_bonus.
func TestCombat_UrukHaiVsFortified(t *testing.T) {
	urukCfg := config.UnitConfig{IgnoresFortress: true}
	attacker := makeUnit("uruk", 5, urukCfg)
	defender := makeUnit("d", 5, config.UnitConfig{})
	region := combat.RegionState{Terrain: config.TerrainFortress, Fortified: true}

	result := combat.ResolveCombat([]combat.UnitSnapshot{attacker}, []combat.UnitSnapshot{defender}, region)

	// terrain_bonus=0 (ignored) + fortification_bonus=2 → defender_power=7.
	if result.AttackerWon {
		t.Error("expected defender to win with fortification bonus")
	}
	if result.DefenderPower != 7 {
		t.Errorf("expected defender_power=7 (5+0+2), got %d", result.DefenderPower)
	}
}

// Case 5: Aragorn(5, leader+1) + Gimli(3) attack → Gimli effective=4; 5+4=9 vs 5.
func TestCombat_LeadershipBonus(t *testing.T) {
	aragornCfg := config.UnitConfig{Leadership: true, LeadershipBonus: 1}
	aragorn := makeUnit("aragorn", 5, aragornCfg)
	gimli := makeUnit("gimli", 3, config.UnitConfig{})
	defender := makeUnit("d", 5, config.UnitConfig{})
	region := combat.RegionState{Terrain: config.TerrainPlains}

	result := combat.ResolveCombat(
		[]combat.UnitSnapshot{aragorn, gimli},
		[]combat.UnitSnapshot{defender},
		region,
	)

	if !result.AttackerWon {
		t.Error("expected attackers to win with leadership bonus")
	}
	// attacker_power = 5 + (3+1) = 9.
	if result.AttackerPower != 9 {
		t.Errorf("expected attacker_power=9, got %d", result.AttackerPower)
	}
}

// Case 6: Indestructible unit takes fatal damage → strength=1, ACTIVE.
func TestCombat_IndestructibleFloorsAtOne(t *testing.T) {
	indestructCfg := config.UnitConfig{Indestructible: true}
	attacker := makeUnit("a", 10, config.UnitConfig{})
	indestructUnit := makeUnit("ind", 1, indestructCfg)
	region := combat.RegionState{Terrain: config.TerrainPlains}

	result := combat.ResolveCombat(
		[]combat.UnitSnapshot{attacker},
		[]combat.UnitSnapshot{indestructUnit},
		region,
	)

	if !result.AttackerWon {
		t.Error("expected attacker to win vs strength-1 defender")
	}
	destroyed := result.UpdatedDefenders[0]
	if destroyed.Strength < 1 {
		t.Errorf("indestructible unit strength should be >= 1, got %d", destroyed.Strength)
	}
	if destroyed.Status == "DESTROYED" {
		t.Error("indestructible unit should never be DESTROYED")
	}
}
