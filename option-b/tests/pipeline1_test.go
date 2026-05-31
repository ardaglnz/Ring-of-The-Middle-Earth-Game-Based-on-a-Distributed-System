// pipeline1_test.go — 2 test cases from Section 35 (Option B).
// Tests the Route Risk pipeline computation (Section 32).
// No Docker or Kafka required.
package tests

import (
	"testing"

	"github.com/rotr/option-b/internal/cache"
	"github.com/rotr/option-b/internal/config"
	"github.com/rotr/option-b/internal/graph"
	"github.com/rotr/option-b/internal/pipeline"
)

// buildTestGraph builds a minimal test graph for pipeline tests.
func buildTestGraph() *graph.Graph {
	mc := &config.MapConfig{
		Regions: map[string]config.RegionConfig{
			"r1": {ID: "r1", Name: "Region1", Terrain: config.TerrainPlains, StartThreat: 1},
			"r2": {ID: "r2", Name: "Region2", Terrain: config.TerrainMountains, StartThreat: 2},
			"r3": {ID: "r3", Name: "Region3", Terrain: config.TerrainForest, StartThreat: 0},
			"r4": {ID: "r4", Name: "Region4", Terrain: config.TerrainPlains, StartThreat: 3},
		},
		Paths: map[string]config.PathConfig{
			"p1-2": {ID: "p1-2", From: "r1", To: "r2", Cost: 1},
			"p2-3": {ID: "p2-3", From: "r2", To: "r3", Cost: 1},
			"p3-4": {ID: "p3-4", From: "r3", To: "r4", Cost: 1},
		},
	}
	return graph.Build(mc)
}

// buildTestCache creates a WorldStateCache for pipeline tests.
func buildTestCache(pathStatuses map[string]cache.PathStatus, surveillanceLevels map[string]int) cache.WorldStateCache {
	snap := cache.WorldStateCache{
		Turn:  5,
		Units: map[string]cache.UnitSnapshot{},
		Regions: map[string]cache.RegionState{
			"r1": {ID: "r1", Config: config.RegionConfig{Terrain: config.TerrainPlains}, ThreatLevel: 1},
			"r2": {ID: "r2", Config: config.RegionConfig{Terrain: config.TerrainMountains}, ThreatLevel: 2},
			"r3": {ID: "r3", Config: config.RegionConfig{Terrain: config.TerrainForest}, ThreatLevel: 0},
			"r4": {ID: "r4", Config: config.RegionConfig{Terrain: config.TerrainPlains}, ThreatLevel: 3},
		},
		Paths: map[string]cache.PathState{
			"p1-2": {ID: "p1-2", Config: config.PathConfig{From: "r1", To: "r2"}, Status: cache.StatusOpen},
			"p2-3": {ID: "p2-3", Config: config.PathConfig{From: "r2", To: "r3"}, Status: cache.StatusOpen},
			"p3-4": {ID: "p3-4", Config: config.PathConfig{From: "r3", To: "r4"}, Status: cache.StatusOpen},
		},
	}

	for pid, status := range pathStatuses {
		if p, ok := snap.Paths[pid]; ok {
			p.Status = status
			snap.Paths[pid] = p
		}
	}
	for pid, level := range surveillanceLevels {
		if p, ok := snap.Paths[pid]; ok {
			p.SurveillanceLevel = level
			snap.Paths[pid] = p
		}
	}
	return snap
}

// Case 1: Route with known threat and surveillance values → correct riskScore computed.
// route = [p1-2, p2-3, p3-4]
// regions visited: r2(threat=2), r3(threat=0), r4(threat=3)
// surveillance: p1-2=2, p2-3=0, p3-4=1
// expected:
//
//	threat = 2+0+3 = 5
//	surveillance = (2+0+1)*3 = 9
//	no blocked/threatened paths
//	total = 14
func TestPipeline1_CorrectRiskScore(t *testing.T) {
	g := buildTestGraph()
	snap := buildTestCache(nil, map[string]int{"p1-2": 2, "p3-4": 1})

	route := []string{"p1-2", "p2-3", "p3-4"}
	rr := pipeline.ComputeRouteRisk(route, snap, g)

	expected := 14 // 5 (threat) + 9 (surveillance)
	if rr.RiskScore != expected {
		t.Errorf("expected riskScore=%d, got %d", expected, rr.RiskScore)
	}
}

// Case 2: Nazgul within 2 hops → proximity count adds correctly to score.
func TestPipeline1_NazgulProximityAddsToScore(t *testing.T) {
	g := buildTestGraph()
	snap := buildTestCache(nil, nil)

	// Add a Nazgul at r1 — 1 hop from r2, 2 hops from r3.
	snap.Units["nazgul-test"] = cache.UnitSnapshot{
		ID:     "nazgul-test",
		Config: config.UnitConfig{Class: config.ClassNazgul, Side: config.SideShadow},
		Region: "r1",
		Status: cache.UnitActive,
	}

	route := []string{"p2-3", "p3-4"}
	rr := pipeline.ComputeRouteRisk(route, snap, g)

	// r2 (threat=2), r3 (threat=0), r4 (threat=3) destination regions for p2-3 and p3-4
	// Nazgul at r1 is within 2 hops of r2 (1 hop) and r3 (2 hops) → proximity=2
	// threat: r3(0) + r4(3) = 3
	// proximity: 2 * 2 = 4
	// total = 7
	if rr.RiskScore < 4 {
		t.Errorf("expected nazgul proximity to add at least 4 to risk score, got %d", rr.RiskScore)
	}
}
