// pipeline2_test.go — 2 test cases from Section 35 (Option B).
// Tests the Interception pipeline computation (Section 33).
// No Docker or Kafka required.
package tests

import (
	"testing"

	"github.com/rotr/option-b/internal/pipeline"
)

// Case 1: Positive intercept window → score > 0.
// turnsToIntercept = 2, rbTurnsToReach = 5, routeLength = 8
// interceptWindow = 5 - 2 = 3 (>= 0)
// score = 1.0 - (2/8) = 0.75
func TestPipeline2_PositiveInterceptWindow(t *testing.T) {
	g := buildTestGraph()

	task := pipeline.InterceptTask{
		NazgulID:       "witch-king",
		NazgulRegion:   "r1",
		RouteRegion:    "r3", // 2 hops from r1 via r2
		RBTurnsToReach: 5,
		RouteLength:    8,
	}

	result := pipeline.ComputeIntercept(task, g)

	if result.Score <= 0 {
		t.Errorf("expected positive intercept score for positive window, got %f", result.Score)
	}

	// score = 1.0 - (2/8) = 0.75
	expected := 1.0 - float64(2)/float64(8)
	tolerance := 0.01
	if result.Score < expected-tolerance || result.Score > expected+tolerance {
		t.Errorf("expected score≈%f, got %f", expected, result.Score)
	}
}

// Case 2: Negative intercept window → score = 0.0.
// turnsToIntercept = 5, rbTurnsToReach = 2 → interceptWindow = -3
// score = 0.0 (Nazgul can't make it in time)
func TestPipeline2_NegativeInterceptWindow(t *testing.T) {
	g := buildTestGraph()

	task := pipeline.InterceptTask{
		NazgulID:       "nazgul-2",
		NazgulRegion:   "r4", // far away from r1
		RouteRegion:    "r1",
		RBTurnsToReach: 2,    // ring bearer gets there in 2 turns
		RouteLength:    10,
	}

	result := pipeline.ComputeIntercept(task, g)

	if result.Score != 0.0 {
		t.Errorf("expected score=0.0 for negative intercept window, got %f", result.Score)
	}
}
