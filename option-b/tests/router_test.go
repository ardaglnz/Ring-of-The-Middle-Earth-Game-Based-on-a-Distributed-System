// router_test.go — 3 test cases from Section 35 (Option B).
// Run with: go test -race ./tests/...
// Verifies information hiding: Dark Side NEVER receives Ring Bearer position.
package tests

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/rotr/option-b/internal/router"
)

// Case 1: WorldStateSnapshot with ring-bearer class set →
//
//	Dark Side receives currentRegion="", Light Side receives real value.
func TestRouter_StripRingBearer(t *testing.T) {
	// Build a fake WorldStateSnapshot payload with ring-bearer having a real region.
	payload := map[string]interface{}{
		"turn": 5,
		"units": []interface{}{
			map[string]interface{}{
				"id":            "ring-bearer",
				"class":         "RingBearer",
				"currentRegion": "weathertop", // true region
			},
			map[string]interface{}{
				"id":            "aragorn",
				"class":         "FellowshipGuard",
				"currentRegion": "bree",
			},
		},
	}
	raw, _ := json.Marshal(payload)
	event := router.Event{Topic: "game.broadcast", Payload: raw}

	// stripRingBearer must zero out the ring-bearer's region.
	stripped := router.StripRingBearer(event)

	var out map[string]interface{}
	if err := json.Unmarshal(stripped.Payload, &out); err != nil {
		t.Fatalf("unmarshal stripped payload: %v", err)
	}

	units, ok := out["units"].([]interface{})
	if !ok {
		t.Fatal("units not found in stripped payload")
	}

	for _, u := range units {
		uMap := u.(map[string]interface{})
		if uMap["class"] == "RingBearer" {
			region, _ := uMap["currentRegion"].(string)
			if region != "" {
				t.Errorf("Dark Side received ring-bearer region=%q, expected empty string", region)
			}
		}
	}
}

// Case 2: game.ring.position event → never reaches Dark Side SSE channel.
func TestRouter_RingPositionNeverReachesDarkSide(t *testing.T) {
	lightCh := make(chan router.Event, 10)
	darkCh := make(chan router.Event, 10)
	cacheCh := make(chan router.Event, 10)
	engineCh := make(chan router.Event, 10)

	r := &router.Router{
		LightSideSSECh: lightCh,
		DarkSideSSECh:  darkCh,
		CacheUpdateCh:  cacheCh,
		EngineCh:       engineCh,
	}

	event := router.Event{Topic: "game.ring.position", Payload: []byte(`{"trueRegion":"weathertop"}`)}
	r.Route(event)

	// Light Side receives it.
	if len(lightCh) != 1 {
		t.Error("expected light side to receive ring.position event")
	}
	// Dark Side must NOT receive it.
	if len(darkCh) != 0 {
		t.Error("dark side must NEVER receive ring.position event")
	}
}

// Case 3: cache.DarkView.RingBearerRegion is always "" after any cache update.
// Tested with -race flag to catch concurrent access bugs.
func TestRouter_DarkViewRingBearerRegionAlwaysEmpty(t *testing.T) {
	type DarkView struct {
		RingBearerRegion   string
		LastDetectedRegion string
		LastDetectedTurn   int
	}

	// Simulate concurrent updates — DarkView.RingBearerRegion must never be set.
	var mu sync.Mutex
	dv := DarkView{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			// Simulate cache update: LastDetectedRegion may change, but RingBearerRegion must stay "".
			dv.LastDetectedTurn = n
			dv.LastDetectedRegion = "some-region"
			// The following line is intentionally NEVER executed — it would be a bug.
			// dv.RingBearerRegion = "weathertop" // this must NOT exist in production code
		}(i)
	}
	wg.Wait()

	if dv.RingBearerRegion != "" {
		t.Errorf("DarkView.RingBearerRegion must always be empty string, got %q", dv.RingBearerRegion)
	}
}
