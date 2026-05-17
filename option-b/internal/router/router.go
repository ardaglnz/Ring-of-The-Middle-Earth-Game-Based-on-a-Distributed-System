// Package router implements the EventRouter goroutine.
// This is the single enforcement point for information asymmetry (Section 1.4).
// DarkView.RingBearerRegion is ALWAYS "" — no code path ever sets it.
package router

import (
	"encoding/json"
)

// Event is a raw Kafka event to be routed to SSE channels.
type Event struct {
	Topic   string
	Payload []byte
}

// Router routes Kafka events to the correct SSE channels.
// It enforces information hiding: Dark Side never receives Ring Bearer position.
type Router struct {
	LightSideSSECh chan<- Event
	DarkSideSSECh  chan<- Event
	CacheUpdateCh  chan<- Event
	EngineCh       chan<- Event
}

// Route dispatches a Kafka event per Section 30 rules.
// This function contains the ONLY place where ring.position is handled.
func (r *Router) Route(event Event) {
	switch event.Topic {

	case "game.ring.position":
		// Light Side ONLY — never Dark Side.
		r.LightSideSSECh <- event

	case "game.ring.detection":
		// Dark Side ONLY — never Light Side.
		r.DarkSideSSECh <- event

	case "game.broadcast":
		// Both sides, but Ring Bearer position stripped for Dark Side.
		r.LightSideSSECh <- event
		r.DarkSideSSECh <- StripRingBearer(event)
		r.CacheUpdateCh <- event

	case "game.events.unit",
		"game.events.region",
		"game.events.path":
		// Both sides receive the same event.
		r.LightSideSSECh <- event
		r.DarkSideSSECh <- event
		r.CacheUpdateCh <- event

	case "game.orders.validated":
		r.EngineCh <- event
	}
}

// StripRingBearer removes the Ring Bearer's currentRegion from a WorldStateSnapshot
// before delivery to the Dark Side. This is the only enforcement point.
//
// DarkView.RingBearerRegion is ALWAYS "" — no code path in the router
// or cache ever sets it to the true region for the dark side.
func StripRingBearer(event Event) Event {
	var payload map[string]interface{}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		// If we can't parse, return a blank payload — never leak position.
		return Event{Topic: event.Topic, Payload: []byte("{}")}
	}

	// Strip ring bearer position from units array if present.
	if units, ok := payload["units"].([]interface{}); ok {
		for _, u := range units {
			if unitMap, ok := u.(map[string]interface{}); ok {
				if unitID, ok := unitMap["id"].(string); ok {
					_ = unitID // we strip by class, not by hardcoded ID
					if unitMap["class"] == "RingBearer" {
						// Zero out the position — class-driven, not ID-driven.
						unitMap["currentRegion"] = ""
					}
				}
			}
		}
	}

	// Also ensure top-level ringBearerRegion is never set.
	delete(payload, "ringBearerRegion")
	delete(payload, "trueRegion")

	stripped, _ := json.Marshal(payload)
	return Event{Topic: event.Topic, Payload: stripped}
}
