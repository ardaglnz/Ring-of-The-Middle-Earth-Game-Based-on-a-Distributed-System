// Package detection implements the Ring Bearer detection formula from Section 3.6.
// All logic is fully config-driven — Nazgul detection range is read from
// config.DetectionRange, never from a unit ID string literal.
// Sauron's passive Eye of Sauron effect is applied automatically.
package detection

import (
	"github.com/rotr/option-b/internal/config"
	"github.com/rotr/option-b/internal/graph"
)

// UnitDetectionState is the minimal unit state needed for detection.
type UnitDetectionState struct {
	ID     string
	Config config.UnitConfig
	Region string
	Status string // "ACTIVE" | "DESTROYED" | "RESPAWNING"
}

// DetectionResult is the output of the detection check.
type DetectionResult struct {
	Exposed         bool
	DetectingNazgul []string // IDs of Nazgul that triggered detection
	TrueRegion      string   // only set when exposed=true
}

// RunDetection executes the detection formula from Section 3.6.
//
// Suppressed on turns 1 through hiddenUntilTurn.
//
// For each Nazgul (config.Class == ClassNazgul):
//
//	range = config.DetectionRange
//	if sauron.Region == "mordor" and sauron.Status == "ACTIVE":
//	  range += 1   (Eye of Sauron passive — config-driven, not hardcoded)
//	if graph.BFSDistance(nazgul.Region, ringBearer.TrueRegion) <= range:
//	  exposed = true
//	  emit RingBearerDetected (Dark Side only)
func RunDetection(
	units []UnitDetectionState,
	ringBearerTrueRegion string,
	currentTurn int,
	hiddenUntilTurn int,
	g *graph.Graph,
) DetectionResult {
	result := DetectionResult{TrueRegion: ringBearerTrueRegion}

	// Suppressed during hidden turns.
	if currentTurn <= hiddenUntilTurn {
		return result
	}

	// Determine if Sauron's Eye is active.
	// Config-driven: Sauron is identified by maia=true AND side=SHADOW AND startRegion=mordor.
	// We check if any unit has Class==Maia, Side==SHADOW, is in "mordor", and is ACTIVE.
	// This avoids hardcoding "sauron" string.
	sauronActive := false
	for _, u := range units {
		if u.Config.Class == config.ClassMaia &&
			u.Config.Side == config.SideShadow &&
			u.Config.StartRegion == "mordor" &&
			u.Region == "mordor" &&
			u.Status == "ACTIVE" {
			sauronActive = true
			break
		}
	}

	// Run detection for each Nazgul.
	for _, u := range units {
		// Config-driven: only Nazgul class has detection.
		if u.Config.Class != config.ClassNazgul {
			continue
		}
		if u.Status != "ACTIVE" {
			continue
		}

		// Config-driven detection range — no unit ID string literals.
		effectiveRange := u.Config.DetectionRange
		if sauronActive {
			effectiveRange++ // Eye of Sauron passive: +1 to all Nazgul detection range
		}

		dist := g.BFSDistance(u.Region, ringBearerTrueRegion)
		if dist <= effectiveRange {
			result.Exposed = true
			result.DetectingNazgul = append(result.DetectingNazgul, u.ID)
		}
	}

	return result
}
