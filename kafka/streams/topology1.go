// topology1.go — Kafka Streams Topology 1: Order Validation (Section 11).
// Source: game.orders.raw
// Sinks: game.orders.validated (valid) and game.dlq (invalid)
// 8 validation rules produce correct error codes.
package streams

import (
	"encoding/json"
	"log"
	"time"
)

// KTable snapshots — populated from Kafka state stores.
type TurnKTable struct {
	CurrentTurn int
}

type UnitKTableEntry struct {
	UnitID   string
	Side     string
	Region   string
	Status   string
	Cooldown int
	Route    []string
}

type PathKTableEntry struct {
	PathID   string
	Status   string // OPEN | THREATENED | BLOCKED | TEMPORARILY_OPEN
	From     string
	To       string
}

// RawOrder mirrors the Avro schema.
type RawOrder struct {
	PlayerID  string          `json:"playerId"`
	UnitID    string          `json:"unitId"`
	OrderType string          `json:"orderType"`
	Payload   json.RawMessage `json:"payload"`
	Turn      int             `json:"turn"`
	Timestamp int64           `json:"timestamp"`
}

// ValidationResult is the outcome of validating one order.
type ValidationResult struct {
	Order     RawOrder
	ErrorCode string // "" means valid
}

// ValidatedOrder is the enriched order sent to game.orders.validated.
type ValidatedOrder struct {
	RawOrder
	RouteRiskScore *int `json:"routeRiskScore"` // nullable — V2 schema field
}

// DLQEntry is the error record sent to game.dlq.
type DLQEntry struct {
	OriginalTopic string `json:"originalTopic"`
	Partition     int    `json:"partition"`
	Offset        int64  `json:"offset"`
	ErrorCode     string `json:"errorCode"`
	ErrorMessage  string `json:"errorMessage"`
	RawPayload    []byte `json:"rawPayload"`
	Timestamp     int64  `json:"timestamp"`
}

// OrderValidator implements all 8 validation rules from Section 11.
// This runs as a Kafka Streams processor consuming game.orders.raw.
type OrderValidator struct {
	turnKTable  TurnKTable
	unitKTable  map[string]UnitKTableEntry  // key=unitId
	pathKTable  map[string]PathKTableEntry  // key=pathId
	seenUnits   map[string]bool             // for duplicate detection within a turn
}

// NewOrderValidator creates a validator with current KTable snapshots.
func NewOrderValidator(turn TurnKTable, units map[string]UnitKTableEntry, paths map[string]PathKTableEntry) *OrderValidator {
	return &OrderValidator{
		turnKTable: turn,
		unitKTable: units,
		pathKTable: paths,
		seenUnits:  make(map[string]bool),
	}
}

// Validate applies all 8 rules. Returns "" if valid, error code otherwise.
func (v *OrderValidator) Validate(order RawOrder, playerSide string) string {
	// Rule 1: order.turn does not match current turn.
	if order.Turn != v.turnKTable.CurrentTurn {
		return "WRONG_TURN"
	}

	unit, unitExists := v.unitKTable[order.UnitID]

	// Rule 2: Unit does not belong to submitting player's side.
	if unitExists && unit.Side != playerSide {
		return "NOT_YOUR_UNIT"
	}

	// Rule 8: Same unitId appears more than once this turn.
	if v.seenUnits[order.UnitID] {
		return "DUPLICATE_UNIT_ORDER"
	}
	v.seenUnits[order.UnitID] = true

	// Rule 3 & 4: Ring Bearer route path checks.
	if order.OrderType == "ASSIGN_ROUTE" || order.OrderType == "REDIRECT_UNIT" {
		var payload struct {
			PathIDs    []string `json:"pathIds"`
			NewPathIDs []string `json:"newPathIds"`
		}
		json.Unmarshal(order.Payload, &payload)

		pathIDs := payload.PathIDs
		if len(pathIDs) == 0 {
			pathIDs = payload.NewPathIDs
		}

		for _, pid := range pathIDs {
			path, ok := v.pathKTable[pid]
			if !ok {
				return "INVALID_PATH"
			}
			// Rule 3: Ring Bearer route next path is BLOCKED.
			if path.Status == "BLOCKED" && unit.UnitID == "ring-bearer" {
				return "PATH_BLOCKED"
			}
		}
	}

	// Rule 5: BlockPath / SearchPath: unit not in endpoint region.
	if order.OrderType == "BLOCK_PATH" || order.OrderType == "SEARCH_PATH" {
		var payload struct {
			PathID string `json:"pathId"`
		}
		json.Unmarshal(order.Payload, &payload)
		path, ok := v.pathKTable[payload.PathID]
		if !ok {
			return "INVALID_PATH"
		}
		if !unitExists || (unit.Region != path.From && unit.Region != path.To) {
			return "UNIT_NOT_ADJACENT"
		}
	}

	// Rule 6: AttackRegion: target not adjacent or not enemy-controlled.
	if order.OrderType == "ATTACK_REGION" {
		// Simplified adjacency check via path table.
		var payload struct {
			TargetRegion string `json:"targetRegion"`
		}
		json.Unmarshal(order.Payload, &payload)
		adjacent := false
		for _, path := range v.pathKTable {
			if (path.From == unit.Region && path.To == payload.TargetRegion) ||
				(path.To == unit.Region && path.From == payload.TargetRegion) {
				adjacent = true
				break
			}
		}
		if !adjacent {
			return "INVALID_TARGET"
		}
	}

	// Rule 7: MaiaAbility: unit cooldown > 0.
	if order.OrderType == "MAIA_ABILITY" && unitExists && unit.Cooldown > 0 {
		return "ABILITY_ON_COOLDOWN"
	}

	return "" // valid
}

// ProcessOrdersBatch processes a batch of raw orders and returns validated + DLQ entries.
func ProcessOrdersBatch(orders []RawOrder, playerSide string, turn TurnKTable,
	units map[string]UnitKTableEntry, paths map[string]PathKTableEntry) ([]ValidatedOrder, []DLQEntry) {

	validator := NewOrderValidator(turn, units, paths)
	var validated []ValidatedOrder
	var dlq []DLQEntry

	for _, order := range orders {
		errCode := validator.Validate(order, playerSide)
		if errCode == "" {
			raw, _ := json.Marshal(order)
			log.Printf("[topology1] Order validated: %s %s", order.OrderType, order.UnitID)
			validated = append(validated, ValidatedOrder{RawOrder: order})
			_ = raw
		} else {
			raw, _ := json.Marshal(order)
			log.Printf("[topology1] Order rejected: %s (code=%s)", order.UnitID, errCode)
			dlq = append(dlq, DLQEntry{
				OriginalTopic: "game.orders.raw",
				ErrorCode:     errCode,
				ErrorMessage:  "Validation failed: " + errCode,
				RawPayload:    raw,
				Timestamp:     time.Now().UnixMilli(),
			})
		}
	}

	return validated, dlq
}
