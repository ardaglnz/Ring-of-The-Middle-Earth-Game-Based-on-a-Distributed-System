// Package kafka provides the Kafka producer/consumer interfaces and topic definitions.
// The concrete confluent-kafka-go implementation is in kafka_impl.go (not needed for tests).
// GameOver is produced with enable.idempotence=true for exactly-once semantics.
package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// Topics used in the game.
const (
	TopicOrdersRaw       = "game.orders.raw"
	TopicOrdersValidated = "game.orders.validated"
	TopicEventsUnit      = "game.events.unit"
	TopicEventsRegion    = "game.events.region"
	TopicEventsPath      = "game.events.path"
	TopicSession         = "game.session"
	TopicBroadcast       = "game.broadcast"
	TopicRingPosition    = "game.ring.position"
	TopicRingDetection   = "game.ring.detection"
	TopicDLQ             = "game.dlq"
)

// Message is a Kafka message to produce or that was consumed.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition int32
	Offset    int64
}

// Producer wraps Kafka producer operations.
// In production this wraps confluent-kafka-go; in tests it's replaced by a mock.
type Producer interface {
	Produce(topic string, key []byte, value []byte) error
	ProduceExactlyOnce(topic string, key []byte, value []byte) error
	Close()
}

// Consumer wraps Kafka consumer operations.
type Consumer interface {
	Subscribe(topics []string) error
	Poll(timeoutMs int) (*Message, error)
	Close()
}

// MockProducer is a test-only producer that captures messages.
type MockProducer struct {
	Messages []Message
}

func (m *MockProducer) Produce(topic string, key []byte, value []byte) error {
	m.Messages = append(m.Messages, Message{Topic: topic, Key: key, Value: value})
	return nil
}

func (m *MockProducer) ProduceExactlyOnce(topic string, key []byte, value []byte) error {
	m.Messages = append(m.Messages, Message{Topic: topic, Key: key, Value: value})
	return nil
}

func (m *MockProducer) Close() {}

// GameEventEmitter implements engine.EventEmitter using a Kafka Producer.
type GameEventEmitter struct {
	producer Producer
}

// NewGameEventEmitter creates a GameEventEmitter.
func NewGameEventEmitter(p Producer) *GameEventEmitter {
	return &GameEventEmitter{producer: p}
}

func (e *GameEventEmitter) EmitUnitEvent(eventType string, payload interface{}) error {
	return e.emit(TopicEventsUnit, eventType, payload)
}

func (e *GameEventEmitter) EmitRegionEvent(eventType string, payload interface{}) error {
	return e.emit(TopicEventsRegion, eventType, payload)
}

func (e *GameEventEmitter) EmitPathEvent(eventType string, payload interface{}) error {
	return e.emit(TopicEventsPath, eventType, payload)
}

func (e *GameEventEmitter) EmitBroadcast(payload interface{}) error {
	return e.emit(TopicBroadcast, "WorldStateSnapshot", payload)
}

func (e *GameEventEmitter) EmitRingPosition(payload interface{}) error {
	return e.emit(TopicRingPosition, "RingBearerMoved", payload)
}

func (e *GameEventEmitter) EmitRingDetection(playerID string, payload interface{}) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal ring detection: %w", err)
	}
	return e.producer.Produce(TopicRingDetection, []byte(playerID), b)
}

// EmitGameOver produces GameOver with exactly-once semantics (idempotent producer).
func (e *GameEventEmitter) EmitGameOver(winner, cause string, turn int) error {
	payload := map[string]interface{}{
		"winner":    winner,
		"cause":     cause,
		"turn":      turn,
		"timestamp": time.Now().UnixMilli(),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal GameOver: %w", err)
	}
	log.Printf("[kafka] EmitGameOver exactly-once: winner=%s turn=%d", winner, turn)
	return e.producer.ProduceExactlyOnce(TopicBroadcast, []byte("game-over"), b)
}

func (e *GameEventEmitter) EmitDLQ(errorCode, errorMessage string, rawPayload []byte) error {
	payload := map[string]interface{}{
		"errorCode":    errorCode,
		"errorMessage": errorMessage,
		"rawPayload":   rawPayload,
		"timestamp":    time.Now().UnixMilli(),
	}
	return e.emit(TopicDLQ, "DLQEntry", payload)
}

func (e *GameEventEmitter) emit(topic, eventType string, payload interface{}) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", eventType, err)
	}
	return e.producer.Produce(topic, []byte(eventType), b)
}

// TopicConfig returns the topic configuration for all 10 required topics.
func TopicConfig() []TopicSpec {
	return []TopicSpec{
		{Name: TopicOrdersRaw, Partitions: 3, Replication: 3, Cleanup: "delete", Retention: "3600000"},
		{Name: TopicOrdersValidated, Partitions: 6, Replication: 3, Cleanup: "delete", Retention: "3600000"},
		{Name: TopicEventsUnit, Partitions: 6, Replication: 3, Cleanup: "delete", Retention: "604800000"},
		{Name: TopicEventsRegion, Partitions: 6, Replication: 3, Cleanup: "delete", Retention: "604800000"},
		{Name: TopicEventsPath, Partitions: 6, Replication: 3, Cleanup: "delete", Retention: "604800000"},
		{Name: TopicSession, Partitions: 1, Replication: 3, Cleanup: "compact", Retention: ""},
		{Name: TopicBroadcast, Partitions: 1, Replication: 3, Cleanup: "delete", Retention: "3600000"},
		{Name: TopicRingPosition, Partitions: 1, Replication: 3, Cleanup: "delete", Retention: "3600000"},
		{Name: TopicRingDetection, Partitions: 2, Replication: 3, Cleanup: "delete", Retention: "3600000"},
		{Name: TopicDLQ, Partitions: 3, Replication: 3, Cleanup: "delete", Retention: "604800000"},
	}
}

// TopicSpec defines the configuration for one Kafka topic.
type TopicSpec struct {
	Name        string
	Partitions  int
	Replication int
	Cleanup     string // "delete" or "compact"
	Retention   string // ms, or "" for compact
}
