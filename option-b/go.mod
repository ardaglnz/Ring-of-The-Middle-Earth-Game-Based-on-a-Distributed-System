module github.com/rotr/option-b

go 1.22

// No external dependencies required for unit tests.
// confluent-kafka-go is injected via the Producer interface at runtime.
// Run: go test ./tests/... -race
