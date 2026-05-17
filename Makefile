# Ring of the Middle Earth — Makefile
# Usage:
#   make up    — start the entire system (requires Docker)
#   make down  — stop and remove containers
#   make test  — run all unit tests (no Docker required)
#   make logs  — tail all container logs
#   make demo3 — Demo Scenario 3: stop go-2, observe rebalance, restart

.PHONY: up down test logs demo3 topics health

# Start all services.
up:
	docker compose up --build -d
	@echo "System starting. UI available at http://localhost"
	@echo "Go instances: http://localhost:8080 / :8082 / :8083"
	@echo "Schema Registry: http://localhost:8081"

# Stop all services.
down:
	docker compose down -v

# Run all unit tests without Docker or Kafka.
test:
	cd option-b && go test ./tests/... -v -race
	@echo "All tests passed."

# Tail logs.
logs:
	docker compose logs -f

# Describe all Kafka topics (verify K1 criterion).
topics:
	docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe

# Health check all Go instances.
health:
	@echo "=== go-1 ===" && curl -s http://localhost:8080/health
	@echo "=== go-2 ===" && curl -s http://localhost:8082/health
	@echo "=== go-3 ===" && curl -s http://localhost:8083/health

# Demo Scenario 3: fault tolerance.
# Stop go-2, observe Kafka consumer group rebalance, restart go-2.
demo3:
	@echo "--- Stopping go-2 ---"
	docker stop go-2
	@echo "--- Observe consumer group rebalance (30s) ---"
	sleep 5
	docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --describe --group game-engine-group
	@echo "--- Restarting go-2 ---"
	docker start go-2
	sleep 5
	docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --describe --group game-engine-group
	@echo "--- go-2 rejoined ---"

# Verify GameOver appears exactly once (K6 criterion).
check-gameover:
	docker exec kafka-1 kafka-console-consumer \
		--bootstrap-server kafka-1:29092 \
		--topic game.broadcast \
		--from-beginning \
		--max-messages 100 \
		| grep -c GameOver
