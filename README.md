# Ring of the Middle Earth

**Distributed Application Development — Term Project**

## Technology Choice: Option B — Go + Kafka KTables

This project implements the Ring of the Middle Earth game using **Option B**:
- **Application tier:** 3 stateless Go instances in a Kafka consumer group
- **State tier:** Kafka KTable state stores (UnitKTable, RegionKTable, PathKTable, RingBearerKTable)
- **Event backbone:** Apache Kafka 3.6+ with Confluent Schema Registry 7.x
- **UI:** Vanilla JavaScript + SSE (no React/Vue/Angular)

## Quick Start

```bash
make up     # starts everything (requires Docker Desktop)
make test   # runs unit tests without Docker
make down   # stops and removes containers
```

## Architecture

```
Browser A (Light Side)     Browser B (Dark Side)
  POST /order                 POST /order
  GET  /events (SSE)          GET  /events (SSE)
       |                           |
       +--------nginx LB-----------+
                |
    go-1 : go-2 : go-3   (stateless, 3 instances)
                |
           Kafka (3 brokers)
           Schema Registry
                |
    KTable state stores:
      UnitKTable | RegionKTable | PathKTable | RingBearerKTable
```

## Repository Structure

```
ring-of-the-middle-earth/
├── docker-compose.yml      # Kafka, Schema Registry, 3 Go instances, nginx
├── Makefile                # make up / make test / make demo3
├── nginx.conf              # Load balancer config
├── README.md               # This file
├── config/
│   ├── units.conf          # 14 units — all config-driven, zero hardcoded IDs
│   └── map.conf            # 22 regions + 37 paths
├── kafka/
│   ├── streams/            # Topology 1 (validation) + Topology 2 (enrichment)
│   └── schemas/            # All Avro schemas (.avsc)
├── option-b/
│   ├── go.mod
│   ├── Dockerfile
│   ├── main.go
│   ├── internal/
│   │   ├── config/         # Config loader
│   │   ├── graph/          # BFS + Dijkstra
│   │   ├── combat/         # Combat resolver (Section 4)
│   │   ├── detection/      # Detection formula (Section 3.6)
│   │   ├── engine/         # 13-step turn processor (Section 6)
│   │   ├── router/         # EventRouter — information hiding
│   │   ├── cache/          # WorldStateCache
│   │   ├── pipeline/       # Pipeline 1 (route risk) + Pipeline 2 (intercept)
│   │   ├── kafka/          # Producer/consumer wrappers
│   │   └── api/            # HTTP API + SSE server
│   └── tests/
│       ├── combat_test.go      # 6 cases
│       ├── router_test.go      # 3 cases (-race)
│       ├── pipeline1_test.go   # 2 cases
│       └── pipeline2_test.go   # 2 cases
└── ui/
    ├── index.html
    ├── game.js
    └── style.css
```

## Key Design Decisions

1. **Zero unit ID string literals in game logic.** All combat, detection, and Maia dispatch logic reads `config.Class`, `config.Side`, `config.Indestructible`, `config.DetectionRange` etc. — never `"witch-king"` or `"sauron"`.

2. **Single MaiaAbility order type.** Gandalf's OpenPath vs Saruman's CorruptPath is dispatched by reading `config.MaiaAbilityPaths` (empty → Gandalf, non-empty → Saruman) and `config.StartRegion` (mordor → Sauron passive only).

3. **Information hiding.** The `EventRouter` is the single enforcement point. `DarkView.RingBearerRegion` is structurally always `""`. `game.ring.position` topic is never delivered to Dark Side SSE channels.

4. **Exactly-once GameOver.** Produced using `enable.idempotence=true` on the Kafka producer.

5. **Fault tolerance.** 3 Go instances share one Kafka consumer group. If one crashes, Kafka rebalances partitions to the remaining instances. On restart, the instance replays its partitions from Kafka and rebuilds its KTable view.

## Verification Commands

```bash
# K1: Verify all 10 topics
make topics

# K6: GameOver appears exactly once
make check-gameover

# B7: Race condition check
cd option-b && go test -race ./tests/router_test.go

# Demo Scenario 3: fault tolerance
make demo3
```

## Technology Versions

| Component | Version |
|---|---|
| Kafka | 3.6+ (confluentinc/cp-kafka:7.5.0) |
| Confluent Schema Registry | 7.5.0 |
| Go | 1.22+ |
| confluent-kafka-go | 2.x |
| UI | Vanilla JS + SSE |
