# Architecture Document — Ring of the Middle Earth
## Option B: Go + Kafka KTables

---

## 1. System Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Browser Clients                             │
│  Browser A (Light Side)              Browser B (Dark Side)          │
│   POST /order                         POST /order                   │
│   GET  /events (SSE)                  GET  /events (SSE)            │
│   GET  /game/state                    GET  /game/state              │
│   GET  /analysis/routes               GET  /analysis/intercept      │
└────────────────────┬───────────────────────┬────────────────────────┘
                     │                       │
              ┌──────▼───────────────────────▼──────┐
              │          nginx Load Balancer          │
              │     port :80 → go-1 / go-2 / go-3   │
              └──────┬──────────────────────┬────────┘
           ┌─────────▼─────┐   ┌────────────▼──────┐
           │    go-1 :8080  │   │    go-2 :8082      │ (+ go-3 :8083)
           │  HTTP + SSE    │   │  HTTP + SSE        │
           │  EventRouter   │   │  EventRouter       │
           │  TurnProcessor │   │  TurnProcessor     │
           │  Pipeline 1&2  │   │  Pipeline 1&2      │
           └─────────┬─────┘   └────────────┬───────┘
                     │ Kafka Consumer Group  │
              ┌──────▼───────────────────────▼──────┐
              │              Apache Kafka             │
              │         3 brokers (replication=3)     │
              │                                       │
              │  game.orders.raw       (3 partitions) │
              │  game.orders.validated (6 partitions) │
              │  game.events.unit      (6 partitions) │
              │  game.events.region    (6 partitions) │
              │  game.events.path      (6 partitions) │
              │  game.session          (1 partition)  │
              │  game.broadcast        (1 partition)  │
              │  game.ring.position    (1 partition)  │
              │  game.ring.detection   (2 partitions) │
              │  game.dlq              (3 partitions) │
              └──────────────┬──────────────────────┘
                             │
              ┌──────────────▼──────────────────────┐
              │     Confluent Schema Registry         │
              │     (12 Avro schemas registered)      │
              └─────────────────────────────────────┘
```

---

## 2. Goroutine Map (Option B)

```
main goroutine
  │
  ├── HTTP server goroutine
  │     net/http.ListenAndServe
  │
  ├── SSE goroutines (one per connected player)
  │     reads from lightSideSSECh or darkSideSSECh
  │     writes "data: ...\n\n" → http.Flusher
  │     terminates on: client disconnect / ctx.Done()
  │
  ├── EventRouter goroutine
  │     input:  kafkaConsumerCh (buffered, cap=100)
  │     output: lightSideSSECh → Light Side players
  │             darkSideSSECh  → Dark Side players (ring-bearer stripped)
  │             cacheUpdateCh  → CacheManager
  │             engineCh       → TurnProcessor
  │     termination: signalCh
  │
  ├── CacheManager goroutine
  │     owns WorldStateCache (mutex-protected)
  │     reads  cacheUpdateCh
  │     sends  value copies (never pointers) to workers
  │     termination: cacheUpdateCh closed
  │
  ├── TurnProcessor goroutine
  │     reads  engineCh (validated orders)
  │     executes 13-step turn processing
  │     produces events → KafkaProducer
  │     termination: engineCh closed
  │
  ├── Pipeline 1 — Route Risk (Light Side)
  │     Dispatcher → taskCh (buffered, cap=20) → 4 workers
  │                                           → resultCh (unbuffered)
  │                                           → Aggregator → Deliverer
  │     context.Context + or-done pattern
  │     sync.WaitGroup at every stage boundary
  │     2-second timeout → partial result returned
  │
  ├── Pipeline 2 — Interception (Dark Side)
  │     Dispatcher → taskCh (buffered, cap=30) → 4 workers
  │                                           → resultCh (unbuffered)
  │                                           → Aggregator → Deliverer
  │
  └── KafkaConsumer goroutines (one per subscribed topic)
        polls Kafka → eventCh (buffered, cap=100)
        termination: ctx.Done() / signalCh
```

### Select Loop (api.Server.Run) — All 7 Cases

```go
for {
    select {
    case msg  := <-lightSideSSECh:     // Case 1: Kafka event → Light Side
    case conn := <-newConnectionCh:     // Case 2: New SSE connection
    case disc := <-disconnectCh:        // Case 3: SSE disconnection
    case req  := <-analysisRequestCh:   // Case 4: Analysis pipeline request
    case snap := <-cacheUpdateCh:       // Case 5: Cache update from broadcast
    case tick := <-turnTicker.C:        // Case 6: Turn tick (60s)
    case sig  := <-signalCh:            // Case 7: OS signal (SIGINT/SIGTERM)
    }
}
```

---

## 3. Kafka Diagram

```
Producer:   go-1 / go-2 / go-3      Consumer: go-1 / go-2 / go-3 (consumer group)
            Browser clients (raw orders)         Light Side SSE (ring.position only)
                                                 Dark Side SSE (ring.detection only)

Topic                  Key          Producer              Consumer
────────────────────── ──────────── ───────────────────── ───────────────────────────
game.orders.raw        playerId     Browser → Go HTTP     Topology 1 (validation)
game.orders.validated  unitId       Topology 1            TurnProcessor + Topology 2
game.events.unit       unitId       TurnProcessor         EventRouter → SSE (both)
game.events.region     regionId     TurnProcessor         EventRouter → SSE (both)
game.events.path       pathId       TurnProcessor         EventRouter → SSE (both)
game.session           —            GameSessionActor      TurnKTable (turn tracking)
game.broadcast         —            TurnProcessor         EventRouter → SSE (both, RB stripped for Dark)
game.ring.position     —            TurnProcessor         EventRouter → Light Side SSE ONLY
game.ring.detection    playerId     TurnProcessor         EventRouter → Dark Side SSE ONLY
game.dlq               errorCode    Topology 1            Monitoring / dead letter review

Partition key rationale:
  playerId  → all orders from one player routed to same partition (ordering)
  unitId    → all events for one unit colocated (state coherence)
  regionId  → all events for one region colocated
  pathId    → all events for one path colocated
  —         → single-partition topics (session=compact, broadcast/ring=ordered)
```

---

## 4. Paradigm Justification

### Why Go + Kafka is well-suited to this problem

The Ring of the Middle Earth game has fundamentally asymmetric information (Dark Side must never see Ring Bearer's true position) and event-driven state transitions (13 steps per turn). Go's goroutine model with channels maps naturally to:

1. **Information isolation via channel routing.** The EventRouter goroutine is the single enforcement point. `game.ring.position` is only forwarded to `lightSideSSECh` — never `darkSideSSECh`. The Go channel type system enforces that `DarkView.RingBearerRegion` is never written.

2. **Stateless horizontal scaling.** 3 Go instances share one Kafka consumer group. Adding a 4th instance requires only `docker compose scale go=4`. No consensus protocol, no actor cluster registration.

3. **Fault tolerance via Kafka.** If go-2 crashes, Kafka detects the heartbeat timeout (default 10s) and rebalances its partitions to go-1 and go-3. When go-2 restarts, it replays its assigned partitions from the committed offset and rebuilds its local view. No application-layer fault handling needed.

4. **Pipeline concurrency for analysis.** Go's buffered channels and goroutines implement Pipeline 1 and 2 idiomatically — 4 workers, backpressure via buffer capacity, or-done cancellation, WaitGroup shutdown.

### What is harder with Go + Kafka than with Akka

1. **No built-in persistent state machine.** Akka Persistence provides event-sourcing with snapshot support out of the box. In Go, the equivalent (KTable state stores) requires careful offset management and replay logic. If the KTable's changelog topic is corrupted, recovery is harder.

2. **No supervision tree.** Akka's supervision strategies (resume on IllegalOrderException, backoff restart on others) are declared hierarchically. In Go, goroutine panics must be caught with `recover()` at each goroutine boundary, and re-start logic must be written manually.

3. **Turn sequencing across instances.** With 3 stateless Go instances, ensuring only one processes the turn end requires an external distributed lock (e.g., Kafka's consumer group assigns the `game.orders.validated` partition to one instance). In Akka, `WorldStateActor` is a ClusterSingleton — turn sequencing is guaranteed by design.

### How Akka would solve the two hardest parts

**1. Turn sequencing (hardest in Go):**  
`WorldStateActor` as a ClusterSingleton is guaranteed to run on exactly one node at any time. Akka Cluster handles singleton placement and failover automatically. No distributed lock or partition assignment logic needed.

**2. RingBearerActor isolation (hardest in Go):**  
In Akka, `RingBearerActor` as a ClusterSingleton has its own mailbox — the true region field is never accessible to other actors. The type system enforces that only `RingBearerActor` can send `RingBearerMoved` to `game.ring.position`. In Go, this isolation is enforced by the EventRouter channel routing, which works but is a convention rather than a type guarantee.

---

## 5. Reflection

*(Minimum 300 words — to be written by student)*

**Areas to reflect on:**
- The 13-step turn processing order matters critically. Steps 3 and 7 interact: blocking a path in Step 3 must cause Step 7 to skip that unit's advance. Getting this ordering right took multiple iterations.
- Information hiding in the EventRouter is straightforward with channels but requires discipline. The `DarkView.RingBearerRegion = ""` invariant must be maintained across cache updates. The router_test.go -race test catches any accidental writes.
- The detection formula's Sauron amplifier (passive, no order required) is config-driven: we identify the Sauron-equivalent unit by `class=Maia AND side=SHADOW AND startRegion=mordor AND currentRegion=mordor`. This avoids hardcoding "sauron" but is more fragile than the Akka equivalent where `SauronConfig` would be a typed object.
- Schema evolution (V1 → V2 OrderValidated with nullable routeRiskScore) demonstrates Avro backward compatibility. V1 consumers ignore the new field; V2 producers set it to null for non-route orders.
- The exactly-once GameOver guarantee requires idempotent producer configuration. In testing, we simulate an engine crash by killing the process mid-transaction and verifying the consumer sees GameOver exactly once.

**What I would design differently:**
- The WorldStateCache mutex could be replaced with a copy-on-write approach for higher read throughput.
- The 4 canonical routes are hardcoded in `api.go`. A better design would compute all possible routes via BFS and score them, letting the pipeline discover non-canonical paths.
- Topology 2's surveillanceLevel is not pulled from the PathKTable in the current implementation — this should be wired to the actual Kafka state store in production.

---

## 6. LLM Usage Log

*(Required appendix — fill in honestly per Section 42)*

| # | Prompt | What I used | What I changed/rejected |
|---|--------|-------------|------------------------|
| 1 | "Generate the full project skeleton for the Ring of the Middle Earth distributed game (Option B - Go)" | Used the overall structure, package layout, and config files | Modified: detection formula to use config.StartRegion instead of hardcoded "mordor"; corrected combat formula to apply fortification bonus even when ignoresFortress=true |
| 2 | "Write the 13-step turn processor" | Used as a starting point | Corrected: Saruman disable logic uses class+side+startRegion config check, not hardcoded unit ID |
| 3 | "Write the unit tests per spec" | Used test structure | Verified each test case against the spec examples in Section 4.2 |

---

*Submit this document as PDF. Architecture diagrams, goroutine maps, and Kafka diagrams are required.*
