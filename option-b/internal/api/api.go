// Package api implements the HTTP REST API and SSE server from Section 34.
// The select loop in Run() handles all 7 required cases (Section 31).
//
// FIX: pendingOrders and fastForwardVotes were instance-local, causing orders
// to be lost when nginx round-robined requests across go-1/go-2/go-3.
// Solution:
//   - /order and /game/fast-forward are pinned to go-1 via nginx (coordinator pattern).
//   - broadcastWorldState() pushes state to peer instances so all SSE clients
//     receive updates regardless of which instance they connected to.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rotr/option-b/internal/cache"
	"github.com/rotr/option-b/internal/config"
	"github.com/rotr/option-b/internal/engine"
	"github.com/rotr/option-b/internal/graph"
	"github.com/rotr/option-b/internal/pipeline"
	"github.com/rotr/option-b/internal/router"
)

// Server is the HTTP + SSE server.
type Server struct {
	cache      *cache.WorldStateCache
	graph      *graph.Graph
	gameConfig *config.GameConfig

	// SSE channels — separate per side.
	lightSideSSECh chan router.Event
	darkSideSSECh  chan router.Event
	cacheUpdateCh  chan router.Event
	engineCh       chan router.Event

	// SSE client management.
	mu           sync.Mutex
	lightClients map[string]chan []byte // playerID → channel
	darkClients  map[string]chan []byte

	newConnectionCh   chan sseConn
	disconnectCh      chan sseConn
	analysisRequestCh chan analysisReq

	turnTicker *time.Ticker
	signalCh   chan os.Signal

	// Pending orders for the current turn.
	// These are only written on the coordinator instance (go-1).
	// nginx pins /order requests to go-1 so this slice is always consistent.
	pendingOrders []pendingOrder
	orderMu       sync.Mutex

	// Fast-forward votes. Also coordinator-only (pinned via nginx).
	fastForwardVotes map[string]int
	ffMu             sync.Mutex
	forceTurnCh      chan struct{}

	// Peer instance addresses for cross-instance broadcast.
	// Populated from PEER_ADDRS env var (comma-separated) or defaults.
	peerAddrs []string

	// httpClient reused for peer notifications.
	httpClient *http.Client
}

type pendingOrder struct {
	OrderType    string          `json:"orderType"`
	PlayerID     string          `json:"playerId"`
	UnitID       string          `json:"unitId"`
	Turn         int             `json:"turn"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	PathIds      []string        `json:"pathIds,omitempty"`
	NewPathIds   []string        `json:"newPathIds,omitempty"`
	PathId       string          `json:"pathId,omitempty"`
	TargetRegion string          `json:"targetRegion,omitempty"`
	TargetPathId string          `json:"targetPathId,omitempty"`
}

type sseConn struct {
	playerID string
	side     config.Side
	ch       chan []byte
}

type analysisReq struct {
	side     config.Side
	resultCh chan interface{}
}

// internalBroadcastBody is the payload sent to /internal/broadcast on peer instances.
type internalBroadcastBody struct {
	Light    []byte `json:"light"`
	Dark     []byte `json:"dark"`
	RawCache []byte `json:"rawCache"`
}

// cors wraps an http.HandlerFunc with CORS headers.
func cors(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

// New creates a Server.
// peerAddrs is a list of peer instance base URLs, e.g. ["http://go-2:8080", "http://go-3:8080"].
// Pass nil or empty slice when running as a non-coordinator or in single-instance mode.
func New(
	c *cache.WorldStateCache,
	g *graph.Graph,
	gc *config.GameConfig,
	lightSSE, darkSSE chan router.Event,
	cacheUpd, engineCh chan router.Event,
	peerAddrs []string,
) *Server {
	return &Server{
		cache:             c,
		graph:             g,
		gameConfig:        gc,
		lightSideSSECh:    lightSSE,
		darkSideSSECh:     darkSSE,
		cacheUpdateCh:     cacheUpd,
		engineCh:          engineCh,
		lightClients:      make(map[string]chan []byte),
		darkClients:       make(map[string]chan []byte),
		newConnectionCh:   make(chan sseConn, 10),
		disconnectCh:      make(chan sseConn, 10),
		analysisRequestCh: make(chan analysisReq, 10),
		signalCh:          make(chan os.Signal, 1),
		fastForwardVotes:  make(map[string]int),
		forceTurnCh:       make(chan struct{}, 1),
		peerAddrs:         peerAddrs,
		httpClient:        &http.Client{Timeout: 3 * time.Second},
	}
}

// RegisterRoutes attaches all HTTP handlers.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/game/start", cors(s.handleGameStart))
	mux.HandleFunc("/order", cors(s.handleOrder))
	mux.HandleFunc("/game/state", cors(s.handleGameState))
	mux.HandleFunc("/orders/available", cors(s.handleOrdersAvailable))
	mux.HandleFunc("/analysis/routes", cors(s.handleAnalysisRoutes))
	mux.HandleFunc("/analysis/intercept", cors(s.handleAnalysisIntercept))
	mux.HandleFunc("/game/fast-forward", cors(s.handleFastForward))
	mux.HandleFunc("/events", cors(s.handleSSE))
	mux.HandleFunc("/health", cors(s.handleHealth))

	// Internal endpoint: receives broadcast payloads pushed by the coordinator.
	// Not exposed via nginx — only reachable by peer instances on the Docker network.
	mux.HandleFunc("/internal/broadcast", s.handleInternalBroadcast)
}

// Run starts the main select loop (Section 31). All 7 cases handled.
func (s *Server) Run(ctx context.Context) {
	signal.Notify(s.signalCh, syscall.SIGINT, syscall.SIGTERM)
	s.turnTicker = time.NewTicker(time.Duration(s.gameConfig.TurnDurationSeconds) * time.Second)
	defer s.turnTicker.Stop()

	for {
		select {
		// Case 1a: Light side SSE events.
		case msg := <-s.lightSideSSECh:
			s.broadcastToClients(s.lightClients, msg.Payload)

		// Case 1b: Dark side SSE events.
		case msg := <-s.darkSideSSECh:
			s.broadcastToClients(s.darkClients, msg.Payload)

		// Case 2: New SSE connection.
		case conn := <-s.newConnectionCh:
			s.registerClient(conn)

		// Case 3: SSE disconnection.
		case conn := <-s.disconnectCh:
			s.unregisterClient(conn)

		// Case 4: Analysis request.
		case req := <-s.analysisRequestCh:
			s.handleAnalysisRequest(req)

		// Case 5: Cache update from broadcast events.
		case update := <-s.cacheUpdateCh:
			s.applyUpdate(update)

		// Case 6: Turn tick — process orders and broadcast state.
		case <-s.turnTicker.C:
			if os.Getenv("INSTANCE_ID") == "go-1" || os.Getenv("INSTANCE_ID") == "" {
				s.processTurnTick()
			}

		// Case 6.5: Fast Forward forced tick.
		case <-s.forceTurnCh:
			if os.Getenv("INSTANCE_ID") == "go-1" || os.Getenv("INSTANCE_ID") == "" {
				s.turnTicker.Reset(time.Duration(s.gameConfig.TurnDurationSeconds) * time.Second)
				s.processTurnTick()
			}

		// Case 7: OS signal — graceful shutdown.
		case sig := <-s.signalCh:
			log.Printf("[api] Received signal %v — shutting down", sig)
			return
		}
	}
}

func (s *Server) registerClient(conn sseConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conn.side == config.SideFreePeoples {
		s.lightClients[conn.playerID] = conn.ch
	} else {
		s.darkClients[conn.playerID] = conn.ch
	}
	log.Printf("[api] SSE client connected: %s (%s)", conn.playerID, conn.side)
}

func (s *Server) unregisterClient(conn sseConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conn.side == config.SideFreePeoples {
		delete(s.lightClients, conn.playerID)
	} else {
		delete(s.darkClients, conn.playerID)
	}
	close(conn.ch)
	log.Printf("[api] SSE client disconnected: %s", conn.playerID)
}

func (s *Server) broadcastToClients(clients map[string]chan []byte, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range clients {
		select {
		case ch <- payload:
		default: // drop if slow consumer
		}
	}
}

func (s *Server) applyUpdate(event router.Event) {
	log.Printf("[api] Cache update from topic %s", event.Topic)
}

// processTurnTick collects pending orders, runs the engine, and broadcasts updated state.
func (s *Server) processTurnTick() {
	s.orderMu.Lock()
	pending := s.pendingOrders
	s.pendingOrders = nil
	s.orderMu.Unlock()

	snap := s.cache.Snapshot()
	if snap.GameOver {
		return
	}

	// Convert pending orders to engine orders.
	var engineOrders []engine.Order
	for _, po := range pending {
		engineOrders = append(engineOrders, engine.Order{
			PlayerID:  po.PlayerID,
			UnitID:    po.UnitID,
			OrderType: po.OrderType,
			Turn:      po.Turn,
			Payload:   po.Payload,
		})
	}

	log.Printf("[api] Processing turn %d with %d orders", snap.Turn, len(engineOrders))

	emitter := newEmitter(s)
	tp := engine.New(s.cache, s.graph, s.gameConfig, emitter)
	tp.ProcessTurn(engineOrders)

	// Broadcast updated state to all SSE clients on this instance
	// AND push to peer instances so their clients also receive the update.
	s.broadcastWorldState(s.cache.Snapshot())
}

// broadcastWorldState sends the current world state to:
//  1. SSE clients connected to THIS instance.
//  2. Peer instances via /internal/broadcast so their clients also get the update.
func (s *Server) broadcastWorldState(snap cache.WorldStateCache) {
	lightData := s.buildStateForSide(snap, true)
	darkData := s.buildStateForSide(snap, false)

	// Deliver to clients on this instance.
	s.broadcastToClients(s.lightClients, lightData)
	s.broadcastToClients(s.darkClients, darkData)

	// Push to peer instances in the background — non-blocking.
	if len(s.peerAddrs) > 0 {
		go s.notifyPeers(lightData, darkData, snap)
	}
}

// notifyPeers pushes the current world state snapshot to all peer instances.
// Called in a goroutine so it never blocks the select loop.
func (s *Server) notifyPeers(lightData, darkData []byte, snap cache.WorldStateCache) {
	rawCache, _ := json.Marshal(snap)
	body, err := json.Marshal(internalBroadcastBody{
		Light:    lightData,
		Dark:     darkData,
		RawCache: rawCache,
	})
	if err != nil {
		log.Printf("[api] notifyPeers: marshal error: %v", err)
		return
	}

	for _, addr := range s.peerAddrs {
		url := addr + "/internal/broadcast"
		resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("[api] notifyPeers: %s unreachable: %v", addr, err)
			continue
		}
		resp.Body.Close()
		log.Printf("[api] notifyPeers: pushed state to %s", addr)
	}
}

// handleInternalBroadcast receives a world state snapshot from the coordinator
// and delivers it to locally-connected SSE clients.
// This endpoint is NOT exposed via nginx — only peer instances call it.
func (s *Server) handleInternalBroadcast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body internalBroadcastBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if len(body.RawCache) > 0 {
		var newSnap cache.WorldStateCache
		if err := json.Unmarshal(body.RawCache, &newSnap); err == nil {
			s.cache.Update(func(c *cache.WorldStateCache) {
				c.Turn = newSnap.Turn
				c.Units = newSnap.Units
				c.Regions = newSnap.Regions
				c.Paths = newSnap.Paths
				c.LightView = newSnap.LightView
				c.DarkView = newSnap.DarkView
				c.RingBearer = newSnap.RingBearer
				c.GameOver = newSnap.GameOver
				c.Winner = newSnap.Winner
			})
		}
	}

	if len(body.Light) > 0 {
		s.broadcastToClients(s.lightClients, body.Light)
	}
	if len(body.Dark) > 0 {
		s.broadcastToClients(s.darkClients, body.Dark)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) buildStateForSide(snap cache.WorldStateCache, isLight bool) []byte {
	type UnitJ struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		Class         string `json:"class"`
		Side          string `json:"side"`
		CurrentRegion string `json:"currentRegion"`
		Strength      int    `json:"strength"`
		Status        string `json:"status"`
	}
	type RegionJ struct {
		ID           string `json:"id"`
		ControlledBy string `json:"controlledBy"`
		ThreatLevel  int    `json:"threatLevel"`
		Fortified    bool   `json:"fortified"`
	}
	type PathJ struct {
		ID                string `json:"id"`
		Status            string `json:"status"`
		SurveillanceLevel int    `json:"surveillanceLevel"`
	}
	type StateJ struct {
		Turn               int       `json:"turn"`
		Units              []UnitJ   `json:"units"`
		Regions            []RegionJ `json:"regions"`
		Paths              []PathJ   `json:"paths"`
		TurnLogs           []string  `json:"turnLogs"`
		LastDetectedRegion string    `json:"lastDetectedRegion,omitempty"`
		LastDetectedTurn   int       `json:"lastDetectedTurn,omitempty"`
	}
	st := StateJ{Turn: snap.Turn, TurnLogs: snap.TurnLogs}
	
	if !isLight {
		st.LastDetectedRegion = snap.DarkView.LastDetectedRegion
		st.LastDetectedTurn = snap.DarkView.LastDetectedTurn
	}

	for id, u := range snap.Units {
		region := u.Region
		if u.Config.Class == config.ClassRingBearer {
			if isLight {
				region = snap.RingBearer.TrueRegion
			} else {
				region = "" // Dark Side NEVER receives true region.
			}
		}
		st.Units = append(st.Units, UnitJ{
			ID: id, Name: u.Config.Name, Class: string(u.Config.Class),
			Side: string(u.Config.Side), CurrentRegion: region,
			Strength: u.Strength, Status: string(u.Status),
		})
	}
	for id, r := range snap.Regions {
		st.Regions = append(st.Regions, RegionJ{
			ID: id, ControlledBy: string(r.ControlledBy),
			ThreatLevel: r.ThreatLevel, Fortified: r.Fortified,
		})
	}
	for id, p := range snap.Paths {
		st.Paths = append(st.Paths, PathJ{
			ID: id, Status: string(p.Status),
			SurveillanceLevel: p.SurveillanceLevel,
		})
	}
	b, _ := json.Marshal(st)
	return b
}

// ----- Emitter -----

type localEmitterImpl struct{ s *Server }

func newEmitter(s *Server) engine.EventEmitter {
	return &localEmitterImpl{s: s}
}

func (e *localEmitterImpl) EmitUnitEvent(t string, p interface{}) error   { return nil }
func (e *localEmitterImpl) EmitRegionEvent(t string, p interface{}) error { return nil }
func (e *localEmitterImpl) EmitPathEvent(t string, p interface{}) error   { return nil }
func (e *localEmitterImpl) EmitBroadcast(p interface{}) error             { return nil }

func (e *localEmitterImpl) EmitRingPosition(p interface{}) error {
	b, _ := json.Marshal(p)
	e.s.broadcastToClients(e.s.lightClients, b)
	// Also push to peers so the light-side client on any instance gets it.
	if len(e.s.peerAddrs) > 0 {
		go e.s.notifyPeersRingPosition(b)
	}
	return nil
}

func (e *localEmitterImpl) EmitRingDetection(id string, p interface{}) error {
	b, _ := json.Marshal(p)
	e.s.broadcastToClients(e.s.darkClients, b)
	// Also push to peers so the dark-side client on any instance gets it.
	if len(e.s.peerAddrs) > 0 {
		go e.s.notifyPeersRingDetection(b)
	}
	return nil
}

func (e *localEmitterImpl) EmitGameOver(w, c string, t int) error  { return nil }
func (e *localEmitterImpl) EmitDLQ(ec, em string, rp []byte) error { return nil }

// internalRingBody is used to push ring-specific events to peers.
type internalRingBody struct {
	Side    string `json:"side"` // "light" or "dark"
	Payload []byte `json:"payload"`
}

// notifyPeersRingPosition pushes a RingBearerMoved event to peer instances (light side only).
func (s *Server) notifyPeersRingPosition(payload []byte) {
	body, _ := json.Marshal(internalRingBody{Side: "light", Payload: payload})
	for _, addr := range s.peerAddrs {
		resp, err := s.httpClient.Post(addr+"/internal/ring", "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		resp.Body.Close()
	}
}

// notifyPeersRingDetection pushes a RingBearerDetected event to peer instances (dark side only).
func (s *Server) notifyPeersRingDetection(payload []byte) {
	body, _ := json.Marshal(internalRingBody{Side: "dark", Payload: payload})
	for _, addr := range s.peerAddrs {
		resp, err := s.httpClient.Post(addr+"/internal/ring", "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		resp.Body.Close()
	}
}

// handleInternalRing receives ring-specific events (position or detection) from the coordinator.
func (s *Server) handleInternalRing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body internalRingBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	switch body.Side {
	case "light":
		s.broadcastToClients(s.lightClients, body.Payload)
	case "dark":
		s.broadcastToClients(s.darkClients, body.Payload)
	}
	w.WriteHeader(http.StatusOK)
}

// ----- HTTP Handlers -----

func (s *Server) handleGameStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.cache.Update(func(c *cache.WorldStateCache) {
		c.Turn = 1
	})
	log.Println("[api] Game started — HVH mode, turn 1")
	s.broadcastWorldState(s.cache.Snapshot())
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started", "mode": "HVH"})
}

func (s *Server) handleFastForward(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		PlayerID string `json:"playerId"`
		Turn     int    `json:"turn"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	s.ffMu.Lock()
	defer s.ffMu.Unlock()

	// Clear stale votes if the turn has already advanced.
	if s.cache.Turn > req.Turn {
		s.fastForwardVotes = make(map[string]int)
		w.WriteHeader(http.StatusOK)
		return
	}

	s.fastForwardVotes[req.PlayerID] = req.Turn

	// Trigger fast-forward when both players have voted for this turn.
	votes := 0
	for _, t := range s.fastForwardVotes {
		if t == req.Turn {
			votes++
		}
	}

	if votes >= 2 {
		log.Printf("[api] Fast forward triggered for turn %d", req.Turn)
		s.fastForwardVotes = make(map[string]int)
		select {
		case s.forceTurnCh <- struct{}{}:
		default:
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var order pendingOrder
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Lightweight pre-validation: Section 11 rules 1, 2, 8.
	snap := s.cache.Snapshot()
	playerSide := engine.PlayerSideFromID(order.PlayerID)

	if order.Turn != snap.Turn {
		writeOrderError(w, http.StatusConflict, "WRONG_TURN",
			"submitted turn does not match current turn")
		return
	}

	if u, ok := snap.Units[order.UnitID]; ok && u.Config.Side != playerSide {
		writeOrderError(w, http.StatusForbidden, "NOT_YOUR_UNIT",
			"you can only order units on your side")
		return
	}

	s.orderMu.Lock()
	// DUPLICATE_UNIT_ORDER: reject a second order for the same unit this turn.
	for _, existing := range s.pendingOrders {
		if existing.UnitID == order.UnitID && existing.Turn == order.Turn {
			s.orderMu.Unlock()
			writeOrderError(w, http.StatusConflict, "DUPLICATE_UNIT_ORDER",
				"this unit already has an order this turn")
			return
		}
	}
	s.pendingOrders = append(s.pendingOrders, order)
	s.orderMu.Unlock()

	log.Printf("[api] Order received: %s for unit %s (turn %d)", order.OrderType, order.UnitID, order.Turn)

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

func writeOrderError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"errorCode":    code,
		"errorMessage": message,
	})
}

func (s *Server) handleGameState(w http.ResponseWriter, r *http.Request) {
	playerID := r.URL.Query().Get("playerId")
	snap := s.cache.Snapshot()

	type UnitOut struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		Class         string `json:"class"`
		Side          string `json:"side"`
		CurrentRegion string `json:"currentRegion"`
		Strength      int    `json:"strength"`
		Status        string `json:"status"`
	}
	type RegionOut struct {
		ID           string `json:"id"`
		ControlledBy string `json:"controlledBy"`
		ThreatLevel  int    `json:"threatLevel"`
		Fortified    bool   `json:"fortified"`
	}
	type PathOut struct {
		ID                string `json:"id"`
		Status            string `json:"status"`
		SurveillanceLevel int    `json:"surveillanceLevel"`
	}
	type Out struct {
		Turn    int         `json:"turn"`
		Winner  string      `json:"winner"`
		Units   []UnitOut   `json:"units"`
		Regions []RegionOut `json:"regions"`
		Paths   []PathOut   `json:"paths"`
	}

	isLightSide := len(playerID) > 5 && playerID[:6] == "light-"

	out := Out{Turn: snap.Turn, Winner: snap.Winner}

	for id, u := range snap.Units {
		region := u.Region
		if u.Config.Class == config.ClassRingBearer {
			if isLightSide {
				region = snap.RingBearer.TrueRegion
			} else {
				region = ""
			}
		}
		out.Units = append(out.Units, UnitOut{
			ID:            id,
			Name:          u.Config.Name,
			Class:         string(u.Config.Class),
			Side:          string(u.Config.Side),
			CurrentRegion: region,
			Strength:      u.Strength,
			Status:        string(u.Status),
		})
	}

	for id, reg := range snap.Regions {
		out.Regions = append(out.Regions, RegionOut{
			ID:           id,
			ControlledBy: string(reg.ControlledBy),
			ThreatLevel:  reg.ThreatLevel,
			Fortified:    reg.Fortified,
		})
	}

	for id, p := range snap.Paths {
		out.Paths = append(out.Paths, PathOut{
			ID:                id,
			Status:            string(p.Status),
			SurveillanceLevel: p.SurveillanceLevel,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (s *Server) handleOrdersAvailable(w http.ResponseWriter, r *http.Request) {
	unitID := r.URL.Query().Get("unitId")
	playerID := r.URL.Query().Get("playerId")

	snap := s.cache.Snapshot()
	u, ok := snap.Units[unitID]
	if !ok {
		http.Error(w, "unit not found", http.StatusNotFound)
		return
	}

	orders := availableOrders(u, snap, playerID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"orders": orders})
}

func (s *Server) handleAnalysisRoutes(w http.ResponseWriter, r *http.Request) {
	playerID := r.URL.Query().Get("playerId")
	if len(playerID) < 6 || playerID[:6] != "light-" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	resultCh := make(chan interface{}, 1)
	s.analysisRequestCh <- analysisReq{side: config.SideFreePeoples, resultCh: resultCh}

	select {
	case result := <-resultCh:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	case <-time.After(3 * time.Second):
		http.Error(w, "timeout", http.StatusGatewayTimeout)
	}
}

func (s *Server) handleAnalysisIntercept(w http.ResponseWriter, r *http.Request) {
	playerID := r.URL.Query().Get("playerId")
	if len(playerID) < 5 || playerID[:5] != "dark-" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	resultCh := make(chan interface{}, 1)
	s.analysisRequestCh <- analysisReq{side: config.SideShadow, resultCh: resultCh}

	select {
	case result := <-resultCh:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	case <-time.After(3 * time.Second):
		http.Error(w, "timeout", http.StatusGatewayTimeout)
	}
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	playerID := r.URL.Query().Get("playerId")
	if playerID == "" {
		http.Error(w, "playerId required", http.StatusBadRequest)
		return
	}

	var side config.Side
	if len(playerID) > 5 && playerID[:6] == "light-" {
		side = config.SideFreePeoples
	} else {
		side = config.SideShadow
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Send an initial comment to trigger onopen on the client.
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	// Send current state immediately so the client isn't blank on (re)connect.
	snap := s.cache.Snapshot()
	initialData := s.buildStateForSide(snap, side == config.SideFreePeoples)
	fmt.Fprintf(w, "data: %s\n\n", initialData)
	flusher.Flush()

	ch := make(chan []byte, 32)
	conn := sseConn{playerID: playerID, side: side, ch: ch}
	s.newConnectionCh <- conn
	defer func() { s.disconnectCh <- conn }()

	ctx := r.Context()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-ticker.C:
			// Send a keep-alive comment to prevent ngrok/nginx from dropping idle connections
			fmt.Fprintf(w, ": keep-alive\n\n")
			flusher.Flush()
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleAnalysisRequest(req analysisReq) {
	snap := s.cache.Snapshot()
	if req.side == config.SideFreePeoples {
		routes := canonicalRoutes()
		result := pipeline.RunPipeline1(context.Background(), routes, snap, s.graph)
		req.resultCh <- result
	} else {
		regions := canonicalRouteRegions()
		tasks := pipeline.BuildInterceptTasks(snap, regions, len(regions))
		result := pipeline.RunPipeline2(context.Background(), tasks, snap, s.graph)
		req.resultCh <- result
	}
}

// ----- Helpers -----

func availableOrders(u cache.UnitSnapshot, snap cache.WorldStateCache, playerID string) []string {
	var orders []string
	if u.Status != cache.UnitActive {
		return orders
	}
	orders = append(orders, "ASSIGN_ROUTE", "REDIRECT_UNIT")

	if u.Config.Maia && u.Cooldown == 0 && u.Status == cache.UnitActive && u.Config.StartRegion != "mordor" {
		orders = append(orders, "MAIA_ABILITY")
	}
	if u.Config.CanFortify {
		orders = append(orders, "FORTIFY_REGION")
	}
	orders = append(orders, "BLOCK_PATH")
	if u.Config.Side == config.SideShadow {
		orders = append(orders, "SEARCH_PATH")
	}

	if u.Config.Class == config.ClassRingBearer {
		rb := snap.RingBearer
		if rb.TrueRegion == "mount-doom" {
			orders = append(orders, "DESTROY_RING")
		} else if len(rb.Route) > 0 && rb.RouteIdx < len(rb.Route) {
			nextPath := rb.Route[rb.RouteIdx]
			if path, ok := snap.Paths[nextPath]; ok {
				if path.Config.To == "mount-doom" || path.Config.From == "mount-doom" {
					orders = append(orders, "DESTROY_RING")
				}
			}
		}
	}

	orders = append(orders, "ATTACK_REGION", "REINFORCE_REGION")
	return orders
}

func canonicalRoutes() [][]string {
	return [][]string{
		{"shire-to-bree", "bree-to-weathertop", "weathertop-to-rivendell",
			"rivendell-to-moria", "moria-to-lothlorien", "lothlorien-to-emyn-muil",
			"emyn-muil-to-ithilien", "ithilien-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
		{"shire-to-bree", "bree-to-rivendell", "rivendell-to-lothlorien",
			"lothlorien-to-emyn-muil", "emyn-muil-to-dead-marshes",
			"dead-marshes-to-ithilien", "ithilien-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
		{"shire-to-bree", "bree-to-rivendell", "rivendell-to-lothlorien",
			"lothlorien-to-emyn-muil", "emyn-muil-to-dead-marshes",
			"dead-marshes-to-mordor", "mordor-to-mount-doom"},
		{"shire-to-tharbad", "tharbad-to-fords-of-isen", "fords-of-isen-to-edoras",
			"edoras-to-minas-tirith", "minas-tirith-to-osgiliath",
			"osgiliath-to-minas-morgul", "minas-morgul-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
	}
}

func canonicalRouteRegions() []string {
	return []string{
		"bree", "weathertop", "rivendell", "moria", "lothlorien",
		"emyn-muil", "dead-marshes", "ithilien", "cirith-ungol",
		"tharbad", "fords-of-isen", "edoras", "minas-tirith",
		"osgiliath", "minas-morgul", "mordor", "mount-doom",
	}
}
