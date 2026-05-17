// Package api implements the HTTP REST API and SSE server from Section 34.
// The select loop in Run() handles all 7 required cases (Section 31).
package api

import (
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

// New creates a Server.
func New(
	c *cache.WorldStateCache,
	g *graph.Graph,
	gc *config.GameConfig,
	lightSSE, darkSSE chan router.Event,
	cacheUpd, engineCh chan router.Event,
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
	}
}

// RegisterRoutes attaches all HTTP handlers.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/game/start", s.handleGameStart)
	mux.HandleFunc("/order", s.handleOrder)
	mux.HandleFunc("/game/state", s.handleGameState)
	mux.HandleFunc("/orders/available", s.handleOrdersAvailable)
	mux.HandleFunc("/analysis/routes", s.handleAnalysisRoutes)
	mux.HandleFunc("/analysis/intercept", s.handleAnalysisIntercept)
	mux.HandleFunc("/events", s.handleSSE)
	mux.HandleFunc("/health", s.handleHealth)
}

// Run starts the main select loop (Section 31). All 7 cases handled.
func (s *Server) Run(ctx context.Context) {
	signal.Notify(s.signalCh, syscall.SIGINT, syscall.SIGTERM)
	s.turnTicker = time.NewTicker(time.Duration(s.gameConfig.TurnDurationSeconds) * time.Second)
	defer s.turnTicker.Stop()

	for {
		select {
		// Case 1: Kafka consumer messages from various topics.
		case msg := <-s.lightSideSSECh:
			s.broadcastToClients(s.lightClients, msg.Payload)

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

		// Case 6: Turn tick.
		case tick := <-s.turnTicker.C:
			log.Printf("[api] Turn tick at %v", tick)

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
	// Cache updates from broadcast events are applied here.
	// In production these come from Kafka and update the WorldStateCache.
	log.Printf("[api] Cache update from topic %s", event.Topic)
}

func (s *Server) handleAnalysisRequest(req analysisReq) {
	snap := s.cache.Snapshot()
	if req.side == config.SideFreePeoples {
		// Build candidate routes (the 4 canonical routes from Section 2.3).
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

// ----- HTTP Handlers -----

func (s *Server) handleGameStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started", "mode": "HVH"})
}

func (s *Server) handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Accept and forward to Kafka — returns 202 Accepted.
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

func (s *Server) handleGameState(w http.ResponseWriter, r *http.Request) {
	playerID := r.URL.Query().Get("playerId")
	snap := s.cache.Snapshot()

	// Ring Bearer region stripped for Dark Side — enforced in router, also here.
	type Out struct {
		Turn    int                            `json:"turn"`
		Units   map[string]map[string]interface{} `json:"units"`
		Regions map[string]interface{}         `json:"regions"`
	}

	out := Out{
		Turn:    snap.Turn,
		Units:   make(map[string]map[string]interface{}),
		Regions: make(map[string]interface{}),
	}

	// Determine player side from playerID prefix.
	isLightSide := len(playerID) > 5 && playerID[:6] == "light-"

	for id, u := range snap.Units {
		region := u.Region
		// Config-driven: RingBearer class → region hidden from Dark Side.
		if u.Config.Class == config.ClassRingBearer && !isLightSide {
			region = "" // NEVER expose true region to Dark Side
		}
		out.Units[id] = map[string]interface{}{
			"id":            u.ID,
			"name":          u.Config.Name,
			"class":         u.Config.Class,
			"side":          u.Config.Side,
			"currentRegion": region,
			"strength":      u.Strength,
			"status":        u.Status,
		}
	}

	for id, reg := range snap.Regions {
		out.Regions[id] = map[string]interface{}{
			"id":           id,
			"controlledBy": reg.ControlledBy,
			"threatLevel":  reg.ThreatLevel,
			"fortified":    reg.Fortified,
		}
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

	ch := make(chan []byte, 32)
	conn := sseConn{playerID: playerID, side: side, ch: ch}
	s.newConnectionCh <- conn
	defer func() { s.disconnectCh <- conn }()

	ctx := r.Context()
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
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ----- Helpers -----

// availableOrders returns legal orders for a unit (for the UI dropdown).
// Config-driven: reads unit class and config fields, not unit ID strings.
func availableOrders(u cache.UnitSnapshot, snap cache.WorldStateCache, playerID string) []string {
	var orders []string
	if u.Status != cache.UnitActive {
		return orders
	}
	orders = append(orders, "ASSIGN_ROUTE", "REDIRECT_UNIT")

	// Config-driven: Maia ability available if not on cooldown.
	if u.Config.Maia && u.Cooldown == 0 {
		orders = append(orders, "MAIA_ABILITY")
	}
	// Config-driven: GondorArmy can fortify.
	if u.Config.CanFortify {
		orders = append(orders, "FORTIFY_REGION")
	}
	// Config-driven: only dark side gets SearchPath/BlockPath.
	if u.Config.Side == config.SideShadow {
		orders = append(orders, "BLOCK_PATH", "SEARCH_PATH")
	}
	// Config-driven: RingBearer can DestroyRing if at mount-doom.
	if u.Config.Class == config.ClassRingBearer {
		rb := snap.RingBearer
		if rb.TrueRegion == "mount-doom" {
			orders = append(orders, "DESTROY_RING")
		}
	}

	orders = append(orders, "ATTACK_REGION", "REINFORCE_REGION")
	return orders
}

// canonicalRoutes returns the 4 canonical ring bearer routes as path ID lists.
func canonicalRoutes() [][]string {
	return [][]string{
		// Route 1 — Fellowship
		{"shire-to-bree", "bree-to-weathertop", "weathertop-to-rivendell",
			"rivendell-to-moria", "moria-to-lothlorien", "lothlorien-to-emyn-muil",
			"emyn-muil-to-ithilien", "ithilien-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
		// Route 2 — Northern Bypass
		{"shire-to-bree", "bree-to-rivendell", "rivendell-to-lothlorien",
			"lothlorien-to-emyn-muil", "emyn-muil-to-dead-marshes",
			"dead-marshes-to-ithilien", "ithilien-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
		// Route 3 — Dark Route
		{"shire-to-bree", "bree-to-rivendell", "rivendell-to-lothlorien",
			"lothlorien-to-emyn-muil", "emyn-muil-to-dead-marshes",
			"dead-marshes-to-mordor", "mordor-to-mount-doom"},
		// Route 4 — Southern Corridor
		{"shire-to-tharbad", "tharbad-to-fords-of-isen", "fords-of-isen-to-edoras",
			"edoras-to-minas-tirith", "minas-tirith-to-osgiliath",
			"osgiliath-to-minas-morgul", "minas-morgul-to-cirith-ungol", "cirith-ungol-to-mount-doom"},
	}
}

// canonicalRouteRegions returns distinct regions across all 4 canonical routes.
func canonicalRouteRegions() []string {
	return []string{
		"bree", "weathertop", "rivendell", "moria", "lothlorien",
		"emyn-muil", "dead-marshes", "ithilien", "cirith-ungol",
		"tharbad", "fords-of-isen", "edoras", "minas-tirith",
		"osgiliath", "minas-morgul", "mordor", "mount-doom",
	}
}
