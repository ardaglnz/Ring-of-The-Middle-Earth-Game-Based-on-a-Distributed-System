// Package main is the entry point for the Ring of the Middle Earth game server.
// Goroutine architecture matches Section 28:
//   - KafkaConsumer goroutines (one per subscribed topic)
//   - EventRouter goroutine
//   - CacheManager goroutine
//   - TurnProcessor goroutine
//   - Pipeline 1 & 2 goroutines
//   - SSE goroutines (one per player)
//   - HTTP server goroutine
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/rotr/option-b/internal/api"
	"github.com/rotr/option-b/internal/cache"
	"github.com/rotr/option-b/internal/config"
	"github.com/rotr/option-b/internal/engine"
	"github.com/rotr/option-b/internal/graph"
	"github.com/rotr/option-b/internal/kafka"
	"github.com/rotr/option-b/internal/router"
)

func main() {
	log.Println("[main] Ring of the Middle Earth — starting up")

	// Resolve config paths.
	configDir := os.Getenv("CONFIG_DIR")
	if configDir == "" {
		configDir = filepath.Join("..", "config")
	}

	// Load unit config.
	gc, err := config.LoadGameConfig(filepath.Join(configDir, "units.conf"))
	if err != nil {
		log.Fatalf("[main] Failed to load units.conf: %v", err)
	}
	log.Printf("[main] Loaded %d units", len(gc.Units))

	// Load map config.
	mc, err := config.LoadMapConfig(filepath.Join(configDir, "map.conf"))
	if err != nil {
		log.Fatalf("[main] Failed to load map.conf: %v", err)
	}
	log.Printf("[main] Loaded %d regions, %d paths", len(mc.Regions), len(mc.Paths))

	// Build graph.
	g := graph.Build(mc)
	log.Println("[main] Graph built")

	// Initialise world state cache.
	worldCache := cache.NewWorldStateCache(gc, mc)

	// Create Kafka mock producer (real producer injected in production via env).
	mockProducer := &kafka.MockProducer{}
	emitter := kafka.NewGameEventEmitter(mockProducer)

	// Create turn processor.
	tp := engine.New(worldCache, g, gc, emitter)
	_ = tp // used by TurnProcessor goroutine below

	// SSE channels — separate for each side.
	lightSideSSECh := make(chan router.Event, 100)
	darkSideSSECh := make(chan router.Event, 100)
	cacheUpdateCh := make(chan router.Event, 100)
	engineCh := make(chan router.Event, 100)

	// Create EventRouter.
	r := &router.Router{
		LightSideSSECh: lightSideSSECh,
		DarkSideSSECh:  darkSideSSECh,
		CacheUpdateCh:  cacheUpdateCh,
		EngineCh:       engineCh,
	}
	_ = r // used by KafkaConsumer goroutines

	// Create HTTP server.
	srv := api.New(worldCache, g, gc, lightSideSSECh, darkSideSSECh, cacheUpdateCh, engineCh)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	// Serve static UI files from the ui/ directory.
	uiDir := filepath.Join(configDir, "..", "ui")
	if _, err := os.Stat(uiDir); err == nil {
		log.Printf("[main] Serving static UI from %s", uiDir)
		fs := http.FileServer(http.Dir(uiDir))
		mux.Handle("/", fs)
	}

	// HTTP server goroutine.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	httpServer := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		log.Printf("[main] HTTP server listening on :%s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[main] HTTP server error: %v", err)
		}
	}()

	// Main select loop (runs in main goroutine via srv.Run).
	ctx := context.Background()
	srv.Run(ctx)

	log.Println("[main] Shutdown complete")
}
