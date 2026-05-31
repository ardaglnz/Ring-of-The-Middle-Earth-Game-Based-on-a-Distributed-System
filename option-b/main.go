package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

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

	configDir := os.Getenv("CONFIG_DIR")
	if configDir == "" {
		configDir = filepath.Join("..", "config")
	}

	gc, err := config.LoadGameConfig(filepath.Join(configDir, "units.conf"))
	if err != nil {
		log.Fatalf("[main] Failed to load units.conf: %v", err)
	}
	log.Printf("[main] Loaded %d units", len(gc.Units))

	mc, err := config.LoadMapConfig(filepath.Join(configDir, "map.conf"))
	if err != nil {
		log.Fatalf("[main] Failed to load map.conf: %v", err)
	}
	log.Printf("[main] Loaded %d regions, %d paths", len(mc.Regions), len(mc.Paths))

	g := graph.Build(mc)
	log.Println("[main] Graph built")

	worldCache := cache.NewWorldStateCache(gc, mc)

	mockProducer := &kafka.MockProducer{}
	emitter := kafka.NewGameEventEmitter(mockProducer)

	tp := engine.New(worldCache, g, gc, emitter)
	_ = tp

	lightSideSSECh := make(chan router.Event, 100)
	darkSideSSECh := make(chan router.Event, 100)
	cacheUpdateCh := make(chan router.Event, 100)
	engineCh := make(chan router.Event, 100)

	r := &router.Router{
		LightSideSSECh: lightSideSSECh,
		DarkSideSSECh:  darkSideSSECh,
		CacheUpdateCh:  cacheUpdateCh,
		EngineCh:       engineCh,
	}
	_ = r

	// PEER_ADDRS: comma-separated list of peer instance base URLs.
	// Set in docker-compose per instance:
	//   go-1: PEER_ADDRS=http://go-2:8080,http://go-3:8080
	//   go-2: PEER_ADDRS=http://go-1:8080,http://go-3:8080
	//   go-3: PEER_ADDRS=http://go-1:8080,http://go-2:8080
	var peerAddrs []string
	if raw := os.Getenv("PEER_ADDRS"); raw != "" {
		for _, addr := range strings.Split(raw, ",") {
			if addr = strings.TrimSpace(addr); addr != "" {
				peerAddrs = append(peerAddrs, addr)
			}
		}
		log.Printf("[main] Peer addresses: %v", peerAddrs)
	}

	srv := api.New(worldCache, g, gc, lightSideSSECh, darkSideSSECh, cacheUpdateCh, engineCh, peerAddrs)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	// Serve static UI files — nginx also serves these, but this fallback
	// allows direct access to go-1:8080 without nginx for local dev.
	uiDir := filepath.Join(configDir, "..", "ui")
	if _, err := os.Stat(uiDir); err == nil {
		log.Printf("[main] Serving static UI from %s", uiDir)
		fs := http.FileServer(http.Dir(uiDir))
		mux.Handle("/", fs)
	}

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

	ctx := context.Background()
	srv.Run(ctx)

	log.Println("[main] Shutdown complete")
}
