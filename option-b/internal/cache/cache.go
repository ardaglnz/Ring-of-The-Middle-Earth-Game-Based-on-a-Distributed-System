// Package cache defines the WorldStateCache and all runtime game state types.
// The CacheManager goroutine owns the cache; it sends value copies to workers.
package cache

import (
	"sync"

	"github.com/rotr/option-b/internal/config"
)

// PathStatus represents the current status of a path.
type PathStatus string

const (
	StatusOpen            PathStatus = "OPEN"
	StatusThreatened      PathStatus = "THREATENED"
	StatusBlocked         PathStatus = "BLOCKED"
	StatusTemporarilyOpen PathStatus = "TEMPORARILY_OPEN"
	StatusCorrupted       PathStatus = "CORRUPTED"
)

// UnitStatus represents the lifecycle state of a unit.
type UnitStatus string

const (
	UnitActive     UnitStatus = "ACTIVE"
	UnitDestroyed  UnitStatus = "DESTROYED"
	UnitRespawning UnitStatus = "RESPAWNING"
)

// UnitSnapshot is the mutable runtime state of one unit.
type UnitSnapshot struct {
	ID           string
	Config       config.UnitConfig
	Region       string // always "" for ring-bearer in public state
	Strength     int
	Status       UnitStatus
	RespawnTurns int
	Route        []string // ordered list of path IDs
	RouteIdx     int
	Cooldown     int
}

// RegionState is the mutable runtime state of one region.
type RegionState struct {
	ID           string
	Config       config.RegionConfig
	ControlledBy config.Controller
	ThreatLevel  int
	Fortified    bool
	FortifyTurns int
	UnitsPresent []string // unit IDs
}

// PathState is the mutable runtime state of one path.
type PathState struct {
	ID                string
	Config            config.PathConfig
	Status            PathStatus
	SurveillanceLevel int
	TempOpenTurns     int
	BlockedByUnit     string // unit ID blocking the path, or ""
}

// RingBearerState is the private state of the Ring Bearer.
// trueRegion is NEVER exposed to shared topics.
type RingBearerState struct {
	TrueRegion         string
	Exposed            bool
	Route              []string
	RouteIdx           int
	LastDetectedTurn   int
	LastDetectedRegion string
}

// LightSideView contains information only the Light Side can see.
type LightSideView struct {
	RingBearerRegion string
	AssignedRoute    []string
	RouteIdx         int
}

// DarkSideView contains information the Dark Side can see.
// RingBearerRegion is ALWAYS "" — no code path ever sets this.
type DarkSideView struct {
	RingBearerRegion   string // ALWAYS ""
	LastDetectedRegion string
	LastDetectedTurn   int
}

// WorldStateCache is the authoritative in-memory game state.
// The CacheManager goroutine owns it; workers receive value copies.
type WorldStateCache struct {
	mu          sync.RWMutex
	Turn        int
	Units       map[string]UnitSnapshot
	Regions     map[string]RegionState
	Paths       map[string]PathState
	UnitConfigs map[string]config.UnitConfig // read-only after startup
	LightView   LightSideView
	DarkView    DarkSideView
	RingBearer  RingBearerState // private — never serialized to shared topics
	GameOver    bool
	Winner      string
	TurnLogs    []string
}

// NewWorldStateCache initialises the cache from config.
func NewWorldStateCache(gc *config.GameConfig, mc *config.MapConfig) *WorldStateCache {
	c := &WorldStateCache{
		Units:       make(map[string]UnitSnapshot),
		Regions:     make(map[string]RegionState),
		Paths:       make(map[string]PathState),
		UnitConfigs: gc.Units,
	}

	// Initialise units from config.
	for id, uc := range gc.Units {
		region := uc.StartRegion
		// Ring Bearer public region is always "".
		publicRegion := region
		if uc.Class == config.ClassRingBearer {
			publicRegion = ""
		}
		c.Units[id] = UnitSnapshot{
			ID:       id,
			Config:   uc,
			Region:   publicRegion,
			Strength: uc.Strength,
			Status:   UnitActive,
		}
	}

	// Initialise regions from config.
	for id, rc := range mc.Regions {
		c.Regions[id] = RegionState{
			ID:           id,
			Config:       rc,
			ControlledBy: rc.StartControl,
			ThreatLevel:  rc.StartThreat,
		}
	}

	// Initialise paths from config.
	for id, pc := range mc.Paths {
		c.Paths[id] = PathState{
			ID:     id,
			Config: pc,
			Status: StatusOpen,
		}
	}

	// Ring Bearer private state.
	for _, uc := range gc.Units {
		if uc.Class == config.ClassRingBearer {
			c.RingBearer = RingBearerState{TrueRegion: uc.StartRegion}
			break
		}
	}

	return c
}

// Snapshot returns a deep copy of the cache for safe concurrent use.
func (c *WorldStateCache) Snapshot() WorldStateCache {
	c.mu.RLock()
	defer c.mu.RUnlock()

	snap := WorldStateCache{
		Turn:       c.Turn,
		LightView:  c.LightView,
		DarkView:   c.DarkView,
		GameOver:   c.GameOver,
		Winner:     c.Winner,
		RingBearer: c.RingBearer,
		TurnLogs:   c.TurnLogs,
	}

	snap.Units = make(map[string]UnitSnapshot, len(c.Units))
	for k, v := range c.Units {
		// Deep-copy route slice so concurrent analysis workers see a stable view.
		if len(v.Route) > 0 {
			routeCopy := make([]string, len(v.Route))
			copy(routeCopy, v.Route)
			v.Route = routeCopy
		}
		snap.Units[k] = v
	}
	snap.Regions = make(map[string]RegionState, len(c.Regions))
	for k, v := range c.Regions {
		if len(v.UnitsPresent) > 0 {
			cp := make([]string, len(v.UnitsPresent))
			copy(cp, v.UnitsPresent)
			v.UnitsPresent = cp
		}
		snap.Regions[k] = v
	}
	snap.Paths = make(map[string]PathState, len(c.Paths))
	for k, v := range c.Paths {
		snap.Paths[k] = v
	}
	// Deep-copy ring bearer route too.
	if len(snap.RingBearer.Route) > 0 {
		rc := make([]string, len(snap.RingBearer.Route))
		copy(rc, snap.RingBearer.Route)
		snap.RingBearer.Route = rc
	}
	snap.UnitConfigs = c.UnitConfigs // read-only, safe to share reference
	return snap
}

// Update replaces the cache state under a write lock.
func (c *WorldStateCache) Update(fn func(*WorldStateCache)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(c)
}
