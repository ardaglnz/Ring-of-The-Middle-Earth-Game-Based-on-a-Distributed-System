// Package config loads unit and map configuration from HOCON-like config files.
// No unit ID string literals appear in game logic — all behaviour is config-driven.
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ----- Unit Config -----

// UnitClass represents the class of a unit.
type UnitClass string

const (
	ClassRingBearer      UnitClass = "RingBearer"
	ClassFellowshipGuard UnitClass = "FellowshipGuard"
	ClassGondorArmy      UnitClass = "GondorArmy"
	ClassNazgul          UnitClass = "Nazgul"
	ClassUrukHaiLegion   UnitClass = "UrukHaiLegion"
	ClassMaia            UnitClass = "Maia"
)

// Side represents which side a unit belongs to.
type Side string

const (
	SideFreePeoples Side = "FREE_PEOPLES"
	SideShadow      Side = "SHADOW"
)

// UnitConfig is the fully config-driven unit descriptor.
// No unit ID string literals ever appear in game logic —
// all behaviour is determined by reading these fields.
type UnitConfig struct {
	ID               string
	Name             string
	Class            UnitClass
	Side             Side
	StartRegion      string
	Strength         int
	Leadership       bool
	LeadershipBonus  int
	Indestructible   bool
	DetectionRange   int
	Respawns         bool
	RespawnTurns     int
	Maia             bool
	MaiaAbilityPaths []string
	IgnoresFortress  bool
	CanFortify       bool
	Cooldown         int
}

// GameConfig holds top-level game settings.
type GameConfig struct {
	HiddenUntilTurn    int
	MaxTurns           int
	TurnDurationSeconds int
	Units              map[string]UnitConfig // key = unit ID
}

// ----- Map Config -----

// Terrain types.
type Terrain string

const (
	TerrainPlains   Terrain = "PLAINS"
	TerrainMountains Terrain = "MOUNTAINS"
	TerrainForest   Terrain = "FOREST"
	TerrainFortress Terrain = "FORTRESS"
	TerrainVolcanic Terrain = "VOLCANIC"
	TerrainSwamp    Terrain = "SWAMP"
)

// SpecialRole of a region.
type SpecialRole string

const (
	RoleNone               SpecialRole = "NONE"
	RoleRingBearerStart    SpecialRole = "RING_BEARER_START"
	RoleRingDestructionSite SpecialRole = "RING_DESTRUCTION_SITE"
	RoleShadowStronghold   SpecialRole = "SHADOW_STRONGHOLD"
)

// Controller of a region.
type Controller string

const (
	ControlFreePeoples Controller = "FREE_PEOPLES"
	ControlShadow      Controller = "SHADOW"
	ControlNeutral     Controller = "NEUTRAL"
)

// RegionConfig is the static configuration of a region.
type RegionConfig struct {
	ID           string
	Name         string
	Terrain      Terrain
	SpecialRole  SpecialRole
	StartControl Controller
	StartThreat  int
}

// PathConfig is the static configuration of a path.
type PathConfig struct {
	ID   string
	From string
	To   string
	Cost int
}

// MapConfig holds all region and path configurations.
type MapConfig struct {
	Regions map[string]RegionConfig // key = region ID
	Paths   map[string]PathConfig   // key = path ID
}

// ----- Loaders -----

// LoadGameConfig parses units.conf. Returns GameConfig with all units.
// This is a simple line-by-line parser — not a full HOCON parser.
func LoadGameConfig(path string) (*GameConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open units.conf: %w", err)
	}
	defer f.Close()

	cfg := &GameConfig{
		HiddenUntilTurn:     3,
		MaxTurns:            40,
		TurnDurationSeconds: 60,
		Units:               make(map[string]UnitConfig),
	}

	scanner := bufio.NewScanner(f)
	var inUnit bool
	var cur UnitConfig

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Top-level settings
		if strings.HasPrefix(line, "hidden-until-turn") {
			cfg.HiddenUntilTurn = parseInt(line)
			continue
		}
		if strings.HasPrefix(line, "max-turns") {
			cfg.MaxTurns = parseInt(line)
			continue
		}
		if strings.HasPrefix(line, "turn-duration-seconds") {
			cfg.TurnDurationSeconds = parseInt(line)
			continue
		}

		// Unit block start
		if strings.HasPrefix(line, "{") {
			inUnit = true
			cur = UnitConfig{}
			line = strings.TrimPrefix(line, "{")
		}

		if inUnit {
			parseUnitField(&cur, line)
			// End of unit
			if strings.Contains(line, "}") {
				if cur.ID != "" {
					cfg.Units[cur.ID] = cur
				}
				inUnit = false
				cur = UnitConfig{}
			}
		}
	}

	return cfg, scanner.Err()
}

func parseUnitField(u *UnitConfig, line string) {
	// Remove closing brace if present
	line = strings.TrimRight(line, " }")
	// Smart split: split on commas that are NOT inside quotes or brackets.
	parts := smartSplit(line)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		val = strings.Trim(val, "\"")

		switch key {
		case "id":
			u.ID = val
		case "name":
			u.Name = val
		case "class":
			u.Class = UnitClass(val)
		case "side":
			u.Side = Side(val)
		case "start":
			u.StartRegion = val
		case "strength":
			u.Strength, _ = strconv.Atoi(val)
		case "leadership":
			u.Leadership = val == "true"
		case "leadershipBonus":
			u.LeadershipBonus, _ = strconv.Atoi(val)
		case "indestructible":
			u.Indestructible = val == "true"
		case "detectionRange":
			u.DetectionRange, _ = strconv.Atoi(val)
		case "respawns":
			u.Respawns = val == "true"
		case "respawnTurns":
			u.RespawnTurns, _ = strconv.Atoi(val)
		case "maia":
			u.Maia = val == "true"
		case "maiaAbilityPaths":
			u.MaiaAbilityPaths = parsePaths(val)
		case "ignoresFortress":
			u.IgnoresFortress = val == "true"
		case "canFortify":
			u.CanFortify = val == "true"
		case "cooldown":
			u.Cooldown, _ = strconv.Atoi(val)
		}
	}
}

// smartSplit splits a string by commas, but respects quoted strings and brackets.
func smartSplit(s string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	bracketDepth := 0

	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '"':
			inQuotes = !inQuotes
			current.WriteByte(ch)
		case ch == '[' && !inQuotes:
			bracketDepth++
			current.WriteByte(ch)
		case ch == ']' && !inQuotes:
			bracketDepth--
			current.WriteByte(ch)
		case ch == ',' && !inQuotes && bracketDepth == 0:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

func parsePaths(val string) []string {
	val = strings.Trim(val, "[]")
	if val == "" {
		return []string{}
	}
	parts := strings.Split(val, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "\"")
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func parseInt(line string) int {
	parts := strings.SplitN(line, "=", 2)
	if len(parts) != 2 {
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
	return n
}

// LoadMapConfig parses map.conf. Returns MapConfig with all regions and paths.
func LoadMapConfig(path string) (*MapConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open map.conf: %w", err)
	}
	defer f.Close()

	mc := &MapConfig{
		Regions: make(map[string]RegionConfig),
		Paths:   make(map[string]PathConfig),
	}

	scanner := bufio.NewScanner(f)
	var section string // "regions" or "paths"
	var inBlock bool
	var curRegion RegionConfig
	var curPath PathConfig

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "regions") {
			section = "regions"
			continue
		}
		if strings.HasPrefix(line, "paths") {
			section = "paths"
			continue
		}
		if line == "]" {
			continue
		}

		if strings.HasPrefix(line, "{") {
			inBlock = true
			curRegion = RegionConfig{}
			curPath = PathConfig{}
			line = strings.TrimPrefix(line, "{")
		}

		if inBlock {
			// Parse fields first, then check if block ends on the same line
			if section == "regions" {
				parseRegionFields(&curRegion, line)
			} else if section == "paths" {
				parsePathFields(&curPath, line)
			}

			if strings.Contains(line, "}") {
				if section == "regions" && curRegion.ID != "" {
					mc.Regions[curRegion.ID] = curRegion
				} else if section == "paths" && curPath.ID != "" {
					mc.Paths[curPath.ID] = curPath
				}
				inBlock = false
			}
		}
	}

	return mc, scanner.Err()
}

func parseRegionFields(r *RegionConfig, line string) {
	parts := strings.Split(line, ",")
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(strings.Trim(kv[1], "\", "))
		switch key {
		case "id":
			r.ID = val
		case "name":
			r.Name = val
		case "terrain":
			r.Terrain = Terrain(val)
		case "specialRole":
			r.SpecialRole = SpecialRole(val)
		case "startControl":
			r.StartControl = Controller(val)
		case "startThreat":
			r.StartThreat, _ = strconv.Atoi(val)
		}
	}
}

func parsePathFields(p *PathConfig, line string) {
	parts := strings.Split(line, ",")
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(strings.Trim(kv[1], "\", "))
		switch key {
		case "id":
			p.ID = val
		case "from":
			p.From = val
		case "to":
			p.To = val
		case "cost":
			p.Cost, _ = strconv.Atoi(val)
		}
	}
}
