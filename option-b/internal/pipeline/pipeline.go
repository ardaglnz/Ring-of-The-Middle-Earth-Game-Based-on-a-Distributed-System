// Package pipeline implements the two Go analysis pipelines.
// Pipeline 1: Route Risk (Light Side) — 4 workers, buffer cap 20.
// Pipeline 2: Interception (Dark Side) — 4 workers, buffer cap 30.
package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/rotr/option-b/internal/cache"
	"github.com/rotr/option-b/internal/config"
	"github.com/rotr/option-b/internal/graph"
)

// ----- Pipeline 1: Route Risk -----

// RouteRisk computes the risk score for a candidate route.
type RouteRisk struct {
	PathIDs      []string
	RegionIDs    []string // destination regions along the route
	RiskScore    int
	ThreatPaths  []string
	BlockedPaths []string
	Recommended  bool
	Warnings     []string
}

// RankedRouteList is the output of Pipeline 1.
type RankedRouteList struct {
	Routes      []RouteRisk
	Recommended int // index of recommended route
	Warnings    []string
}

// ComputeRouteRisk computes the route risk formula from Section 32:
//
//	riskScore =
//	    sum(region.threatLevel for each destination region)
//	  + sum(path.surveillanceLevel for each path) * 3
//	  + count(BLOCKED paths)    * 5
//	  + count(THREATENED paths) * 2
//	  + nazgulProximityCount    * 2
func ComputeRouteRisk(pathIDs []string, snap cache.WorldStateCache, g *graph.Graph) RouteRisk {
	rr := RouteRisk{PathIDs: pathIDs}

	regionSet := map[string]bool{}
	for _, pid := range pathIDs {
		path, ok := snap.Paths[pid]
		if !ok {
			continue
		}
		pc := path.Config
		regionSet[pc.To] = true

		rr.RiskScore += path.SurveillanceLevel * 3

		switch path.Status {
		case cache.StatusBlocked:
			rr.RiskScore += 5
			rr.BlockedPaths = append(rr.BlockedPaths, pid)
		case cache.StatusThreatened:
			rr.RiskScore += 2
			rr.ThreatPaths = append(rr.ThreatPaths, pid)
		}
	}

	for rid := range regionSet {
		rr.RegionIDs = append(rr.RegionIDs, rid)
		if region, ok := snap.Regions[rid]; ok {
			rr.RiskScore += region.ThreatLevel
		}
	}

	// Nazgul proximity: count Nazgul within 2 graph hops of any region in the route.
	rr.RiskScore += countNazgulProximity(rr.RegionIDs, snap, g) * 2

	if len(rr.BlockedPaths) > 0 {
		rr.Warnings = append(rr.Warnings, "Route has BLOCKED paths")
	}

	return rr
}

func countNazgulProximity(regionIDs []string, snap cache.WorldStateCache, g *graph.Graph) int {
	// Collect all Nazgul positions.
	nazgulRegions := map[string]bool{}
	for _, u := range snap.Units {
		if u.Config.Class == config.ClassNazgul && u.Status == cache.UnitActive {
			nazgulRegions[u.Region] = true
		}
	}

	count := 0
	for _, rid := range regionIDs {
		nearby := g.BFSWithinHops(rid, 2)
		for _, nr := range nearby {
			if nazgulRegions[nr] {
				count++
			}
		}
	}
	return count
}

// RunPipeline1 executes the Route Risk pipeline with 4 workers and a 2-second timeout.
// Each worker receives a value copy of the cache (never a pointer).
func RunPipeline1(ctx context.Context, candidateRoutes [][]string, snap cache.WorldStateCache, g *graph.Graph) RankedRouteList {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// Dispatcher → buffered channel (cap=20) → 4 workers → results
	taskCh := make(chan []string, 20)
	resultCh := make(chan RouteRisk, len(candidateRoutes))

	var wg sync.WaitGroup

	// Start 4 workers.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case pathIDs, ok := <-taskCh:
					if !ok {
						return
					}
					// Worker receives value copy of snap — never a pointer.
					result := ComputeRouteRisk(pathIDs, snap, g)
					select {
					case resultCh <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	// Dispatcher — or-done pattern.
	go func() {
		defer close(taskCh)
		for _, route := range candidateRoutes {
			select {
			case <-ctx.Done():
				return
			case taskCh <- route:
			}
		}
	}()

	// Wait for workers then close results.
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Aggregator.
	var results []RouteRisk
	for r := range resultCh {
		results = append(results, r)
	}

	// Sort by risk score ascending — lower is better.
	sortByRisk(results)

	recommended := 0 // lowest risk
	return RankedRouteList{Routes: results, Recommended: recommended}
}

func sortByRisk(routes []RouteRisk) {
	for i := 1; i < len(routes); i++ {
		for j := i; j > 0 && routes[j].RiskScore < routes[j-1].RiskScore; j-- {
			routes[j], routes[j-1] = routes[j-1], routes[j]
		}
	}
}

// ----- Pipeline 2: Interception -----

// InterceptTask is one (Nazgul, route-candidate-region) pair.
type InterceptTask struct {
	NazgulID       string
	NazgulRegion   string
	RouteRegion    string
	RBTurnsToReach int
	RouteLength    int
}

// InterceptResult is the score for one pair.
type InterceptResult struct {
	UnitID       string
	TargetRegion string
	Score        float64
}

// InterceptPlan is the output of Pipeline 2.
type InterceptPlan struct {
	ByUnit []InterceptResult
}

// ComputeIntercept calculates one (Nazgul, region) intercept score per Section 33:
//
//	turnsToIntercept = graph.shortestPath(nazgul.region, routeRegion)
//	interceptWindow  = rbTurnsToReach - turnsToIntercept
//	score = interceptWindow >= 0 ? 1.0 - (turnsToIntercept / routeLength) : 0.0
func ComputeIntercept(task InterceptTask, g *graph.Graph) InterceptResult {
	turnsToIntercept := g.ShortestPath(task.NazgulRegion, task.RouteRegion)
	interceptWindow := task.RBTurnsToReach - turnsToIntercept

	score := 0.0
	if interceptWindow >= 0 && task.RouteLength > 0 {
		score = 1.0 - float64(turnsToIntercept)/float64(task.RouteLength)
		if score < 0 {
			score = 0
		}
	}

	return InterceptResult{
		UnitID:       task.NazgulID,
		TargetRegion: task.RouteRegion,
		Score:        score,
	}
}

// RunPipeline2 executes the Interception pipeline with 4 workers, buffer cap 30.
func RunPipeline2(ctx context.Context, tasks []InterceptTask, snap cache.WorldStateCache, g *graph.Graph) InterceptPlan {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	taskCh := make(chan InterceptTask, 30)
	resultCh := make(chan InterceptResult, len(tasks))

	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskCh:
					if !ok {
						return
					}
					result := ComputeIntercept(task, g)
					select {
					case resultCh <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	go func() {
		defer close(taskCh)
		for _, t := range tasks {
			select {
			case <-ctx.Done():
				return
			case taskCh <- t:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var results []InterceptResult
	for r := range resultCh {
		results = append(results, r)
	}

	// Sort by score descending — highest intercept opportunity first.
	for i := 1; i < len(results); i++ {
		for j := i; j > 0 && results[j].Score > results[j-1].Score; j-- {
			results[j], results[j-1] = results[j-1], results[j]
		}
	}

	return InterceptPlan{ByUnit: results}
}

// BuildInterceptTasks creates intercept tasks for all Nazgul × all route regions.
func BuildInterceptTasks(snap cache.WorldStateCache, candidateRouteRegions []string, rbRouteLen int) []InterceptTask {
	var tasks []InterceptTask
	for _, u := range snap.Units {
		if u.Config.Class != config.ClassNazgul || u.Status != cache.UnitActive {
			continue
		}
		for i, region := range candidateRouteRegions {
			tasks = append(tasks, InterceptTask{
				NazgulID:       u.ID,
				NazgulRegion:   u.Region,
				RouteRegion:    region,
				RBTurnsToReach: i + 1,
				RouteLength:    rbRouteLen,
			})
		}
	}
	return tasks
}
