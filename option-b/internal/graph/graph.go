// Package graph implements the Middle Earth map as a weighted undirected graph.
// BFS and Dijkstra are used for shortest-path computation.
// The 4 canonical routes (Sections 2.3) are discoverable via BFS.
package graph

import (
	"container/heap"
	"math"

	"github.com/rotr/option-b/internal/config"
)

// Graph is the immutable map graph. Built once at startup.
type Graph struct {
	// adjacency: regionID → []Edge
	adj map[string][]Edge
	// pathsByEndpoints: "from:to" → pathID (both orderings stored)
	pathsByEndpoints map[string]string
	// paths: pathID → PathConfig
	paths map[string]config.PathConfig
}

// Edge represents a connection from one region to an adjacent one.
type Edge struct {
	To     string
	PathID string
	Cost   int
}

// Build constructs a Graph from MapConfig.
func Build(mc *config.MapConfig) *Graph {
	g := &Graph{
		adj:              make(map[string][]Edge),
		pathsByEndpoints: make(map[string]string),
		paths:            make(map[string]config.PathConfig),
	}

	for _, p := range mc.Paths {
		g.paths[p.ID] = p
		// Bidirectional
		g.adj[p.From] = append(g.adj[p.From], Edge{To: p.To, PathID: p.ID, Cost: p.Cost})
		g.adj[p.To] = append(g.adj[p.To], Edge{To: p.From, PathID: p.ID, Cost: p.Cost})
		// Both orderings for endpoint lookup
		g.pathsByEndpoints[p.From+":"+p.To] = p.ID
		g.pathsByEndpoints[p.To+":"+p.From] = p.ID
	}

	return g
}

// Neighbors returns adjacent edges from a region.
func (g *Graph) Neighbors(regionID string) []Edge {
	return g.adj[regionID]
}

// PathBetween returns the path ID between two adjacent regions, or "".
func (g *Graph) PathBetween(from, to string) string {
	return g.pathsByEndpoints[from+":"+to]
}

// PathConfig returns the config for a path ID.
func (g *Graph) PathConfig(pathID string) (config.PathConfig, bool) {
	p, ok := g.paths[pathID]
	return p, ok
}

// AllPathIDs returns all path IDs.
func (g *Graph) AllPathIDs() []string {
	ids := make([]string, 0, len(g.paths))
	for id := range g.paths {
		ids = append(ids, id)
	}
	return ids
}

// AllRegionIDs returns all region IDs from the adjacency map.
func (g *Graph) AllRegionIDs() []string {
	ids := make([]string, 0, len(g.adj))
	for id := range g.adj {
		ids = append(ids, id)
	}
	return ids
}

// AreEndpoints returns true if regionID is one of the path's endpoint regions.
func (g *Graph) AreEndpoints(pathID, regionID string) bool {
	p, ok := g.paths[pathID]
	if !ok {
		return false
	}
	return p.From == regionID || p.To == regionID
}

// BFSDistance returns the minimum hop count between two regions (ignoring costs).
// Used for detection range checks (Section 3.6).
func (g *Graph) BFSDistance(from, to string) int {
	if from == to {
		return 0
	}
	visited := map[string]bool{from: true}
	queue := []struct {
		region string
		dist   int
	}{{from, 0}}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, edge := range g.adj[cur.region] {
			if edge.To == to {
				return cur.dist + 1
			}
			if !visited[edge.To] {
				visited[edge.To] = true
				queue = append(queue, struct {
					region string
					dist   int
				}{edge.To, cur.dist + 1})
			}
		}
	}
	return math.MaxInt32 // unreachable
}

// Dijkstra returns shortest turn-cost from src to all reachable regions.
func (g *Graph) Dijkstra(src string) map[string]int {
	dist := map[string]int{src: 0}
	pq := &priorityQueue{{region: src, cost: 0}}
	heap.Init(pq)

	for pq.Len() > 0 {
		cur := heap.Pop(pq).(*pqItem)
		if cur.cost > dist[cur.region] {
			continue
		}
		for _, edge := range g.adj[cur.region] {
			newCost := cur.cost + edge.Cost
			if prev, ok := dist[edge.To]; !ok || newCost < prev {
				dist[edge.To] = newCost
				heap.Push(pq, &pqItem{region: edge.To, cost: newCost})
			}
		}
	}
	return dist
}

// ShortestPath returns the minimum turn-cost from src to dst.
func (g *Graph) ShortestPath(src, dst string) int {
	dist := g.Dijkstra(src)
	if d, ok := dist[dst]; ok {
		return d
	}
	return math.MaxInt32
}

// BFSWithinHops returns all regions reachable within hops hop-count steps.
func (g *Graph) BFSWithinHops(src string, hops int) []string {
	visited := map[string]int{src: 0}
	queue := []struct {
		region string
		depth  int
	}{{src, 0}}
	var result []string

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.depth > 0 {
			result = append(result, cur.region)
		}
		if cur.depth >= hops {
			continue
		}
		for _, edge := range g.adj[cur.region] {
			if _, seen := visited[edge.To]; !seen {
				visited[edge.To] = cur.depth + 1
				queue = append(queue, struct {
					region string
					depth  int
				}{edge.To, cur.depth + 1})
			}
		}
	}
	return result
}

// ----- Priority Queue for Dijkstra -----

type pqItem struct {
	region string
	cost   int
	index  int
}

type priorityQueue []*pqItem

func (pq priorityQueue) Len() int           { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].cost < pq[j].cost }
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*pqItem)
	item.index = len(*pq)
	*pq = append(*pq, item)
}
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[:n-1]
	return item
}
