/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package graph is the pure, kind-agnostic dependsOn DAG (DESIGN §3.2): no
// Kubernetes, no I/O. It derives strongly connected components (cycles),
// transitive ancestors, and unknown (dangling) edge targets from a flat
// edge map of adapter.NodeRef.
package graph

import (
	"iter"
	"slices"
	"sync"

	"github.com/isometry/wavefront-controller/internal/adapter"
)

// Graph is the derived dependsOn graph. It is immutable after Build except
// for the TransitiveAncestors memoization cache, which is safe for
// concurrent use.
type Graph struct {
	// edges holds dependsOn edges for every node, known or external (dangling
	// targets registered as empty-edge nodes). A nil slice and an empty
	// slice are equivalent.
	edges map[adapter.NodeRef][]adapter.NodeRef

	// unknown are refs that appeared only as edge targets, not as a key of
	// the map passed to Build.
	unknown []adapter.NodeRef

	// cycles holds the strongly connected components with more than one
	// member, plus single-node self-loops. Each member list is sorted; the
	// outer list is sorted by its first (smallest) member.
	cycles [][]adapter.NodeRef

	// inCycle is the set of nodes that are themselves members of a cycle.
	inCycle map[adapter.NodeRef]bool

	ancestorsMu    sync.Mutex
	ancestorsCache map[adapter.NodeRef][]adapter.NodeRef
}

// Build constructs the DAG from nodes and their dependsOn edges.
// Edges to refs absent from nodes are auto-registered as external nodes
// (retrievable via Unknown()); the caller decides their semantics.
// Build never fails: cycles are reported, not errors (DESIGN §3.2).
func Build(nodes map[adapter.NodeRef][]adapter.NodeRef) *Graph {
	edges := make(map[adapter.NodeRef][]adapter.NodeRef, len(nodes))
	for ref, deps := range nodes {
		edges[ref] = dedupSorted(deps)
	}

	var unknown []adapter.NodeRef
	for _, deps := range nodes {
		for _, dep := range deps {
			if _, known := nodes[dep]; known {
				continue
			}
			if _, seen := edges[dep]; !seen {
				edges[dep] = nil
				unknown = append(unknown, dep)
			}
		}
	}
	sortRefs(unknown)

	sccs := tarjanSCC(sortedKeys(edges), edges)

	inCycle := make(map[adapter.NodeRef]bool)
	var cycles [][]adapter.NodeRef
	for _, scc := range sccs {
		cyclic := len(scc) > 1
		if !cyclic && len(scc) == 1 {
			v := scc[0]
			if slices.Contains(edges[v], v) {
				cyclic = true
			}
		}
		if !cyclic {
			continue
		}
		member := slices.Clone(scc)
		sortRefs(member)
		cycles = append(cycles, member)
		for _, v := range member {
			inCycle[v] = true
		}
	}
	sortCycles(cycles)

	return &Graph{
		edges:          edges,
		unknown:        unknown,
		cycles:         cycles,
		inCycle:        inCycle,
		ancestorsCache: make(map[adapter.NodeRef][]adapter.NodeRef),
	}
}

// Nodes returns every node known to the graph (both supplied and external),
// in deterministic (String()-sorted) order.
func (g *Graph) Nodes() iter.Seq[adapter.NodeRef] {
	all := sortedKeys(g.edges)
	return func(yield func(adapter.NodeRef) bool) {
		for _, ref := range all {
			if !yield(ref) {
				return
			}
		}
	}
}

// DependsOn returns ref's direct dependencies, deduplicated and sorted by
// String(). It returns nil for a ref unknown to the graph.
func (g *Graph) DependsOn(ref adapter.NodeRef) []adapter.NodeRef {
	return slices.Clone(g.edges[ref])
}

// TransitiveAncestors returns every ancestor reachable via dependsOn
// (memoized), deduplicated and sorted by String().
func (g *Graph) TransitiveAncestors(ref adapter.NodeRef) []adapter.NodeRef {
	g.ancestorsMu.Lock()
	if cached, ok := g.ancestorsCache[ref]; ok {
		g.ancestorsMu.Unlock()
		return slices.Clone(cached)
	}
	g.ancestorsMu.Unlock()

	visited := make(map[adapter.NodeRef]bool)
	stack := append([]adapter.NodeRef(nil), g.edges[ref]...)
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[n] {
			continue
		}
		visited[n] = true
		stack = append(stack, g.edges[n]...)
	}

	result := make([]adapter.NodeRef, 0, len(visited))
	for n := range visited {
		result = append(result, n)
	}
	sortRefs(result)

	g.ancestorsMu.Lock()
	g.ancestorsCache[ref] = result
	g.ancestorsMu.Unlock()

	return slices.Clone(result)
}

// Cycles returns the strongly connected components with len > 1 (plus
// self-loops). Each member list is sorted by String(); the outer list is
// sorted by its first member.
func (g *Graph) Cycles() [][]adapter.NodeRef {
	out := make([][]adapter.NodeRef, len(g.cycles))
	for i, c := range g.cycles {
		out[i] = slices.Clone(c)
	}
	return out
}

// InCycle reports whether ref belongs to, or transitively depends on, a
// cycle.
func (g *Graph) InCycle(ref adapter.NodeRef) bool {
	if g.inCycle[ref] {
		return true
	}
	for _, ancestor := range g.TransitiveAncestors(ref) {
		if g.inCycle[ancestor] {
			return true
		}
	}
	return false
}

// Unknown returns refs that appear only as edge targets (not supplied as
// nodes), sorted by String().
func (g *Graph) Unknown() []adapter.NodeRef {
	return slices.Clone(g.unknown)
}

// compareRefs orders refs by their String() form, giving the whole package
// a single, deterministic sort key.
func compareRefs(a, b adapter.NodeRef) int {
	switch as, bs := a.String(), b.String(); {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

func sortRefs(refs []adapter.NodeRef) {
	slices.SortFunc(refs, compareRefs)
}

// dedupSorted returns a sorted, duplicate-free copy of refs. A nil or empty
// input yields nil, so an absent DependsOn and an explicit empty one are
// indistinguishable to callers.
func dedupSorted(refs []adapter.NodeRef) []adapter.NodeRef {
	if len(refs) == 0 {
		return nil
	}
	out := slices.Clone(refs)
	sortRefs(out)
	out = slices.CompactFunc(out, func(a, b adapter.NodeRef) bool { return a == b })
	return out
}

func sortedKeys(m map[adapter.NodeRef][]adapter.NodeRef) []adapter.NodeRef {
	out := make([]adapter.NodeRef, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sortRefs(out)
	return out
}

// sortCycles orders a cycle list by its (already sorted) first member, so
// Cycles() output is stable across Build calls with different map iteration
// orders.
func sortCycles(cycles [][]adapter.NodeRef) {
	slices.SortFunc(cycles, func(a, b []adapter.NodeRef) int {
		return compareRefs(a[0], b[0])
	})
}

// tarjanSCC computes the strongly connected components of edges, visiting
// nodes in order for deterministic output. order must contain every key of
// edges.
func tarjanSCC(order []adapter.NodeRef, edges map[adapter.NodeRef][]adapter.NodeRef) [][]adapter.NodeRef {
	index := 0
	indices := make(map[adapter.NodeRef]int, len(order))
	lowlink := make(map[adapter.NodeRef]int, len(order))
	onStack := make(map[adapter.NodeRef]bool, len(order))
	var stack []adapter.NodeRef
	var sccs [][]adapter.NodeRef

	var strongConnect func(v adapter.NodeRef)
	strongConnect = func(v adapter.NodeRef) {
		indices[v] = index
		lowlink[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true

		for _, w := range edges[v] {
			if _, ok := indices[w]; !ok {
				strongConnect(w)
				lowlink[v] = min(lowlink[v], lowlink[w])
			} else if onStack[w] {
				lowlink[v] = min(lowlink[v], indices[w])
			}
		}

		if lowlink[v] != indices[v] {
			return
		}
		var scc []adapter.NodeRef
		for {
			w := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[w] = false
			scc = append(scc, w)
			if w == v {
				break
			}
		}
		sccs = append(sccs, scc)
	}

	for _, v := range order {
		if _, ok := indices[v]; !ok {
			strongConnect(v)
		}
	}
	return sccs
}
