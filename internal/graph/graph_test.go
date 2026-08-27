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

package graph_test

import (
	"slices"
	"testing"

	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/graph"
)

// ref builds a NodeRef of a fixed kind/namespace so test tables stay terse.
func ref(name string) adapter.NodeRef {
	return adapter.NodeRef{Kind: "Kustomization", Namespace: "ns", Name: name}
}

func refs(names ...string) []adapter.NodeRef {
	out := make([]adapter.NodeRef, len(names))
	for i, n := range names {
		out[i] = ref(n)
	}
	sortRefs(out)
	return out
}

func sortRefs(rs []adapter.NodeRef) {
	slices.SortFunc(rs, func(a, b adapter.NodeRef) int {
		switch {
		case a.String() < b.String():
			return -1
		case a.String() > b.String():
			return 1
		default:
			return 0
		}
	})
}

// sortedCycles normalizes a Cycles() result for comparison: each member list
// sorted, then the outer list sorted by its first (smallest) member.
func sortedCycles(cs [][]adapter.NodeRef) [][]adapter.NodeRef {
	out := make([][]adapter.NodeRef, len(cs))
	for i, c := range cs {
		cc := slices.Clone(c)
		sortRefs(cc)
		out[i] = cc
	}
	slices.SortFunc(out, func(a, b []adapter.NodeRef) int {
		switch {
		case a[0].String() < b[0].String():
			return -1
		case a[0].String() > b[0].String():
			return 1
		default:
			return 0
		}
	})
	return out
}

type testCase struct {
	name  string
	nodes map[adapter.NodeRef][]adapter.NodeRef
	check func(t *testing.T, g *graph.Graph)
}

func TestGraph(t *testing.T) {
	cases := []testCase{
		{
			name: "linear chain",
			// C depends on B depends on A.
			nodes: map[adapter.NodeRef][]adapter.NodeRef{
				ref("a"): nil,
				ref("b"): {ref("a")},
				ref("c"): {ref("b")},
			},
			check: func(t *testing.T, g *graph.Graph) {
				got := g.TransitiveAncestors(ref("c"))
				want := refs("a", "b")
				if !slices.Equal(got, want) {
					t.Errorf("TransitiveAncestors(c) = %v, want %v", got, want)
				}
				if len(g.TransitiveAncestors(ref("a"))) != 0 {
					t.Errorf("TransitiveAncestors(a) should be empty, got %v", g.TransitiveAncestors(ref("a")))
				}
			},
		},
		{
			name: "diamond, no duplicate ancestors",
			// D depends on B and C; both depend on A.
			nodes: map[adapter.NodeRef][]adapter.NodeRef{
				ref("a"): nil,
				ref("b"): {ref("a")},
				ref("c"): {ref("a")},
				ref("d"): {ref("b"), ref("c")},
			},
			check: func(t *testing.T, g *graph.Graph) {
				got := g.TransitiveAncestors(ref("d"))
				want := refs("a", "b", "c")
				if !slices.Equal(got, want) {
					t.Errorf("TransitiveAncestors(d) = %v, want %v", got, want)
				}
			},
		},
		{
			name: "disconnected branches have no cross ancestors",
			nodes: map[adapter.NodeRef][]adapter.NodeRef{
				ref("a1"): nil,
				ref("b1"): {ref("a1")},
				ref("a2"): nil,
				ref("b2"): {ref("a2")},
			},
			check: func(t *testing.T, g *graph.Graph) {
				got := g.TransitiveAncestors(ref("b1"))
				want := refs("a1")
				if !slices.Equal(got, want) {
					t.Errorf("TransitiveAncestors(b1) = %v, want %v", got, want)
				}
				got2 := g.TransitiveAncestors(ref("b2"))
				want2 := refs("a2")
				if !slices.Equal(got2, want2) {
					t.Errorf("TransitiveAncestors(b2) = %v, want %v", got2, want2)
				}
			},
		},
		{
			name: "self-loop is a cycle of one",
			nodes: map[adapter.NodeRef][]adapter.NodeRef{
				ref("a"): {ref("a")},
			},
			check: func(t *testing.T, g *graph.Graph) {
				if !g.InCycle(ref("a")) {
					t.Errorf("InCycle(a) = false, want true (self-loop)")
				}
				want := [][]adapter.NodeRef{refs("a")}
				got := sortedCycles(g.Cycles())
				if !slices.EqualFunc(got, want, slices.Equal) {
					t.Errorf("Cycles() = %v, want %v", got, want)
				}
			},
		},
		{
			name: "2-cycle: members and descendants InCycle, unrelated branch untouched",
			nodes: map[adapter.NodeRef][]adapter.NodeRef{
				ref("a"): {ref("b")},
				ref("b"): {ref("a")},
				ref("d"): {ref("a")}, // descendant of the cycle
				ref("e"): nil,        // unrelated
			},
			check: func(t *testing.T, g *graph.Graph) {
				if !g.InCycle(ref("a")) {
					t.Errorf("InCycle(a) = false, want true")
				}
				if !g.InCycle(ref("b")) {
					t.Errorf("InCycle(b) = false, want true")
				}
				if !g.InCycle(ref("d")) {
					t.Errorf("InCycle(d) = false, want true (depends on cycle)")
				}
				if g.InCycle(ref("e")) {
					t.Errorf("InCycle(e) = true, want false (unrelated)")
				}
				want := [][]adapter.NodeRef{refs("a", "b")}
				got := sortedCycles(g.Cycles())
				if !slices.EqualFunc(got, want, slices.Equal) {
					t.Errorf("Cycles() = %v, want %v", got, want)
				}
			},
		},
		{
			name: "3-cycle with descendants, unrelated branch untouched",
			nodes: map[adapter.NodeRef][]adapter.NodeRef{
				ref("a"): {ref("b")},
				ref("b"): {ref("c")},
				ref("c"): {ref("a")},
				ref("d"): {ref("a")}, // descendant of the cycle
				ref("e"): nil,        // unrelated
			},
			check: func(t *testing.T, g *graph.Graph) {
				for _, n := range []string{"a", "b", "c", "d"} {
					if !g.InCycle(ref(n)) {
						t.Errorf("InCycle(%s) = false, want true", n)
					}
				}
				if g.InCycle(ref("e")) {
					t.Errorf("InCycle(e) = true, want false (unrelated)")
				}
				want := [][]adapter.NodeRef{refs("a", "b", "c")}
				got := sortedCycles(g.Cycles())
				if !slices.EqualFunc(got, want, slices.Equal) {
					t.Errorf("Cycles() = %v, want %v", got, want)
				}
			},
		},
		{
			name: "dangling edge target is Unknown and still an ancestor",
			nodes: map[adapter.NodeRef][]adapter.NodeRef{
				ref("a"): {ref("ghost")},
			},
			check: func(t *testing.T, g *graph.Graph) {
				wantUnknown := refs("ghost")
				gotUnknown := g.Unknown()
				if !slices.Equal(gotUnknown, wantUnknown) {
					t.Errorf("Unknown() = %v, want %v", gotUnknown, wantUnknown)
				}
				gotAncestors := g.TransitiveAncestors(ref("a"))
				wantAncestors := refs("ghost")
				if !slices.Equal(gotAncestors, wantAncestors) {
					t.Errorf("TransitiveAncestors(a) = %v, want %v", gotAncestors, wantAncestors)
				}
			},
		},
		{
			name: "nil vs empty DependsOn slice are equivalent",
			nodes: map[adapter.NodeRef][]adapter.NodeRef{
				ref("a"): nil,
				ref("b"): {},
			},
			check: func(t *testing.T, g *graph.Graph) {
				if got := g.DependsOn(ref("a")); len(got) != 0 {
					t.Errorf("DependsOn(a) = %v, want empty", got)
				}
				if got := g.DependsOn(ref("b")); len(got) != 0 {
					t.Errorf("DependsOn(b) = %v, want empty", got)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := graph.Build(tc.nodes)
			tc.check(t, g)
		})
	}
}

// TestDeterministicOrdering asserts every slice-returning accessor comes back
// sorted by NodeRef.String(), regardless of insertion order, so status output
// is stable across reconciles.
func TestDeterministicOrdering(t *testing.T) {
	// Insert dependencies and cycle members in reverse-alphabetical order to
	// make sure sorting isn't an accident of map/slice iteration order.
	nodes := map[adapter.NodeRef][]adapter.NodeRef{
		ref("z"): {ref("y"), ref("x"), ref("w")},
		ref("y"): {ref("x")},
		ref("x"): {ref("w")},
		ref("w"): nil,
		// a 3-cycle inserted "backwards"
		ref("cc"): {ref("cb")},
		ref("cb"): {ref("ca")},
		ref("ca"): {ref("cc")},
		// dangling edges inserted out of order
		ref("dep"): {ref("zzz"), ref("aaa"), ref("mmm")},
	}
	g := graph.Build(nodes)

	if got, want := g.DependsOn(ref("z")), refs("w", "x", "y"); !slices.Equal(got, want) {
		t.Errorf("DependsOn(z) = %v, want sorted %v", got, want)
	}
	if got, want := g.TransitiveAncestors(ref("z")), refs("w", "x", "y"); !slices.Equal(got, want) {
		t.Errorf("TransitiveAncestors(z) = %v, want sorted %v", got, want)
	}
	if got, want := g.Unknown(), refs("aaa", "mmm", "zzz"); !slices.Equal(got, want) {
		t.Errorf("Unknown() = %v, want sorted %v", got, want)
	}

	cycles := g.Cycles()
	if len(cycles) != 1 {
		t.Fatalf("Cycles() len = %d, want 1", len(cycles))
	}
	if want := refs("ca", "cb", "cc"); !slices.Equal(cycles[0], want) {
		t.Errorf("Cycles()[0] = %v, want sorted %v", cycles[0], want)
	}

	// Nodes() must also enumerate deterministically.
	var seen []adapter.NodeRef
	for n := range g.Nodes() {
		seen = append(seen, n)
	}
	sortRefs(seen)
	got := slices.Clone(seen)
	sortRefs(got)
	orig := slices.Clone(seen)
	if !slices.Equal(got, orig) {
		t.Errorf("Nodes() enumeration was not internally consistent")
	}
	var unsorted []adapter.NodeRef
	for n := range g.Nodes() {
		unsorted = append(unsorted, n)
	}
	if !slices.IsSortedFunc(unsorted, func(a, b adapter.NodeRef) int {
		switch {
		case a.String() < b.String():
			return -1
		case a.String() > b.String():
			return 1
		default:
			return 0
		}
	}) {
		t.Errorf("Nodes() = %v, want sorted by String()", unsorted)
	}
}

// TestNodesEarlyBreak exercises the yield=false path of the iter.Seq
// returned by Nodes() (range-over-func with an early break).
func TestNodesEarlyBreak(t *testing.T) {
	g := graph.Build(map[adapter.NodeRef][]adapter.NodeRef{
		ref("a"): nil,
		ref("b"): nil,
		ref("c"): nil,
	})
	count := 0
	for range g.Nodes() {
		count++
		if count == 1 {
			break
		}
	}
	if count != 1 {
		t.Errorf("count after early break = %d, want 1", count)
	}
}

// TestMultipleCyclesAreSorted builds two disjoint cycles to exercise
// Cycles() ordering across more than one component.
func TestMultipleCyclesAreSorted(t *testing.T) {
	nodes := map[adapter.NodeRef][]adapter.NodeRef{
		ref("z2"): {ref("z1")},
		ref("z1"): {ref("z2")},
		ref("a2"): {ref("a1")},
		ref("a1"): {ref("a2")},
	}
	g := graph.Build(nodes)
	got := g.Cycles()
	want := [][]adapter.NodeRef{refs("a1", "a2"), refs("z1", "z2")}
	if !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("Cycles() = %v, want %v (sorted by first member)", got, want)
	}
}

// TestUnknownAsAncestorOfMultiple ensures a dangling target reached through
// more than one path is still reported once, in Unknown() and as an ancestor.
func TestUnknownAsAncestorOfMultiple(t *testing.T) {
	nodes := map[adapter.NodeRef][]adapter.NodeRef{
		ref("a"): {ref("ghost")},
		ref("b"): {ref("ghost")},
	}
	g := graph.Build(nodes)

	want := refs("ghost")
	if got := g.Unknown(); !slices.Equal(got, want) {
		t.Errorf("Unknown() = %v, want %v", got, want)
	}
	if got := g.TransitiveAncestors(ref("a")); !slices.Equal(got, want) {
		t.Errorf("TransitiveAncestors(a) = %v, want %v", got, want)
	}
	if got := g.TransitiveAncestors(ref("b")); !slices.Equal(got, want) {
		t.Errorf("TransitiveAncestors(b) = %v, want %v", got, want)
	}
}
