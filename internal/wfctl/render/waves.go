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

package render

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// missingState is what a dependsOn target with no node of its own is: a gate
// the graph waits on and the cluster does not have. It layers at wave 0
// because that is how it behaves — an unsatisfiable wave-0 blocker.
const missingState = "MissingGate"

// gateRole labels a dependency target the snapshot has no reading for.
const gateRole = "Gate"

// graphModel is the shared view every graph renderer needs: the wave of each
// reference, the node behind it (when there is one), and the reverse edges.
//
// It is built from the snapshot's own edges via snapshot.Waves, so it works
// identically for a status snapshot, a derive snapshot and a replayed file —
// none of which has a cluster to rebuild internal/graph from.
type graphModel struct {
	waves    map[adapter.NodeRef]int
	nodes    map[adapter.NodeRef]*snapshot.NodeView
	children map[adapter.NodeRef][]adapter.NodeRef
	// order is every reference in the graph, node or missing, sorted.
	order []adapter.NodeRef
}

func newGraphModel(s *snapshot.Snapshot) *graphModel {
	model := &graphModel{
		waves:    snapshot.Waves(s.Nodes),
		nodes:    make(map[adapter.NodeRef]*snapshot.NodeView, len(s.Nodes)),
		children: make(map[adapter.NodeRef][]adapter.NodeRef, len(s.Nodes)),
	}
	for i := range s.Nodes {
		model.nodes[s.Nodes[i].Ref] = &s.Nodes[i]
	}
	for i := range s.Nodes {
		node := &s.Nodes[i]
		for _, dep := range node.DependsOn {
			model.children[dep] = append(model.children[dep], node.Ref)
		}
	}

	model.order = make([]adapter.NodeRef, 0, len(model.waves))
	for ref := range model.waves {
		model.order = append(model.order, ref)
	}
	slices.SortFunc(model.order, compareRefs)
	for ref := range model.children {
		slices.SortFunc(model.children[ref], compareRefs)
	}
	return model
}

// compareRefs orders references the way the snapshot itself is sorted.
func compareRefs(a, b adapter.NodeRef) int {
	return strings.Compare(a.String(), b.String())
}

// missing reports a reference that is a dependsOn target without a node.
func (m *graphModel) missing(ref adapter.NodeRef) bool {
	_, known := m.nodes[ref]
	return !known
}

// role renders a reference's role, naming a dangling target for what it is.
func (m *graphModel) role(ref adapter.NodeRef) string {
	if node, known := m.nodes[ref]; known {
		return orAbsent(node.Role)
	}
	return gateRole
}

// state renders a reference's state.
func (m *graphModel) state(ref adapter.NodeRef, o Options) string {
	node, known := m.nodes[ref]
	if !known {
		return o.Palette.Dim(missingState)
	}
	return o.Palette.stateCell(node.State, node.Held)
}

// inCycle reports a reference that never layered: it is in, or behind, a
// cycle (snapshot.Waves).
func (m *graphModel) inCycle(ref adapter.NodeRef) bool {
	return m.waves[ref] < 0
}

// Graph renders the default graph view: dependsOn depth layers, or "waves".
//
// Every node in a wave can advance concurrently with every other node in it,
// which is the property the layering exists to show: the frontier is wide,
// not a queue. Anything that never layered is listed apart, because a cycle
// has no depth and inventing one would suggest an order the engine
// explicitly refuses to assume.
func Graph(w io.Writer, s *snapshot.Snapshot, o Options) error {
	model := newGraphModel(s)

	layers := map[int][]adapter.NodeRef{}
	var unlayered []adapter.NodeRef
	for _, ref := range model.order {
		wave := model.waves[ref]
		if wave < 0 {
			unlayered = append(unlayered, ref)
			continue
		}
		layers[wave] = append(layers[wave], ref)
	}

	depths := make([]int, 0, len(layers))
	for depth := range layers {
		depths = append(depths, depth)
	}
	slices.Sort(depths)

	out := newSink(w)
	table := newPlainTable(w)
	for i, depth := range depths {
		if i > 0 {
			table.Row("")
		}
		table.Row(waveHeading(depth, len(layers[depth])))
		for _, ref := range layers[depth] {
			table.Row("  "+nodeName(ref), model.role(ref), model.state(ref, o))
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}

	if len(unlayered) == 0 {
		return out.err
	}

	out.blank()
	if out.err != nil {
		return out.err
	}
	trailing := newPlainTable(w)
	trailing.Row(unlayeredHeading(s, len(unlayered)))
	for _, ref := range unlayered {
		trailing.Row("  "+nodeName(ref), model.role(ref), model.state(ref, o))
	}
	return trailing.Flush()
}

// waveHeading titles one depth layer.
func waveHeading(depth, count int) string {
	return fmt.Sprintf("wave %d (%d)", depth, count)
}

// unlayeredHeading titles the never-dequeued group, naming the cycle that
// caused it when the snapshot knows one.
func unlayeredHeading(s *snapshot.Snapshot, count int) string {
	cycles := cycleDescriptions(s)
	if len(cycles) == 0 {
		return fmt.Sprintf("unlayered (%d)", count)
	}
	return fmt.Sprintf("unlayered (%d) (cycle: %s)", count, strings.Join(cycles, "; "))
}

// cycleDescriptions renders each detected cycle as "a→b→a": closing the loop
// is what makes it read as a cycle rather than a path.
func cycleDescriptions(s *snapshot.Snapshot) []string {
	descriptions := make([]string, 0, len(s.Graph.Cycles))
	for _, cycle := range s.Graph.Cycles {
		if len(cycle) == 0 {
			continue
		}
		names := make([]string, 0, len(cycle)+1)
		for _, ref := range cycle {
			names = append(names, nodeName(ref))
		}
		names = append(names, nodeName(cycle[0]))
		descriptions = append(descriptions, strings.Join(names, "→"))
	}
	return descriptions
}
