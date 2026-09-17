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
	"io"
	"strings"

	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// Graphviz colours, one per state (plan B3). Named colours rather than hex:
// they render identically in every Graphviz build and read as themselves in
// the source of the diagram.
const (
	dotSettled    = "forestgreen"
	dotPending    = "goldenrod"
	dotConverging = "steelblue"
	dotUnhealthy  = "firebrick"
	dotMissing    = "gray60"
)

// Graphviz shapes: a gate is a health-only participant with no pin of its
// own, and the hexagon says so at a glance (DESIGN §3.2).
const (
	dotNodeShape = "box"
	dotGateShape = "hexagon"
)

// DOT writes the graph as Graphviz source (plan B3, `-o dot`).
//
// State is carried by colour, role by shape, and the two exceptional
// structures — a hold and a cycle — by border treatments, so a rendered
// diagram answers "what is stuck, and why" without a legend lookup. The
// dashed red edges are the engine's own attribution: each points from a
// blocked node's blocker to the node it is blocking.
func DOT(w io.Writer, s *snapshot.Snapshot) error {
	model := newGraphModel(s)
	out := newSink(w)

	out.line("digraph wavefront {")
	out.line(`  rankdir="LR";`)
	out.line(`  node [fontname="sans-serif", fontsize=10];`)
	out.line(`  edge [fontname="sans-serif", fontsize=9];`)

	for _, ref := range model.order {
		out.printf("  %s [%s];\n", dotQuote(ref.String()), strings.Join(dotAttrs(model, ref), ", "))
	}

	for i := range s.Nodes {
		node := &s.Nodes[i]
		for _, dep := range node.DependsOn {
			out.printf("  %s -> %s;\n", dotQuote(dep.String()), dotQuote(node.Ref.String()))
		}
	}
	for i := range s.Nodes {
		node := &s.Nodes[i]
		if node.Blocked == nil || node.Blocked.Ancestor == nil {
			continue
		}
		blocker := nodeRef(*node.Blocked.Ancestor)
		out.printf("  %s -> %s [style=%s, color=%s, constraint=false, label=%s];\n",
			dotQuote(blocker.String()), dotQuote(node.Ref.String()),
			dotQuote("dashed"), dotQuote(dotUnhealthy), dotQuote(node.Blocked.Reason))
	}

	out.line("}")
	return out.err
}

// dotAttrs builds one node's attribute list.
func dotAttrs(model *graphModel, ref adapter.NodeRef) []string {
	label := nodeName(ref)
	shape := dotNodeShape
	styles := []string{"rounded"}
	colour := dotMissing

	node, known := model.nodes[ref]
	switch {
	case !known:
		// A dangling dependsOn target: grey and dotted, because the graph is
		// waiting on something the cluster does not have.
		label += `\n` + missingState
		shape = dotGateShape
		styles = append(styles, "dotted")
	default:
		label += `\n` + node.State
		colour = dotColour(node.State)
		if node.Role == gateRole {
			shape = dotGateShape
		}
		if node.Held {
			styles = append(styles, "dashed")
		}
	}

	attrs := []string{
		"label=" + dotQuote(label),
		"shape=" + dotQuote(shape),
		"style=" + dotQuote(strings.Join(styles, ",")),
		"color=" + dotQuote(colour),
	}
	if model.inCycle(ref) {
		attrs = append(attrs, "peripheries=2")
	}
	return attrs
}

// dotColour maps a node state to its Graphviz colour.
func dotColour(state string) string {
	switch engine.State(state) {
	case engine.StateSettled:
		return dotSettled
	case engine.StatePending, engine.StateAdmissible:
		return dotPending
	case engine.StateConverging:
		return dotConverging
	case engine.StateUnhealthy:
		return dotUnhealthy
	default:
		return dotMissing
	}
}

// dotQuote quotes an identifier or label for DOT. The `\n` in a label is
// already an escape DOT understands, so only the quote itself needs work.
func dotQuote(text string) string {
	return `"` + strings.ReplaceAll(text, `"`, `\"`) + `"`
}
