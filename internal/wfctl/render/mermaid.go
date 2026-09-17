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
	"strconv"
	"strings"

	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// Mermaid stroke colours, matching the DOT palette (plan B3). Hex because
// Mermaid styles are CSS.
const (
	mmdSettled    = "#2e7d32"
	mmdPending    = "#b58900"
	mmdConverging = "#1f6feb"
	mmdUnhealthy  = "#d1242f"
	mmdMissing    = "#8b949e"
)

// Mermaid stroke treatments for the exceptional structures.
const (
	mmdHeldDash  = "6 3"
	mmdMissDash  = "2 3"
	mmdCycleWide = "4px"
	mmdNormWide  = "1px"
)

// Mermaid writes the graph as Mermaid flowchart source (plan B3,
// `-o mermaid`).
//
// The encoding is the DOT one restated for a renderer that lives in a pull
// request or a runbook: colour is state, a hexagon is a gate, a dashed
// border is a hold, a thick border is a cycle, and the dashed red links are
// the engine's blocked-by attribution. Node ids are positional (n0, n1, …)
// because Mermaid ids may not contain the slashes a node reference is made
// of; the label carries the real name.
func Mermaid(w io.Writer, s *snapshot.Snapshot) error {
	model := newGraphModel(s)
	out := newSink(w)

	ids := make(map[adapter.NodeRef]string, len(model.order))
	for i, ref := range model.order {
		ids[ref] = "n" + strconv.Itoa(i)
	}

	out.line("graph LR")
	for _, ref := range model.order {
		out.printf("  %s%s\n", ids[ref], mermaidShape(model, ref))
	}

	// Link indices are positional in Mermaid, so the dependency edges are
	// emitted first as one block and the attribution edges after it: the
	// linkStyle lines below index into exactly that order.
	edge := 0
	for i := range s.Nodes {
		node := &s.Nodes[i]
		for _, dep := range node.DependsOn {
			out.printf("  %s --> %s\n", ids[dep], ids[node.Ref])
			edge++
		}
	}
	blockedEdges := make([]int, 0, len(s.Nodes))
	for i := range s.Nodes {
		node := &s.Nodes[i]
		if node.Blocked == nil || node.Blocked.Ancestor == nil {
			continue
		}
		blocker := ids[nodeRef(*node.Blocked.Ancestor)]
		if blocker == "" {
			continue
		}
		out.printf("  %s -. %s .-> %s\n", blocker, sanitizeMermaid(node.Blocked.Reason), ids[node.Ref])
		blockedEdges = append(blockedEdges, edge)
		edge++
	}

	for _, ref := range model.order {
		out.printf("  style %s %s\n", ids[ref], mermaidStyle(model, ref))
	}
	for _, index := range blockedEdges {
		out.printf("  linkStyle %d stroke:%s,stroke-width:2px,stroke-dasharray:%s\n",
			index, mmdUnhealthy, mmdMissDash)
	}

	return out.err
}

// mermaidShape renders a node's shape and label: a hexagon for a gate, a
// rectangle for anything that carries a pin.
func mermaidShape(model *graphModel, ref adapter.NodeRef) string {
	node, known := model.nodes[ref]
	label := nodeName(ref)
	if !known {
		return fmt.Sprintf("{{%q}}", label+" — "+missingState)
	}
	label += " — " + node.State
	if node.Role == gateRole {
		return fmt.Sprintf("{{%q}}", label)
	}
	return fmt.Sprintf("[%q]", label)
}

// mermaidStyle renders one node's stroke. A per-node style statement rather
// than a class: state, hold and cycle are independent facts and a node can
// carry all three at once.
func mermaidStyle(model *graphModel, ref adapter.NodeRef) string {
	node, known := model.nodes[ref]

	stroke := mmdMissing
	dash := mmdMissDash
	if known {
		stroke = mermaidColour(node.State)
		dash = ""
		if node.Held {
			dash = mmdHeldDash
		}
	}

	width := mmdNormWide
	if model.inCycle(ref) {
		width = mmdCycleWide
	}

	parts := []string{"stroke:" + stroke, "stroke-width:" + width}
	if dash != "" {
		parts = append(parts, "stroke-dasharray:"+dash)
	}
	return strings.Join(parts, ",")
}

// mermaidColour maps a node state to its stroke colour.
func mermaidColour(state string) string {
	switch engine.State(state) {
	case engine.StateSettled:
		return mmdSettled
	case engine.StatePending, engine.StateAdmissible:
		return mmdPending
	case engine.StateConverging:
		return mmdConverging
	case engine.StateUnhealthy:
		return mmdUnhealthy
	default:
		return mmdMissing
	}
}

// sanitizeMermaid strips the characters that would end an edge label early.
func sanitizeMermaid(text string) string {
	return strings.NewReplacer(".", "", "\"", "", "\n", " ", "\t", " ").Replace(text)
}
