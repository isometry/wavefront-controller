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

	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// Tree drawing glyphs.
const (
	treeBranch = "├── "
	treeLast   = "└── "
	treeRail   = "│   "
	treeGap    = "    "
	// shownAbove marks a node already expanded under an earlier parent.
	shownAbove = " ↑ (shown above)"
	// cycleMark labels a node in, or behind, a cycle.
	cycleMark = " [cycle]"
	// missingMark labels a dependsOn target the snapshot has no node for.
	missingMark = " (missing)"
)

// Tree renders the graph as rooted trees, for `wfctl graph --tree`.
//
// Roots are wave 0 — the nodes nothing gates — and each is expanded depth
// first. The DAG is not a tree, so a node with several parents is expanded
// under the first one that reaches it and referred back to everywhere else:
// printing its subtree once per parent would multiply a shared platform
// component across the whole fleet and bury the shape it is meant to show.
func Tree(w io.Writer, s *snapshot.Snapshot, o Options) error {
	model := newGraphModel(s)
	table := newPlainTable(w)
	expanded := map[adapter.NodeRef]bool{}

	var walk func(ref adapter.NodeRef, prefix, branch, childPrefix string)
	walk = func(ref adapter.NodeRef, prefix, branch, childPrefix string) {
		label := prefix + branch + nodeName(ref)

		switch {
		case model.missing(ref):
			// A dangling target still gates everything that depends on it, so
			// its subtree is drawn: that subtree is the blast radius.
			label += o.Palette.Dim(missingMark)
		case model.inCycle(ref):
			// A cycle is reported once — here if a root reached it, in its
			// own group below otherwise. Recursing into one would not
			// terminate, and neither would the reader.
			expanded[ref] = true
			table.Row(label+o.Palette.Warn(cycleMark), model.role(ref), model.state(ref, o))
			return
		case expanded[ref]:
			table.Row(label+o.Palette.Dim(shownAbove), model.role(ref), model.state(ref, o))
			return
		}

		expanded[ref] = true
		table.Row(label, model.role(ref), model.state(ref, o))

		children := model.children[ref]
		for i, child := range children {
			last := i == len(children)-1
			if last {
				walk(child, childPrefix, treeLast, childPrefix+treeGap)
				continue
			}
			walk(child, childPrefix, treeBranch, childPrefix+treeRail)
		}
	}

	roots := make([]adapter.NodeRef, 0, len(model.order))
	for _, ref := range model.order {
		if model.waves[ref] == 0 {
			roots = append(roots, ref)
		}
	}
	for _, root := range roots {
		walk(root, "", "", "")
	}

	if err := table.Flush(); err != nil {
		return err
	}

	// Whatever never layered is unreachable from a wave-0 root by
	// construction, so it gets its own group rather than being silently
	// dropped from the view.
	var unlayered []adapter.NodeRef
	for _, ref := range model.order {
		if model.inCycle(ref) && !expanded[ref] {
			unlayered = append(unlayered, ref)
		}
	}
	if len(unlayered) == 0 {
		return nil
	}

	out := newSink(w)
	out.blank()
	if out.err != nil {
		return out.err
	}
	trailing := newPlainTable(w)
	trailing.Row(unlayeredHeading(s, len(unlayered)))
	for _, ref := range unlayered {
		trailing.Row("  "+nodeName(ref)+o.Palette.Warn(cycleMark), model.role(ref), model.state(ref, o))
	}
	return trailing.Flush()
}
