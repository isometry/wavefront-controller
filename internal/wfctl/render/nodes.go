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
	"strconv"

	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// cycleWave is what a wave of -1 means: no depth is meaningful for a node in
// or behind a cycle, and printing "-1" would invite the reader to believe it
// sits before wave 0.
const cycleWave = "cycle"

// Nodes renders one row per evaluated node (plan B3).
//
// The row is the fleet's per-node truth in the order an operator asks for
// it: what it is, what state it reached, and — when it did not reach
// Settled — who is holding it up. PIN and OBSERVED are short SHAs because
// the question they answer is "are these the same?", which seven characters
// settle.
func Nodes(w io.Writer, s *snapshot.Snapshot, o Options) error {
	header := []string{"NODE", "ROLE", "STATE", "HELD", "BLOCKED", "ANCESTOR", "SOURCE", "PIN", colObserved, "LAG"}
	if o.Wide {
		header = append(header, "WAVE", "READY", "DEPENDS")
	}

	now := o.now()
	table := NewTable(w, header...)
	footnote := false

	for i := range s.Nodes {
		node := &s.Nodes[i]

		blocked, ancestor := absent, absent
		if node.Blocked != nil {
			blocked = node.Blocked.Reason
			ancestor = referenceName(node.Blocked.Ancestor)
		}

		observed, lag := absent, absent
		switch {
		case observationUnknown(s, node):
			observed, lag = unknown, unknown
			footnote = true
		case node.Source != nil:
			observed = shortSHA(node.ObservedSHA)
			lag = pendingAge(s, now, node.PendingSince)
		}

		cells := []string{
			nodeName(node.Ref),
			node.Role,
			o.Palette.stateCell(node.State, node.Held),
			yesNo(node.Held),
			blocked,
			ancestor,
			orAbsent(derefSource(node)),
			shortSHA(node.Pin),
			observed,
			lag,
		}
		if o.Wide {
			cells = append(cells, waveCell(node.Wave), yesNo(node.Ready), joinRefs(node.DependsOn))
		}
		table.Row(cells...)
	}

	if err := table.Flush(); err != nil {
		return err
	}

	if footnote {
		out := newSink(w)
		out.line(o.Palette.Dim(unknownFootnote(s)))
		return out.err
	}
	return nil
}

// derefSource yields a node's source name, or "" for a gate.
func derefSource(node *snapshot.NodeView) string {
	if node.Source == nil {
		return ""
	}
	return *node.Source
}

// waveCell renders a node's dependsOn depth.
func waveCell(wave int) string {
	if wave < 0 {
		return cycleWave
	}
	return strconv.Itoa(wave)
}
