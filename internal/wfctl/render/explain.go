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
	"strings"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// hopIndent is how far each hop of the attribution chain is indented.
const hopIndent = "  "

// Explain walks a node's attribution chain to its root cause and names the
// fix.
//
// The walk is the engine's own attribution read backwards: each blocked node
// names its nearest unsettled ancestor — or, for SharedSourceBlocked, the
// sibling that shares its source — so following those links terminates at
// the one node whose state actually has to change. SelfHeld and GraphCycle
// name no ancestor and end the walk where they are; a visited set makes the
// walk total even against an attribution chain that loops.
func Explain(w io.Writer, s *snapshot.Snapshot, ref adapter.NodeRef, o Options) error {
	node := nodeView(s, ref)
	if node == nil {
		return fmt.Errorf("no node %q in this snapshot: try `wfctl nodes`", nodeName(ref))
	}

	out := newSink(w)
	visited := map[adapter.NodeRef]bool{}
	depth := 0

	for {
		visited[node.Ref] = true
		out.line(strings.Repeat(hopIndent, depth) + chainLine(node, o))
		if out.err != nil {
			return out.err
		}

		next, stop := nextHop(s, node)
		if stop != "" {
			depth++
			out.line(strings.Repeat(hopIndent, depth) + o.Palette.Dim(stop))
		}
		if next == nil {
			break
		}
		if visited[next.Ref] {
			depth++
			out.line(strings.Repeat(hopIndent, depth) +
				o.Palette.Dim(fmt.Sprintf("%s (already explained above; the chain loops here)", nodeName(next.Ref))))
			break
		}
		node = next
		depth++
	}
	if out.err != nil {
		return out.err
	}

	out.blank()
	out.printf("root cause: %s\n", chainLine(node, o))
	for _, detail := range rootCauseDetail(s, node) {
		out.printf("%s%s\n", hopIndent, detail)
	}
	out.printf("fix: %s\n", fixPath(s, node))
	return out.err
}

// chainLine renders one node of the chain: what it is, and who it points at.
func chainLine(node *snapshot.NodeView, o Options) string {
	line := fmt.Sprintf("%s is %s", nodeName(node.Ref), o.Palette.stateCell(node.State, node.Held))
	if node.Held {
		line += o.Palette.Held(" (held)")
	}
	if node.Blocked == nil {
		return line
	}
	line += ": " + sanitize(node.Blocked.Reason)
	if node.Blocked.Ancestor != nil {
		line += fmt.Sprintf(" → %s %s", hopLabel(node.Blocked.Reason), referenceName(node.Blocked.Ancestor))
	}
	return line
}

// hopLabel names what the blocked node is pointing at. SharedSourceBlocked
// is the one reason that names a sibling rather than an ancestor: a shared
// source's pin is one commit, so a node can be blocked by a peer it does not
// depend on at all (the shared-source dedup, gateSharedSources).
func hopLabel(reason string) string {
	if engine.BlockedReason(reason) == engine.ReasonSharedSourceBlocked {
		return "sibling"
	}
	return "ancestor"
}

// nextHop returns the node to walk to, plus a note when the walk stops for a
// reason the reader should be told about.
func nextHop(s *snapshot.Snapshot, node *snapshot.NodeView) (*snapshot.NodeView, string) {
	blocked := node.Blocked
	if blocked == nil {
		return nil, ""
	}

	switch engine.BlockedReason(blocked.Reason) {
	case engine.ReasonSelfHeld:
		// The hold is the root cause; there is no ancestor to blame.
		return nil, ""
	case engine.ReasonGraphCycle:
		// Nothing in a cyclic component is admitted and an ancestor walk
		// inside one would be arbitrary.
		return nil, ""
	}

	if blocked.Ancestor == nil {
		return nil, ""
	}
	ref := nodeRef(*blocked.Ancestor)
	next := nodeView(s, ref)
	if next == nil {
		return nil, fmt.Sprintf("%s is not in this snapshot (missing dependency)", nodeName(ref))
	}
	return next, ""
}

// rootCauseDetail is the evidence behind the verdict: the hold's actor, the
// missing observation, or — under the derive origin, the only origin that
// carries it — the Ready condition's own message.
func rootCauseDetail(s *snapshot.Snapshot, node *snapshot.NodeView) []string {
	details := make([]string, 0, 3)

	if node.Source != nil {
		source := sourceView(s, *node.Source)
		detail := fmt.Sprintf("source: %s", *node.Source)
		if source != nil && source.Hold != nil {
			detail += fmt.Sprintf(", held by %s", holdCell(source.Hold))
		}
		details = append(details, detail)
	}
	if observationUnknown(s, node) {
		details = append(details, "observation: unknown — this ref has no observed SHA in this snapshot")
	}
	if s.Origin == snapshot.OriginDerive && node.ReadyMessage != "" {
		details = append(details, "ready: "+sanitize(node.ReadyMessage))
	}
	return details
}

// fixPath names what to do about the root cause, in the rolling-admission
// rules' own terms.
//
// The order is the order the reasons actually override each other: a cycle
// admits nothing regardless of health, a hold is never advanced regardless
// of what the ref advertises, an unhealthy node's fix is its own next
// admission, and an unobserved ref cannot be admitted because nothing has
// been proposed for it yet.
func fixPath(s *snapshot.Snapshot, node *snapshot.NodeView) string {
	held := node.Held
	var hold *snapshot.HoldView
	if node.Source != nil {
		if source := sourceView(s, *node.Source); source != nil {
			hold = source.Hold
		}
	}

	switch {
	case node.Blocked != nil && engine.BlockedReason(node.Blocked.Reason) == engine.ReasonGraphCycle:
		return "break the dependsOn cycle. Nothing in a cyclic component is ever admitted; " +
			"`wfctl graph` names the cycle."

	case held && hold != nil && hold.Kind == wavefrontv1alpha1.HoldReasonSuspend:
		return fmt.Sprintf(
			"release the hold: lift spec.suspend on %s. The controller then admits normally from the current pin.",
			*node.Source)

	case held:
		return fmt.Sprintf(
			"release the hold on %s%s (`wfctl release %s`). The controller then admits normally from the current pin.",
			sourceOrNode(node), holdActor(hold), sourceOrNode(node))

	case engine.State(node.State) == engine.StateUnhealthy:
		return fmt.Sprintf(
			"fix %s and push to its tracking ref. A fix is not a special case: "+
				"it is that node's next admission and sits at the front of its own subtree, "+
				"so its descendants unblock as it converges.",
			nodeName(node.Ref))

	case observationUnknown(s, node):
		return "wait for the controller's next sweep to observe the ref (spec.poll.interval), " +
			"or re-run with `--derive --poll` to list refs now."

	case engine.State(node.State) == engine.StateSettled:
		return fmt.Sprintf("nothing to do: %s is Settled.", nodeName(node.Ref))

	default:
		return fmt.Sprintf(
			"wait for convergence: %s is %s, and its descendants are admitted once it settles.",
			nodeName(node.Ref), node.State)
	}
}

// sourceOrNode names whatever `release` would be pointed at.
func sourceOrNode(node *snapshot.NodeView) string {
	if node.Source == nil {
		return nodeName(node.Ref)
	}
	return *node.Source
}

// holdActor names the field manager behind a hand-pin, when there is one.
func holdActor(hold *snapshot.HoldView) string {
	if hold == nil || hold.Manager == "" {
		return ""
	}
	return ", held by " + sanitize(hold.Manager)
}
