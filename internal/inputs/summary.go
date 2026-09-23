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

package inputs

import (
	"cmp"
	"maps"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
)

// Summary is the whole publishable picture of one resolved pass: everything
// status carries, plus the uncapped by-reason tallies a caller may want for
// gauges. Deriving it here rather than in the reconciler is what lets the CLI
// re-derive byte-identical numbers from the same Result: summarisation is a
// pure function of the already-resolved evaluation, with no hidden state of
// its own to diverge from a live pass.
type Summary struct {
	Counts wavefrontv1alpha1.NodeCounts
	Phase  wavefrontv1alpha1.Phase
	// Blocked and Held are the capped status lists; Held is exactly
	// HeldSources(res): the same capped, source-sorted list that both becomes
	// status.held and is diffed pass-to-pass to edge-trigger the
	// HoldDetected/HoldReleased events, rather than an unbounded internal
	// ledger.
	Blocked []wavefrontv1alpha1.BlockedNode
	Held    []wavefrontv1alpha1.HeldNode
	// Members is every evaluated node's derived state, sorted by
	// kind/namespace/name and capped at MembersCap; MembersOmitted counts the
	// remainder. Write-only output: nothing here or in the reconciler ever
	// reads it back — admissibility is re-derived from live cluster state on
	// every pass, never from a previous status write.
	Members        []wavefrontv1alpha1.Member
	MembersOmitted int
	// BlockedByReason is uncapped, unlike Blocked: a gauge must count every
	// blocked node, not just the ones that fit the status list.
	BlockedByReason map[engine.BlockedReason]int
	FetchFailures   int
}

// Summarise derives the fleet counts, exceptional-state lists, members and
// phase from a completed evaluation. now supplies the fallback timestamp for a
// blocked node with no observation of its own; it is never used to make a
// decision, so a caller's clock choice cannot change what is reported.
//
// Callers must only call this for a resolved Result: an aborted pass has
// derived nothing, and its zero values would claim a settled, empty fleet.
func Summarise(res *Result, now time.Time) Summary {
	summary := Summary{BlockedByReason: map[engine.BlockedReason]int{}}
	if res == nil {
		return summary
	}

	counts := wavefrontv1alpha1.NodeCounts{}
	for _, input := range res.Inputs {
		counts.Observed++
		if input.Role != engine.RolePinned {
			counts.Gates++
			continue
		}
		counts.Pinned++
		if input.Source != nil && input.Source.FetchFailing {
			summary.FetchFailures++
		}
	}

	blocked := make([]wavefrontv1alpha1.BlockedNode, 0, len(res.Eval.Nodes))
	stalled := false
	for ref, result := range res.Eval.Nodes {
		switch result.State {
		case engine.StatePending, engine.StateAdmissible:
			counts.Pending++
		case engine.StateConverging:
			counts.Converging++
		}
		if result.Blocked == nil {
			continue
		}
		counts.Blocked++
		summary.BlockedByReason[result.Blocked.Reason]++
		stalled = stalled || blocking(result.Blocked.Reason)
		blocked = append(blocked, blockedNode(ref, result, now))
	}
	// Holds covers both hand-pins and suspends, matching the engine's own
	// NodeResult.Held semantics — which treats a suspended source as held
	// too — rather than undercounting suspended sources by counting only
	// field-manager hand-pins.
	counts.Held = len(res.Holds)

	slices.SortFunc(blocked, func(a, b wavefrontv1alpha1.BlockedNode) int {
		return cmp.Compare(nodeKey(a.Node), nodeKey(b.Node))
	})

	summary.Counts = counts
	summary.Blocked = Capped(blocked)
	summary.Held = HeldSources(res)
	summary.Members, summary.MembersOmitted = members(res)
	summary.Phase = phaseOf(counts, len(res.Eval.Admissions)+len(res.Eval.Initial), stalled)
	return summary
}

// members renders every evaluated node — pinned and gate alike — as one
// status entry, so that status alone is enough to explain a fleet without
// re-reading the cluster. Beyond MembersCap the tail is dropped
// and counted: the counts, not the list, stay authoritative.
func members(res *Result) ([]wavefrontv1alpha1.Member, int) {
	refs := slices.SortedFunc(maps.Keys(res.Eval.Nodes), compareRefs)

	list := make([]wavefrontv1alpha1.Member, 0, len(refs))
	for _, ref := range refs {
		list = append(list, member(res, ref, res.Eval.Nodes[ref]))
	}

	omitted := 0
	if len(list) > wavefrontv1alpha1.MembersCap {
		omitted = len(list) - wavefrontv1alpha1.MembersCap
		list = list[:wavefrontv1alpha1.MembersCap]
	}
	if len(list) == 0 {
		return nil, 0
	}
	return list, omitted
}

// member renders one node. A gate carries no source, pin or observation:
// those are properties of a managed GitRepository, which by definition a gate
// has none of.
func member(res *Result, ref adapter.NodeRef, result engine.NodeResult) wavefrontv1alpha1.Member {
	input := res.Inputs[ref]

	entry := wavefrontv1alpha1.Member{
		Node:  nodeReference(ref),
		Role:  string(input.Role),
		State: string(result.State),
		Ready: input.Ready,
		Held:  result.Held,
	}

	// dependsOn is carried in status so a reader can reconstruct the graph
	// from status alone; a missing (dangling) node simply has no edges.
	if deps := res.Nodes[ref].DependsOn; len(deps) > 0 {
		entry.DependsOn = make([]wavefrontv1alpha1.NodeReference, 0, len(deps))
		for _, dep := range deps {
			entry.DependsOn = append(entry.DependsOn, nodeReference(dep))
		}
	}

	if src := input.Source; src != nil {
		entry.Source = src.Source.String()
		entry.Pin = src.Pin
		entry.ObservedSHA = src.ObservedSHA
	}

	if !result.PendingSince.IsZero() {
		since := metav1.NewTime(result.PendingSince)
		entry.PendingSince = &since
	}

	if result.Blocked != nil {
		entry.Blocked = &wavefrontv1alpha1.BlockedRef{Reason: string(result.Blocked.Reason)}
		if result.Blocked.Ancestor != (adapter.NodeRef{}) {
			ancestor := nodeReference(result.Blocked.Ancestor)
			entry.Blocked.Ancestor = &ancestor
		}
	}

	return entry
}

// phaseOf derives status.phase — Quiescent, Advancing, or Blocked — from the
// node counts and whether anything blocking is stalling the fleet.
func phaseOf(counts wavefrontv1alpha1.NodeCounts, admissions int, stalled bool) wavefrontv1alpha1.Phase {
	switch {
	case stalled:
		return wavefrontv1alpha1.PhaseBlocked
	case counts.Pending+counts.Converging+admissions > 0:
		return wavefrontv1alpha1.PhaseAdvancing
	default:
		return wavefrontv1alpha1.PhaseQuiescent
	}
}

// blocking reports the reasons that make the fleet Blocked rather than merely
// Advancing: something is wrong, or someone must act.
func blocking(reason engine.BlockedReason) bool {
	switch reason {
	case engine.ReasonAncestorUnhealthy, engine.ReasonAncestorHeld,
		engine.ReasonSelfHeld, engine.ReasonGraphCycle:
		return true
	case engine.ReasonSharedSourceBlocked:
		// The blocking sibling already reports the actionable reason;
		// counting both would double-blame one root cause.
		return false
	default:
		return false
	}
}

func blockedNode(
	ref adapter.NodeRef,
	result engine.NodeResult,
	now time.Time,
) wavefrontv1alpha1.BlockedNode {
	// status.blocked[].since is a required date-time: a zero PendingSince would
	// serialise as null and have the whole status update rejected. Every
	// pending node has an observation and therefore a first-observed time, so
	// this only ever guards against an unforeseen combination.
	since := result.PendingSince
	if since.IsZero() {
		since = now
	}

	entry := wavefrontv1alpha1.BlockedNode{
		Node:   nodeReference(ref),
		Since:  metav1.NewTime(since),
		Reason: string(result.Blocked.Reason),
	}
	if result.Blocked.Ancestor != (adapter.NodeRef{}) {
		ancestor := nodeReference(result.Blocked.Ancestor)
		entry.Ancestor = &ancestor
	}
	return entry
}
