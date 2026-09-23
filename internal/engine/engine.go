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

// Package engine is the rolling-admission correctness core: a pure, stateless
// derivation of every node's state and the admissible set from live inputs
// (pins, observed refs, readiness). No clock, no I/O, no Kubernetes.
// Restart-safe by construction — every evaluation is a full recalculation
// from those live inputs, never from a previous evaluation's output.
package engine

import (
	"cmp"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/graph"
)

// Role is how a node participates in admission.
type Role string

const (
	RolePinned Role = "Pinned" // selected node whose source is a managed GitRepository
	RoleGate   Role = "Gate"   // health-only participant
)

// State is a node's position in the rolling-admission state machine.
type State string

const (
	StateSettled    State = "Settled"
	StatePending    State = "Pending"
	StateAdmissible State = "Admissible"
	StateConverging State = "Converging"
	StateUnhealthy  State = "Unhealthy"
)

// BlockedReason attributes why a pending node was not admitted.
type BlockedReason string

const (
	ReasonAncestorUnhealthy  BlockedReason = "AncestorUnhealthy"
	ReasonAncestorPending    BlockedReason = "AncestorPending"    // pending or converging
	ReasonAncestorHeld       BlockedReason = "AncestorHeld"       // held or suspended
	ReasonAncestorUnobserved BlockedReason = "AncestorUnobserved" // no ref observation yet
	ReasonSelfHeld           BlockedReason = "SelfHeld"
	ReasonGraphCycle         BlockedReason = "GraphCycle"
	// ReasonSharedSourceBlocked marks a node that is itself admissible but
	// shares its GitRepository with a sibling node that is not: a shared
	// source's pin is one commit, so pending-ness is a property of the
	// source, not of any one referencing node, and it advances only when
	// every referencing node is admissible (gateSharedSources).
	ReasonSharedSourceBlocked BlockedReason = "SharedSourceBlocked"
)

// SourceState is the controller's read of one managed GitRepository,
// combined with the poller's observation of its tracking ref.
type SourceState struct {
	Source        types.NamespacedName
	TrackingRef   string // full ref name being tracked, e.g. "refs/heads/main"
	Pin           string // spec.ref.commit ("" = unpinned)
	Held          bool   // commit owned by a foreign field manager
	HeldBy        string
	Suspended     bool      // spec.suspend (human incident action)
	ArtifactSHA   string    // parsed from status.artifact.revision ("" if no artifact)
	FetchFailing  bool      // sourcev1 FetchFailed condition True (metrics only)
	ObservedSHA   string    // latest advertised SHA of TrackingRef ("" = not observed)
	FirstObserved time.Time // when ObservedSHA was first seen (admission_wait, pin lag)
}

// NodeInput is one graph member's evaluation input.
type NodeInput struct {
	Ref        adapter.NodeRef
	Role       Role
	Ready      bool // adapter.Readiness.Ready
	Failing    bool // adapter.Readiness.Failing
	AppliedSHA string
	Source     *SourceState // nil iff Role == RoleGate
}

// Admission is a pin advance the controller should perform.
type Admission struct {
	Node         adapter.NodeRef
	Source       types.NamespacedName
	From         string // previous pin ("" for initial pin)
	To           string // the observed SHA being admitted
	ObservedRef  string
	Initial      bool // true = initial-pin-on-discovery, not ancestor-gated
	PendingSince time.Time
}

// NodeResult is the derived state of one node.
type NodeResult struct {
	State        State
	Held         bool
	Blocked      *Blocked  // set when Pending but not Admissible
	PendingSince time.Time // zero unless pending
}

// Blocked attributes a pending node's non-admission.
type Blocked struct {
	// Ancestor is the nearest unsettled transitive ancestor, or the blocking
	// sibling for SharedSourceBlocked (zero for SelfHeld/GraphCycle).
	Ancestor adapter.NodeRef
	Reason   BlockedReason
}

// Evaluation is one complete derivation over the graph.
type Evaluation struct {
	Nodes      map[adapter.NodeRef]NodeResult
	Admissions []Admission // ancestor-gated pin advances, deterministic order
	Initial    []Admission // initial pins (not ancestor-gated)
}

// Evaluate derives all node states and the admissible set. Pure: same inputs,
// same outputs; restart-safe by construction.
//
// Only nodes present in inputs are evaluated; a graph member without an input
// is never assigned a NodeResult, and — being unproven — counts as unsettled
// wherever it appears as an ancestor.
//
// At most one admission (gated or initial) is ever emitted per Source: two or
// more selected nodes sharing one GitRepository (a standard Flux monorepo
// topology) advance it in lockstep, gated on every referencing node being
// admissible, not on the least-blocked one alone (gateSharedSources).
func Evaluate(g *graph.Graph, inputs map[adapter.NodeRef]NodeInput) Evaluation {
	ev := Evaluation{Nodes: make(map[adapter.NodeRef]NodeResult, len(inputs))}

	// Settledness is a per-node property, so derive it once for the
	// whole evaluation before any ancestor walk consults it.
	settled := make(map[adapter.NodeRef]bool, len(inputs))
	for ref, in := range inputs {
		settled[ref] = isSettled(in)
	}

	for ref, in := range inputs {
		res, admission := evaluateNode(g, ref, in, inputs, settled)
		ev.Nodes[ref] = res
		if admission != nil {
			ev.Admissions = append(ev.Admissions, *admission)
		}
		if initial, ok := initialPin(ref, in); ok {
			ev.Initial = append(ev.Initial, initial)
		}
	}

	gateSharedSources(&ev, inputs)

	// A deterministic order is part of the contract — the reconciler's
	// writes, events, and metrics must not depend on map iteration order.
	slices.SortFunc(ev.Admissions, byNode)
	slices.SortFunc(ev.Initial, byNode)

	return ev
}

// gateSharedSources enforces the shared-GitRepository invariant: two or more
// selected nodes sourcing from the same GitRepository (a standard Flux
// monorepo topology) share one pin, so "pending" is a property of the
// source, not of any one referencing node — either every referencing node is
// a candidate, or none is. Called after the per-node loop, before the
// deterministic sort, so it sees every node's unsorted first-pass result.
func gateSharedSources(ev *Evaluation, inputs map[adapter.NodeRef]NodeInput) {
	bySource := map[types.NamespacedName][]adapter.NodeRef{}
	for ref, in := range inputs {
		if in.Role == RolePinned && in.Source != nil {
			bySource[in.Source.Source] = append(bySource[in.Source.Source], ref)
		}
	}

	drop := map[adapter.NodeRef]bool{}              // admissions to remove from ev.Admissions
	demote := map[adapter.NodeRef]adapter.NodeRef{} // node -> blocking sibling

	for _, refs := range bySource {
		if len(refs) < 2 {
			continue // referenced by exactly one node: untouched
		}
		slices.SortFunc(refs, byNodeRef)

		var blocker adapter.NodeRef
		allAdmissible := true
		for _, ref := range refs {
			if ev.Nodes[ref].State != StateAdmissible {
				allAdmissible = false
				blocker = ref
				break
			}
		}

		if allAdmissible {
			// Every referencing node is a candidate: keep exactly one
			// admission — the first by node order — and leave every
			// NodeResult at StateAdmissible.
			for _, ref := range refs[1:] {
				drop[ref] = true
			}
			continue
		}

		// Not every referencing node is admissible: none may advance. Any
		// node that WAS admissible loses its admission and is demoted,
		// attributed to the blocking sibling rather than re-walking ancestors.
		for _, ref := range refs {
			if ev.Nodes[ref].State == StateAdmissible {
				drop[ref] = true
				demote[ref] = blocker
			}
		}
	}

	if len(drop) > 0 {
		ev.Admissions = slices.DeleteFunc(ev.Admissions, func(a Admission) bool { return drop[a.Node] })
	}
	for ref, blocker := range demote {
		res := ev.Nodes[ref]
		res.State = StatePending
		res.Blocked = &Blocked{Ancestor: blocker, Reason: ReasonSharedSourceBlocked}
		ev.Nodes[ref] = res
	}

	// initialPin is source-derived and (Task 1) refuses held/suspended
	// sources for every candidate alike, so every node referencing an
	// unpinned shared source produces an identical Initial admission: the
	// all-nodes gate above is trivially satisfied and only deduping to one
	// per Source is needed. Skipped when there is nothing to dedupe, sparing
	// the sort Evaluate's own rule-8 pass repeats regardless.
	if len(ev.Initial) > 1 {
		ev.Initial = dedupeBySource(ev.Initial)
	}
}

// dedupeBySource keeps the first admission (by node order) per Source,
// discarding the rest. Sorted internally so the result does not depend on
// the caller's (map-iteration-derived) order.
func dedupeBySource(admissions []Admission) []Admission {
	slices.SortFunc(admissions, byNode)
	seen := make(map[types.NamespacedName]bool, len(admissions))
	kept := admissions[:0]
	for _, a := range admissions {
		if seen[a.Source] {
			continue
		}
		seen[a.Source] = true
		kept = append(kept, a)
	}
	return kept
}

// byNodeRef orders NodeRefs the same way byNode orders their Admissions.
func byNodeRef(a, b adapter.NodeRef) int {
	return cmp.Compare(a.String(), b.String())
}

// evaluateNode assigns one node's state and, when it is admissible,
// the pin advance to emit.
func evaluateNode(
	g *graph.Graph,
	ref adapter.NodeRef,
	in NodeInput,
	inputs map[adapter.NodeRef]NodeInput,
	settled map[adapter.NodeRef]bool,
) (NodeResult, *Admission) {
	src := in.Source

	// A gate (or a pinned node whose source could not be resolved) is a
	// health-only participant: settled iff Ready, unhealthy otherwise.
	if in.Role == RoleGate || src == nil {
		if in.Ready {
			return NodeResult{State: StateSettled}, nil
		}
		return NodeResult{State: StateUnhealthy}, nil
	}

	// Held and Suspended are both external holds: reported identically and
	// never advanced.
	res := NodeResult{Held: src.Held || src.Suspended}

	if !isPending(in) {
		switch {
		case in.Failing:
			res.State = StateUnhealthy
		case settled[ref]:
			res.State = StateSettled
		default:
			res.State = StateConverging
		}
		return res, nil
	}

	// Admissible: pending ∧ ¬held ∧ ¬suspended ∧ ¬cycle ∧ every transitive
	// ancestor settled.
	res.PendingSince = src.FirstObserved
	inCycle := g.InCycle(ref)
	if !res.Held && !inCycle && ancestorsSettled(g, ref, settled) {
		res.State = StateAdmissible
		return res, &Admission{
			Node:         ref,
			Source:       src.Source,
			From:         src.Pin,
			To:           src.ObservedSHA,
			ObservedRef:  src.TrackingRef,
			PendingSince: src.FirstObserved,
		}
	}
	res.State = StatePending
	res.Blocked = attribute(g, ref, inputs, settled, res.Held, inCycle)
	return res, nil
}

// isSettled is the load-bearing predicate for ancestor-gating: nothing
// pending, and Ready at the node's own pin.
func isSettled(in NodeInput) bool {
	src := in.Source
	if in.Role == RoleGate || src == nil {
		return in.Ready
	}
	if src.Held || src.Suspended {
		// A held node is settled only while nothing is pending on its ref (an
		// unobserved ref cannot contradict that, so it does not block) and its
		// workload has actually converged to the pin ("Ready at that revision").
		// The src.Pin == "" escape is deliberate: with initialPin (above) also
		// refusing a held/suspended source, a
		// suspended never-pinned source can never acquire a pin while
		// suspended; requiring AppliedSHA == Pin unconditionally would leave a
		// Ready, quiescent, suspended-unpinned node permanently unsettled and
		// livelock all descendants.
		return in.Ready &&
			(src.ObservedSHA == "" || src.ObservedSHA == src.Pin) &&
			(src.Pin == "" || in.AppliedSHA == src.Pin)
	}
	// An unpinned or unobserved pinned node cannot prove quiescence,
	// so it is conservatively unsettled (ObservedSHA "" never equals a pin).
	return src.Pin != "" && src.ObservedSHA == src.Pin && in.Ready && in.AppliedSHA == src.Pin
}

// isPending reports an advertised SHA that differs from the current pin.
// An unpinned source is not pending — it is a
// candidate for an initial pin instead.
func isPending(in NodeInput) bool {
	src := in.Source
	return in.Role == RolePinned && src != nil &&
		src.Pin != "" && src.ObservedSHA != "" && src.ObservedSHA != src.Pin
}

// initialPin derives the ungated initial-pin-on-discovery admission: the
// current artifact's commit, or absent an artifact the first observed
// SHA. With neither, there is nothing safe to pin yet and the node waits for
// its first observation. A held or suspended source is an external hold —
// the controller treats it as such and does not advance it — and never
// receives an initial pin either.
func initialPin(ref adapter.NodeRef, in NodeInput) (Admission, bool) {
	src := in.Source
	if in.Role != RolePinned || src == nil || src.Pin != "" || src.Held || src.Suspended {
		return Admission{}, false
	}
	to := cmp.Or(src.ArtifactSHA, src.ObservedSHA)
	if to == "" {
		return Admission{}, false
	}
	return Admission{
		Node:         ref,
		Source:       src.Source,
		To:           to,
		ObservedRef:  src.TrackingRef,
		Initial:      true,
		PendingSince: src.FirstObserved,
	}, true
}

// ancestorsSettled reports whether every transitive dependsOn ancestor is
// settled — transitive, not merely direct, so an unhealthy node blocks its
// whole descendant subtree even through quiescent intermediates.
func ancestorsSettled(g *graph.Graph, ref adapter.NodeRef, settled map[adapter.NodeRef]bool) bool {
	for _, ancestor := range g.TransitiveAncestors(ref) {
		// A node inside a cycle lists itself among its ancestors; such nodes
		// are excluded from admission by InCycle, so ignore the self-edge.
		if ancestor == ref {
			continue
		}
		if !settled[ancestor] {
			return false
		}
	}
	return true
}

// attribute explains a pending node's non-admission.
func attribute(
	g *graph.Graph,
	ref adapter.NodeRef,
	inputs map[adapter.NodeRef]NodeInput,
	settled map[adapter.NodeRef]bool,
	held, inCycle bool,
) *Blocked {
	switch {
	case held:
		return &Blocked{Reason: ReasonSelfHeld}
	case inCycle:
		// Nothing in a cyclic component is admitted; an ancestor walk there
		// would be arbitrary.
		return &Blocked{Reason: ReasonGraphCycle}
	}
	ancestor, found := nearestUnsettled(g, ref, settled)
	if !found {
		// Unreachable: not held, not cyclic and no unsettled ancestor is
		// exactly admissibility. Reported as unattributed rather than guessed.
		return nil
	}
	in, known := inputs[ancestor]
	return &Blocked{Ancestor: ancestor, Reason: ancestorReason(in, known)}
}

// nearestUnsettled walks dependsOn edges breadth-first from ref, returning the
// closest unsettled ancestor. Breadth-first is what makes the attribution the
// *nearest* one; the graph's sorted edge lists keep ties deterministic.
func nearestUnsettled(g *graph.Graph, ref adapter.NodeRef, settled map[adapter.NodeRef]bool) (adapter.NodeRef, bool) {
	visited := map[adapter.NodeRef]bool{ref: true}
	var queue []adapter.NodeRef
	enqueue := func(refs []adapter.NodeRef) {
		for _, next := range refs {
			if !visited[next] {
				visited[next] = true
				queue = append(queue, next)
			}
		}
	}

	enqueue(g.DependsOn(ref))
	for len(queue) > 0 {
		ancestor := queue[0]
		queue = queue[1:]
		if !settled[ancestor] {
			return ancestor, true
		}
		enqueue(g.DependsOn(ancestor))
	}
	return adapter.NodeRef{}, false
}

// ancestorReason derives a blocked reason from the unsettled ancestor itself,
// most-actionable first.
func ancestorReason(in NodeInput, known bool) BlockedReason {
	src := in.Source
	switch {
	case !known:
		// A graph member the controller has no reading for: unproven, so
		// treated as merely not yet quiescent.
		return ReasonAncestorPending
	case in.Failing:
		return ReasonAncestorUnhealthy
	case in.Role == RoleGate || src == nil:
		// A gate is unsettled only by being unready.
		return ReasonAncestorUnhealthy
	case src.Held || src.Suspended:
		return ReasonAncestorHeld
	case src.ObservedSHA == "":
		return ReasonAncestorUnobserved
	default:
		return ReasonAncestorPending
	}
}

func byNode(a, b Admission) int {
	return cmp.Compare(a.Node.String(), b.Node.String())
}
