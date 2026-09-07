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

package snapshot

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/inputs"
	"github.com/isometry/wavefront-controller/internal/selection"
)

// DeriveSource re-derives the picture live, through the very pipeline the
// reconciler uses (decision "Seam"): inputs.Build for discovery, resolution,
// graph and evaluation, inputs.Summarise for the numbers.
//
// That is what makes `--derive` two things at once — the answer when the
// controller is down, and the check on it when it is not: a REPORTED column
// that disagrees with a DERIVED one is evidence about the controller, not
// about two different algorithms.
//
// Observations are opt-in. Without Poll (or an injected Observations map) the
// evaluation runs with no observed SHAs at all, which is honest but blind:
// nothing can be pending, so the snapshot says Observed=false and every
// renderer marks the observed columns unknown rather than empty (plan B3).
type DeriveSource struct {
	Reader    client.Reader
	Wavefront string
	// Poll, when set, lists each source's advertised refs before the
	// evaluation, exactly as the controller's poller would.
	Poll *PollOptions
	// Observations, when non-nil, supplies the observation set directly and
	// Poll is not consulted. It is the seam a future --live provider hands the
	// controller's own coherent sweep through, and what a test injects to
	// derive a pending fleet without a git host; a nil map (the default) means
	// no observations at all.
	Observations map[types.NamespacedName]gitpoll.Observation
	// Now is the clock used for CapturedAt, Evaluated and the observation
	// timestamps; nil means time.Now.
	Now func() time.Time
}

var _ Source = (*DeriveSource)(nil)

// Capture implements Source.
func (d *DeriveSource) Capture(ctx context.Context) (*Snapshot, error) {
	wf, err := SelectWavefront(ctx, d.Reader, d.Wavefront)
	if err != nil {
		return nil, err
	}

	now := nowFunc(d.Now)()
	strategy := selection.TrackRef()

	build := func(observations map[types.NamespacedName]gitpoll.Observation) (*inputs.Result, error) {
		return inputs.Build(ctx, d.Reader, inputs.Params{
			Wavefront:    wf,
			Adapter:      adapter.NewKustomizationAdapter(),
			Strategy:     strategy,
			Observations: observations,
		})
	}

	observations, diags, err := d.observe(ctx, build, now)
	if err != nil {
		return nil, err
	}

	res, err := build(observations)
	if err != nil {
		return nil, err
	}

	snap := &Snapshot{
		APIVersion: Version,
		Kind:       KindSnapshot,
		Origin:     OriginDerive,
		CapturedAt: now,
		// A derive snapshot was evaluated exactly when it was captured.
		Evaluated: timePtr(now),
		// Observed only when an observation set was actually obtained: an
		// unobserved derivation is blind, and must say so rather than pass its
		// empty observations off as "nothing pending".
		Observed:  observations != nil,
		Wavefront: wavefrontView(wf),
		Graph: GraphView{
			Cycles:  res.Cycles,
			Unknown: res.Graph.Unknown(),
			Missing: slices.SortedFunc(maps.Keys(res.Missing), compareNodeRefs),
		},
		Derived: derivedStatus(res, now),
	}

	snap.Nodes = deriveNodes(res)
	applyWaves(snap.Nodes)
	snap.Sources = deriveSources(res, strategy)
	snap.Diagnostics = append(diags, structuralDiagnostics(res)...)

	return snap, nil
}

// buildPass is one full inputs.Build against a given observation set.
type buildPass func(map[types.NamespacedName]gitpoll.Observation) (*inputs.Result, error)

// observe resolves the observation set the evaluation runs against: an
// injected one wins, otherwise Poll drives a single sweep, otherwise there are
// none and the derivation is blind by design.
func (d *DeriveSource) observe(
	ctx context.Context,
	build buildPass,
	now time.Time,
) (map[types.NamespacedName]gitpoll.Observation, []string, error) {
	switch {
	case d.Observations != nil:
		return d.Observations, nil, nil
	case d.Poll == nil:
		return nil, nil, nil
	}

	// Only a Build discovers the poll Targets, so polling costs a preliminary
	// pass; its evaluation is discarded, having proved nothing about
	// pending-ness with no observation to compare a pin to.
	prelim, err := build(nil)
	if err != nil {
		return nil, nil, err
	}
	observations, diags := Observe(ctx, d.Reader, prelim.Targets, d.Poll, now)
	return observations, diags, nil
}

// deriveNodes renders every evaluated node. It is deliberately the same
// mapping inputs.Summarise writes into status.members, plus the three fields
// only a live read carries (see NodeView), so StatusSource and DeriveSource
// agree node for node — the parity the envtest suite asserts.
func deriveNodes(res *inputs.Result) []NodeView {
	refs := slices.SortedFunc(maps.Keys(res.Eval.Nodes), compareNodeRefs)

	nodes := make([]NodeView, 0, len(refs))
	for _, ref := range refs {
		input := res.Inputs[ref]
		result := res.Eval.Nodes[ref]

		node := NodeView{
			Ref:          ref,
			Role:         string(input.Role),
			State:        string(result.State),
			Held:         result.Held,
			Blocked:      blockedRef(result.Blocked),
			PendingSince: stampTime(result.PendingSince),
			Ready:        input.Ready,
			Failing:      input.Failing,
			ReadyMessage: res.Nodes[ref].Readiness.Message,
			AppliedSHA:   input.AppliedSHA,
		}
		// Only when there are edges: status.members omits an empty dependsOn
		// entirely, and a nil and an empty slice must not describe the same
		// node differently under the two origins.
		if deps := res.Nodes[ref].DependsOn; len(deps) > 0 {
			node.DependsOn = slices.Clone(deps)
		}
		if src := input.Source; src != nil {
			name := src.Source.String()
			node.Source, node.Pin, node.ObservedSHA = &name, src.Pin, src.ObservedSHA
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// blockedRef converts the engine's attribution to the schema's form — the
// same one status.members carries, so the two origins are comparable.
func blockedRef(blocked *engine.Blocked) *wavefrontv1alpha1.BlockedRef {
	if blocked == nil {
		return nil
	}
	out := &wavefrontv1alpha1.BlockedRef{Reason: string(blocked.Reason)}
	if blocked.Ancestor != (adapter.NodeRef{}) {
		out.Ancestor = &wavefrontv1alpha1.NodeReference{
			Kind:      blocked.Ancestor.Kind,
			Namespace: blocked.Ancestor.Namespace,
			Name:      blocked.Ancestor.Name,
		}
	}
	return out
}

// deriveSources renders one SourceView per managed GitRepository backing a
// pinned node — exactly the set status.members names a source for, so the two
// origins list the same sources.
func deriveSources(res *inputs.Result, strategy selection.Strategy) []SourceView {
	views := make([]SourceView, 0, len(res.NodeBySource))
	for src, refs := range res.NodeBySource {
		view := SourceView{
			Name:  src.String(),
			Nodes: slices.Clone(refs),
		}

		if repo, ok := res.Repos[src]; ok {
			describeRepo(&view, repo, strategy)
		} else {
			// Unreachable: a source only reaches NodeBySource after a
			// successful read, which records it in Repos. Reported rather
			// than asserted, so a future resolution change degrades visibly.
			view.Partial = true
		}
		// The engine's own read of the source last, so pin, tracking ref and
		// observation are exactly the values the evaluation used rather than
		// a second, possibly disagreeing, interpretation of the object.
		if state := sourceState(res, refs); state != nil {
			view.TrackingRef = state.TrackingRef
			view.Pin = state.Pin
			view.Suspended = state.Suspended
			view.ArtifactSHA = state.ArtifactSHA
			view.FetchFailing = state.FetchFailing
			view.ObservedSHA = state.ObservedSHA
			view.FirstObserved = stampTime(state.FirstObserved)
		}
		if hold, held := res.Holds[src]; held {
			view.Hold = &HoldView{Kind: string(hold.Kind), Manager: hold.Manager}
		}

		views = append(views, view)
	}

	slices.SortFunc(views, func(a, b SourceView) int { return cmp.Compare(a.Name, b.Name) })
	return views
}

// sourceState recovers the engine's SourceState for a source from any of its
// referencing nodes; resolution memoizes one state per source, so every
// referencing node points at the same value.
func sourceState(res *inputs.Result, refs []adapter.NodeRef) *engine.SourceState {
	for _, ref := range refs {
		if src := res.Inputs[ref].Source; src != nil {
			return src
		}
	}
	return nil
}

// derivedStatus is what only a live re-derivation proves: inputs.Summarise's
// numbers, the graph verdict, and the admissions this evaluation would
// perform. `wfctl status --derive` prints these against the reported ones.
func derivedStatus(res *inputs.Result, now time.Time) DerivedStatus {
	summary := inputs.Summarise(res, now)
	verdict := res.GraphVerdict()

	byReason := make(map[string]int, len(summary.BlockedByReason))
	for reason, count := range summary.BlockedByReason {
		byReason[string(reason)] = count
	}
	if len(byReason) == 0 {
		byReason = nil
	}

	return DerivedStatus{
		Phase:           summary.Phase,
		Counts:          summary.Counts,
		Blocked:         summary.Blocked,
		Held:            summary.Held,
		BlockedByReason: byReason,
		FetchFailures:   summary.FetchFailures,
		GraphValid:      verdict.Valid,
		GraphReason:     verdict.Reason,
		GraphMessage:    verdict.Message,
		Admissions:      admissionViews(res.Eval.Admissions),
		Initial:         admissionViews(res.Eval.Initial),
	}
}

// admissionViews renders the engine's admissions, which arrive already in
// deterministic node order.
func admissionViews(admissions []engine.Admission) []AdmissionView {
	if len(admissions) == 0 {
		return nil
	}
	views := make([]AdmissionView, 0, len(admissions))
	for _, admission := range admissions {
		views = append(views, AdmissionView{
			Node:         admission.Node,
			Source:       admission.Source.String(),
			From:         admission.From,
			To:           admission.To,
			ObservedRef:  admission.ObservedRef,
			Initial:      admission.Initial,
			PendingSince: stampTime(admission.PendingSince),
		})
	}
	return views
}

// structuralDiagnostics reports what the pass discovered about the
// configuration itself: a selector overlap suppresses admissions fleet-wide,
// and a source whose ref style v1 cannot sequence is silently demoted to a
// gate unless someone says so (DESIGN D10).
func structuralDiagnostics(res *inputs.Result) []string {
	var diags []string
	if res.Overlap != "" {
		diags = append(diags, fmt.Sprintf(
			"node selector overlaps Wavefront %q: admissions are suppressed fleet-wide until the selectors are disjoint",
			res.Overlap))
	}
	for _, src := range res.UnsupportedSources {
		diags = append(diags, fmt.Sprintf(
			"GitRepository %s uses a ref style this version cannot sequence (semver): it is treated as a gate, never pinned",
			src))
	}
	return diags
}

// compareNodeRefs is the one sort key for node refs, matching the
// "Kind/namespace/name" ordering status.members is written in.
func compareNodeRefs(a, b adapter.NodeRef) int {
	return cmp.Compare(a.String(), b.String())
}
