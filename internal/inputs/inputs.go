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

// Package inputs assembles one Wavefront evaluation from live cluster state:
// discovery, selector-overlap detection, source resolution, graph derivation
// and the engine evaluation over them (DESIGN §3, §4).
//
// It is read-only and side-effect free — no writes, no events, no metrics, no
// poller — so the reconciler and the CLI derive the *same* picture from the
// same reads: the reconciler adds execution, status and telemetry on top,
// while the CLI renders the Result directly. Every pass is a full
// recalculation from live inputs (DESIGN D9); nothing here reads back
// previously published status.
package inputs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/fluxcd/pkg/git"
	"github.com/fluxcd/pkg/runtime/conditions"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/graph"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
)

// managedOptIn is the only value of pin.ManagedLabel that opts a
// GitRepository into pin management (DESIGN §8.2).
const managedOptIn = "true"

// Params is everything Build needs beyond the cluster reader.
type Params struct {
	Wavefront *wavefrontv1alpha1.Wavefront
	Adapter   adapter.Adapter
	Strategy  selection.Strategy
	// Observations is the caller's own coherent snapshot of the poller; nil
	// (or a missing entry) means unobserved. Strict ordering for co-arriving
	// changes (DESIGN §3.3) is only structural if every node is evaluated
	// against the same sweep, so the snapshot is taken by the caller — once,
	// before anything reconfigures the poller — never re-read per source here.
	Observations map[types.NamespacedName]gitpoll.Observation
}

// HoldKind distinguishes how a source is held (DESIGN §3.5.3, §10): a foreign
// field manager owning spec.ref.commit, or spec.suspend. Both are reported
// identically by the engine (NodeResult.Held, engine.go SelfHeld/AncestorHeld)
// and must be reported identically by consumers.
type HoldKind string

const (
	HoldHandPin HoldKind = wavefrontv1alpha1.HoldReasonHandPin
	HoldSuspend HoldKind = wavefrontv1alpha1.HoldReasonSuspend
)

// Hold is one source's entry in the unified hold ledger (decision D-B):
// Manager is "" for a Suspend hold, which names no owning actor.
type Hold struct {
	Manager string
	Kind    HoldKind
}

// GraphVerdict is the structural verdict on one pass: the GraphValid condition
// in all but name.
type GraphVerdict struct {
	Valid   bool
	Reason  string
	Message string
}

// Result is one pass's complete read-only derivation.
//
// It is returned non-nil even when Build fails, so a caller can still report
// whatever the pass managed to prove: Resolved and GraphChecked record how far
// it got, and every consumer must respect them rather than mistake a partial
// Result for a proven-empty fleet.
type Result struct {
	// discovery
	Nodes    map[adapter.NodeRef]adapter.Node // selected set plus dependsOn closure
	Selected map[adapter.NodeRef]bool
	Missing  map[adapter.NodeRef]bool // dependsOn targets that do not exist

	// source resolution
	Inputs map[adapter.NodeRef]engine.NodeInput
	Repos  map[types.NamespacedName]*sourcev1.GitRepository
	// NodeBySource lists every selected, pinned node referencing a source,
	// each slice in compareRefs order (decision D-D): a shared source's
	// events and status attribution need every referencing node, not just
	// whichever last overwrote a single value.
	NodeBySource map[types.NamespacedName][]adapter.NodeRef
	// Holds is the unified hold ledger (decision D-B): every source the
	// engine reports Held for, whether a hand-pin (a foreign field manager
	// owns spec.ref.commit) or a suspend (spec.suspend). It feeds
	// counts.Held, status.held[], the HoldDetected/HoldReleased edge-trigger
	// and the admission gate alike. A caller that discovers a further hold
	// while executing (an SSA conflict, pin.ErrHeld) may add to it after
	// Build returns.
	Holds   map[types.NamespacedName]Hold
	Targets []gitpoll.Target
	// UnsupportedSources lists managed sources whose ref style v1 cannot
	// sequence (DESIGN D10), demoted to gates. Recorded once per source and
	// sorted, so a caller can announce each exactly once.
	UnsupportedSources []types.NamespacedName

	// fleet
	Wavefronts *wavefrontv1alpha1.WavefrontList
	Overlap    string // name of the Wavefront whose selector overlaps this one

	// graph and evaluation
	Graph  *graph.Graph
	Cycles [][]adapter.NodeRef
	Eval   engine.Evaluation

	// Resolved and GraphChecked record how far the pass got: an aborted pass
	// has proven nothing about the fleet (Resolved false) and, before
	// derivation, nothing about the graph either (GraphChecked false).
	Resolved     bool
	GraphChecked bool
}

// GraphVerdict renders the structural verdict. A selector overlap outranks a
// cycle: overlap suppresses admissions fleet-wide and is the more urgent
// configuration error to report (DESIGN §4.1).
func (res *Result) GraphVerdict() GraphVerdict {
	switch {
	case res == nil:
		return GraphVerdict{Valid: true, Reason: wavefrontv1alpha1.GraphValidReasonValid, Message: graphValidMessage}
	case res.Overlap != "":
		return GraphVerdict{
			Reason:  wavefrontv1alpha1.GraphValidReasonSelectorOverlap,
			Message: fmt.Sprintf("node selector overlaps Wavefront %q; admissions suppressed", res.Overlap),
		}
	case len(res.Cycles) > 0:
		return GraphVerdict{
			Reason:  wavefrontv1alpha1.GraphValidReasonCyclesDetected,
			Message: "dependsOn cycle: " + strings.Join(refStrings(res.Cycles[0]), " -> "),
		}
	default:
		return GraphVerdict{Valid: true, Reason: wavefrontv1alpha1.GraphValidReasonValid, Message: graphValidMessage}
	}
}

const graphValidMessage = "no dependsOn cycles and no selector overlap"

// SkipAdmissions reports whether every write must be suppressed for the pass,
// without suppressing status: selector overlap is a configuration error, not a
// reason to go blind (DESIGN §4.1).
func (res *Result) SkipAdmissions() bool {
	return res != nil && res.Overlap != ""
}

// Build performs one full read-only pass: discovery, overlap detection, source
// resolution, graph derivation and evaluation.
//
// The Result is always non-nil, populated as far as the pass got, even when an
// error is returned.
func Build(ctx context.Context, r client.Reader, p Params) (*Result, error) {
	b := &builder{reader: r, params: p, res: &Result{}}
	err := b.run(ctx)
	// Sorted whether or not the pass completed, so a caller announcing them
	// does so in the same order every time.
	slices.SortFunc(b.res.UnsupportedSources, compareSources)
	return b.res, err
}

// builder threads one pass's derived state through the numbered steps so that
// each stays a pure-ish function of what came before.
type builder struct {
	reader client.Reader
	params Params
	res    *Result

	// resolvedSources memoizes each GitRepository's resolution (decision
	// D-A): two or more nodes sharing one source (a standard Flux monorepo
	// topology) see one Get, one trackingRef computation and one
	// *engine.SourceState, rather than a separate — and possibly
	// disagreeing — read per referencing node.
	resolvedSources map[types.NamespacedName]*resolvedSource
}

// resolvedSource is one GitRepository's memoized resolution (decision D-A).
// state != nil means the source itself is eligible (managed, resolvable ref
// style) — not that any node was pinned to it: only resolve's caller, gated
// on Selected, decides whether a given referencing node becomes RolePinned.
// state is nil for an absent or unmanaged source, or one whose ref style v1
// cannot sequence, in which case target is also nil.
type resolvedSource struct {
	state  *engine.SourceState
	target *gitpoll.Target
}

func (b *builder) run(ctx context.Context) error {
	if err := b.discover(ctx); err != nil {
		return err
	}
	if err := b.detectOverlap(ctx); err != nil {
		return err
	}
	if b.res.Overlap != "" {
		// A verdict is already proven, even if resolution aborts below.
		b.res.GraphChecked = true
	}
	if err := b.resolve(ctx); err != nil {
		return err
	}
	b.derive()
	return nil
}

// discover implements step 3: the selected node set, closed transitively over
// dependsOn targets that fall outside the selector.
func (b *builder) discover(ctx context.Context) error {
	selector, err := metav1.LabelSelectorAsSelector(&b.params.Wavefront.Spec.Nodes.Selector)
	if err != nil {
		return fmt.Errorf("invalid node selector: %w", err)
	}

	selected, err := b.params.Adapter.List(ctx, b.reader, selector)
	if err != nil {
		return err
	}

	res := b.res
	res.Nodes = make(map[adapter.NodeRef]adapter.Node, len(selected))
	res.Selected = make(map[adapter.NodeRef]bool, len(selected))
	res.Missing = map[adapter.NodeRef]bool{}

	queue := make([]adapter.NodeRef, 0, len(selected))
	for _, node := range selected {
		res.Nodes[node.Ref] = node
		res.Selected[node.Ref] = true
		queue = append(queue, node.Ref)
	}

	// Breadth-first until no new refs: an out-of-selector dependency is still
	// a health gate, and its own dependencies gate it in turn (DESIGN §3.2).
	for len(queue) > 0 {
		ref := queue[0]
		queue = queue[1:]

		for _, dep := range res.Nodes[ref].DependsOn {
			if _, known := res.Nodes[dep]; known || res.Missing[dep] {
				continue
			}
			node, found, err := b.params.Adapter.Get(ctx, b.reader, dep)
			if err != nil {
				return err
			}
			if !found {
				// A dangling dependency is recorded as a permanently unready
				// gate, so descendants block exactly as they do under Flux's
				// own handling of a missing dependsOn target.
				res.Missing[dep] = true
				continue
			}
			res.Nodes[dep] = node
			queue = append(queue, dep)
		}
	}

	return nil
}

// detectOverlap implements step 2 (DESIGN §4.1): another Wavefront whose
// selector matches any of this one's selected nodes. It records the full
// Wavefront list too, which poll-set maintenance needs to prune deleted
// Wavefronts' contributions.
func (b *builder) detectOverlap(ctx context.Context) error {
	all := &wavefrontv1alpha1.WavefrontList{}
	if err := b.reader.List(ctx, all); err != nil {
		return fmt.Errorf("listing Wavefronts: %w", err)
	}
	b.res.Wavefronts = all

	// Sorted node refs keep the reported overlap stable across passes.
	refs := slices.SortedFunc(maps.Keys(b.res.Selected), compareRefs)

	for i := range all.Items {
		other := &all.Items[i]
		if other.Name == b.params.Wavefront.Name {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(&other.Spec.Nodes.Selector)
		if err != nil {
			// Another Wavefront's broken selector is its own problem to report.
			continue
		}
		for _, ref := range refs {
			if selector.Matches(labels.Set(b.res.Nodes[ref].Labels)) {
				b.res.Overlap = other.Name
				return nil
			}
		}
	}

	return nil
}

// resolve implements step 4: each node's role and, for pinned nodes, the
// GitRepository reading the engine evaluates against.
func (b *builder) resolve(ctx context.Context) error {
	res := b.res
	res.Inputs = make(map[adapter.NodeRef]engine.NodeInput, len(res.Nodes)+len(res.Missing))
	res.Repos = map[types.NamespacedName]*sourcev1.GitRepository{}
	res.NodeBySource = map[types.NamespacedName][]adapter.NodeRef{}
	res.Holds = map[types.NamespacedName]Hold{}
	res.Targets = make([]gitpoll.Target, 0, len(res.Nodes))
	b.resolvedSources = map[types.NamespacedName]*resolvedSource{}

	for _, ref := range slices.SortedFunc(maps.Keys(res.Nodes), compareRefs) {
		node := res.Nodes[ref]
		input := engine.NodeInput{
			Ref:        ref,
			Role:       engine.RoleGate,
			Ready:      node.Readiness.Ready,
			Failing:    node.Readiness.Failing,
			AppliedSHA: node.Readiness.AppliedSHA,
		}

		// Resolution (the read, trackingRef, unsupported-ref record and Target
		// registration) is triggered only by a selected node: a dependency
		// dragged in by the closure is a health gate even when it shares a
		// source with a pinned sibling, or when its own source is managed only
		// by another Wavefront — and must never register a poll Target or
		// report UnsupportedRefStyle for a source this Wavefront has no
		// selected interest in (WP2 review finding).
		if node.SourceRef != nil && res.Selected[ref] {
			rs, err := b.resolveSource(ctx, *node.SourceRef)
			if err != nil {
				return err
			}

			if rs.state != nil {
				input.Role, input.Source = engine.RolePinned, rs.state
				// Nodes are walked in compareRefs order above, so each
				// source's slice accumulates already sorted.
				res.NodeBySource[*node.SourceRef] = append(res.NodeBySource[*node.SourceRef], ref)
				// A source can be both hand-pinned and suspended at once;
				// HandPin wins because it names an actor and Suspend does
				// not (decision D-B, finding 7).
				switch {
				case rs.state.Held:
					res.Holds[*node.SourceRef] = Hold{Manager: rs.state.HeldBy, Kind: HoldHandPin}
				case rs.state.Suspended:
					res.Holds[*node.SourceRef] = Hold{Kind: HoldSuspend}
				}
			}
		}

		res.Inputs[ref] = input
	}

	// A dependency that does not exist can never be Ready, so it evaluates as
	// an unhealthy gate rather than being silently omitted.
	for ref := range res.Missing {
		res.Inputs[ref] = engine.NodeInput{Ref: ref, Role: engine.RoleGate}
	}

	res.Resolved = true
	return nil
}

// resolveSource returns src's memoized resolution (decision D-A, WP2). Only
// called for a selected node (the caller's guard): the first selected node
// to reference a GitRepository triggers resolveSourceOnce; every later
// selected referencing node in this pass reuses the result without a second
// read, a second trackingRef resolution, or a second UnsupportedSources
// record. A source referenced only by non-selected (dependency-closure) nodes
// is never resolved at all, and registers no poll Target.
func (b *builder) resolveSource(ctx context.Context, src types.NamespacedName) (*resolvedSource, error) {
	if rs, done := b.resolvedSources[src]; done {
		return rs, nil
	}

	rs, err := b.resolveSourceOnce(ctx, src)
	if err != nil {
		return nil, err
	}
	b.resolvedSources[src] = rs
	return rs, nil
}

// resolveSourceOnce reads one GitRepository. It returns a zero-value
// resolvedSource — nil state, nil target — for every gate source: absent,
// unmanaged, or a ref style v1 cannot sequence (DESIGN D10), which is what
// the engine requires of a gate.
func (b *builder) resolveSourceOnce(ctx context.Context, src types.NamespacedName) (*resolvedSource, error) {
	repo := &sourcev1.GitRepository{}
	if err := b.reader.Get(ctx, src, repo); err != nil {
		if apierrors.IsNotFound(err) {
			return &resolvedSource{}, nil
		}
		return nil, fmt.Errorf("getting GitRepository %s: %w", src, err)
	}
	b.res.Repos[src] = repo

	// Opted in by the catalog's participation label (DESIGN §8.2); the
	// per-node selected check is applied by the caller.
	if repo.Labels[pin.ManagedLabel] != managedOptIn {
		return &resolvedSource{}, nil
	}

	trackingRef, err := b.params.Strategy.TrackingRef(repo.Spec.Reference)
	if err != nil {
		if errors.Is(err, selection.ErrUnsupportedRef) {
			// Recorded rather than announced: this package fires no events.
			// Memoization above keeps it to one record per source, so the
			// caller announces each exactly once.
			b.res.UnsupportedSources = append(b.res.UnsupportedSources, src)
			return &resolvedSource{}, nil
		}
		return nil, fmt.Errorf("resolving tracking ref of %s: %w", src, err)
	}

	manager, held := pin.Hold(repo)
	state := &engine.SourceState{
		Source:       src,
		TrackingRef:  trackingRef,
		Pin:          currentPin(repo),
		Held:         held,
		HeldBy:       manager,
		Suspended:    repo.Spec.Suspend,
		ArtifactSHA:  artifactSHA(repo),
		FetchFailing: conditions.IsTrue(repo, sourcev1.FetchFailedCondition),
	}
	// Params.Observations is the caller's snapshot, taken before anything
	// prunes records whose plumbing no longer matches, so a record for the
	// GitRepository's *previous* URL or tracking ref can still be present
	// here. Presence alone is not enough: verify it against the plumbing just
	// resolved, or a one-pass window lets an edit's old SHA get pinned under
	// the new ref (DESIGN §3.1, "no stale candidate can survive a plumbing
	// change").
	if observation, ok := b.params.Observations[src]; ok &&
		observedCurrentPlumbing(observation, repo.Spec.URL, trackingRef) {
		state.ObservedSHA, state.FirstObserved = observation.SHA, observation.FirstObserved
	}

	target := &gitpoll.Target{
		Source:      src,
		URL:         repo.Spec.URL,
		TrackingRef: trackingRef,
	}
	if repo.Spec.SecretRef != nil {
		target.SecretRef = &types.NamespacedName{Namespace: repo.Namespace, Name: repo.Spec.SecretRef.Name}
	}
	// Once per source (WP2): every other referencing node reuses this same
	// Target via resolvedSources rather than appending a duplicate.
	b.res.Targets = append(b.res.Targets, *target)

	return &resolvedSource{state: state, target: target}, nil
}

// observedCurrentPlumbing reports whether obs was observed against exactly
// the plumbing now in effect: a URL change carries the same one-pass stale
// window as a tracking-ref change, so both are checked (DESIGN §3.1).
func observedCurrentPlumbing(obs gitpoll.Observation, url, trackingRef string) bool {
	return obs.URL == url && obs.TrackingRef == trackingRef
}

// derive implements step 6: the dependsOn DAG and one full evaluation over it.
func (b *builder) derive() {
	res := b.res
	edges := make(map[adapter.NodeRef][]adapter.NodeRef, len(res.Nodes)+len(res.Missing))
	for ref, node := range res.Nodes {
		edges[ref] = node.DependsOn
	}
	for ref := range res.Missing {
		edges[ref] = nil
	}

	res.Graph = graph.Build(edges)
	res.GraphChecked = true

	// A cycle would deadlock Flux itself; it is surfaced rather than admitted
	// into (DESIGN §3.2). Nodes outside the cyclic component keep advancing —
	// the engine excludes only the component itself.
	res.Cycles = res.Graph.Cycles()

	res.Eval = engine.Evaluate(res.Graph, res.Inputs)
}

// HeldSources builds the capped, source-sorted hold ledger (decision D-C).
// It is the ONE list: consumers write exactly this to status.held and
// edge-trigger against exactly this, so the ledger written and the ledger
// diffed are byte-identical. A source beyond StatusListCap is counted in
// status.Nodes.Held but neither listed nor announced until a freed slot
// promotes it into the cap.
func HeldSources(res *Result) []wavefrontv1alpha1.HeldNode {
	if res == nil {
		return nil
	}
	held := make([]wavefrontv1alpha1.HeldNode, 0, len(res.Holds))
	for src, h := range res.Holds {
		// A held source shared by more than one node (WP2) is attributed to
		// the first referencing node in compareRefs order — deterministic,
		// not an arbitrary map read.
		var node wavefrontv1alpha1.NodeReference
		if refs := res.NodeBySource[src]; len(refs) > 0 {
			node = nodeReference(refs[0])
		}
		held = append(held, wavefrontv1alpha1.HeldNode{
			Node:    node,
			Source:  src.String(),
			Manager: h.Manager,
			Reason:  string(h.Kind),
		})
	}
	slices.SortFunc(held, func(a, b wavefrontv1alpha1.HeldNode) int {
		return cmp.Compare(a.Source, b.Source)
	})
	return Capped(held)
}

// Capped bounds an exceptional-state list; the counts stay authoritative
// (DESIGN §4.1).
func Capped[T any](list []T) []T {
	if len(list) > wavefrontv1alpha1.StatusListCap {
		return list[:wavefrontv1alpha1.StatusListCap]
	}
	if len(list) == 0 {
		return nil
	}
	return list
}

func nodeReference(ref adapter.NodeRef) wavefrontv1alpha1.NodeReference {
	return wavefrontv1alpha1.NodeReference{Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
}

// nodeKey renders a NodeReference as the "Kind/namespace/name" sort key the
// status lists are ordered by.
func nodeKey(ref wavefrontv1alpha1.NodeReference) string {
	return fmt.Sprintf("%s/%s/%s", ref.Kind, ref.Namespace, ref.Name)
}

func compareRefs(a, b adapter.NodeRef) int {
	return cmp.Compare(a.String(), b.String())
}

func compareSources(a, b types.NamespacedName) int {
	return cmp.Compare(a.String(), b.String())
}

func refStrings(refs []adapter.NodeRef) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref.String())
	}
	return out
}

// currentPin reads spec.ref.commit, "" when unpinned.
func currentPin(repo *sourcev1.GitRepository) string {
	if repo.Spec.Reference == nil {
		return ""
	}
	return repo.Spec.Reference.Commit
}

// artifactSHA extracts the commit of the last successful reconciliation, which
// is what an initial pin bootstraps from (DESIGN §3.5.4).
func artifactSHA(repo *sourcev1.GitRepository) string {
	if repo.Status.Artifact == nil {
		return ""
	}
	return git.ExtractHashFromRevision(repo.Status.Artifact.Revision).String()
}
