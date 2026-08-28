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

// Package controller hosts the Wavefront reconciler: the control loop that
// wires detection (internal/gitpoll), discovery (internal/adapter), topology
// (internal/graph), the rolling-admission core (internal/engine) and the pin
// mechanism (internal/pin) into one stateless pass (DESIGN §3, §4).
package controller

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/fluxcd/pkg/git"
	"github.com/fluxcd/pkg/runtime/conditions"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/graph"
	"github.com/isometry/wavefront-controller/internal/metrics"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
)

// Event reasons (DESIGN §4.2).
const (
	reasonInitialPin          = "InitialPin"
	reasonPinAdvanced         = "PinAdvanced"
	reasonShadowAdmission     = "ShadowAdmission"
	reasonHoldDetected        = "HoldDetected"
	reasonHoldReleased        = "HoldReleased"
	reasonPinFailed           = "PinFailed"
	reasonUnsupportedRefStyle = "UnsupportedRefStyle"
)

// Condition reasons.
const (
	reasonSucceeded       = "Succeeded"
	reasonFailed          = "ReconciliationFailed"
	reasonValid           = "Valid"
	reasonSelectorOverlap = "SelectorOverlap"
	reasonCyclesDetected  = "CyclesDetected"
)

// wavefront_admissions_total result labels (DESIGN §6).
const (
	resultAdmitted = "admitted"
	resultInitial  = "initial"
	resultShadow   = "shadow"
	resultConflict = "conflict"
)

// unknownManager labels a hold the controller can see the effect of (an SSA
// conflict) but not the owner of.
const unknownManager = "unknown"

// managedOptIn is the only value of pin.ManagedLabel that opts a
// GitRepository into pin management (DESIGN §8.2).
const managedOptIn = "true"

// WavefrontReconciler reconciles a Wavefront object.
//
// Every pass is a full recalculation: discovery, source resolution, graph
// derivation and admissibility are all derived from live cluster state plus
// the poller's observations, never from stored orchestration state
// (DESIGN D9). The only thing status carries forward is the hold ledger, and
// only to edge-trigger events.
type WavefrontReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Recorder  events.EventRecorder
	Adapter   adapter.Adapter
	Strategy  selection.Strategy
	Poller    *gitpoll.Poller
	PinWriter *pin.Writer
	Clock     func() time.Time
	Metrics   *metrics.Instruments

	// pollSets records each Wavefront's contribution to the shared Poller.
	// That Poller is a single fleet-wide runnable whose SetTargets replaces the
	// whole target set and whose Configure is last-writer-wins, so N Wavefronts
	// must be merged — a union of targets, the tightest cadence — rather than
	// overwrite one another. It is bookkeeping for a shared collaborator, not
	// admission state: it is rebuilt from scratch on every pass and pruned
	// against the live Wavefront list.
	pollSetsMu sync.Mutex
	pollSets   map[string]pollSet
}

// pollSet is one Wavefront's contribution to the shared Poller.
type pollSet struct {
	interval           time.Duration
	perHostConcurrency int
	targets            []gitpoll.Target
}

// pass is one reconciliation's derived state, threaded through the numbered
// steps of the flow so that each stays a pure-ish function of what came before.
type pass struct {
	wf *wavefrontv1alpha1.Wavefront

	// discovery (step 3)
	nodes    map[adapter.NodeRef]adapter.Node // selected set plus dependsOn closure
	selected map[adapter.NodeRef]bool
	missing  map[adapter.NodeRef]bool // dependsOn targets that do not exist

	// source resolution (step 4)
	resolved     bool
	inputs       map[adapter.NodeRef]engine.NodeInput
	repos        map[types.NamespacedName]*sourcev1.GitRepository
	nodeBySource map[types.NamespacedName]adapter.NodeRef
	holds        map[types.NamespacedName]string // field-manager holds only (R9)
	targets      []gitpoll.Target

	// observations is the pass's single snapshot of the poller. Strict
	// ordering for co-arriving changes (DESIGN §3.3) is only structural if
	// every node's observation comes from the same sweep: read one source at a
	// time, a sweep landing mid-pass presents a fresh descendant against a
	// stale — and therefore apparently settled — ancestor. Poller.Observations
	// guarantees the snapshot never mixes sweeps.
	observations map[types.NamespacedName]gitpoll.Observation

	// graph and evaluation (steps 2, 6)
	graph *graph.Graph
	eval  engine.Evaluation
	// graphChecked records that the pass reached a graph verdict at all; an
	// abort before that must leave the previous GraphValid condition standing.
	graphChecked bool
	graphValid   bool
	graphReason,
	graphMessage string

	// skipAdmissions suppresses every write for the pass, without suppressing
	// status: selector overlap is a configuration error, not a reason to go
	// blind (DESIGN §4.1).
	skipAdmissions bool
}

// +kubebuilder:rbac:groups=wavefront.as-code.io,resources=wavefronts,verbs=get;list;watch
// +kubebuilder:rbac:groups=wavefront.as-code.io,resources=wavefronts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=gitrepositories,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=kustomize.toolkit.fluxcd.io,resources=kustomizations,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update

// event records one Kubernetes event against obj.
//
// events.EventRecorder.Eventf takes a "related" secondary object (none of
// this controller's events have one) and distinguishes a machine-readable
// "action" from the human-readable "reason". This controller has never
// modelled the two separately — every reason (DESIGN §4.2) is already a
// short, unique, UpperCamelCase identifier — so action mirrors reason here
// rather than inventing a second taxonomy with nothing to distinguish.
func (r *WavefrontReconciler) event(obj runtime.Object, eventtype, reason, messageFmt string, args ...any) {
	r.Recorder.Eventf(obj, nil, eventtype, reason, reason, messageFmt, args...)
}

// Reconcile runs one full evaluation of the fleet and executes the admissions
// it derives.
//
// There is no finalizer by design: deleting a Wavefront releases the fleet
// from management and leaves every pin exactly where it stands (DESIGN D8).
func (r *WavefrontReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	wf := &wavefrontv1alpha1.Wavefront{}
	if err := r.Get(ctx, req.NamespacedName, wf); err != nil {
		if apierrors.IsNotFound(err) {
			r.forgetPollSet(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	before := wf.DeepCopy()
	p := &pass{wf: wf, graphValid: true}

	passErr := r.evaluate(ctx, p)
	if passErr == nil {
		passErr = r.execute(ctx, p)
	}
	r.holdEvents(p)
	r.summarise(p, passErr)

	if err := r.Status().Patch(ctx, wf, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, errors.Join(passErr, fmt.Errorf("patching Wavefront status: %w", err))
	}
	if passErr != nil {
		return ctrl.Result{}, passErr
	}

	// Events drive the loop; the interval is only a safety net (Requeue is
	// deprecated in controller-runtime v0.24, RequeueAfter is not).
	return ctrl.Result{RequeueAfter: pollInterval(wf)}, nil
}

// evaluate performs steps 2–6: discovery, overlap detection, source and role
// resolution, poll-set maintenance, and graph/engine derivation.
func (r *WavefrontReconciler) evaluate(ctx context.Context, p *pass) error {
	if err := r.discover(ctx, p); err != nil {
		return err
	}

	all, overlapping, err := r.detectOverlap(ctx, p)
	if err != nil {
		return err
	}
	if overlapping != "" {
		p.graphChecked = true
		p.graphValid = false
		p.graphReason = reasonSelectorOverlap
		p.graphMessage = fmt.Sprintf("node selector overlaps Wavefront %q; admissions suppressed", overlapping)
		p.skipAdmissions = true
	}

	// One coherent snapshot for the whole pass: every node is evaluated
	// against the same sweep, which is what makes co-arrival ordering
	// structural rather than a race the pass usually wins.
	p.observations = r.Poller.Observations()

	if err := r.resolve(ctx, p); err != nil {
		return err
	}

	r.updatePollSet(p, all)
	r.derive(p)
	return nil
}

// discover implements step 3: the selected node set, closed transitively over
// dependsOn targets that fall outside the selector.
func (r *WavefrontReconciler) discover(ctx context.Context, p *pass) error {
	selector, err := metav1.LabelSelectorAsSelector(&p.wf.Spec.Nodes.Selector)
	if err != nil {
		return fmt.Errorf("invalid node selector: %w", err)
	}

	selected, err := r.Adapter.List(ctx, r.Client, selector)
	if err != nil {
		return err
	}

	p.nodes = make(map[adapter.NodeRef]adapter.Node, len(selected))
	p.selected = make(map[adapter.NodeRef]bool, len(selected))
	p.missing = map[adapter.NodeRef]bool{}

	queue := make([]adapter.NodeRef, 0, len(selected))
	for _, node := range selected {
		p.nodes[node.Ref] = node
		p.selected[node.Ref] = true
		queue = append(queue, node.Ref)
	}

	// Breadth-first until no new refs: an out-of-selector dependency is still
	// a health gate, and its own dependencies gate it in turn (DESIGN §3.2).
	for len(queue) > 0 {
		ref := queue[0]
		queue = queue[1:]

		for _, dep := range p.nodes[ref].DependsOn {
			if _, known := p.nodes[dep]; known || p.missing[dep] {
				continue
			}
			node, found, err := r.Adapter.Get(ctx, r.Client, dep)
			if err != nil {
				return err
			}
			if !found {
				// A dangling dependency is recorded as a permanently unready
				// gate, so descendants block exactly as they do under Flux's
				// own handling of a missing dependsOn target.
				p.missing[dep] = true
				continue
			}
			p.nodes[dep] = node
			queue = append(queue, dep)
		}
	}

	return nil
}

// detectOverlap implements step 2 (DESIGN §4.1): another Wavefront whose
// selector matches any of this one's selected nodes. It returns the full
// Wavefront list too, which step 5 needs to prune the shared poll set.
func (r *WavefrontReconciler) detectOverlap(
	ctx context.Context,
	p *pass,
) (*wavefrontv1alpha1.WavefrontList, string, error) {
	all := &wavefrontv1alpha1.WavefrontList{}
	if err := r.List(ctx, all); err != nil {
		return nil, "", fmt.Errorf("listing Wavefronts: %w", err)
	}

	// Sorted node refs keep the reported overlap stable across passes.
	refs := slices.SortedFunc(maps.Keys(p.selected), compareRefs)

	for i := range all.Items {
		other := &all.Items[i]
		if other.Name == p.wf.Name {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(&other.Spec.Nodes.Selector)
		if err != nil {
			// Another Wavefront's broken selector is its own problem to report.
			continue
		}
		for _, ref := range refs {
			if selector.Matches(labels.Set(p.nodes[ref].Labels)) {
				return all, other.Name, nil
			}
		}
	}

	return all, "", nil
}

// resolve implements step 4: each node's role and, for pinned nodes, the
// GitRepository reading the engine evaluates against.
func (r *WavefrontReconciler) resolve(ctx context.Context, p *pass) error {
	p.inputs = make(map[adapter.NodeRef]engine.NodeInput, len(p.nodes)+len(p.missing))
	p.repos = map[types.NamespacedName]*sourcev1.GitRepository{}
	p.nodeBySource = map[types.NamespacedName]adapter.NodeRef{}
	p.holds = map[types.NamespacedName]string{}
	p.targets = make([]gitpoll.Target, 0, len(p.nodes))

	for _, ref := range slices.SortedFunc(maps.Keys(p.nodes), compareRefs) {
		node := p.nodes[ref]
		input := engine.NodeInput{
			Ref:        ref,
			Role:       engine.RoleGate,
			Ready:      node.Readiness.Ready,
			Failing:    node.Readiness.Failing,
			AppliedSHA: node.Readiness.AppliedSHA,
		}

		state, target, err := r.resolveSource(ctx, p, node)
		if err != nil {
			return err
		}
		if state != nil {
			input.Role, input.Source = engine.RolePinned, state
			p.nodeBySource[state.Source] = ref
			if state.Held {
				p.holds[state.Source] = state.HeldBy
			}
			if target != nil {
				p.targets = append(p.targets, *target)
			}
		}

		p.inputs[ref] = input
	}

	// A dependency that does not exist can never be Ready, so it evaluates as
	// an unhealthy gate rather than being silently omitted.
	for ref := range p.missing {
		p.inputs[ref] = engine.NodeInput{Ref: ref, Role: engine.RoleGate}
	}

	p.resolved = true
	return nil
}

// resolveSource reads one node's GitRepository. It returns a nil SourceState
// for every gate node — an absent or unmanaged source, or a ref style v1
// cannot sequence (DESIGN D10) — which is what the engine requires of a gate.
func (r *WavefrontReconciler) resolveSource(
	ctx context.Context,
	p *pass,
	node adapter.Node,
) (*engine.SourceState, *gitpoll.Target, error) {
	if node.SourceRef == nil {
		return nil, nil, nil
	}

	repo := &sourcev1.GitRepository{}
	if err := r.Get(ctx, *node.SourceRef, repo); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("getting GitRepository %s: %w", node.SourceRef, err)
	}
	p.repos[*node.SourceRef] = repo

	// Pinned iff selected *and* opted in by the catalog's participation label:
	// a dependency dragged in by the closure is a health gate even when its
	// own source is managed by another Wavefront.
	if !p.selected[node.Ref] || repo.Labels[pin.ManagedLabel] != managedOptIn {
		return nil, nil, nil
	}

	trackingRef, err := r.Strategy.TrackingRef(repo.Spec.Reference)
	if err != nil {
		if errors.Is(err, selection.ErrUnsupportedRef) {
			r.event(p.wf, corev1.EventTypeWarning, reasonUnsupportedRefStyle,
				"%s tracks a ref style this version cannot sequence; %s demoted to a gate",
				node.SourceRef, node.Ref)
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("resolving tracking ref of %s: %w", node.SourceRef, err)
	}

	manager, held := pin.Hold(repo)
	state := &engine.SourceState{
		Source:       *node.SourceRef,
		TrackingRef:  trackingRef,
		Pin:          currentPin(repo),
		Held:         held,
		HeldBy:       manager,
		Suspended:    repo.Spec.Suspend,
		ArtifactSHA:  artifactSHA(repo),
		FetchFailing: conditions.IsTrue(repo, sourcev1.FetchFailedCondition),
	}
	// No observation is not a stale observation: the poller deliberately drops
	// one whose plumbing changed, and an unobserved source is handled
	// conservatively by the engine rather than guessed at.
	if observation, ok := p.observations[*node.SourceRef]; ok {
		state.ObservedSHA, state.FirstObserved = observation.SHA, observation.FirstObserved
	}

	target := &gitpoll.Target{
		Source:      *node.SourceRef,
		URL:         repo.Spec.URL,
		TrackingRef: trackingRef,
	}
	if repo.Spec.SecretRef != nil {
		target.SecretRef = &types.NamespacedName{Namespace: repo.Namespace, Name: repo.Spec.SecretRef.Name}
	}

	return state, target, nil
}

// updatePollSet implements step 5. The Poller is shared fleet-wide, so this
// Wavefront's contribution is merged with every other live Wavefront's and the
// sets of deleted Wavefronts are dropped.
func (r *WavefrontReconciler) updatePollSet(p *pass, all *wavefrontv1alpha1.WavefrontList) {
	live := make(map[string]bool, len(all.Items))
	for i := range all.Items {
		live[all.Items[i].Name] = true
	}

	r.pollSetsMu.Lock()
	defer r.pollSetsMu.Unlock()

	if r.pollSets == nil {
		r.pollSets = map[string]pollSet{}
	}
	r.pollSets[p.wf.Name] = pollSet{
		interval:           pollInterval(p.wf),
		perHostConcurrency: perHostConcurrency(p.wf),
		targets:            p.targets,
	}
	maps.DeleteFunc(r.pollSets, func(name string, _ pollSet) bool { return !live[name] })

	r.applyPollSetsLocked()
}

// forgetPollSet drops a deleted Wavefront's contribution to the poll set.
func (r *WavefrontReconciler) forgetPollSet(name string) {
	r.pollSetsMu.Lock()
	defer r.pollSetsMu.Unlock()

	if _, tracked := r.pollSets[name]; !tracked {
		return
	}
	delete(r.pollSets, name)
	r.applyPollSetsLocked()
}

// applyPollSetsLocked pushes the merged poll policy onto the shared Poller.
// Callers must hold pollSetsMu.
func (r *WavefrontReconciler) applyPollSetsLocked() {
	interval, perHost := cadenceOf(r.pollSets)
	r.Poller.Configure(interval, perHost)
	r.Poller.SetTargets(unionOf(r.pollSets))
}

// cadenceOf reconciles N Wavefronts' poll policies onto one shared Poller: the
// tightest interval and the most generous concurrency any of them asked for.
//
// Configure is last-writer-wins and Start only re-reads the interval between
// sweeps, so taking whichever Wavefront happened to reconcile last would let a
// 90s Wavefront stall a co-resident 30s Wavefront's observations for a full
// 90s. Polling faster than a Wavefront asked for is harmless — the interval is
// an upper bound on staleness, not a contract with the git host — whereas
// polling slower breaks the tighter one's stated cadence.
//
// An empty set yields (0, 0), which Configure clamps to the CRD defaults.
func cadenceOf(sets map[string]pollSet) (time.Duration, int) {
	var interval time.Duration
	var perHost int
	for _, set := range sets {
		if interval == 0 || set.interval < interval {
			interval = set.interval
		}
		perHost = max(perHost, set.perHostConcurrency)
	}
	return interval, perHost
}

// unionOf flattens every tracked poll set into one deterministic target slice.
func unionOf(sets map[string]pollSet) []gitpoll.Target {
	union := map[types.NamespacedName]gitpoll.Target{}
	for _, set := range sets {
		for _, target := range set.targets {
			union[target.Source] = target
		}
	}
	return slices.SortedFunc(maps.Values(union), func(a, b gitpoll.Target) int {
		return cmp.Compare(a.Source.String(), b.Source.String())
	})
}

// derive implements step 6: the dependsOn DAG and one full evaluation over it.
func (r *WavefrontReconciler) derive(p *pass) {
	edges := make(map[adapter.NodeRef][]adapter.NodeRef, len(p.nodes)+len(p.missing))
	for ref, node := range p.nodes {
		edges[ref] = node.DependsOn
	}
	for ref := range p.missing {
		edges[ref] = nil
	}

	p.graph = graph.Build(edges)
	p.graphChecked = true

	// A cycle would deadlock Flux itself; it is surfaced rather than admitted
	// into (DESIGN §3.2). Nodes outside the cyclic component keep advancing —
	// the engine excludes only the component itself.
	if cycles := p.graph.Cycles(); len(cycles) > 0 && p.graphValid {
		p.graphValid = false
		p.graphReason = reasonCyclesDetected
		p.graphMessage = "dependsOn cycle: " + strings.Join(refStrings(cycles[0]), " -> ")
	}

	p.eval = engine.Evaluate(p.graph, p.inputs)
}

// execute implements step 7: initial pins first, then ancestor-gated
// admissions, in the engine's deterministic order.
func (r *WavefrontReconciler) execute(ctx context.Context, p *pass) error {
	admissions := slices.Concat(p.eval.Initial, p.eval.Admissions)
	if len(admissions) == 0 {
		return nil
	}

	switch {
	case p.skipAdmissions:
		return nil
	case p.wf.Spec.Suspend:
		// The gentle fleet-level brake: writes freeze, visibility persists
		// and no Flux resource is touched (DESIGN §4.1).
		return nil
	case p.wf.Spec.Mode != wavefrontv1alpha1.ModeEnforce:
		// Shadow suppresses every write, initial pins included (DESIGN §3.5.4).
		for _, admission := range admissions {
			r.event(p.wf, corev1.EventTypeNormal, reasonShadowAdmission,
				"would pin %s to %s (from %s, ref %s)",
				admission.Source, admission.To, previousPin(admission), admission.ObservedRef)
			r.Metrics.AdmissionsTotal.WithLabelValues(resultShadow).Inc()
		}
		return nil
	}

	var errs []error
	for _, admission := range admissions {
		if err := r.advance(ctx, p, admission); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// advance performs one pin write. A hold is never forced past: it is detected
// before the write (an SSA apply of a value equal to a hand-pin raises no
// conflict and would silently co-own it, DESIGN §3.5.3) and re-detected from
// any conflict the apply does raise.
func (r *WavefrontReconciler) advance(ctx context.Context, p *pass, admission engine.Admission) error {
	if _, held := p.holds[admission.Source]; held {
		return nil
	}

	err := r.PinWriter.Advance(ctx, admission.Source,
		admission.From, admission.To, admission.ObservedRef, r.Clock())
	switch {
	case err == nil:
		r.pinEvent(p, admission)
		return nil
	case errors.Is(err, pin.ErrHeld):
		p.holds[admission.Source] = r.holderOf(ctx, admission.Source)
		r.Metrics.AdmissionsTotal.WithLabelValues(resultConflict).Inc()
		return nil
	default:
		r.event(p.wf, corev1.EventTypeWarning, reasonPinFailed,
			"failed to pin %s to %s: %s", admission.Source, admission.To, err)
		return err
	}
}

// holderOf re-reads a source to name the field manager that just conflicted.
func (r *WavefrontReconciler) holderOf(ctx context.Context, src types.NamespacedName) string {
	repo := &sourcev1.GitRepository{}
	if err := r.Get(ctx, src, repo); err != nil {
		return unknownManager
	}
	if manager, held := pin.Hold(repo); held {
		return manager
	}
	return unknownManager
}

// pinEvent records a successful advance on the GitRepository (where the
// provenance lives) and mirrors it on the Wavefront (DESIGN §4.2), and
// updates the admissions counter and the observed→admitted wait histogram
// (DESIGN §6, D13).
func (r *WavefrontReconciler) pinEvent(p *pass, admission engine.Admission) {
	result, reason, message := resultAdmitted, reasonPinAdvanced,
		fmt.Sprintf("advanced pin of %s to %s (from %s, ref %s)",
			admission.Source, admission.To, previousPin(admission), admission.ObservedRef)
	if admission.Initial {
		result, reason, message = resultInitial, reasonInitialPin, fmt.Sprintf("initial pin of %s to %s (ref %s)",
			admission.Source, admission.To, admission.ObservedRef)
	}

	if repo, ok := p.repos[admission.Source]; ok {
		r.event(repo, corev1.EventTypeNormal, reason, "%s", message)
	}
	r.event(p.wf, corev1.EventTypeNormal, reason, "%s", message)

	r.Metrics.AdmissionsTotal.WithLabelValues(result).Inc()
	if since := admission.PendingSince; !since.IsZero() {
		r.Metrics.AdmissionWaitSeconds.Observe(r.Clock().Sub(since).Seconds())
	}
}

// holdEvents implements step 8: status.held from the previous pass is the
// ledger the hold transitions are edge-triggered against, which keeps the
// evaluation itself stateless.
func (r *WavefrontReconciler) holdEvents(p *pass) {
	if !p.resolved {
		// An aborted pass proves nothing about holds; claiming release would
		// be a lie the next pass has to undo.
		return
	}

	previous := make(map[string]string, len(p.wf.Status.Held))
	for _, held := range p.wf.Status.Held {
		previous[held.Source] = held.Manager
	}

	current := make(map[string]string, len(p.holds))
	for src, manager := range p.holds {
		current[src.String()] = manager
	}

	for _, src := range slices.Sorted(maps.Keys(current)) {
		if _, was := previous[src]; was {
			continue
		}
		r.event(p.wf, corev1.EventTypeWarning, reasonHoldDetected,
			"pin of %s is held by field manager %q; not advancing", src, current[src])
	}
	for _, src := range slices.Sorted(maps.Keys(previous)) {
		if _, still := current[src]; still {
			continue
		}
		r.event(p.wf, corev1.EventTypeNormal, reasonHoldReleased,
			"hold on %s released by %q", src, previous[src])
	}
}

// summarise implements step 9: fleet counts, capped exceptional-state lists,
// phase and conditions.
//
// An aborted pass republishes conditions only. Overwriting the counts and
// lists with the zero values a failed pass derived would not merely be
// uninformative: phaseOf(0, 0, false) reads Quiescent, so the status would
// actively claim a settled fleet while Ready is False, and status.held — the
// ledger holdEvents edge-triggers against — would be cleared, re-firing
// HoldDetected for every still-held source on the next good pass. The last
// known-good picture stands until a pass can prove a new one.
func (r *WavefrontReconciler) summarise(p *pass, passErr error) {
	if p.resolved {
		r.summariseNodes(p)
	}

	p.wf.Status.ObservedGeneration = p.wf.Generation

	ready := metav1.Condition{
		Type:               wavefrontv1alpha1.ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             reasonSucceeded,
		Message:            fmt.Sprintf("observed %d nodes", p.wf.Status.Nodes.Observed),
		ObservedGeneration: p.wf.Generation,
	}
	if passErr != nil {
		ready.Status, ready.Reason, ready.Message = metav1.ConditionFalse, reasonFailed, passErr.Error()
	}
	apimeta.SetStatusCondition(&p.wf.Status.Conditions, ready)

	// GraphValid is republished only when the pass actually reached a verdict:
	// an abort before derivation must not overwrite a known SelectorOverlap or
	// CyclesDetected with an unproven True.
	if !p.graphChecked {
		return
	}

	graphValid := metav1.Condition{
		Type:               wavefrontv1alpha1.ConditionGraphValid,
		Status:             metav1.ConditionTrue,
		Reason:             reasonValid,
		Message:            "no dependsOn cycles and no selector overlap",
		ObservedGeneration: p.wf.Generation,
	}
	if !p.graphValid {
		graphValid.Status = metav1.ConditionFalse
		graphValid.Reason, graphValid.Message = p.graphReason, p.graphMessage
	}
	apimeta.SetStatusCondition(&p.wf.Status.Conditions, graphValid)
}

// summariseNodes derives the fleet counts, exceptional-state lists and phase
// from a completed evaluation, and recomputes the pin-lag, blocked-nodes and
// pinned-fetch-failures gauges wholesale (DESIGN §6): retire this Wavefront's
// series, then set, so a node that dropped out of the fleet since the last
// pass does not linger.
//
// The retirement is DeletePartialMatch on this Wavefront's own label, never
// Reset(): Wavefronts are cluster-scoped and several may be co-resident, and
// a Reset would erase a *sibling's* pin-lag series until its next pass — and
// pin staleness is a D4 safety alarm that must not blink out.
func (r *WavefrontReconciler) summariseNodes(p *pass) {
	status := &p.wf.Status
	mine := prometheus.Labels{metrics.LabelWavefront: p.wf.Name}

	r.Metrics.PinLagSeconds.DeletePartialMatch(mine)
	r.Metrics.BlockedNodes.DeletePartialMatch(mine)

	counts := wavefrontv1alpha1.NodeCounts{}
	fetchFailures := 0
	for _, input := range p.inputs {
		counts.Observed++
		if input.Role != engine.RolePinned {
			counts.Gates++
			continue
		}
		counts.Pinned++
		if input.Source != nil && input.Source.FetchFailing {
			fetchFailures++
		}
	}
	r.Metrics.PinnedFetchFailures.With(mine).Set(float64(fetchFailures))

	blockedByReason := map[engine.BlockedReason]int{}
	blocked := make([]wavefrontv1alpha1.BlockedNode, 0, len(p.eval.Nodes))
	stalled := false
	for ref, result := range p.eval.Nodes {
		switch result.State {
		case engine.StatePending, engine.StateAdmissible:
			counts.Pending++
		case engine.StateConverging:
			counts.Converging++
		}
		if !result.PendingSince.IsZero() {
			r.Metrics.PinLagSeconds.WithLabelValues(p.wf.Name, ref.Kind, ref.Namespace, ref.Name).
				Set(r.Clock().Sub(result.PendingSince).Seconds())
		}
		if result.Blocked == nil {
			continue
		}
		counts.Blocked++
		blockedByReason[result.Blocked.Reason]++
		stalled = stalled || blocking(result.Blocked.Reason)
		blocked = append(blocked, blockedNode(ref, result, r.Clock))
	}
	for reason, count := range blockedByReason {
		r.Metrics.BlockedNodes.WithLabelValues(p.wf.Name, string(reason)).Set(float64(count))
	}
	counts.Held = len(p.holds)

	slices.SortFunc(blocked, func(a, b wavefrontv1alpha1.BlockedNode) int {
		return cmp.Compare(nodeKey(a.Node), nodeKey(b.Node))
	})

	held := make([]wavefrontv1alpha1.HeldNode, 0, len(p.holds))
	for src, manager := range p.holds {
		held = append(held, wavefrontv1alpha1.HeldNode{
			Node:    nodeReference(p.nodeBySource[src]),
			Source:  src.String(),
			Manager: manager,
		})
	}
	slices.SortFunc(held, func(a, b wavefrontv1alpha1.HeldNode) int {
		return cmp.Compare(a.Source, b.Source)
	})

	status.Nodes = counts
	status.Blocked = capped(blocked)
	status.Held = capped(held)
	status.Phase = phaseOf(counts, len(p.eval.Admissions)+len(p.eval.Initial), stalled)
}

// SetupWithManager wires the controller into the manager. The Flux watches
// carry no GenerationChangedPredicate: gates are reachable through unlabelled
// objects and it is precisely their *status* changes that unblock a wavefront.
func (r *WavefrontReconciler) SetupWithManager(mgr ctrl.Manager, pollEvents <-chan event.GenericEvent) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&wavefrontv1alpha1.Wavefront{}).
		Named("wavefront").
		Watches(&kustomizev1.Kustomization{}, handler.EnqueueRequestsFromMapFunc(r.mapToWavefronts)).
		Watches(&sourcev1.GitRepository{}, handler.EnqueueRequestsFromMapFunc(r.mapToWavefronts)).
		WatchesRawSource(source.Channel(pollEvents, &handler.EnqueueRequestForObject{})).
		Complete(r)
}

// mapToWavefronts enqueues every Wavefront. Fleet counts are ~1, so there is
// nothing to gain by filtering and a gate to lose by it.
func (r *WavefrontReconciler) mapToWavefronts(ctx context.Context, _ client.Object) []reconcile.Request {
	var list wavefrontv1alpha1.WavefrontList
	if err := r.List(ctx, &list); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list Wavefronts for watch mapping")
		return nil
	}

	requests := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: list.Items[i].Name},
		})
	}
	return requests
}

// phaseOf summarises the fleet (DESIGN §4.1).
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
	default:
		return false
	}
}

func blockedNode(
	ref adapter.NodeRef,
	result engine.NodeResult,
	now func() time.Time,
) wavefrontv1alpha1.BlockedNode {
	// status.blocked[].since is a required date-time: a zero PendingSince would
	// serialise as null and have the whole status update rejected. Every
	// pending node has an observation and therefore a first-observed time, so
	// this only ever guards against an unforeseen combination.
	since := result.PendingSince
	if since.IsZero() {
		since = now()
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

func nodeReference(ref adapter.NodeRef) wavefrontv1alpha1.NodeReference {
	return wavefrontv1alpha1.NodeReference{Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
}

func nodeKey(ref wavefrontv1alpha1.NodeReference) string {
	return fmt.Sprintf("%s/%s/%s", ref.Kind, ref.Namespace, ref.Name)
}

// capped bounds an exceptional-state list; the counts stay authoritative
// (DESIGN §4.1).
func capped[T any](list []T) []T {
	if len(list) > wavefrontv1alpha1.StatusListCap {
		return list[:wavefrontv1alpha1.StatusListCap]
	}
	if len(list) == 0 {
		return nil
	}
	return list
}

func compareRefs(a, b adapter.NodeRef) int {
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

// previousPin renders an admission's outgoing pin for human-readable events.
func previousPin(admission engine.Admission) string {
	if admission.From == "" {
		return "none"
	}
	return admission.From
}

// pollInterval is the reconcile safety net, defaulted defensively: a typed
// client can send an explicit zero past CRD defaulting, and zero must never
// mean "requeue immediately, forever".
func pollInterval(wf *wavefrontv1alpha1.Wavefront) time.Duration {
	if wf.Spec.Poll.Interval.Duration <= 0 {
		return gitpoll.DefaultInterval
	}
	return wf.Spec.Poll.Interval.Duration
}

// perHostConcurrency is defaulted on the same defensive grounds as
// pollInterval, so that merged cadences compare like with like.
func perHostConcurrency(wf *wavefrontv1alpha1.Wavefront) int {
	if wf.Spec.Poll.PerHostConcurrency <= 0 {
		return gitpoll.DefaultPerHostConcurrency
	}
	return wf.Spec.Poll.PerHostConcurrency
}
