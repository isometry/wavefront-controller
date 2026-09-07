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
	"sync"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"github.com/isometry/wavefront-controller/internal/inputs"
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

// WavefrontReconciler reconciles a Wavefront object.
//
// Every pass is a full recalculation: discovery, source resolution, graph
// derivation and admissibility are all derived from live cluster state plus
// the poller's observations, never from stored orchestration state
// (DESIGN D9). The only state status carries forward is edge-trigger
// ledgers, each diffed against the exact capped, sorted mirror it itself
// holds (decision D-C): the hold ledger (status.Held, against the pass's
// derived holds) and the shadow-admission ledger (status.Shadow, against
// this pass's admissions, decision D-H), never against the unbounded
// live-derived set, so a restart replays at most StatusListCap detections
// rather than an unbounded backlog.
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

// pass is one reconciliation's derived state: the read-only derivation
// (internal/inputs) plus the object it is published onto. Everything the
// numbered steps below need is in res; nothing else about the pass is
// remembered.
type pass struct {
	wf  *wavefrontv1alpha1.Wavefront
	res *inputs.Result
}

// resolved reports whether the pass got far enough to have proven anything
// about the fleet. A nil res is an abort before Build even ran.
func (p *pass) resolved() bool {
	return p.res != nil && p.res.Resolved
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
			r.Metrics.Wavefront(req.Name).Forget()
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	before := wf.DeepCopy()
	p := &pass{wf: wf}

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
//
// The derivation itself lives in internal/inputs, which is pure and read-only:
// everything the reconciler adds — events, poll-set maintenance, writes,
// status, metrics — happens out here, so the same reads produce the same
// picture for a CLI that has none of them.
func (r *WavefrontReconciler) evaluate(ctx context.Context, p *pass) error {
	// One coherent snapshot for the whole pass, taken before updatePollSet's
	// SetTargets prunes anything: every node is evaluated against the same
	// sweep, which is what makes co-arrival ordering structural rather than a
	// race the pass usually wins (DESIGN §3.3).
	res, err := inputs.Build(ctx, r.Client, inputs.Params{
		Wavefront:    p.wf,
		Adapter:      r.Adapter,
		Strategy:     r.Strategy,
		Observations: r.Poller.Observations(),
	})
	p.res = res

	// Recorded once per source by Build (memoized), announced once here: a
	// pure derivation cannot emit events, and an aborted pass has still
	// proven this much about whatever it did resolve.
	for _, src := range res.UnsupportedSources {
		r.event(p.wf, corev1.EventTypeWarning, reasonUnsupportedRefStyle,
			"%s tracks a ref style this version cannot sequence; every referencing node demoted to a gate", src)
	}

	if err != nil {
		return err
	}

	r.updatePollSet(p.wf, res.Targets, res.Wavefronts)
	return nil
}

// updatePollSet implements step 5. The Poller is shared fleet-wide, so this
// Wavefront's contribution is merged with every other live Wavefront's and the
// sets of deleted Wavefronts are dropped.
func (r *WavefrontReconciler) updatePollSet(
	wf *wavefrontv1alpha1.Wavefront,
	targets []gitpoll.Target,
	all *wavefrontv1alpha1.WavefrontList,
) {
	live := make(map[string]bool, len(all.Items))
	for i := range all.Items {
		live[all.Items[i].Name] = true
	}

	r.pollSetsMu.Lock()
	defer r.pollSetsMu.Unlock()

	if r.pollSets == nil {
		r.pollSets = map[string]pollSet{}
	}
	r.pollSets[wf.Name] = pollSet{
		interval:           pollInterval(wf),
		perHostConcurrency: perHostConcurrency(wf),
		targets:            targets,
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

// execute implements step 7: initial pins first, then ancestor-gated
// admissions, in the engine's deterministic order. No per-source dedup is
// needed here: the engine emits at most one admission per Source across
// Admissions and Initial combined (WP2, gateSharedSources).
func (r *WavefrontReconciler) execute(ctx context.Context, p *pass) error {
	admissions := slices.Concat(p.res.Eval.Initial, p.res.Eval.Admissions)

	// item 5: the old len(admissions)==0 shortcut ran before skipAdmissions,
	// Suspend and the mode switch alike, so it would also have skipped an
	// Enforce-mode ledger clear on a pass with nothing to admit. Both
	// status.Shadow writes below are therefore unconditioned on admissions
	// being non-empty and live inside their own branch instead: Shadow's
	// shadowAdmissions call recomputes (and, on an empty pass, shrinks) the
	// ledger from this pass's admissions like every other full recomputation
	// in this reconciler (DESIGN D9); Enforce's clear fires on the mode
	// switch itself, admissions or not. skipAdmissions and Suspend, below,
	// return before either branch, leaving the ledger exactly as they found
	// it (DESIGN §4.1).
	switch {
	case p.res.SkipAdmissions():
		return nil
	case p.wf.Spec.Suspend:
		// The gentle fleet-level brake: writes freeze, visibility persists
		// and no Flux resource is touched (DESIGN §4.1).
		return nil
	case p.wf.Spec.Mode != wavefrontv1alpha1.ModeEnforce:
		// Shadow suppresses every write, initial pins included (DESIGN §3.5.4).
		r.shadowAdmissions(p, admissions)
		return nil
	}

	// Stale shadow entries must not survive a mode flip (DESIGN §3.5.4): a
	// Wavefront that flips Shadow -> Enforce clears its ledger here, even on
	// a pass with zero admissions.
	p.wf.Status.Shadow = nil

	if len(admissions) == 0 {
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

// shadowAdmissions implements the Shadow branch of execute (finding 9,
// decision D-H): the engine re-derives the identical would-be admission
// every reconcile regardless of mode, so the once-only announcement has to
// be edge-triggered here, against status.Shadow, exactly as holdEvents
// edge-triggers against status.Held (DESIGN §3.5.4, §4.2, §6).
//
// current is computed once — sorted and capped — and used both as the diff's
// current side and as the value written to status.Shadow, so the ledger
// written and the ledger diffed are the same list (mirrors heldSources). A
// pair is diffed on VALUE (Source, To), not key presence: a new To for a
// known Source is a new pair and refires. A pair that drops out of this
// pass's admissions (no longer computed, e.g. the source became held) is
// dropped from the ledger silently — Shadow mode has nothing analogous to
// HoldReleased to announce.
func (r *WavefrontReconciler) shadowAdmissions(p *pass, admissions []engine.Admission) {
	bySource := make(map[string]engine.Admission, len(admissions))
	current := make([]wavefrontv1alpha1.ShadowAdmission, 0, len(admissions))
	for _, admission := range admissions {
		src := admission.Source.String()
		bySource[src] = admission
		current = append(current, wavefrontv1alpha1.ShadowAdmission{Source: src, To: admission.To})
	}
	slices.SortFunc(current, func(a, b wavefrontv1alpha1.ShadowAdmission) int {
		return cmp.Compare(a.Source, b.Source)
	})
	current = inputs.Capped(current)

	currentMap := make(map[string]string, len(current))
	for _, sa := range current {
		currentMap[sa.Source] = sa.To
	}
	previousMap := make(map[string]string, len(p.wf.Status.Shadow))
	for _, sa := range p.wf.Status.Shadow {
		previousMap[sa.Source] = sa.To
	}

	diffLedger(previousMap, currentMap,
		func(src string, to string) {
			admission := bySource[src]
			r.event(p.wf, corev1.EventTypeNormal, reasonShadowAdmission,
				"would pin %s to %s (from %s, ref %s)",
				admission.Source, to, previousPin(admission), admission.ObservedRef)
			r.Metrics.Wavefront(p.wf.Name).CountAdmission(resultShadow)
		},
		func(string, string) {})

	p.wf.Status.Shadow = current
}

// advance performs one pin write. A hold is never forced past: it is detected
// before the write (an SSA apply of a value equal to a hand-pin raises no
// conflict and would silently co-own it, DESIGN §3.5.3) and re-detected from
// any conflict the apply does raise.
func (r *WavefrontReconciler) advance(ctx context.Context, p *pass, admission engine.Admission) error {
	// Defense-in-depth: the engine no longer emits admissions for a held or
	// suspended source at all (finding 7), so this lookup should never match
	// in practice. It stays as the controller's own backstop against that
	// invariant.
	if _, held := p.res.Holds[admission.Source]; held {
		return nil
	}

	err := r.PinWriter.Advance(ctx, admission.Source,
		admission.From, admission.To, admission.ObservedRef, r.Clock())
	switch {
	case err == nil:
		r.pinEvent(p, admission)
		return nil
	case errors.Is(err, pin.ErrHeld):
		p.res.Holds[admission.Source] = inputs.Hold{Manager: r.holderOf(ctx, admission.Source), Kind: inputs.HoldHandPin}
		r.Metrics.Wavefront(p.wf.Name).CountAdmission(resultConflict)
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

	if repo, ok := p.res.Repos[admission.Source]; ok {
		r.event(repo, corev1.EventTypeNormal, reason, "%s", message)
	}
	r.event(p.wf, corev1.EventTypeNormal, reason, "%s", message)

	r.Metrics.Wavefront(p.wf.Name).CountAdmission(result)
	if since := admission.PendingSince; !since.IsZero() {
		r.Metrics.Wavefront(p.wf.Name).ObserveAdmissionWait(r.Clock().Sub(since).Seconds())
	}
}

// diffLedger compares previous against current by value (not mere key
// presence) in sorted key order, so that a caller's onNew/onGone fire in a
// deterministic sequence: onNew for every key added or changed, onGone for
// every key removed or changed. A key whose value is unchanged fires
// neither. Shared with the shadow-admission ledger (WP5).
func diffLedger[V comparable](previous, current map[string]V, onNew, onGone func(key string, v V)) {
	for _, key := range slices.Sorted(maps.Keys(current)) {
		if was, ok := previous[key]; !ok || was != current[key] {
			onNew(key, current[key])
		}
	}
	for _, key := range slices.Sorted(maps.Keys(previous)) {
		if now, ok := current[key]; !ok || now != previous[key] {
			onGone(key, previous[key])
		}
	}
}

// holdEvents implements step 8: status.held from the previous pass is the
// ledger the hold transitions are edge-triggered against, which keeps the
// evaluation itself stateless.
//
// Events mirror the capped ledger (decision D-C), not the unbounded derived
// hold set: current is built by inputs.HeldSources, the exact same capped,
// source-sorted list summariseNodes writes to status.Held. Beyond
// StatusListCap a hold is counted (status.Nodes.Held, DESIGN §4.1) but not
// individually announced until a released slot promotes it into the cap —
// late but exactly once, never on every reconcile.
func (r *WavefrontReconciler) holdEvents(p *pass) {
	if !p.resolved() {
		// An aborted pass proves nothing about holds; claiming release would
		// be a lie the next pass has to undo.
		return
	}

	previous := make(map[string]inputs.Hold, len(p.wf.Status.Held))
	for _, held := range p.wf.Status.Held {
		// An empty Reason is status written by a pre-upgrade controller
		// (field-manager holds only, R9): treat it as HandPin rather than
		// as a spurious kind change against an unchanged hold.
		kind := inputs.HoldKind(held.Reason)
		if kind == "" {
			kind = inputs.HoldHandPin
		}
		previous[held.Source] = inputs.Hold{Manager: held.Manager, Kind: kind}
	}

	held := inputs.HeldSources(p.res)
	current := make(map[string]inputs.Hold, len(held))
	for _, h := range held {
		current[h.Source] = inputs.Hold{Manager: h.Manager, Kind: inputs.HoldKind(h.Reason)}
	}

	// Diffed on value (kind+manager) equality, not key presence: a kind or
	// manager change on the same source (e.g. Suspend -> HandPin) is a
	// release of the old hold and a detect of the new one in the same pass.
	diffLedger(previous, current,
		func(src string, h inputs.Hold) {
			switch h.Kind {
			case inputs.HoldSuspend:
				r.event(p.wf, corev1.EventTypeWarning, reasonHoldDetected,
					"source %s is suspended; not advancing", src)
			default:
				r.event(p.wf, corev1.EventTypeWarning, reasonHoldDetected,
					"pin of %s is held by field manager %q; not advancing", src, h.Manager)
			}
		},
		func(src string, h inputs.Hold) {
			switch h.Kind {
			case inputs.HoldSuspend:
				r.event(p.wf, corev1.EventTypeNormal, reasonHoldReleased,
					"suspension of %s lifted", src)
			default:
				r.event(p.wf, corev1.EventTypeNormal, reasonHoldReleased,
					"hold on %s released by %q", src, h.Manager)
			}
		})
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
// known-good picture stands until a pass can prove a new one. Its gauges,
// unlike status, are live measurements rather than a last-known-good record:
// they are retired instead, per findings #7/#8 (see summariseNodes).
func (r *WavefrontReconciler) summarise(p *pass, passErr error) {
	if p.resolved() {
		r.summariseNodes(p)
		r.stampEvaluated(p.wf)
	} else {
		// An aborted pass can prove nothing about the fleet: its gauges are
		// live measurements, so they go absent rather than freezing at a
		// stale-but-plausible value the D4 alarm would read as healthy.
		// Status below keeps the last known-good picture, as documented.
		r.Metrics.Wavefront(p.wf.Name).Retire()
	}

	p.wf.Status.ObservedGeneration = p.wf.Generation

	ready := metav1.Condition{
		Type:               wavefrontv1alpha1.ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             wavefrontv1alpha1.ReadyReasonSucceeded,
		Message:            fmt.Sprintf("observed %d nodes", p.wf.Status.Nodes.Observed),
		ObservedGeneration: p.wf.Generation,
	}
	if passErr != nil {
		ready.Status, ready.Reason, ready.Message = metav1.ConditionFalse, wavefrontv1alpha1.ReadyReasonReconciliationFailed, passErr.Error()
	}
	apimeta.SetStatusCondition(&p.wf.Status.Conditions, ready)

	// GraphValid is republished only when the pass actually reached a verdict:
	// an abort before derivation must not overwrite a known SelectorOverlap or
	// CyclesDetected with an unproven True.
	if p.res == nil || !p.res.GraphChecked {
		return
	}

	verdict := p.res.GraphVerdict()
	graphValid := metav1.Condition{
		Type:               wavefrontv1alpha1.ConditionGraphValid,
		Status:             metav1.ConditionTrue,
		Reason:             verdict.Reason,
		Message:            verdict.Message,
		ObservedGeneration: p.wf.Generation,
	}
	if !verdict.Valid {
		graphValid.Status = metav1.ConditionFalse
	}
	apimeta.SetStatusCondition(&p.wf.Status.Conditions, graphValid)
}

// stampEvaluated advances status.lastEvaluated at most once per poll interval.
//
// Every pass re-derives the same picture (DESIGN D9), so a per-pass timestamp
// would rewrite status — and wake every watcher of it — on every
// watch-triggered reconcile while nothing about the fleet had changed. The
// interval the user asked to be polled at is exactly the freshness they asked
// for, so it bounds the rewrite rate too. It is written for readers (a CLI
// judging whether status is stale, DESIGN §4.1); the reconciler never reads it
// back for a decision of its own.
func (r *WavefrontReconciler) stampEvaluated(wf *wavefrontv1alpha1.Wavefront) {
	now := r.Clock()
	if last := wf.Status.LastEvaluated; last != nil && now.Sub(last.Time) < pollInterval(wf) {
		return
	}
	stamped := metav1.NewTime(now)
	wf.Status.LastEvaluated = &stamped
}

// summariseNodes publishes one resolved pass: the derived counts, lists,
// members and phase (all of them inputs.Summarise's, so a CLI re-deriving
// from the same reads reports the same numbers), and the pin-lag,
// blocked-nodes and pinned-fetch-failures gauges recomputed wholesale
// (DESIGN §6): retire this Wavefront's series, then set, so a node that
// dropped out of the fleet since the last pass does not linger.
//
// The retirement is DeletePartialMatch on this Wavefront's own label, never
// Reset(): Wavefronts are cluster-scoped and several may be co-resident, and
// a Reset would erase a *sibling's* pin-lag series until its next pass — and
// pin staleness is a D4 safety alarm that must not blink out.
//
// The gauges are published only when the graph verdict is valid: a selector
// overlap or a dependsOn cycle (findings #7/#8) means this pass's counts are
// not authoritative for occupancy — the very node driving them may be
// double-counted against another Wavefront's pass — so publishing them
// would let the fleet gauges lie even while status.Nodes, below, stays live
// (overlap is not an abort; the counts are still the best available picture
// for status, just not for a measurement other Wavefronts' series must not
// double up on).
func (r *WavefrontReconciler) summariseNodes(p *pass) {
	status := &p.wf.Status
	scope := r.Metrics.Wavefront(p.wf.Name)
	scope.Retire()
	publish := p.res.GraphVerdict().Valid // overlap/cycles: status stays live, gauges suppressed

	now := r.Clock()
	summary := inputs.Summarise(p.res, now)

	if publish {
		scope.SetPinnedFetchFailures(summary.FetchFailures)
		for ref, result := range p.res.Eval.Nodes {
			if result.PendingSince.IsZero() {
				continue
			}
			scope.SetPinLag(ref.Kind, ref.Namespace, ref.Name, now.Sub(result.PendingSince).Seconds())
		}
		// The by-reason tallies are uncapped, unlike status.Blocked: a gauge
		// counts every blocked node, not just the ones that fit the list.
		for reason, count := range summary.BlockedByReason {
			scope.SetBlocked(string(reason), count)
		}
	}

	status.Nodes = summary.Counts
	status.Blocked = summary.Blocked
	status.Held = summary.Held
	status.Members = summary.Members
	status.MembersOmitted = summary.MembersOmitted
	status.Phase = summary.Phase
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
