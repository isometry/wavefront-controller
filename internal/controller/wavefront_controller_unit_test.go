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

package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/metrics"
)

// Unit coverage for the two shared-state hazards the reconciler has to get
// right: what an *aborted* pass may publish, and how N Wavefronts merge onto
// one Poller. Both are about state that outlives a single pass, which the
// envtest scenarios exercise only incidentally.

const (
	fluxNamespace = "flux-system"
	fleetName     = "fleet"
	teamAName     = "team-a"
	alphaSource   = "alpha"
	betaSource    = "beta"

	teamASource  = fluxNamespace + "/" + teamAName
	passFailure  = "listing Kustomizations: connection refused"
	staleOverlap = "node selector overlaps Wavefront \"other\"; admissions suppressed"
)

func teamARef() adapter.NodeRef {
	return adapter.NodeRef{Kind: kindKustomization, Namespace: fluxNamespace, Name: teamAName}
}

func teamAKey() types.NamespacedName {
	return types.NamespacedName{Namespace: fluxNamespace, Name: teamAName}
}

// settledFleet is a Wavefront carrying the status a healthy pass left behind:
// real counts, a blocked entry and a hold ledger.
func settledFleet() *wavefrontv1alpha1.Wavefront {
	return &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{Name: fleetName, Generation: 7},
		Status: wavefrontv1alpha1.WavefrontStatus{
			Phase: wavefrontv1alpha1.PhaseBlocked,
			Nodes: wavefrontv1alpha1.NodeCounts{
				Observed: 12, Pinned: 10, Gates: 2, Pending: 3, Blocked: 1, Held: 1,
			},
			Blocked: []wavefrontv1alpha1.BlockedNode{{
				Node:   wavefrontv1alpha1.NodeReference{Kind: kindKustomization, Namespace: fluxNamespace, Name: "team-b"},
				Since:  metav1.NewTime(time.Unix(1000, 0)),
				Reason: "AncestorUnhealthy",
			}},
			Held: []wavefrontv1alpha1.HeldNode{{
				Node:    wavefrontv1alpha1.NodeReference{Kind: kindKustomization, Namespace: fluxNamespace, Name: teamAName},
				Source:  teamASource,
				Manager: humanManager,
			}},
			ObservedGeneration: 6,
		},
	}
}

func drain(ch chan string) []string {
	var out []string
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return out
		}
	}
}

// TestSummariseAbortedPassPreservesTheFleetPicture: a pass that fails before
// resolution has derived nothing, so it must publish nothing but conditions.
// Writing its zero values through would report nodes:{0,…} and — via
// phaseOf(0,0,false) — a Quiescent fleet alongside Ready:False.
func TestSummariseAbortedPassPreservesTheFleetPicture(t *testing.T) {
	wf := settledFleet()
	before := wf.Status.DeepCopy()

	r := &WavefrontReconciler{Recorder: events.NewFakeRecorder(16), Clock: time.Now, Metrics: metrics.Nop()}
	// resolved and graphChecked both false: the pass aborted in discovery.
	r.summarise(&pass{wf: wf, graphValid: true}, errors.New(passFailure))

	if got := wf.Status.Nodes; got != before.Nodes {
		t.Errorf("counts = %+v, want the previous %+v left untouched", got, before.Nodes)
	}
	if got := wf.Status.Phase; got != wavefrontv1alpha1.PhaseBlocked {
		t.Errorf("phase = %q, want the previous %q (never a derived Quiescent)",
			got, wavefrontv1alpha1.PhaseBlocked)
	}
	if len(wf.Status.Blocked) != 1 {
		t.Errorf("blocked = %+v, want the previous single entry retained", wf.Status.Blocked)
	}
	if len(wf.Status.Held) != 1 || wf.Status.Held[0].Source != teamASource {
		t.Fatalf("held = %+v, want the ledger retained: clearing it re-fires HoldDetected", wf.Status.Held)
	}

	ready := apimeta.FindStatusCondition(wf.Status.Conditions, wavefrontv1alpha1.ConditionReady)
	if ready == nil {
		t.Fatal("Ready condition missing, want the failure surfaced")
	}
	if ready.Status != metav1.ConditionFalse || ready.Reason != reasonFailed {
		t.Errorf("Ready = %s/%s, want False/%s", ready.Status, ready.Reason, reasonFailed)
	}
	if !strings.Contains(ready.Message, "connection refused") {
		t.Errorf("Ready message = %q, want it to carry the pass error", ready.Message)
	}
	if ready.ObservedGeneration != wf.Generation || wf.Status.ObservedGeneration != wf.Generation {
		t.Errorf("observedGeneration = %d/%d, want %d on both the condition and the status",
			ready.ObservedGeneration, wf.Status.ObservedGeneration, wf.Generation)
	}
}

// TestSummariseAbortedPassLeavesGraphValidStanding: an abort before graph
// derivation has disproved nothing, so a known-bad verdict must survive rather
// than be replaced with an unproven True.
func TestSummariseAbortedPassLeavesGraphValidStanding(t *testing.T) {
	wf := settledFleet()
	apimeta.SetStatusCondition(&wf.Status.Conditions, metav1.Condition{
		Type:               wavefrontv1alpha1.ConditionGraphValid,
		Status:             metav1.ConditionFalse,
		Reason:             reasonSelectorOverlap,
		Message:            staleOverlap,
		ObservedGeneration: 6,
	})

	r := &WavefrontReconciler{Recorder: events.NewFakeRecorder(16), Clock: time.Now, Metrics: metrics.Nop()}
	r.summarise(&pass{wf: wf, graphValid: true}, errors.New(passFailure))

	graphValid := apimeta.FindStatusCondition(wf.Status.Conditions, wavefrontv1alpha1.ConditionGraphValid)
	if graphValid == nil {
		t.Fatal("GraphValid condition disappeared")
	}
	if graphValid.Status != metav1.ConditionFalse || graphValid.Reason != reasonSelectorOverlap {
		t.Errorf("GraphValid = %s/%s, want the previous False/%s to stand",
			graphValid.Status, graphValid.Reason, reasonSelectorOverlap)
	}

	// A pass that *did* reach a verdict republishes it.
	r.summarise(&pass{wf: wf, resolved: true, graphChecked: true, graphValid: true}, nil)
	graphValid = apimeta.FindStatusCondition(wf.Status.Conditions, wavefrontv1alpha1.ConditionGraphValid)
	if graphValid.Status != metav1.ConditionTrue {
		t.Errorf("GraphValid = %s, want True once a pass proved it", graphValid.Status)
	}
}

// TestHoldLedgerSurvivesAnAbortedPass is the consequence that makes the guard
// matter: status.held is the ledger holdEvents edge-triggers against, so an
// aborted pass in the middle of a hold must not cause a second HoldDetected.
func TestHoldLedgerSurvivesAnAbortedPass(t *testing.T) {
	recorder := events.NewFakeRecorder(32)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName, Generation: 1}}
	holding := func() *pass {
		return &pass{
			wf:           wf,
			resolved:     true,
			graphChecked: true,
			graphValid:   true,
			holds:        map[types.NamespacedName]string{teamAKey(): humanManager},
			nodeBySource: map[types.NamespacedName]adapter.NodeRef{teamAKey(): teamARef()},
		}
	}

	// Pass 1: the hold appears.
	first := holding()
	r.holdEvents(first)
	r.summarise(first, nil)

	recorded := drain(recorder.Events)
	if len(recorded) != 1 || !strings.Contains(recorded[0], reasonHoldDetected) {
		t.Fatalf("events after the hold appeared = %v, want exactly one %s", recorded, reasonHoldDetected)
	}
	if len(wf.Status.Held) != 1 {
		t.Fatalf("status.held = %+v, want the ledger written", wf.Status.Held)
	}

	// Pass 2: an abort mid-hold. No events, and the ledger must survive it.
	aborted := &pass{wf: wf, graphValid: true}
	r.holdEvents(aborted)
	r.summarise(aborted, errors.New(passFailure))

	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("events from an aborted pass = %v, want none: it proved nothing about holds", recorded)
	}
	if len(wf.Status.Held) != 1 || wf.Status.Held[0].Manager != humanManager {
		t.Fatalf("status.held = %+v, want the ledger preserved across the abort", wf.Status.Held)
	}

	// Pass 3: recovery, same hold still in place. The transition already fired.
	third := holding()
	r.holdEvents(third)
	r.summarise(third, nil)

	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("events on recovery = %v, want none: the hold never transitioned", recorded)
	}

	// Teeth: had the abort wiped the ledger, the very next pass re-fires.
	wf.Status.Held = nil
	fourth := holding()
	r.holdEvents(fourth)
	if recorded := drain(recorder.Events); len(recorded) != 1 {
		t.Errorf("events after a wiped ledger = %v, want the re-fire this guard prevents", recorded)
	}
}

// --- co-resident Wavefronts and the fleet gauges -----------------------------

// gaugePass builds the minimum pass summariseNodes needs to publish the three
// wholesale-recomputed gauges for one Wavefront: a pinned node with a failing
// source, pending since pendingSince, blocked on an unhealthy ancestor.
func gaugePass(wavefront, node string, pendingSince time.Time) *pass {
	ref := adapter.NodeRef{Kind: kindKustomization, Namespace: fluxNamespace, Name: node}
	return &pass{
		wf:           &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: wavefront}},
		resolved:     true,
		graphChecked: true,
		graphValid:   true,
		inputs: map[adapter.NodeRef]engine.NodeInput{
			ref: {Ref: ref, Role: engine.RolePinned, Source: &engine.SourceState{FetchFailing: true}},
		},
		eval: engine.Evaluation{Nodes: map[adapter.NodeRef]engine.NodeResult{
			ref: {
				State:        engine.StatePending,
				PendingSince: pendingSince,
				Blocked:      &engine.Blocked{Reason: engine.ReasonAncestorUnhealthy},
			},
		}},
	}
}

// TestFleetGaugesAreRetiredPerWavefront: the gauges are fleet-global
// collectors recomputed wholesale on every pass, so a per-Wavefront Reset()
// would erase a *co-resident* Wavefront's series until its own next pass —
// and pin staleness is a D4 safety alarm that must never blink out. Every
// series therefore carries the owning Wavefront, and a pass retires only its
// own with DeletePartialMatch.
func TestFleetGaugesAreRetiredPerWavefront(t *testing.T) {
	now := time.Unix(2000, 0)
	instr := metrics.Nop()
	r := &WavefrontReconciler{
		Recorder: events.NewFakeRecorder(16),
		Clock:    func() time.Time { return now },
		Metrics:  instr,
	}

	r.summariseNodes(gaugePass(fleetName, teamAName, now.Add(-60*time.Second)))
	r.summariseNodes(gaugePass("infra", "team-c", now.Add(-30*time.Second)))

	// "infra"'s pass must have left every one of "fleet"'s series standing.
	lag := testutil.ToFloat64(instr.PinLagSeconds.WithLabelValues(
		fleetName, kindKustomization, fluxNamespace, teamAName))
	if lag != 60 {
		t.Errorf("pin lag for %s/%s = %v, want 60: a co-resident pass erased it", fleetName, teamAName, lag)
	}
	blocked := testutil.ToFloat64(instr.BlockedNodes.WithLabelValues(
		fleetName, string(engine.ReasonAncestorUnhealthy)))
	if blocked != 1 {
		t.Errorf("blocked nodes for %s = %v, want 1", fleetName, blocked)
	}
	if fetch := testutil.ToFloat64(instr.PinnedFetchFailures.WithLabelValues(fleetName)); fetch != 1 {
		t.Errorf("pinned fetch failures for %s = %v, want 1", fleetName, fetch)
	}

	// Each Wavefront reports its own, so a fleet total is a PromQL sum().
	if got := testutil.CollectAndCount(instr.PinLagSeconds); got != 2 {
		t.Errorf("pin-lag series = %d, want 2 (one per co-resident Wavefront)", got)
	}
	if lag := testutil.ToFloat64(instr.PinLagSeconds.WithLabelValues(
		"infra", kindKustomization, fluxNamespace, "team-c")); lag != 30 {
		t.Errorf("pin lag for infra/team-c = %v, want 30", lag)
	}

	// Its own series it does retire: team-a drops out of the fleet, and the
	// stale lag must not linger.
	r.summariseNodes(gaugePass(fleetName, "team-b", now.Add(-10*time.Second)))
	if got := testutil.CollectAndCount(instr.PinLagSeconds); got != 2 {
		t.Errorf("pin-lag series after team-a dropped out = %d, want 2 (team-a retired, infra untouched)", got)
	}
}

// TestSummariseAbortedPassRetiresItsGauges: findings #7 — an aborted pass has
// proven nothing about the fleet, so a stale-but-plausible gauge value must
// not freeze in place, where the D4 pin-staleness alarm would read it as
// healthy. Its series must go absent, while status.Nodes (covered already by
// TestSummariseAbortedPassPreservesTheFleetPicture) keeps the last known-good
// picture.
func TestSummariseAbortedPassRetiresItsGauges(t *testing.T) {
	now := time.Unix(4000, 0)
	instr := metrics.Nop()
	r := &WavefrontReconciler{
		Recorder: events.NewFakeRecorder(16),
		Clock:    func() time.Time { return now },
		Metrics:  instr,
	}

	// A good pass leaves fleet's gauges standing, alongside a co-resident
	// infra's.
	r.summariseNodes(gaugePass(fleetName, teamAName, now.Add(-60*time.Second)))
	r.summariseNodes(gaugePass("infra", "team-c", now.Add(-30*time.Second)))

	wf := settledFleet()
	before := wf.Status.DeepCopy()
	r.summarise(&pass{wf: wf, graphValid: true}, errors.New(passFailure))

	if got := wf.Status.Nodes; got != before.Nodes {
		t.Errorf("counts = %+v, want the previous %+v left untouched", got, before.Nodes)
	}

	if got := testutil.CollectAndCount(instr.PinLagSeconds); got != 1 {
		t.Errorf("PinLagSeconds series after the abort = %d, want 1 (fleet retired, infra survives)", got)
	}
	if got := testutil.CollectAndCount(instr.BlockedNodes); got != 1 {
		t.Errorf("BlockedNodes series after the abort = %d, want 1 (fleet retired, infra survives)", got)
	}
	if got := testutil.CollectAndCount(instr.PinnedFetchFailures); got != 1 {
		t.Errorf("PinnedFetchFailures series after the abort = %d, want 1 (fleet retired, infra survives)", got)
	}
	if lag := testutil.ToFloat64(instr.PinLagSeconds.WithLabelValues(
		"infra", kindKustomization, fluxNamespace, "team-c")); lag != 30 {
		t.Errorf("surviving infra pin lag = %v, want 30: an unrelated abort must not disturb it", lag)
	}
}

// TestSummariseNodesOverlapPassSuppressesGauges: findings #8 — a selector
// overlap (or a cycle) means p.graphValid is false and this pass's counts are
// not authoritative for occupancy, so publishing its gauges alongside the
// Wavefront it overlaps with would double-count the shared node. status.Nodes
// stays live (overlap is not an abort — DESIGN's overlap rule), but the gauge
// writes are suppressed, and any series a prior valid pass left behind are
// still retired rather than left to go stale.
func TestSummariseNodesOverlapPassSuppressesGauges(t *testing.T) {
	now := time.Unix(5000, 0)
	instr := metrics.Nop()
	r := &WavefrontReconciler{
		Recorder: events.NewFakeRecorder(16),
		Clock:    func() time.Time { return now },
		Metrics:  instr,
	}

	// A prior valid pass leaves fleet's gauges standing, alongside infra's.
	r.summariseNodes(gaugePass(fleetName, teamAName, now.Add(-60*time.Second)))
	r.summariseNodes(gaugePass("infra", "team-c", now.Add(-30*time.Second)))

	overlapping := gaugePass(fleetName, teamAName, now.Add(-90*time.Second))
	overlapping.graphValid = false
	r.summariseNodes(overlapping)

	if got := testutil.CollectAndCount(instr.PinLagSeconds); got != 1 {
		t.Errorf("PinLagSeconds series after an overlapping pass = %d, want 1 (fleet retired, no republish, infra survives)", got)
	}
	if got := testutil.CollectAndCount(instr.BlockedNodes); got != 1 {
		t.Errorf("BlockedNodes series after an overlapping pass = %d, want 1 (fleet retired, no republish, infra survives)", got)
	}
	if got := testutil.CollectAndCount(instr.PinnedFetchFailures); got != 1 {
		t.Errorf("PinnedFetchFailures series after an overlapping pass = %d, want 1 (fleet retired, no republish, infra survives)", got)
	}

	if got := overlapping.wf.Status.Nodes.Observed; got != 1 {
		t.Errorf("status.Nodes.Observed = %d, want 1: overlap keeps status live", got)
	}
	if got := overlapping.wf.Status.Nodes.Blocked; got != 1 {
		t.Errorf("status.Nodes.Blocked = %d, want 1: overlap keeps status live", got)
	}
}

// TestWavefrontDeletionRetiresItsMetricSeries: deletion carries no finalizer
// by design (DESIGN D8), so Reconcile's IsNotFound branch is the only signal
// that a Wavefront is gone. It must retire every series that Wavefront ever
// contributed — both the per-pass gauges a pass recomputes wholesale and the
// cumulative admission counters no pass ever reconciles — while leaving a
// co-resident Wavefront's series standing.
func TestWavefrontDeletionRetiresItsMetricSeries(t *testing.T) {
	now := time.Unix(3000, 0)
	instr := metrics.Nop()

	scheme := runtime.NewScheme()
	if err := wavefrontv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	r := &WavefrontReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).Build(),
		Recorder: events.NewFakeRecorder(16),
		Clock:    func() time.Time { return now },
		Metrics:  instr,
	}

	// Populate the per-pass gauges (the fixture from
	// TestFleetGaugesAreRetiredPerWavefront) and the cumulative admission
	// counters for the doomed "fleet" Wavefront and a co-resident "infra"
	// sibling that must survive.
	r.summariseNodes(gaugePass(fleetName, teamAName, now.Add(-60*time.Second)))
	r.summariseNodes(gaugePass("infra", "team-c", now.Add(-30*time.Second)))

	instr.Wavefront(fleetName).CountAdmission("admitted")
	instr.Wavefront(fleetName).ObserveAdmissionWait(45)
	instr.Wavefront("infra").CountAdmission("admitted")
	instr.Wavefront("infra").ObserveAdmissionWait(45)

	// "fleet" no longer exists in the client: Reconcile takes the IsNotFound
	// branch.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: fleetName},
	}); err != nil {
		t.Fatalf("Reconcile on a deleted Wavefront returned %v, want nil", err)
	}

	if got := testutil.CollectAndCount(instr.PinLagSeconds); got != 1 {
		t.Errorf("PinLagSeconds series = %d, want 1 (infra survives, fleet retired)", got)
	}
	if got := testutil.CollectAndCount(instr.BlockedNodes); got != 1 {
		t.Errorf("BlockedNodes series = %d, want 1 (infra survives, fleet retired)", got)
	}
	if got := testutil.CollectAndCount(instr.PinnedFetchFailures); got != 1 {
		t.Errorf("PinnedFetchFailures series = %d, want 1 (infra survives, fleet retired)", got)
	}
	if got := testutil.CollectAndCount(instr.AdmissionsTotal); got != 1 {
		t.Errorf("AdmissionsTotal series = %d, want 1 (infra survives, fleet retired)", got)
	}
	if got := testutil.CollectAndCount(instr.AdmissionWaitSeconds); got != 1 {
		t.Errorf("AdmissionWaitSeconds series = %d, want 1 (infra survives, fleet retired)", got)
	}

	if lag := testutil.ToFloat64(instr.PinLagSeconds.WithLabelValues(
		"infra", kindKustomization, fluxNamespace, "team-c")); lag != 30 {
		t.Errorf("surviving infra pin lag = %v, want 30: an unrelated deletion must not disturb it", lag)
	}
	if got := testutil.ToFloat64(instr.AdmissionsTotal.WithLabelValues("infra", "admitted")); got != 1 {
		t.Errorf("surviving infra AdmissionsTotal = %v, want 1: an unrelated deletion must not disturb it", got)
	}
}

// --- shared-Poller bookkeeping ----------------------------------------------

func pollTarget(name string) gitpoll.Target {
	return gitpoll.Target{
		Source:      types.NamespacedName{Namespace: fluxNamespace, Name: name},
		URL:         "https://git.example.com/org/" + name + ".git",
		TrackingRef: mainRef,
	}
}

func pollPass(name string, interval time.Duration, perHost int, targets ...gitpoll.Target) *pass {
	return &pass{
		wf: &wavefrontv1alpha1.Wavefront{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: wavefrontv1alpha1.WavefrontSpec{
				Poll: wavefrontv1alpha1.PollSpec{
					Interval:           metav1.Duration{Duration: interval},
					PerHostConcurrency: perHost,
				},
			},
		},
		targets: targets,
	}
}

func wavefrontList(names ...string) *wavefrontv1alpha1.WavefrontList {
	list := &wavefrontv1alpha1.WavefrontList{}
	for _, name := range names {
		list.Items = append(list.Items, wavefrontv1alpha1.Wavefront{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		})
	}
	return list
}

func targetNames(targets []gitpoll.Target) []string {
	names := make([]string, 0, len(targets))
	for _, target := range targets {
		names = append(names, target.Source.Name)
	}
	return names
}

// TestPollSetsMergeCadenceAndTargets covers the whole N-Wavefronts-through-one-
// Poller hazard: Configure is last-writer-wins and SetTargets replaces the whole
// set, so whichever Wavefront reconciled last would otherwise dictate both. A
// 90s Wavefront must not stall a co-resident 30s one, and neither may erase the
// other's targets.
func TestPollSetsMergeCadenceAndTargets(t *testing.T) {
	r := &WavefrontReconciler{Poller: gitpoll.NewPoller(nil, nil, nil, nil, nil), Metrics: metrics.Nop()}

	r.updatePollSet(pollPass("slow", 90*time.Second, 2, pollTarget(alphaSource)), wavefrontList("slow"))

	interval, perHost := cadenceOf(r.pollSets)
	if interval != 90*time.Second || perHost != 2 {
		t.Errorf("cadence with one Wavefront = %v/%d, want 90s/2", interval, perHost)
	}

	r.updatePollSet(pollPass("fast", 30*time.Second, 4, pollTarget(betaSource)), wavefrontList("slow", "fast"))

	interval, perHost = cadenceOf(r.pollSets)
	if interval != 30*time.Second {
		t.Errorf("merged interval = %v, want the tightest 30s: a slow Wavefront must not stall a fast one", interval)
	}
	if perHost != 4 {
		t.Errorf("merged perHostConcurrency = %d, want the most generous 4", perHost)
	}
	if got := targetNames(unionOf(r.pollSets)); !slices.Equal(got, []string{alphaSource, betaSource}) {
		t.Errorf("targets = %v, want the union [alpha beta]", got)
	}

	// The fast Wavefront reconciles again: still merged, not overwritten.
	r.updatePollSet(pollPass("fast", 30*time.Second, 4, pollTarget(betaSource)), wavefrontList("slow", "fast"))
	if got := targetNames(unionOf(r.pollSets)); !slices.Equal(got, []string{alphaSource, betaSource}) {
		t.Errorf("targets after a repeat pass = %v, want [alpha beta]", got)
	}
}

// TestPollSetsPruneRestoresTheSurvivingCadence: when the fast Wavefront goes
// away, both its targets and its claim on the cadence must go with it.
func TestPollSetsPruneRestoresTheSurvivingCadence(t *testing.T) {
	cases := []struct {
		name   string
		remove func(r *WavefrontReconciler)
	}{
		{
			name: "deleted Wavefront observed on the next pass",
			remove: func(r *WavefrontReconciler) {
				r.updatePollSet(pollPass("slow", 90*time.Second, 2, pollTarget(alphaSource)), wavefrontList("slow"))
			},
		},
		{
			name:   "deletion seen as a NotFound reconcile",
			remove: func(r *WavefrontReconciler) { r.forgetPollSet("fast") },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &WavefrontReconciler{Poller: gitpoll.NewPoller(nil, nil, nil, nil, nil), Metrics: metrics.Nop()}
			r.updatePollSet(pollPass("slow", 90*time.Second, 2, pollTarget(alphaSource)), wavefrontList("slow"))
			r.updatePollSet(pollPass("fast", 30*time.Second, 4, pollTarget(betaSource)), wavefrontList("slow", "fast"))

			tc.remove(r)

			interval, perHost := cadenceOf(r.pollSets)
			if interval != 90*time.Second || perHost != 2 {
				t.Errorf("cadence after the fast Wavefront went = %v/%d, want the survivor's 90s/2",
					interval, perHost)
			}
			if got := targetNames(unionOf(r.pollSets)); !slices.Equal(got, []string{alphaSource}) {
				t.Errorf("targets = %v, want only the survivor's [alpha]", got)
			}
		})
	}
}

// TestPollSetsCadenceDefaults: an empty set yields the zero cadence Configure
// clamps, and a Wavefront that sent explicit zeros past CRD defaulting must not
// drag the merged interval to zero and tight-loop the poller.
func TestPollSetsCadenceDefaults(t *testing.T) {
	if interval, perHost := cadenceOf(nil); interval != 0 || perHost != 0 {
		t.Errorf("cadence of an empty set = %v/%d, want 0/0 for Configure to clamp", interval, perHost)
	}

	r := &WavefrontReconciler{Poller: gitpoll.NewPoller(nil, nil, nil, nil, nil), Metrics: metrics.Nop()}
	r.updatePollSet(pollPass("zeroes", 0, 0, pollTarget(alphaSource)), wavefrontList("zeroes"))

	interval, perHost := cadenceOf(r.pollSets)
	if interval != gitpoll.DefaultInterval || perHost != gitpoll.DefaultPerHostConcurrency {
		t.Errorf("cadence from explicit zeros = %v/%d, want the CRD defaults %v/%d",
			interval, perHost, gitpoll.DefaultInterval, gitpoll.DefaultPerHostConcurrency)
	}
}
