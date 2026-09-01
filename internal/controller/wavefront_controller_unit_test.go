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
	"fmt"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/metrics"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
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
				Reason:  string(holdHandPin),
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
			holds:        map[types.NamespacedName]hold{teamAKey(): {manager: humanManager, kind: holdHandPin}},
			nodeBySource: map[types.NamespacedName][]adapter.NodeRef{teamAKey(): {teamARef()}},
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

// --- shared-source resolution (WP2) -----------------------------------------

const teamBName = "team-b"

func teamBRef() adapter.NodeRef {
	return adapter.NodeRef{Kind: kindKustomization, Namespace: fluxNamespace, Name: teamBName}
}

// TestResolveMemoizesASharedSource is decision D-A's controller-side
// counterpart to the engine's gateSharedSources: two Kustomizations sharing
// one GitRepository must see one r.Get, one *engine.SourceState and one poll
// Target, and nodeBySource must list both referencing nodes rather than
// silently keeping only the last writer.
func TestResolveMemoizesASharedSource(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := sourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	src := types.NamespacedName{Namespace: fluxNamespace, Name: "shared"}
	repo := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: src.Namespace,
			Name:      src.Name,
			Labels:    map[string]string{pin.ManagedLabel: managedLabelValue},
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:       "https://git.example.com/org/shared.git",
			Reference: &sourcev1.GitRepositoryRef{Name: mainRef},
		},
	}

	r := &WavefrontReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(repo).Build(),
		Strategy: selection.TrackRef(),
		Recorder: events.NewFakeRecorder(16),
	}

	nodeA, nodeB := teamARef(), teamBRef()
	p := &pass{
		wf: &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}},
		nodes: map[adapter.NodeRef]adapter.Node{
			nodeA: {Ref: nodeA, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
			nodeB: {Ref: nodeB, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
		},
		selected:     map[adapter.NodeRef]bool{nodeA: true, nodeB: true},
		missing:      map[adapter.NodeRef]bool{},
		observations: map[types.NamespacedName]gitpoll.Observation{},
	}

	if err := r.resolve(context.Background(), p); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	stateA, stateB := p.inputs[nodeA].Source, p.inputs[nodeB].Source
	if stateA == nil || stateB == nil {
		t.Fatalf("both sharers must resolve pinned, got a=%+v b=%+v", p.inputs[nodeA], p.inputs[nodeB])
	}
	if stateA != stateB {
		t.Errorf("sharers got distinct *SourceState pointers (%p, %p), want the memoized one shared", stateA, stateB)
	}

	if len(p.targets) != 1 {
		t.Errorf("targets = %d, want exactly 1 for the shared source, not one per referencing node", len(p.targets))
	}

	if got, want := p.nodeBySource[src], []adapter.NodeRef{nodeA, nodeB}; !slices.Equal(got, want) {
		t.Errorf("nodeBySource[%s] = %v, want both referencing nodes in compareRefs order %v", src, got, want)
	}
}

// TestResolveSkipsASourceReferencedOnlyByNonSelectedNodes is the fix for the
// WP2 review finding: a source with no selected referencing node must never
// register a poll Target or fire UnsupportedRefStyle — that would leak a
// source this Wavefront has zero selected interest in into its pollSet
// contribution (and misattribute the warning) purely because a
// dependency-closure gate happens to reference it. The source's ref style is
// deliberately unsupported (SemVer), the worst case: even that must not
// resolve or fire an event when nothing selected reaches it.
func TestResolveSkipsASourceReferencedOnlyByNonSelectedNodes(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := sourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	src := types.NamespacedName{Namespace: fluxNamespace, Name: "upstream"}
	repo := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: src.Namespace,
			Name:      src.Name,
			Labels:    map[string]string{pin.ManagedLabel: managedLabelValue},
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:       "https://git.example.com/org/upstream.git",
			Reference: &sourcev1.GitRepositoryRef{SemVer: ">=1.0.0"},
		},
	}

	recorder := events.NewFakeRecorder(16)
	r := &WavefrontReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(repo).Build(),
		Strategy: selection.TrackRef(),
		Recorder: recorder,
	}

	gateOnly := teamARef()
	p := &pass{
		wf: &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}},
		nodes: map[adapter.NodeRef]adapter.Node{
			gateOnly: {Ref: gateOnly, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
		},
		selected:     map[adapter.NodeRef]bool{}, // gateOnly is a dependency-closure gate, not selected
		missing:      map[adapter.NodeRef]bool{},
		observations: map[types.NamespacedName]gitpoll.Observation{},
	}

	if err := r.resolve(context.Background(), p); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if got := p.inputs[gateOnly]; got.Role != engine.RoleGate || got.Source != nil {
		t.Errorf("non-selected referencing node = %+v, want a plain gate with no Source", got)
	}
	if len(p.targets) != 0 {
		t.Errorf("targets = %v, want none: no selected node references this source", p.targets)
	}
	if len(p.nodeBySource) != 0 {
		t.Errorf("nodeBySource = %v, want empty", p.nodeBySource)
	}
	if len(p.resolvedSources) != 0 {
		t.Errorf("resolvedSources = %v, want empty: the source was never resolved", p.resolvedSources)
	}
	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("events = %v, want none: UnsupportedRefStyle must not fire for a source no selected node references", recorded)
	}
}

// TestResolveMixedSelectedAndGateSharersOfOneSource covers the mixed
// topology the mono-repo gate above simplified away: one selected (pinned)
// node and one non-selected (gate) node referencing the same source. The
// source still resolves exactly once (one Target), the selected node is
// pinned to it, and the gate node stays a plain gate — never added to
// nodeBySource.
func TestResolveMixedSelectedAndGateSharersOfOneSource(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := sourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	src := types.NamespacedName{Namespace: fluxNamespace, Name: "shared"}
	repo := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: src.Namespace,
			Name:      src.Name,
			Labels:    map[string]string{pin.ManagedLabel: managedLabelValue},
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:       "https://git.example.com/org/shared.git",
			Reference: &sourcev1.GitRepositoryRef{Name: mainRef},
		},
	}

	r := &WavefrontReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(repo).Build(),
		Strategy: selection.TrackRef(),
		Recorder: events.NewFakeRecorder(16),
	}

	pinned, gate := teamARef(), teamBRef()
	p := &pass{
		wf: &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}},
		nodes: map[adapter.NodeRef]adapter.Node{
			pinned: {Ref: pinned, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
			gate:   {Ref: gate, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
		},
		selected:     map[adapter.NodeRef]bool{pinned: true}, // gate deliberately absent
		missing:      map[adapter.NodeRef]bool{},
		observations: map[types.NamespacedName]gitpoll.Observation{},
	}

	if err := r.resolve(context.Background(), p); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if got := p.inputs[pinned]; got.Role != engine.RolePinned || got.Source == nil {
		t.Errorf("selected node = %+v, want RolePinned with a Source", got)
	}
	if got := p.inputs[gate]; got.Role != engine.RoleGate || got.Source != nil {
		t.Errorf("non-selected node = %+v, want a plain gate with no Source", got)
	}
	if len(p.targets) != 1 {
		t.Errorf("targets = %d, want exactly 1: registered once, by the selected node", len(p.targets))
	}
	if got, want := p.nodeBySource[src], []adapter.NodeRef{pinned}; !slices.Equal(got, want) {
		t.Errorf("nodeBySource[%s] = %v, want only the selected node %v", src, got, want)
	}
}

// --- holds (WP3): suspended sources unify with hand-pins -------------------

// TestResolveSuspendedSourceYieldsSuspendHold covers finding 7: a suspended
// source must land in p.holds (kind Suspend, no manager), not just in the
// engine's Held/Blocked signal.
func TestResolveSuspendedSourceYieldsSuspendHold(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := sourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	src := types.NamespacedName{Namespace: fluxNamespace, Name: "suspended"}
	repo := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: src.Namespace,
			Name:      src.Name,
			Labels:    map[string]string{pin.ManagedLabel: managedLabelValue},
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:       "https://git.example.com/org/suspended.git",
			Reference: &sourcev1.GitRepositoryRef{Name: mainRef},
			Suspend:   true,
		},
	}

	r := &WavefrontReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(repo).Build(),
		Strategy: selection.TrackRef(),
		Recorder: events.NewFakeRecorder(16),
	}

	node := teamARef()
	p := &pass{
		wf: &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}},
		nodes: map[adapter.NodeRef]adapter.Node{
			node: {Ref: node, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
		},
		selected:     map[adapter.NodeRef]bool{node: true},
		missing:      map[adapter.NodeRef]bool{},
		observations: map[types.NamespacedName]gitpoll.Observation{},
	}

	if err := r.resolve(context.Background(), p); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	got, ok := p.holds[src]
	if !ok {
		t.Fatalf("holds[%s] missing, want a Suspend hold", src)
	}
	if got.kind != holdSuspend || got.manager != "" {
		t.Errorf("hold = %+v, want {manager: \"\", kind: Suspend}", got)
	}
}

// TestResolveHandPinAndSuspendYieldsHandPin: a source both hand-pinned and
// suspended reports HandPin — it names an actor, so it wins over the
// actor-less Suspend (brief D-B).
func TestResolveHandPinAndSuspendYieldsHandPin(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := sourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	src := types.NamespacedName{Namespace: fluxNamespace, Name: "both"}
	repo := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: src.Namespace,
			Name:      src.Name,
			Labels:    map[string]string{pin.ManagedLabel: managedLabelValue},
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:       "https://git.example.com/org/both.git",
			Reference: &sourcev1.GitRepositoryRef{Name: mainRef},
			Suspend:   true,
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(repo).WithReturnManagedFields().Build()

	// Hand-pin spec.ref.commit under a foreign field manager, exactly as the
	// envtest "hand-pin holds" scenario does, so pin.Hold sees a real
	// managedFields entry rather than a hand-built one.
	ctx := context.Background()
	live := &sourcev1.GitRepository{}
	if err := fakeClient.Get(ctx, src, live); err != nil {
		t.Fatalf("Get: %v", err)
	}
	live.Spec.Reference.Commit = shaHand
	if err := fakeClient.Update(ctx, live, client.FieldOwner(humanManager)); err != nil {
		t.Fatalf("Update: %v", err)
	}

	r := &WavefrontReconciler{
		Client:   fakeClient,
		Strategy: selection.TrackRef(),
		Recorder: events.NewFakeRecorder(16),
	}

	node := teamARef()
	p := &pass{
		wf: &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}},
		nodes: map[adapter.NodeRef]adapter.Node{
			node: {Ref: node, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
		},
		selected:     map[adapter.NodeRef]bool{node: true},
		missing:      map[adapter.NodeRef]bool{},
		observations: map[types.NamespacedName]gitpoll.Observation{},
	}

	if err := r.resolve(ctx, p); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	got, ok := p.holds[src]
	if !ok {
		t.Fatalf("holds[%s] missing, want a HandPin hold", src)
	}
	if got.kind != holdHandPin || got.manager != humanManager {
		t.Errorf("hold = %+v, want {manager: %q, kind: HandPin} even though the source is also suspended",
			got, humanManager)
	}
}

// heldStatus builds a one-entry status.Held ledger, keyed on teamASource, for
// the holdEvents tests below.
func heldStatus(manager, reason string) []wavefrontv1alpha1.HeldNode {
	return []wavefrontv1alpha1.HeldNode{{
		Node:    wavefrontv1alpha1.NodeReference{Kind: kindKustomization, Namespace: fluxNamespace, Name: teamAName},
		Source:  teamASource,
		Manager: manager,
		Reason:  reason,
	}}
}

// TestHoldEventsSuspendMessages covers the Suspend-flavoured detect/release
// text (brief item 8): distinct from the HandPin wording, and naming no
// field manager.
func TestHoldEventsSuspendMessages(t *testing.T) {
	recorder := events.NewFakeRecorder(32)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}}
	detect := &pass{
		wf:           wf,
		resolved:     true,
		holds:        map[types.NamespacedName]hold{teamAKey(): {kind: holdSuspend}},
		nodeBySource: map[types.NamespacedName][]adapter.NodeRef{teamAKey(): {teamARef()}},
	}
	r.holdEvents(detect)

	recorded := drain(recorder.Events)
	if len(recorded) != 1 || !strings.Contains(recorded[0], reasonHoldDetected) ||
		!strings.Contains(recorded[0], "is suspended; not advancing") {
		t.Fatalf("detect events = %v, want exactly one %s with the Suspend wording", recorded, reasonHoldDetected)
	}

	// The ledger now carries the Suspend hold; the next pass releases it.
	wf.Status.Held = heldStatus("", string(holdSuspend))
	release := &pass{wf: wf, resolved: true, holds: map[types.NamespacedName]hold{}}
	r.holdEvents(release)

	recorded = drain(recorder.Events)
	if len(recorded) != 1 || !strings.Contains(recorded[0], reasonHoldReleased) ||
		!strings.Contains(recorded[0], "suspension of") || !strings.Contains(recorded[0], "lifted") {
		t.Fatalf("release events = %v, want exactly one %s with the Suspend wording", recorded, reasonHoldReleased)
	}
}

// TestHoldEventsKindFlipFiresReleaseAndDetect: a source that goes from
// Suspend to HandPin (same source) in one pass is a release of the old kind
// and a detect of the new one, not silence — diffed on value (kind+manager)
// equality, not key presence.
func TestHoldEventsKindFlipFiresReleaseAndDetect(t *testing.T) {
	recorder := events.NewFakeRecorder(32)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{Name: fleetName},
		Status:     wavefrontv1alpha1.WavefrontStatus{Held: heldStatus("", string(holdSuspend))},
	}
	p := &pass{
		wf:           wf,
		resolved:     true,
		holds:        map[types.NamespacedName]hold{teamAKey(): {manager: humanManager, kind: holdHandPin}},
		nodeBySource: map[types.NamespacedName][]adapter.NodeRef{teamAKey(): {teamARef()}},
	}
	r.holdEvents(p)

	recorded := drain(recorder.Events)
	if len(recorded) != 2 {
		t.Fatalf("events on a kind flip = %v, want exactly 2 (release + detect)", recorded)
	}
	var sawRelease, sawDetect bool
	for _, e := range recorded {
		switch {
		case strings.Contains(e, reasonHoldReleased) && strings.Contains(e, "lifted"):
			sawRelease = true
		case strings.Contains(e, reasonHoldDetected) && strings.Contains(e, humanManager):
			sawDetect = true
		}
	}
	if !sawRelease || !sawDetect {
		t.Errorf("events = %v, want a Suspend release and a HandPin detect", recorded)
	}
}

// TestHoldEventsIdenticalLedgerFiresNothing: same source, same kind, same
// manager between passes must not re-fire — the diff is on value equality.
func TestHoldEventsIdenticalLedgerFiresNothing(t *testing.T) {
	recorder := events.NewFakeRecorder(32)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{Name: fleetName},
		Status:     wavefrontv1alpha1.WavefrontStatus{Held: heldStatus(humanManager, string(holdHandPin))},
	}
	p := &pass{
		wf:           wf,
		resolved:     true,
		holds:        map[types.NamespacedName]hold{teamAKey(): {manager: humanManager, kind: holdHandPin}},
		nodeBySource: map[types.NamespacedName][]adapter.NodeRef{teamAKey(): {teamARef()}},
	}
	r.holdEvents(p)

	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("events = %v, want none: the hold is unchanged", recorded)
	}
}

// TestHoldEventsEmptyReasonDefaultsToHandPin: status.held written by a
// pre-upgrade controller carries no Reason at all. Reading it back must treat
// that as HandPin, not as a spurious kind change against an unchanged
// HandPin hold.
func TestHoldEventsEmptyReasonDefaultsToHandPin(t *testing.T) {
	recorder := events.NewFakeRecorder(32)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{Name: fleetName},
		Status:     wavefrontv1alpha1.WavefrontStatus{Held: heldStatus(humanManager, "")},
	}
	p := &pass{
		wf:           wf,
		resolved:     true,
		holds:        map[types.NamespacedName]hold{teamAKey(): {manager: humanManager, kind: holdHandPin}},
		nodeBySource: map[types.NamespacedName][]adapter.NodeRef{teamAKey(): {teamARef()}},
	}
	r.holdEvents(p)

	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("events = %v, want none: an empty pre-upgrade Reason must default to HandPin", recorded)
	}
}

// --- the capped-mirror contract (finding 8) ---------------------------------

// manyHoldSource returns the i-th of a deterministic, zero-padded run of
// held-source names — sorted, by name, in index order — for exercising the
// StatusListCap boundary.
func manyHoldSource(i int) types.NamespacedName {
	return types.NamespacedName{Namespace: fluxNamespace, Name: fmt.Sprintf("src-%02d", i)}
}

// manyHolds builds n distinct HandPin holds, with their nodeBySource
// attribution, for the capped-mirror tests below.
func manyHolds(n int) (map[types.NamespacedName]hold, map[types.NamespacedName][]adapter.NodeRef) {
	holds := make(map[types.NamespacedName]hold, n)
	nodeBySource := make(map[types.NamespacedName][]adapter.NodeRef, n)
	for i := range n {
		src := manyHoldSource(i)
		holds[src] = hold{manager: humanManager, kind: holdHandPin}
		nodeBySource[src] = []adapter.NodeRef{{Kind: kindKustomization, Namespace: fluxNamespace, Name: src.Name}}
	}
	return holds, nodeBySource
}

// TestHoldEventsCapMirrorsStatus is the regression finding 8 describes: with
// more held sources than StatusListCap, holdEvents must fire exactly the
// capped set (the same list summariseNodes writes to status.Held) and must
// never refire for the truncated tail on a later, unchanged pass.
func TestHoldEventsCapMirrorsStatus(t *testing.T) {
	recorder := events.NewFakeRecorder(64)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}}
	holds, nodeBySource := manyHolds(25)

	// Pass 1: 25 sources held, only StatusListCap (20) fit the mirrored ledger.
	first := &pass{wf: wf, resolved: true, holds: holds, nodeBySource: nodeBySource}
	r.holdEvents(first)
	r.summarise(first, nil) // writes status.Held, exactly as Reconcile does

	recorded := drain(recorder.Events)
	if len(recorded) != wavefrontv1alpha1.StatusListCap {
		t.Fatalf("pass 1 HoldDetected events = %d, want exactly %d (the capped set)",
			len(recorded), wavefrontv1alpha1.StatusListCap)
	}
	if len(wf.Status.Held) != wavefrontv1alpha1.StatusListCap {
		t.Fatalf("status.held = %d entries, want %d", len(wf.Status.Held), wavefrontv1alpha1.StatusListCap)
	}

	// Pass 2: identical 25 holds. The bug under test: diffing against the
	// uncapped p.holds re-reports the truncated tail as newly detected on
	// every reconcile, forever.
	second := &pass{wf: wf, resolved: true, holds: holds, nodeBySource: nodeBySource}
	r.holdEvents(second)

	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("pass 2 (unchanged 25 holds) events = %v, want none: no refire for capped-out sources", recorded)
	}
}

// TestHoldEventsReleasePromotes21st: releasing an in-cap hold fires its
// HoldReleased and, in the same pass, HoldDetected for the source promoted
// into the freed cap slot — late but exactly once (D-C).
func TestHoldEventsReleasePromotes21st(t *testing.T) {
	recorder := events.NewFakeRecorder(64)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}}
	holds, nodeBySource := manyHolds(25)

	first := &pass{wf: wf, resolved: true, holds: holds, nodeBySource: nodeBySource}
	r.holdEvents(first)
	r.summarise(first, nil)
	drain(recorder.Events) // discard pass 1's 20 HoldDetected events

	// Release the lowest-sorted (in-cap) source; the 21st (index
	// StatusListCap, capped out of pass 1) is promoted into the freed slot.
	released := manyHoldSource(0)
	promoted := manyHoldSource(wavefrontv1alpha1.StatusListCap)
	holds2 := make(map[types.NamespacedName]hold, len(holds)-1)
	for src, h := range holds {
		if src != released {
			holds2[src] = h
		}
	}

	second := &pass{wf: wf, resolved: true, holds: holds2, nodeBySource: nodeBySource}
	r.holdEvents(second)

	recorded := drain(recorder.Events)
	if len(recorded) != 2 {
		t.Fatalf("events on release = %v, want exactly 2 (release + promoted detect)", recorded)
	}
	var sawRelease, sawDetect bool
	for _, e := range recorded {
		switch {
		case strings.Contains(e, reasonHoldReleased) && strings.Contains(e, released.String()):
			sawRelease = true
		case strings.Contains(e, reasonHoldDetected) && strings.Contains(e, promoted.String()):
			sawDetect = true
		}
	}
	if !sawRelease || !sawDetect {
		t.Errorf("events = %v, want a release of %s and a detect of %s", recorded, released, promoted)
	}
}

// --- the shadow-admission ledger (WP5, finding 9, decision D-H) ------------

// admissionFor builds a minimal would-be admission for the shadowAdmissions
// tests below; ObservedRef is fixed so the rendered event text is stable.
func admissionFor(src types.NamespacedName, to string) engine.Admission {
	return engine.Admission{Source: src, To: to, ObservedRef: mainRef}
}

// manyAdmissions builds n distinct would-be admissions, keyed on the same
// deterministic source names as manyHolds, for the capped-mirror test below.
func manyAdmissions(n int) []engine.Admission {
	admissions := make([]engine.Admission, 0, n)
	for i := range n {
		admissions = append(admissions, admissionFor(manyHoldSource(i), shaA))
	}
	return admissions
}

// TestShadowAdmissionsNoRefireOnIdenticalPass is finding 9's core regression:
// the engine re-derives the identical would-be admission every reconcile, so
// the edge-trigger has to live in the controller, against a status ledger,
// not in the engine.
func TestShadowAdmissionsNoRefireOnIdenticalPass(t *testing.T) {
	recorder := events.NewFakeRecorder(8)
	instr := metrics.Nop()
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: instr}

	wf := &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}}
	admissions := []engine.Admission{admissionFor(teamAKey(), shaA)}

	first := &pass{wf: wf}
	r.shadowAdmissions(first, admissions)

	recorded := drain(recorder.Events)
	if len(recorded) != 1 || !strings.Contains(recorded[0], reasonShadowAdmission) || !strings.Contains(recorded[0], shaA) {
		t.Fatalf("pass 1 events = %v, want exactly one %s carrying %s", recorded, reasonShadowAdmission, shaA)
	}
	if got := testutil.ToFloat64(instr.AdmissionsTotal.WithLabelValues(fleetName, resultShadow)); got != 1 {
		t.Fatalf("AdmissionsTotal{shadow} after pass 1 = %v, want 1", got)
	}

	// Pass 2: the identical admission, re-derived exactly as the engine
	// always has, must not re-announce.
	second := &pass{wf: wf}
	r.shadowAdmissions(second, admissions)

	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("pass 2 (identical admission) events = %v, want none: same (Source, To) pair", recorded)
	}
	if got := testutil.ToFloat64(instr.AdmissionsTotal.WithLabelValues(fleetName, resultShadow)); got != 1 {
		t.Errorf("AdmissionsTotal{shadow} after pass 2 = %v, want still 1 (no refire)", got)
	}
}

// TestShadowAdmissionsNewToRefires: a new To for a known Source is a new
// (Source, To) pair by VALUE, not merely a known key, and must refire.
func TestShadowAdmissionsNewToRefires(t *testing.T) {
	recorder := events.NewFakeRecorder(8)
	instr := metrics.Nop()
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: instr}

	wf := &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}}

	first := &pass{wf: wf}
	r.shadowAdmissions(first, []engine.Admission{admissionFor(teamAKey(), shaA)})
	drain(recorder.Events)

	second := &pass{wf: wf}
	r.shadowAdmissions(second, []engine.Admission{admissionFor(teamAKey(), shaB)})

	recorded := drain(recorder.Events)
	if len(recorded) != 1 || !strings.Contains(recorded[0], shaB) {
		t.Fatalf("events on a new To = %v, want exactly one ShadowAdmission carrying %s", recorded, shaB)
	}
	if got := testutil.ToFloat64(instr.AdmissionsTotal.WithLabelValues(fleetName, resultShadow)); got != 2 {
		t.Errorf("AdmissionsTotal{shadow} = %v, want 2: one per distinct (Source, To) pair", got)
	}
	if len(wf.Status.Shadow) != 1 || wf.Status.Shadow[0].To != shaB {
		t.Errorf("status.shadow = %+v, want one entry with To=%s", wf.Status.Shadow, shaB)
	}
}

// TestExecuteEnforceClearsStaleShadowLedger covers brief item 5(b): flipping
// to Enforce must clear a stale status.Shadow ledger even on a pass with zero
// admissions — the len(admissions)==0 shortcut must not bypass the clear.
func TestExecuteEnforceClearsStaleShadowLedger(t *testing.T) {
	recorder := events.NewFakeRecorder(8)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{Name: fleetName},
		Spec:       wavefrontv1alpha1.WavefrontSpec{Mode: wavefrontv1alpha1.ModeEnforce},
		Status: wavefrontv1alpha1.WavefrontStatus{
			Shadow: []wavefrontv1alpha1.ShadowAdmission{{Source: teamASource, To: shaA}},
		},
	}
	p := &pass{wf: wf} // no admissions this pass (p.eval is the zero Evaluation)

	if err := r.execute(context.Background(), p); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if wf.Status.Shadow != nil {
		t.Errorf("status.shadow = %v, want nil after flipping to Enforce, even with zero admissions this pass", wf.Status.Shadow)
	}
}

// TestExecuteSkipAdmissionsLeavesShadowLedgerUntouched and
// TestExecuteSuspendLeavesShadowLedgerUntouched cover brief item 4: the
// skipAdmissions and Suspend early returns must leave the ledger exactly as
// they found it, so a resumed Wavefront does not refire on unchanged
// would-be admissions.
func TestExecuteSkipAdmissionsLeavesShadowLedgerUntouched(t *testing.T) {
	recorder := events.NewFakeRecorder(8)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{Name: fleetName},
		Spec:       wavefrontv1alpha1.WavefrontSpec{Mode: wavefrontv1alpha1.ModeShadow},
		Status: wavefrontv1alpha1.WavefrontStatus{
			Shadow: []wavefrontv1alpha1.ShadowAdmission{{Source: teamASource, To: shaA}},
		},
	}
	p := &pass{
		wf:             wf,
		skipAdmissions: true,
		eval:           engine.Evaluation{Initial: []engine.Admission{admissionFor(teamAKey(), shaB)}},
	}

	if err := r.execute(context.Background(), p); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(wf.Status.Shadow) != 1 || wf.Status.Shadow[0].To != shaA {
		t.Errorf("status.shadow = %+v, want unchanged: selector overlap suppresses admissions entirely (DESIGN §4.1)", wf.Status.Shadow)
	}
	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("events = %v, want none", recorded)
	}
}

func TestExecuteSuspendLeavesShadowLedgerUntouched(t *testing.T) {
	recorder := events.NewFakeRecorder(8)
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: metrics.Nop()}

	wf := &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{Name: fleetName},
		Spec:       wavefrontv1alpha1.WavefrontSpec{Mode: wavefrontv1alpha1.ModeShadow, Suspend: true},
		Status: wavefrontv1alpha1.WavefrontStatus{
			Shadow: []wavefrontv1alpha1.ShadowAdmission{{Source: teamASource, To: shaA}},
		},
	}
	p := &pass{
		wf:   wf,
		eval: engine.Evaluation{Initial: []engine.Admission{admissionFor(teamAKey(), shaB)}},
	}

	if err := r.execute(context.Background(), p); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(wf.Status.Shadow) != 1 || wf.Status.Shadow[0].To != shaA {
		t.Errorf("status.shadow = %+v, want unchanged: Suspend freezes all writes (DESIGN §4.1)", wf.Status.Shadow)
	}
	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("events = %v, want none", recorded)
	}
}

// TestShadowAdmissionsCapMirrorsStatus mirrors TestHoldEventsCapMirrorsStatus
// (finding 8's Held pattern, reused per decision D-H): with more would-be
// admissions than StatusListCap, shadowAdmissions must announce exactly the
// capped set and never refire for the truncated tail on a later, unchanged
// pass.
func TestShadowAdmissionsCapMirrorsStatus(t *testing.T) {
	recorder := events.NewFakeRecorder(64)
	instr := metrics.Nop()
	r := &WavefrontReconciler{Recorder: recorder, Clock: time.Now, Metrics: instr}

	wf := &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: fleetName}}
	admissions := manyAdmissions(25)

	first := &pass{wf: wf}
	r.shadowAdmissions(first, admissions)

	recorded := drain(recorder.Events)
	if len(recorded) != wavefrontv1alpha1.StatusListCap {
		t.Fatalf("pass 1 ShadowAdmission events = %d, want exactly %d (the capped set)",
			len(recorded), wavefrontv1alpha1.StatusListCap)
	}
	if len(wf.Status.Shadow) != wavefrontv1alpha1.StatusListCap {
		t.Fatalf("status.shadow = %d entries, want %d", len(wf.Status.Shadow), wavefrontv1alpha1.StatusListCap)
	}
	if got := testutil.ToFloat64(instr.AdmissionsTotal.WithLabelValues(fleetName, resultShadow)); got != float64(wavefrontv1alpha1.StatusListCap) {
		t.Fatalf("AdmissionsTotal{shadow} after pass 1 = %v, want %d", got, wavefrontv1alpha1.StatusListCap)
	}

	// Pass 2: the identical 25 admissions. The bug under test: diffing
	// against an uncapped current set would re-report the truncated tail as
	// newly would-be on every reconcile, forever.
	second := &pass{wf: wf}
	r.shadowAdmissions(second, admissions)

	if recorded := drain(recorder.Events); len(recorded) != 0 {
		t.Errorf("pass 2 (unchanged 25 admissions) events = %v, want none: no refire for capped-out sources", recorded)
	}
	if got := testutil.ToFloat64(instr.AdmissionsTotal.WithLabelValues(fleetName, resultShadow)); got != float64(wavefrontv1alpha1.StatusListCap) {
		t.Errorf("AdmissionsTotal{shadow} after pass 2 = %v, want still %d (no refire)", got, wavefrontv1alpha1.StatusListCap)
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
