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
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
)

// The three node shapes members has to render, named once.
const (
	appNode  = "app"
	gateNode = "gate"
	heldNode = "held"
)

// mixedFleet is one resolved pass over three nodes, one of each shape members
// has to render: a pinned node blocked behind an unhealthy gate, the gate
// itself (out-of-selector, sourceless), and a pinned node held by a hand-pin.
func mixedFleet(pendingSince time.Time) *Result {
	app, gate, held := nodeRef(appNode), nodeRef(gateNode), nodeRef(heldNode)
	appSrc := types.NamespacedName{Namespace: fluxNamespace, Name: appNode}
	heldSrc := types.NamespacedName{Namespace: fluxNamespace, Name: heldNode}

	return &Result{
		Resolved:     true,
		GraphChecked: true,
		Nodes: map[adapter.NodeRef]adapter.Node{
			app:  {Ref: app, DependsOn: []adapter.NodeRef{gate}, SourceRef: &appSrc},
			gate: {Ref: gate},
			held: {Ref: held, SourceRef: &heldSrc},
		},
		Selected: map[adapter.NodeRef]bool{app: true, held: true},
		Inputs: map[adapter.NodeRef]engine.NodeInput{
			app: {Ref: app, Role: engine.RolePinned, Ready: true, Source: &engine.SourceState{
				Source: appSrc, Pin: shaA, ObservedSHA: shaHand, FirstObserved: pendingSince,
			}},
			gate: {Ref: gate, Role: engine.RoleGate, Ready: false, Failing: true},
			held: {Ref: held, Role: engine.RolePinned, Ready: true, Source: &engine.SourceState{
				Source: heldSrc, Pin: shaA, ObservedSHA: shaHand, Held: true, HeldBy: humanManager,
				FetchFailing: true,
			}},
		},
		NodeBySource: map[types.NamespacedName][]adapter.NodeRef{
			appSrc:  {app},
			heldSrc: {held},
		},
		Holds: map[types.NamespacedName]Hold{
			heldSrc: {Manager: humanManager, Kind: HoldHandPin},
		},
		Eval: engine.Evaluation{Nodes: map[adapter.NodeRef]engine.NodeResult{
			app: {
				State:        engine.StatePending,
				PendingSince: pendingSince,
				Blocked:      &engine.Blocked{Ancestor: gate, Reason: engine.ReasonAncestorUnhealthy},
			},
			gate: {State: engine.StateUnhealthy},
			held: {
				State:   engine.StatePending,
				Held:    true,
				Blocked: &engine.Blocked{Reason: engine.ReasonSelfHeld},
			},
		}},
	}
}

// members below is the whole derived picture status.members carries, so a
// reader needs no second pass over the cluster to explain a fleet
// (DESIGN §4.1). One case per node shape.

func mixedMembers(t *testing.T, pendingSince time.Time) []wavefrontv1alpha1.Member {
	t.Helper()
	return Summarise(mixedFleet(pendingSince), time.Unix(5000, 0)).Members
}

// TestSummariseMembersCoverEveryEvaluatedNode: gates are evaluated nodes too,
// and the list is sorted so a diff of two passes is readable.
func TestSummariseMembersCoverEveryEvaluatedNode(t *testing.T) {
	summary := Summarise(mixedFleet(time.Unix(1000, 0)), time.Unix(5000, 0))

	if got, want := len(summary.Members), 3; got != want {
		t.Fatalf("members = %d, want %d (every evaluated node, gates included)", got, want)
	}
	for i, want := range []string{appNode, gateNode, heldNode} {
		if got := summary.Members[i].Node.Name; got != want {
			t.Errorf("members[%d].node.name = %q, want %q (kind/namespace/name order)", i, got, want)
		}
		if got := summary.Members[i].Node.Kind; got != kindKustomization {
			t.Errorf("members[%d].node.kind = %q, want %q", i, got, kindKustomization)
		}
	}
	if summary.MembersOmitted != 0 {
		t.Errorf("membersOmitted = %d, want 0 well under the cap", summary.MembersOmitted)
	}
}

// TestSummariseMemberOfABlockedPinnedNode: the source plumbing, the pending
// clock and the attribution that together explain why a node has not advanced.
func TestSummariseMemberOfABlockedPinnedNode(t *testing.T) {
	pendingSince := time.Unix(1000, 0)
	app := mixedMembers(t, pendingSince)[0]

	if app.Role != string(engine.RolePinned) || app.State != string(engine.StatePending) {
		t.Errorf("role/state = %q/%q, want Pinned/Pending", app.Role, app.State)
	}
	if app.Source != fluxNamespace+"/"+appNode || app.Pin != shaA || app.ObservedSHA != shaHand {
		t.Errorf("source/pin/observedSHA = %q/%q/%q, want %s/%s, %s, %s",
			app.Source, app.Pin, app.ObservedSHA, fluxNamespace, appNode, shaA, shaHand)
	}
	if !app.Ready || app.Held {
		t.Errorf("ready/held = %v/%v, want true/false", app.Ready, app.Held)
	}
	if app.PendingSince == nil || !app.PendingSince.Time.Equal(pendingSince) {
		t.Errorf("pendingSince = %v, want %v", app.PendingSince, pendingSince)
	}
	// The graph edges travel with status, so the block is explicable from
	// status alone.
	if len(app.DependsOn) != 1 || app.DependsOn[0].Name != gateNode {
		t.Errorf("dependsOn = %+v, want the gate edge carried in status", app.DependsOn)
	}
	if app.Blocked == nil || app.Blocked.Reason != string(engine.ReasonAncestorUnhealthy) {
		t.Fatalf("blocked = %+v, want AncestorUnhealthy", app.Blocked)
	}
	if app.Blocked.Ancestor == nil || app.Blocked.Ancestor.Name != gateNode {
		t.Errorf("blocked.ancestor = %+v, want the gate", app.Blocked.Ancestor)
	}
}

// TestSummariseMemberOfAGate: a gate has no managed source, so it carries no
// source, pin or observation to report.
func TestSummariseMemberOfAGate(t *testing.T) {
	gate := mixedMembers(t, time.Unix(1000, 0))[1]

	if gate.Role != string(engine.RoleGate) || gate.State != string(engine.StateUnhealthy) {
		t.Errorf("role/state = %q/%q, want Gate/Unhealthy", gate.Role, gate.State)
	}
	if gate.Source != "" || gate.Pin != "" || gate.ObservedSHA != "" {
		t.Errorf("source/pin/observedSHA = %q/%q/%q, want all empty: a gate has no managed source",
			gate.Source, gate.Pin, gate.ObservedSHA)
	}
	if gate.Ready || gate.PendingSince != nil || gate.Blocked != nil || len(gate.DependsOn) != 0 {
		t.Errorf("gate = %+v, want an unready leaf with nothing pending or blocked", gate)
	}
}

// TestSummariseMemberOfAHeldNode: a hand-pinned source is reported as held and
// SelfHeld, which names no ancestor to blame.
func TestSummariseMemberOfAHeldNode(t *testing.T) {
	held := mixedMembers(t, time.Unix(1000, 0))[2]

	if !held.Held {
		t.Errorf("held = %v, want true", held.Held)
	}
	if held.Blocked == nil || held.Blocked.Reason != string(engine.ReasonSelfHeld) {
		t.Fatalf("blocked = %+v, want SelfHeld", held.Blocked)
	}
	if held.Blocked.Ancestor != nil {
		t.Errorf("blocked.ancestor = %+v, want nil: SelfHeld attributes no ancestor", held.Blocked.Ancestor)
	}
	if held.PendingSince != nil {
		t.Errorf("pendingSince = %v, want nil: a zero PendingSince is absent, not epoch", held.PendingSince)
	}
}

// TestSummariseCountsPhaseAndLists guards the split of the old summariseNodes:
// the counts, the capped lists, the hold ledger and the phase all still come
// out of one derivation over one Result.
func TestSummariseCountsPhaseAndLists(t *testing.T) {
	now := time.Unix(5000, 0)
	summary := Summarise(mixedFleet(time.Unix(1000, 0)), now)

	want := wavefrontv1alpha1.NodeCounts{Observed: 3, Pinned: 2, Gates: 1, Pending: 2, Blocked: 2, Held: 1}
	if summary.Counts != want {
		t.Errorf("counts = %+v, want %+v", summary.Counts, want)
	}
	if summary.Phase != wavefrontv1alpha1.PhaseBlocked {
		t.Errorf("phase = %q, want Blocked: an unhealthy ancestor needs someone to act", summary.Phase)
	}
	if summary.FetchFailures != 1 {
		t.Errorf("fetchFailures = %d, want 1", summary.FetchFailures)
	}
	if got := summary.BlockedByReason[engine.ReasonAncestorUnhealthy]; got != 1 {
		t.Errorf("blockedByReason[AncestorUnhealthy] = %d, want 1", got)
	}
	if got := summary.BlockedByReason[engine.ReasonSelfHeld]; got != 1 {
		t.Errorf("blockedByReason[SelfHeld] = %d, want 1", got)
	}

	if len(summary.Blocked) != 2 || summary.Blocked[0].Node.Name != appNode {
		t.Fatalf("blocked = %+v, want both entries, nodeKey-sorted", summary.Blocked)
	}
	if summary.Blocked[0].Ancestor == nil || summary.Blocked[0].Ancestor.Name != gateNode {
		t.Errorf("blocked[0].ancestor = %+v, want the gate", summary.Blocked[0].Ancestor)
	}

	if len(summary.Held) != 1 {
		t.Fatalf("held = %+v, want the single hold", summary.Held)
	}
	if summary.Held[0].Manager != humanManager || summary.Held[0].Reason != wavefrontv1alpha1.HoldReasonHandPin {
		t.Errorf("held[0] = %+v, want the HandPin manager named", summary.Held[0])
	}
	if summary.Held[0].Node.Name != heldNode {
		t.Errorf("held[0].node = %+v, want the referencing node attributed", summary.Held[0].Node)
	}
}

// TestSummariseCapsMembers: status must stay a bounded object however large the
// fleet grows, so the tail is dropped and counted rather than published
// (DESIGN §4.1). The counts, not the list, stay authoritative.
func TestSummariseCapsMembers(t *testing.T) {
	const overflow = 3

	res := &Result{
		Resolved:     true,
		GraphChecked: true,
		Nodes:        map[adapter.NodeRef]adapter.Node{},
		Inputs:       map[adapter.NodeRef]engine.NodeInput{},
		Eval:         engine.Evaluation{Nodes: map[adapter.NodeRef]engine.NodeResult{}},
	}
	for i := range wavefrontv1alpha1.MembersCap + overflow {
		ref := nodeRef(fmt.Sprintf("node-%05d", i))
		res.Nodes[ref] = adapter.Node{Ref: ref}
		res.Inputs[ref] = engine.NodeInput{Ref: ref, Role: engine.RoleGate, Ready: true}
		res.Eval.Nodes[ref] = engine.NodeResult{State: engine.StateSettled}
	}

	summary := Summarise(res, time.Unix(5000, 0))

	if got := len(summary.Members); got != wavefrontv1alpha1.MembersCap {
		t.Errorf("members = %d, want the cap %d", got, wavefrontv1alpha1.MembersCap)
	}
	if summary.MembersOmitted != overflow {
		t.Errorf("membersOmitted = %d, want %d", summary.MembersOmitted, overflow)
	}
	if got, want := summary.Counts.Observed, wavefrontv1alpha1.MembersCap+overflow; got != want {
		t.Errorf("counts.Observed = %d, want %d: the counts stay authoritative past the cap", got, want)
	}
	// The cap keeps the lowest-sorted members, so the omitted tail is
	// predictable rather than an arbitrary map read.
	if got := summary.Members[0].Node.Name; got != "node-00000" {
		t.Errorf("members[0] = %q, want node-00000: the kept prefix is the sorted one", got)
	}
}

// TestSummariseOfAnAbortedPassDerivesNothing: Summarise is only ever called for
// a resolved pass, but a nil or empty Result must still return a harmless zero
// summary rather than panic — its caller decides what to publish.
func TestSummariseOfAnAbortedPassDerivesNothing(t *testing.T) {
	for name, res := range map[string]*Result{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			summary := Summarise(res, time.Unix(5000, 0))
			if summary.Counts != (wavefrontv1alpha1.NodeCounts{}) {
				t.Errorf("counts = %+v, want zero", summary.Counts)
			}
			if summary.Members != nil || summary.MembersOmitted != 0 {
				t.Errorf("members = %+v/%d, want none", summary.Members, summary.MembersOmitted)
			}
			if summary.Blocked != nil || summary.Held != nil {
				t.Errorf("lists = %+v/%+v, want none", summary.Blocked, summary.Held)
			}
		})
	}
}
