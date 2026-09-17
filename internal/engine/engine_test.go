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

package engine_test

import (
	"cmp"
	"fmt"
	"hash/fnv"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/graph"
)

// trackingRef is the ref every test source tracks; it is echoed into every
// expected Admission.ObservedRef.
const trackingRef = "refs/heads/main"

// t0 is the single observation timestamp used across the table, so
// PendingSince passthrough (source FirstObserved -> NodeResult/Admission) is
// asserted rather than ignored.
var t0 = time.Date(2026, 8, 27, 9, 14, 3, 0, time.UTC)

// ref builds a NodeRef of a fixed kind/namespace so the table stays terse.
func ref(name string) adapter.NodeRef {
	return adapter.NodeRef{Kind: "Kustomization", Namespace: "ns", Name: name}
}

func refsOf(names ...string) []adapter.NodeRef {
	out := make([]adapter.NodeRef, len(names))
	for i, n := range names {
		out[i] = ref(n)
	}
	return out
}

// srcName is the GitRepository a node's source resolves to.
func srcName(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: "ns", Name: name}
}

// sourceNamed overrides a node's source identity so two or more nodes can
// share one GitRepository (WP2: the standard Flux monorepo topology), in
// place of srcName's default of deriving it from the node's own name.
func sourceNamed(src string) nodeOpt {
	return func(in *engine.NodeInput) {
		in.Source.Source = srcName(src)
	}
}

type nodeOpt func(*engine.NodeInput)

// pinnedNode builds a Ready, unpinned, unobserved pinned-role node; the
// options move it to the state under test.
func pinnedNode(name string, opts ...nodeOpt) engine.NodeInput {
	in := engine.NodeInput{
		Ref:   ref(name),
		Role:  engine.RolePinned,
		Ready: true,
		Source: &engine.SourceState{
			Source:      srcName(name),
			TrackingRef: trackingRef,
		},
	}
	for _, opt := range opts {
		opt(&in)
	}
	return in
}

// gateNode builds a health-only participant (DESIGN §3.2): no source.
func gateNode(name string, ready bool) engine.NodeInput {
	return engine.NodeInput{
		Ref:     ref(name),
		Role:    engine.RoleGate,
		Ready:   ready,
		Failing: !ready,
	}
}

// observed records an advertised SHA (and when it was first seen).
func observed(sha string) nodeOpt {
	return func(in *engine.NodeInput) {
		in.Source.ObservedSHA = sha
		in.Source.FirstObserved = t0
	}
}

// atPin puts the node fully at rest on sha: pinned, applied, observed.
func atPin(sha string) nodeOpt {
	return func(in *engine.NodeInput) {
		in.Source.Pin = sha
		in.AppliedSHA = sha
		observed(sha)(in)
	}
}

// pinnedUnobserved pins and applies sha but never observed the tracking ref.
func pinnedUnobserved(sha string) nodeOpt {
	return func(in *engine.NodeInput) {
		in.Source.Pin = sha
		in.AppliedSHA = sha
	}
}

// appliedAt overrides AppliedSHA after atPin, leaving the workload converging
// toward an already-observed pin (Ready is unaffected — this isolates the
// AppliedSHA == Pin conjunct from the Ready/Failing signal).
func appliedAt(sha string) nodeOpt {
	return func(in *engine.NodeInput) { in.AppliedSHA = sha }
}

// pendingFrom is a node applied at from with to advertised on its ref.
func pendingFrom(from, to string) nodeOpt {
	return func(in *engine.NodeInput) {
		in.Source.Pin = from
		in.AppliedSHA = from
		observed(to)(in)
	}
}

// unpinned is a freshly discovered source (DESIGN §3.5.4).
func unpinned(artifactSHA, observedSHA string) nodeOpt {
	return func(in *engine.NodeInput) {
		in.Source.ArtifactSHA = artifactSHA
		if observedSHA != "" {
			observed(observedSHA)(in)
		}
	}
}

// notReady is a node converging at its pin: neither Ready nor Failing.
func notReady() nodeOpt {
	return func(in *engine.NodeInput) { in.Ready = false }
}

// failing is a node whose Ready condition is explicitly False.
func failing() nodeOpt {
	return func(in *engine.NodeInput) {
		in.Ready = false
		in.Failing = true
	}
}

// held marks the commit as owned by a foreign field manager (DESIGN §3.5.3).
// HeldBy is fixed rather than parameterized: no case asserts on it, and every
// call site wants the same illustrative value.
func held() nodeOpt {
	return func(in *engine.NodeInput) {
		in.Source.Held = true
		in.Source.HeldBy = "kubectl-edit"
	}
}

// suspended marks spec.suspend on the source (human incident action).
func suspended() nodeOpt {
	return func(in *engine.NodeInput) { in.Source.Suspended = true }
}

// chain renders A <- B <- C as dependsOn edges: B on A, C on B.
func chain(names ...string) map[string][]string {
	deps := make(map[string][]string, len(names))
	for i := 1; i < len(names); i++ {
		deps[names[i]] = []string{names[i-1]}
	}
	return deps
}

func mergeDeps(sets ...map[string][]string) map[string][]string {
	out := map[string][]string{}
	for _, s := range sets {
		maps.Copy(out, s)
	}
	return out
}

// adm is the ancestor-gated admission expected for a pending node.
func adm(name, from, to string) engine.Admission {
	return engine.Admission{
		Node:         ref(name),
		Source:       srcName(name),
		From:         from,
		To:           to,
		ObservedRef:  trackingRef,
		PendingSince: t0,
	}
}

// initialAdm is the ungated initial pin expected on discovery.
func initialAdm(name, to string, pendingSince time.Time) engine.Admission {
	return engine.Admission{
		Node:         ref(name),
		Source:       srcName(name),
		To:           to,
		ObservedRef:  trackingRef,
		Initial:      true,
		PendingSince: pendingSince,
	}
}

// admSrc is adm with an explicit source, for nodes sharing a GitRepository.
func admSrc(node, src, from, to string) engine.Admission {
	a := adm(node, from, to)
	a.Source = srcName(src)
	return a
}

// initialAdmSrc is initialAdm with an explicit source, for nodes sharing a
// GitRepository.
func initialAdmSrc(node, src, to string, pendingSince time.Time) engine.Admission {
	a := initialAdm(node, to, pendingSince)
	a.Source = srcName(src)
	return a
}

func blocked(ancestor string, reason engine.BlockedReason) *engine.Blocked {
	b := &engine.Blocked{Reason: reason}
	if ancestor != "" {
		b.Ancestor = ref(ancestor)
	}
	return b
}

type evalCase struct {
	name           string
	deps           map[string][]string
	inputs         []engine.NodeInput
	want           map[string]engine.NodeResult
	wantAdmissions []engine.Admission
	wantInitial    []engine.Admission
}

func buildGraph(t *testing.T, inputs []engine.NodeInput, deps map[string][]string) *graph.Graph {
	t.Helper()
	nodes := make(map[adapter.NodeRef][]adapter.NodeRef, len(inputs))
	for _, in := range inputs {
		nodes[in.Ref] = nil
	}
	for name, on := range deps {
		if _, ok := nodes[ref(name)]; !ok {
			t.Fatalf("deps names node %q which is absent from inputs", name)
		}
		nodes[ref(name)] = refsOf(on...)
	}
	return graph.Build(nodes)
}

func inputMap(inputs []engine.NodeInput) map[adapter.NodeRef]engine.NodeInput {
	out := make(map[adapter.NodeRef]engine.NodeInput, len(inputs))
	for _, in := range inputs {
		out[in.Ref] = in
	}
	return out
}

func TestEvaluate(t *testing.T) {
	cases := []evalCase{
		{
			name:           "lone pending node with no ancestors is admissible",
			inputs:         []engine.NodeInput{pinnedNode("a", pendingFrom("a1", "a2"))},
			want:           map[string]engine.NodeResult{"a": {State: engine.StateAdmissible, PendingSince: t0}},
			wantAdmissions: []engine.Admission{adm("a", "a1", "a2")},
		},
		{
			name: "co-arriving changes admit strictly in dependency order",
			deps: chain("a", "b"),
			inputs: []engine.NodeInput{
				pinnedNode("a", pendingFrom("a1", "a2")),
				pinnedNode("b", pendingFrom("b1", "b2")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateAdmissible, PendingSince: t0},
				"b": {State: engine.StatePending, Blocked: blocked("a", engine.ReasonAncestorPending), PendingSince: t0},
			},
			wantAdmissions: []engine.Admission{adm("a", "a1", "a2")},
		},
		{
			name: "converging ancestor blocks its descendant",
			deps: chain("a", "b"),
			inputs: []engine.NodeInput{
				pinnedNode("a", atPin("a1"), notReady()),
				pinnedNode("b", pendingFrom("b1", "b2")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateConverging},
				"b": {State: engine.StatePending, Blocked: blocked("a", engine.ReasonAncestorPending), PendingSince: t0},
			},
		},
		{
			name: "unhealthy ancestor blocks transitively through a settled intermediate",
			deps: chain("a", "b", "c"),
			inputs: []engine.NodeInput{
				pinnedNode("a", atPin("a1"), failing()),
				pinnedNode("b", atPin("b1")),
				pinnedNode("c", pendingFrom("c1", "c2")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateUnhealthy},
				"b": {State: engine.StateSettled},
				"c": {State: engine.StatePending, Blocked: blocked("a", engine.ReasonAncestorUnhealthy), PendingSince: t0},
			},
		},
		{
			// Ancestor sets are sorted, so "a" would win a naive scan; only a
			// breadth-first walk reports the *nearest* unsettled ancestor.
			name: "attribution names the nearest unsettled ancestor",
			deps: chain("a", "b", "c"),
			inputs: []engine.NodeInput{
				pinnedNode("a", atPin("a1"), failing()),
				pinnedNode("b", atPin("b1"), notReady()),
				pinnedNode("c", pendingFrom("c1", "c2")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateUnhealthy},
				"b": {State: engine.StateConverging},
				"c": {State: engine.StatePending, Blocked: blocked("b", engine.ReasonAncestorPending), PendingSince: t0},
			},
		},
		{
			name: "unready gate blocks its descendant",
			deps: chain("g", "x"),
			inputs: []engine.NodeInput{
				gateNode("g", false),
				pinnedNode("x", pendingFrom("x1", "x2")),
			},
			want: map[string]engine.NodeResult{
				"g": {State: engine.StateUnhealthy},
				"x": {State: engine.StatePending, Blocked: blocked("g", engine.ReasonAncestorUnhealthy), PendingSince: t0},
			},
		},
		{
			name: "ready gate admits its descendant",
			deps: chain("g", "x"),
			inputs: []engine.NodeInput{
				gateNode("g", true),
				pinnedNode("x", pendingFrom("x1", "x2")),
			},
			want: map[string]engine.NodeResult{
				"g": {State: engine.StateSettled},
				"x": {State: engine.StateAdmissible, PendingSince: t0},
			},
			wantAdmissions: []engine.Admission{adm("x", "x1", "x2")},
		},
		{
			name: "held node with pending changes holds itself and its descendant",
			deps: chain("h", "d"),
			inputs: []engine.NodeInput{
				pinnedNode("h", pendingFrom("h1", "h2"), held()),
				pinnedNode("d", pendingFrom("d1", "d2")),
			},
			want: map[string]engine.NodeResult{
				"h": {State: engine.StatePending, Held: true, Blocked: blocked("", engine.ReasonSelfHeld), PendingSince: t0},
				"d": {State: engine.StatePending, Blocked: blocked("h", engine.ReasonAncestorHeld), PendingSince: t0},
			},
		},
		{
			name: "held node with nothing pending is settled",
			deps: chain("h", "d"),
			inputs: []engine.NodeInput{
				pinnedNode("h", atPin("h1"), held()),
				pinnedNode("d", atPin("d1")),
			},
			want: map[string]engine.NodeResult{
				"h": {State: engine.StateSettled, Held: true},
				"d": {State: engine.StateSettled},
			},
		},
		{
			// Finding 4 regression: the held/suspended branch of isSettled must
			// require AppliedSHA == Pin like the normal branch does, or a freshly
			// hand-pinned-but-still-converging ancestor counts settled and its
			// descendant advances mid-rollout.
			name: "held pin observed but not yet applied is not settled and blocks its descendant",
			deps: chain("h", "d"),
			inputs: []engine.NodeInput{
				pinnedNode("h", atPin("s2"), appliedAt("s1"), held()),
				pinnedNode("d", pendingFrom("d1", "d2")),
			},
			want: map[string]engine.NodeResult{
				"h": {State: engine.StateConverging, Held: true},
				"d": {State: engine.StatePending, Blocked: blocked("h", engine.ReasonAncestorHeld), PendingSince: t0},
			},
		},
		{
			name: "suspended source with pending changes behaves exactly as held",
			deps: chain("h", "d"),
			inputs: []engine.NodeInput{
				pinnedNode("h", pendingFrom("h1", "h2"), suspended()),
				pinnedNode("d", pendingFrom("d1", "d2")),
			},
			want: map[string]engine.NodeResult{
				"h": {State: engine.StatePending, Held: true, Blocked: blocked("", engine.ReasonSelfHeld), PendingSince: t0},
				"d": {State: engine.StatePending, Blocked: blocked("h", engine.ReasonAncestorHeld), PendingSince: t0},
			},
		},
		{
			name: "unpinned source with an artifact initial-pins to it despite an unsettled ancestor",
			deps: chain("a", "b"),
			inputs: []engine.NodeInput{
				pinnedNode("a", atPin("a1"), failing()),
				pinnedNode("b", unpinned("art", "obs")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateUnhealthy},
				"b": {State: engine.StateConverging},
			},
			wantInitial: []engine.Admission{initialAdm("b", "art", t0)},
		},
		{
			name:        "unpinned source without an artifact initial-pins to the observed SHA",
			inputs:      []engine.NodeInput{pinnedNode("b", unpinned("", "obs"))},
			want:        map[string]engine.NodeResult{"b": {State: engine.StateConverging}},
			wantInitial: []engine.Admission{initialAdm("b", "obs", t0)},
		},
		{
			name:   "unpinned source with neither artifact nor observation waits",
			inputs: []engine.NodeInput{pinnedNode("b", unpinned("", ""))},
			want:   map[string]engine.NodeResult{"b": {State: engine.StateConverging}},
		},
		{
			// Finding 2 regression: initialPin must also refuse a held/suspended
			// source (DESIGN §3.5.4, §10) even when it has an artifact to pin to.
			name:   "unpinned source that is suspended never initial-pins",
			inputs: []engine.NodeInput{pinnedNode("a", unpinned("art", "obs"), suspended())},
			want:   map[string]engine.NodeResult{"a": {State: engine.StateConverging, Held: true}},
		},
		{
			name:   "unpinned source that is held never initial-pins",
			inputs: []engine.NodeInput{pinnedNode("b", unpinned("art", "obs"), held())},
			want:   map[string]engine.NodeResult{"b": {State: engine.StateConverging, Held: true}},
		},
		{
			// Decision D-E: a Ready, quiescent, suspended-unpinned source counts as
			// settled despite never having been pinned. Since initialPin (fix 1)
			// refuses it a pin while suspended, requiring AppliedSHA == Pin here too
			// would leave it permanently unsettled and livelock every descendant.
			name: "suspended unpinned unobserved ready source is settled and unblocks its descendant",
			deps: chain("s", "d"),
			inputs: []engine.NodeInput{
				pinnedNode("s", suspended()),
				pinnedNode("d", pendingFrom("d1", "d2")),
			},
			want: map[string]engine.NodeResult{
				"s": {State: engine.StateSettled, Held: true},
				"d": {State: engine.StateAdmissible, PendingSince: t0},
			},
			wantAdmissions: []engine.Admission{adm("d", "d1", "d2")},
		},
		{
			name: "unobserved ancestor cannot prove quiescence",
			deps: chain("a", "b"),
			inputs: []engine.NodeInput{
				pinnedNode("a", pinnedUnobserved("a1")),
				pinnedNode("b", pendingFrom("b1", "b2")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateConverging},
				"b": {State: engine.StatePending, Blocked: blocked("a", engine.ReasonAncestorUnobserved), PendingSince: t0},
			},
		},
		{
			name: "an unhealthy branch does not block an independent branch",
			deps: mergeDeps(chain("a", "b"), chain("c", "d")),
			inputs: []engine.NodeInput{
				pinnedNode("a", atPin("a1"), failing()),
				pinnedNode("b", pendingFrom("b1", "b2")),
				pinnedNode("c", atPin("c1")),
				pinnedNode("d", pendingFrom("d1", "d2")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateUnhealthy},
				"b": {State: engine.StatePending, Blocked: blocked("a", engine.ReasonAncestorUnhealthy), PendingSince: t0},
				"c": {State: engine.StateSettled},
				"d": {State: engine.StateAdmissible, PendingSince: t0},
			},
			wantAdmissions: []engine.Admission{adm("d", "d1", "d2")},
		},
		{
			name: "cycle members and their descendants admit nothing",
			deps: map[string][]string{"x": {"y"}, "y": {"x"}, "z": {"y"}},
			inputs: []engine.NodeInput{
				pinnedNode("x", pendingFrom("x1", "x2")),
				pinnedNode("y", pendingFrom("y1", "y2")),
				pinnedNode("z", pendingFrom("z1", "z2")),
				pinnedNode("p", pendingFrom("p1", "p2")),
			},
			want: map[string]engine.NodeResult{
				"x": {State: engine.StatePending, Blocked: blocked("", engine.ReasonGraphCycle), PendingSince: t0},
				"y": {State: engine.StatePending, Blocked: blocked("", engine.ReasonGraphCycle), PendingSince: t0},
				"z": {State: engine.StatePending, Blocked: blocked("", engine.ReasonGraphCycle), PendingSince: t0},
				"p": {State: engine.StateAdmissible, PendingSince: t0},
			},
			wantAdmissions: []engine.Admission{adm("p", "p1", "p2")},
		},
		{
			name:           "a fix on an unhealthy node is the normal admission path",
			inputs:         []engine.NodeInput{pinnedNode("a", pendingFrom("a1", "a2"), failing())},
			want:           map[string]engine.NodeResult{"a": {State: engine.StateAdmissible, PendingSince: t0}},
			wantAdmissions: []engine.Admission{adm("a", "a1", "a2")},
		},
		{
			name:   "a quiescent system is silent",
			inputs: []engine.NodeInput{pinnedNode("a", atPin("a1"))},
			want:   map[string]engine.NodeResult{"a": {State: engine.StateSettled}},
		},
		{
			// Finding 3 (WP2): two admissible siblings sharing one
			// GitRepository must advance it exactly once, not twice.
			name: "two admissible sharers of one source admit exactly once",
			inputs: []engine.NodeInput{
				pinnedNode("a", pendingFrom("s1", "s2"), sourceNamed("shared")),
				pinnedNode("b", pendingFrom("s1", "s2"), sourceNamed("shared")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateAdmissible, PendingSince: t0},
				"b": {State: engine.StateAdmissible, PendingSince: t0},
			},
			wantAdmissions: []engine.Admission{admSrc("a", "shared", "s1", "s2")},
		},
		{
			// A sibling blocked on its own ancestor withholds the shared
			// source from an otherwise-admissible sibling entirely — the
			// least-blocked node may not bypass the other's gating.
			name: "an ancestor-blocked sharer withholds the shared source from its sibling",
			deps: chain("z", "b"),
			inputs: []engine.NodeInput{
				pinnedNode("a", pendingFrom("s1", "s2"), sourceNamed("shared")),
				pinnedNode("z", atPin("z1"), failing()),
				pinnedNode("b", pendingFrom("s1", "s2"), sourceNamed("shared")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StatePending, Blocked: blocked("b", engine.ReasonSharedSourceBlocked), PendingSince: t0},
				"z": {State: engine.StateUnhealthy},
				"b": {State: engine.StatePending, Blocked: blocked("z", engine.ReasonAncestorUnhealthy), PendingSince: t0},
			},
		},
		{
			// A sharer caught in a dependsOn cycle blocks its sibling via
			// the shared source exactly as an ancestor-blocked one does.
			name: "a sharer in a dependsOn cycle blocks its sibling",
			deps: map[string][]string{"x": {"y"}, "y": {"x"}},
			inputs: []engine.NodeInput{
				pinnedNode("x", pendingFrom("s1", "s2"), sourceNamed("monorepo")),
				pinnedNode("y", pendingFrom("y1", "y2")),
				pinnedNode("b", pendingFrom("s1", "s2"), sourceNamed("monorepo")),
			},
			want: map[string]engine.NodeResult{
				"x": {State: engine.StatePending, Blocked: blocked("", engine.ReasonGraphCycle), PendingSince: t0},
				"y": {State: engine.StatePending, Blocked: blocked("", engine.ReasonGraphCycle), PendingSince: t0},
				"b": {State: engine.StatePending, Blocked: blocked("x", engine.ReasonSharedSourceBlocked), PendingSince: t0},
			},
		},
		{
			name: "an unpinned shared source initial-pins exactly once",
			inputs: []engine.NodeInput{
				pinnedNode("a", unpinned("art", "obs"), sourceNamed("shared")),
				pinnedNode("b", unpinned("art", "obs"), sourceNamed("shared")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateConverging},
				"b": {State: engine.StateConverging},
			},
			wantInitial: []engine.Admission{initialAdmSrc("a", "shared", "art", t0)},
		},
		{
			// A three-node mix proves the gate is scoped to the shared
			// source: an unrelated independent node's own admission is
			// untouched by the sharers' gating.
			name: "an independent node is unaffected by a sibling pair's gating",
			inputs: []engine.NodeInput{
				pinnedNode("a", pendingFrom("s1", "s2"), sourceNamed("shared")),
				pinnedNode("b", pendingFrom("s1", "s2"), sourceNamed("shared")),
				pinnedNode("c", pendingFrom("c1", "c2")),
			},
			want: map[string]engine.NodeResult{
				"a": {State: engine.StateAdmissible, PendingSince: t0},
				"b": {State: engine.StateAdmissible, PendingSince: t0},
				"c": {State: engine.StateAdmissible, PendingSince: t0},
			},
			wantAdmissions: []engine.Admission{admSrc("a", "shared", "s1", "s2"), adm("c", "c1", "c2")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := buildGraph(t, tc.inputs, tc.deps)
			got := engine.Evaluate(g, inputMap(tc.inputs))

			want := make(map[adapter.NodeRef]engine.NodeResult, len(tc.want))
			for name, res := range tc.want {
				want[ref(name)] = res
			}
			if !reflect.DeepEqual(got.Nodes, want) {
				t.Errorf("nodes mismatch:\n got: %s\nwant: %s", formatNodes(got.Nodes), formatNodes(want))
			}
			if !equalAdmissions(got.Admissions, tc.wantAdmissions) {
				t.Errorf("admissions mismatch:\n got: %+v\nwant: %+v", got.Admissions, tc.wantAdmissions)
			}
			if !equalAdmissions(got.Initial, tc.wantInitial) {
				t.Errorf("initial admissions mismatch:\n got: %+v\nwant: %+v", got.Initial, tc.wantInitial)
			}
		})
	}
}

// checkInitialAdmissions asserts the invariants of every initial pin (rule 5):
// ungated (From == ""), Initial == true, its To matches the node's source,
// and — Finding 2 / DESIGN §3.5.4, §10 — its source is neither Held nor
// Suspended.
func checkInitialAdmissions(t *testing.T, initial []engine.Admission, inputs map[adapter.NodeRef]engine.NodeInput) {
	t.Helper()
	for _, a := range initial {
		if a.From != "" {
			t.Errorf("initial admission %+v has non-empty From", a)
		}
		if !a.Initial {
			t.Errorf("initial admission %+v has Initial=false", a)
		}
		src := inputs[a.Node].Source
		switch {
		case src == nil:
			t.Errorf("initial admission %s which has no source", a.Node)
		case src.Held || src.Suspended:
			t.Errorf("initial admission %s whose source is held or suspended", a.Node)
		case a.To != src.ArtifactSHA && a.To != src.ObservedSHA:
			t.Errorf("initial admission %+v matches neither artifact %q nor observed %q", a, src.ArtifactSHA, src.ObservedSHA)
		}
	}
}

// checkSharedSourceInvariants asserts WP2's shared-GitRepository gate holds
// for every generated graph, not just the tabled cases: at most one entry
// per Source across Admissions+Initial combined, and — since pending-ness is
// source-derived — a source with any non-Admissible pinned referencing node
// never has a non-initial admission.
func checkSharedSourceInvariants(t *testing.T, got engine.Evaluation, inputs map[adapter.NodeRef]engine.NodeInput) {
	t.Helper()

	grantedBy := map[types.NamespacedName]adapter.NodeRef{}
	for _, a := range slices.Concat(got.Admissions, got.Initial) {
		if prior, dup := grantedBy[a.Source]; dup {
			t.Errorf("source %s granted to both %s and %s, want at most one", a.Source, prior, a.Node)
		}
		grantedBy[a.Source] = a.Node
	}

	admittedSource := map[types.NamespacedName]bool{}
	for _, a := range got.Admissions {
		admittedSource[a.Source] = true
	}
	referencingPinned := map[types.NamespacedName][]adapter.NodeRef{}
	for ref, in := range inputs {
		if in.Role == engine.RolePinned && in.Source != nil {
			referencingPinned[in.Source.Source] = append(referencingPinned[in.Source.Source], ref)
		}
	}
	for src, refs := range referencingPinned {
		for _, ref := range refs {
			if got.Nodes[ref].State != engine.StateAdmissible && admittedSource[src] {
				t.Errorf("source %s has non-admissible referencing node %s but still carries a non-initial admission",
					src, ref)
			}
		}
	}
}

func equalAdmissions(got, want []engine.Admission) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestEvaluateProperties exercises the invariants that must hold for every
// acyclic graph and every combination of node states, not just the tabled
// ones: the settled-ancestors rule (DESIGN D13), admissions being a subset of
// pending nodes, and determinism (rule 8).
func TestEvaluateProperties(t *testing.T) {
	const iterations = 300
	base := seedFromName(t.Name())

	for i := range uint64(iterations) {
		t.Run(fmt.Sprintf("case-%03d", i), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(base, i))
			edges, inputs := randomDAG(rng)

			g := graph.Build(edges)
			got := engine.Evaluate(g, inputs)

			if len(got.Nodes) != len(inputs) {
				t.Fatalf("evaluated %d nodes, want %d", len(got.Nodes), len(inputs))
			}

			for _, a := range got.Admissions {
				// (a) No admission for a node with any unsettled transitive
				// ancestor (DESIGN D13).
				for _, ancestor := range g.TransitiveAncestors(a.Node) {
					if state := got.Nodes[ancestor].State; state != engine.StateSettled {
						t.Errorf("admitted %s whose ancestor %s is %s, not Settled", a.Node, ancestor, state)
					}
				}

				// (b) Admissions are a subset of the pending, unheld nodes.
				if state := got.Nodes[a.Node].State; state != engine.StateAdmissible {
					t.Errorf("admitted %s in state %s, want Admissible", a.Node, state)
				}
				src := inputs[a.Node].Source
				switch {
				case src == nil:
					t.Errorf("admitted %s which has no source", a.Node)
				case src.Pin == "" || src.ObservedSHA == "" || src.ObservedSHA == src.Pin:
					t.Errorf("admitted %s which is not pending (pin %q, observed %q)", a.Node, src.Pin, src.ObservedSHA)
				case src.Held || src.Suspended:
					t.Errorf("admitted %s which is externally held", a.Node)
				default:
					if a.From != src.Pin || a.To != src.ObservedSHA {
						t.Errorf("admission %+v does not advance %q -> %q", a, src.Pin, src.ObservedSHA)
					}
				}
			}

			checkInitialAdmissions(t, got.Initial, inputs)
			checkSharedSourceInvariants(t, got, inputs)

			for ref, res := range got.Nodes {
				if (res.Blocked != nil) != (res.State == engine.StatePending) {
					t.Errorf("%s: state %s with blocked=%v", ref, res.State, res.Blocked)
				}
				if res.State != engine.StatePending && res.State != engine.StateAdmissible && !res.PendingSince.IsZero() {
					t.Errorf("%s: state %s carries PendingSince %s", ref, res.State, res.PendingSince)
				}
			}

			// The generator only emits DAGs, so nothing may be attributed to a
			// cycle; this also guards the generator itself.
			for ref := range inputs {
				if g.InCycle(ref) {
					t.Fatalf("generated graph is cyclic at %s", ref)
				}
			}

			// (c) Determinism: identical inputs, deeply equal results — across
			// two evaluations and two independently built graphs (Go randomizes
			// map iteration order, so this genuinely re-rolls it).
			if again := engine.Evaluate(g, inputs); !reflect.DeepEqual(got, again) {
				t.Errorf("evaluation is not deterministic:\n first: %+v\nsecond: %+v", got, again)
			}
			if rebuilt := engine.Evaluate(graph.Build(edges), inputs); !reflect.DeepEqual(got, rebuilt) {
				t.Errorf("evaluation depends on graph build order:\n first: %+v\nsecond: %+v", got, rebuilt)
			}
		})
	}
}

// seedFromName derives a fixed seed from the test name, so runs are
// reproducible and independent of the wall clock.
func seedFromName(name string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return h.Sum64()
}

// randomDAG generates edges (only ever from a later node to an earlier one,
// which makes cycles impossible) and a random state for every node.
func randomDAG(rng *rand.Rand) (map[adapter.NodeRef][]adapter.NodeRef, map[adapter.NodeRef]engine.NodeInput) {
	n := 1 + rng.IntN(12)
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("n%02d", i)
	}

	edges := make(map[adapter.NodeRef][]adapter.NodeRef, n)
	inputs := make(map[adapter.NodeRef]engine.NodeInput, n)
	for i, name := range names {
		var deps []adapter.NodeRef
		for j := range i {
			if rng.Float64() < 0.35 {
				deps = append(deps, ref(names[j]))
			}
		}
		edges[ref(name)] = deps
		in := randomInput(rng, name)
		// WP2: draw source identity from a small pool with some probability,
		// reusing an earlier node's source name, so shared-GitRepository
		// collisions actually occur rather than being vanishingly rare.
		if in.Role == engine.RolePinned && rng.Float64() < 0.5 {
			in.Source.Source = srcName(fmt.Sprintf("pool%d", rng.IntN(3)))
		}
		inputs[ref(name)] = in
	}
	return edges, inputs
}

// randomInput picks one node state from across the whole state machine.
// Ready and Failing are never both set — no adapter can report that.
func randomInput(rng *rand.Rand, name string) engine.NodeInput {
	if rng.Float64() < 0.2 {
		return gateNode(name, rng.Float64() < 0.6)
	}
	switch rng.IntN(11) {
	case 0:
		return pinnedNode(name, atPin("s1"))
	case 1:
		return pinnedNode(name, pendingFrom("s1", "s2"))
	case 2:
		return pinnedNode(name, atPin("s1"), notReady())
	case 3:
		return pinnedNode(name, atPin("s1"), failing())
	case 4:
		return pinnedNode(name, pendingFrom("s1", "s2"), held())
	case 5:
		return pinnedNode(name, pendingFrom("s1", "s2"), suspended())
	case 6:
		return pinnedNode(name, unpinned("art", "obs"))
	case 7:
		// Finding 2 shape: unpinned + suspended.
		return pinnedNode(name, unpinned("art", "obs"), suspended())
	case 8:
		// Finding 2 shape: unpinned + held.
		return pinnedNode(name, unpinned("art", "obs"), held())
	case 9:
		// Finding 4 shape: held, pin observed, workload not yet applied.
		return pinnedNode(name, atPin("s2"), appliedAt("s1"), held())
	default:
		return pinnedNode(name, pinnedUnobserved("s1"))
	}
}

func formatNodes(nodes map[adapter.NodeRef]engine.NodeResult) string {
	byName := func(a, b adapter.NodeRef) int { return cmp.Compare(a.String(), b.String()) }
	var out strings.Builder
	for _, r := range slices.SortedFunc(maps.Keys(nodes), byName) {
		res := nodes[r]
		fmt.Fprintf(&out, "\n  %s => %+v", r, res)
		if res.Blocked != nil {
			fmt.Fprintf(&out, " blocked=%+v", *res.Blocked)
		}
	}
	return out.String()
}
