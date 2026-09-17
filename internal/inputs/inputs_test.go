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
	"context"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
)

// The source-resolution, observation-plumbing and hold coverage below moved
// here with the code it exercises (WP2/WP3/WP6): these are properties of the
// pure derivation, not of the reconciler that publishes it. The fake adapter
// stands in for the Kustomization reads discovery would otherwise perform, so
// each case still states its node topology directly.

const (
	kindKustomization = "Kustomization"
	fluxNamespace     = "flux-system"
	fleetName         = "fleet"
	teamAName         = "team-a"
	teamBName         = "team-b"

	managedLabelValue = "true"
	// Field manager standing in for a human hand-pin (DESIGN §3.5.3).
	humanManager = "kubectl-edit"

	mainRef = "refs/heads/main"

	shaA    = "1111111111111111111111111111111111111111"
	shaHand = "3333333333333333333333333333333333333333"
)

func nodeRef(name string) adapter.NodeRef {
	return adapter.NodeRef{Kind: kindKustomization, Namespace: fluxNamespace, Name: name}
}

func teamARef() adapter.NodeRef { return nodeRef(teamAName) }
func teamBRef() adapter.NodeRef { return nodeRef(teamBName) }

// fakeAdapter serves a fixed node topology: selected is what the selector
// matches (discovery's List), and every node in nodes is reachable by Get, so
// a dependsOn target outside the selector is dragged in by the closure exactly
// as a real adapter would.
type fakeAdapter struct {
	nodes    map[adapter.NodeRef]adapter.Node
	selected []adapter.NodeRef
}

func (f *fakeAdapter) Kind() string { return kindKustomization }

func (f *fakeAdapter) List(_ context.Context, _ client.Reader, _ labels.Selector) ([]adapter.Node, error) {
	out := make([]adapter.Node, 0, len(f.selected))
	for _, ref := range f.selected {
		out = append(out, f.nodes[ref])
	}
	return out, nil
}

func (f *fakeAdapter) Get(_ context.Context, _ client.Reader, ref adapter.NodeRef) (adapter.Node, bool, error) {
	node, found := f.nodes[ref]
	return node, found, nil
}

// countingReader counts GitRepository reads, which is how the memoization
// decision (D-A) is observable from outside: one read per source per pass, not
// one per referencing node.
type countingReader struct {
	client.Reader
	repoGets int
}

func (c *countingReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	if _, ok := obj.(*sourcev1.GitRepository); ok {
		c.repoGets++
	}
	return c.Reader.Get(ctx, key, obj, opts...)
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := sourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(sourcev1): %v", err)
	}
	if err := wavefrontv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(wavefront): %v", err)
	}
	return scheme
}

func testWavefront() *wavefrontv1alpha1.Wavefront {
	return &wavefrontv1alpha1.Wavefront{Name: fleetName}
}

// managedRepo is a GitRepository opted into pin management.
func managedRepo(name, url string, ref *sourcev1.GitRepositoryRef) *sourcev1.GitRepository {
	return &sourcev1.GitRepository{
		Namespace: fluxNamespace,
		Name:      name,
		Labels:    map[string]string{pin.ManagedLabel: managedLabelValue},
		Spec:      sourcev1.GitRepositorySpec{URL: url, Reference: ref},
	}
}

// buildFor runs one pass over the given topology.
func buildFor(
	t *testing.T,
	reader client.Reader,
	a adapter.Adapter,
	observations map[types.NamespacedName]gitpoll.Observation,
) *Result {
	t.Helper()
	res, err := Build(context.Background(), reader, Params{
		Wavefront:    testWavefront(),
		Adapter:      a,
		Strategy:     selection.TrackRef(),
		Observations: observations,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return res
}

// --- shared-source resolution (WP2) -----------------------------------------

// TestBuildMemoizesASharedSource is decision D-A's counterpart to the engine's
// gateSharedSources: two Kustomizations sharing one GitRepository must see one
// read, one *engine.SourceState and one poll Target, and NodeBySource must list
// both referencing nodes rather than silently keeping only the last writer.
func TestBuildMemoizesASharedSource(t *testing.T) {
	src := types.NamespacedName{Namespace: fluxNamespace, Name: "shared"}
	repo := managedRepo(src.Name, "https://git.example.com/org/shared.git",
		&sourcev1.GitRepositoryRef{Name: mainRef})

	reader := &countingReader{Reader: fake.NewClientBuilder().
		WithScheme(testScheme(t)).WithObjects(repo).Build()}

	nodeA, nodeB := teamARef(), teamBRef()
	nodes := &fakeAdapter{
		nodes: map[adapter.NodeRef]adapter.Node{
			nodeA: {Ref: nodeA, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
			nodeB: {Ref: nodeB, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
		},
		selected: []adapter.NodeRef{nodeA, nodeB},
	}

	res := buildFor(t, reader, nodes, nil)

	stateA, stateB := res.Inputs[nodeA].Source, res.Inputs[nodeB].Source
	if stateA == nil || stateB == nil {
		t.Fatalf("both sharers must resolve pinned, got a=%+v b=%+v", res.Inputs[nodeA], res.Inputs[nodeB])
	}
	if stateA != stateB {
		t.Errorf("sharers got distinct *SourceState pointers (%p, %p), want the memoized one shared", stateA, stateB)
	}

	if reader.repoGets != 1 {
		t.Errorf("GitRepository reads = %d, want exactly 1 for the shared source, not one per referencing node",
			reader.repoGets)
	}
	if len(res.Targets) != 1 {
		t.Errorf("targets = %d, want exactly 1 for the shared source, not one per referencing node", len(res.Targets))
	}

	if got, want := res.NodeBySource[src], []adapter.NodeRef{nodeA, nodeB}; !slices.Equal(got, want) {
		t.Errorf("NodeBySource[%s] = %v, want both referencing nodes in compareRefs order %v", src, got, want)
	}
}

// TestBuildSkipsASourceReferencedOnlyByNonSelectedNodes is the fix for the
// WP2 review finding: a source with no selected referencing node must never
// register a poll Target or report an unsupported ref style — that would leak a
// source this Wavefront has zero selected interest in into its pollSet
// contribution (and misattribute the warning) purely because a
// dependency-closure gate happens to reference it. The source's ref style is
// deliberately unsupported (SemVer), the worst case: even that must not
// resolve or be reported when nothing selected reaches it.
func TestBuildSkipsASourceReferencedOnlyByNonSelectedNodes(t *testing.T) {
	src := types.NamespacedName{Namespace: fluxNamespace, Name: "upstream"}
	repo := managedRepo(src.Name, "https://git.example.com/org/upstream.git",
		&sourcev1.GitRepositoryRef{SemVer: ">=1.0.0"})

	reader := &countingReader{Reader: fake.NewClientBuilder().
		WithScheme(testScheme(t)).WithObjects(repo).Build()}

	// gateOnly is reachable only through the selected node's dependsOn closure.
	selected, gateOnly := teamBRef(), teamARef()
	nodes := &fakeAdapter{
		nodes: map[adapter.NodeRef]adapter.Node{
			selected: {Ref: selected, DependsOn: []adapter.NodeRef{gateOnly}, Readiness: adapter.Readiness{Ready: true}},
			gateOnly: {Ref: gateOnly, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
		},
		selected: []adapter.NodeRef{selected},
	}

	res := buildFor(t, reader, nodes, nil)

	if got := res.Inputs[gateOnly]; got.Role != engine.RoleGate || got.Source != nil {
		t.Errorf("non-selected referencing node = %+v, want a plain gate with no Source", got)
	}
	if len(res.Targets) != 0 {
		t.Errorf("targets = %v, want none: no selected node references this source", res.Targets)
	}
	if len(res.NodeBySource) != 0 {
		t.Errorf("NodeBySource = %v, want empty", res.NodeBySource)
	}
	if reader.repoGets != 0 {
		t.Errorf("GitRepository reads = %d, want none: the source was never resolved", reader.repoGets)
	}
	if len(res.UnsupportedSources) != 0 {
		t.Errorf("UnsupportedSources = %v, want none: an unsupported ref style must not be reported for a source no selected node references",
			res.UnsupportedSources)
	}
}

// TestBuildMixedSelectedAndGateSharersOfOneSource covers the mixed topology
// the mono-repo case above simplified away: one selected (pinned) node and one
// non-selected (gate) node referencing the same source. The source still
// resolves exactly once (one Target), the selected node is pinned to it, and
// the gate node stays a plain gate — never added to NodeBySource.
func TestBuildMixedSelectedAndGateSharersOfOneSource(t *testing.T) {
	src := types.NamespacedName{Namespace: fluxNamespace, Name: "shared"}
	repo := managedRepo(src.Name, "https://git.example.com/org/shared.git",
		&sourcev1.GitRepositoryRef{Name: mainRef})

	reader := &countingReader{Reader: fake.NewClientBuilder().
		WithScheme(testScheme(t)).WithObjects(repo).Build()}

	pinned, gate := teamARef(), teamBRef()
	nodes := &fakeAdapter{
		nodes: map[adapter.NodeRef]adapter.Node{
			pinned: {
				Ref:       pinned,
				SourceRef: &src,
				DependsOn: []adapter.NodeRef{gate},
				Readiness: adapter.Readiness{Ready: true},
			},
			gate: {Ref: gate, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
		},
		selected: []adapter.NodeRef{pinned}, // gate deliberately absent
	}

	res := buildFor(t, reader, nodes, nil)

	if got := res.Inputs[pinned]; got.Role != engine.RolePinned || got.Source == nil {
		t.Errorf("selected node = %+v, want RolePinned with a Source", got)
	}
	if got := res.Inputs[gate]; got.Role != engine.RoleGate || got.Source != nil {
		t.Errorf("non-selected node = %+v, want a plain gate with no Source", got)
	}
	if len(res.Targets) != 1 {
		t.Errorf("targets = %d, want exactly 1: registered once, by the selected node", len(res.Targets))
	}
	if got, want := res.NodeBySource[src], []adapter.NodeRef{pinned}; !slices.Equal(got, want) {
		t.Errorf("NodeBySource[%s] = %v, want only the selected node %v", src, got, want)
	}
}

// TestBuildRecordsAnUnsupportedSourceOncePerSource is the positive counterpart:
// a selected node's source whose ref style v1 cannot sequence is demoted to a
// gate and recorded exactly once, however many selected nodes reference it, so
// the caller announces it exactly once (UnsupportedRefStyle, DESIGN D10).
func TestBuildRecordsAnUnsupportedSourceOncePerSource(t *testing.T) {
	src := types.NamespacedName{Namespace: fluxNamespace, Name: "semver"}
	repo := managedRepo(src.Name, "https://git.example.com/org/semver.git",
		&sourcev1.GitRepositoryRef{SemVer: ">=1.0.0"})

	reader := &countingReader{Reader: fake.NewClientBuilder().
		WithScheme(testScheme(t)).WithObjects(repo).Build()}

	nodeA, nodeB := teamARef(), teamBRef()
	nodes := &fakeAdapter{
		nodes: map[adapter.NodeRef]adapter.Node{
			nodeA: {Ref: nodeA, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
			nodeB: {Ref: nodeB, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}},
		},
		selected: []adapter.NodeRef{nodeA, nodeB},
	}

	res := buildFor(t, reader, nodes, nil)

	want := []types.NamespacedName{src}
	if !slices.Equal(res.UnsupportedSources, want) {
		t.Errorf("UnsupportedSources = %v, want %v exactly once for two referencing nodes",
			res.UnsupportedSources, want)
	}
	for _, ref := range []adapter.NodeRef{nodeA, nodeB} {
		if got := res.Inputs[ref]; got.Role != engine.RoleGate || got.Source != nil {
			t.Errorf("node %s = %+v, want demotion to a plain gate", ref, got)
		}
	}
	if len(res.Targets) != 0 {
		t.Errorf("targets = %v, want none: an unsequenceable ref style is not polled", res.Targets)
	}
}

// --- observation plumbing (WP6, DESIGN §3.1) --------------------------------
//
// Params.Observations is the caller's snapshot, taken before anything prunes
// stale records for this pass, so an Observation surviving from a
// GitRepository's old plumbing can still be present when a source resolves.
// Resolution must reject it itself rather than rely on the poller having
// pruned it already.

func TestBuildRejectsObservationFromStaleTrackingRef(t *testing.T) {
	src := types.NamespacedName{Namespace: fluxNamespace, Name: "retagged"}
	repoURL := "https://git.example.com/org/retagged.git"
	repo := managedRepo(src.Name, repoURL, &sourcev1.GitRepositoryRef{Tag: "v2"})

	reader := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(repo).Build()

	node := teamARef()
	nodes := &fakeAdapter{
		nodes:    map[adapter.NodeRef]adapter.Node{node: {Ref: node, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}}},
		selected: []adapter.NodeRef{node},
	}

	res := buildFor(t, reader, nodes, map[types.NamespacedName]gitpoll.Observation{
		src: {SHA: shaA, FirstObserved: time.Unix(1000, 0), ObservedAt: time.Unix(1000, 0), URL: repoURL, TrackingRef: mainRef},
	})

	state := res.Inputs[node].Source
	if state == nil {
		t.Fatal("Source is nil, want a resolved SourceState")
	}
	if state.ObservedSHA != "" {
		t.Errorf("ObservedSHA = %q, want \"\": the observation predates the retag", state.ObservedSHA)
	}
	if !state.FirstObserved.IsZero() {
		t.Errorf("FirstObserved = %v, want zero: the observation predates the retag", state.FirstObserved)
	}
}

func TestBuildRejectsObservationFromStaleURL(t *testing.T) {
	src := types.NamespacedName{Namespace: fluxNamespace, Name: "repointed"}
	repo := managedRepo(src.Name, "https://git.example.com/org/repointed-new.git",
		&sourcev1.GitRepositoryRef{Name: mainRef})

	reader := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(repo).Build()

	node := teamARef()
	nodes := &fakeAdapter{
		nodes:    map[adapter.NodeRef]adapter.Node{node: {Ref: node, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}}},
		selected: []adapter.NodeRef{node},
	}

	res := buildFor(t, reader, nodes, map[types.NamespacedName]gitpoll.Observation{
		src: {
			SHA: shaA, FirstObserved: time.Unix(1000, 0), ObservedAt: time.Unix(1000, 0),
			URL: "https://git.example.com/org/repointed-old.git", TrackingRef: mainRef,
		},
	})

	state := res.Inputs[node].Source
	if state == nil {
		t.Fatal("Source is nil, want a resolved SourceState")
	}
	if state.ObservedSHA != "" {
		t.Errorf("ObservedSHA = %q, want \"\": the observation predates the URL change", state.ObservedSHA)
	}
	if !state.FirstObserved.IsZero() {
		t.Errorf("FirstObserved = %v, want zero: the observation predates the URL change", state.FirstObserved)
	}
}

// TestBuildAcceptsObservationMatchingPlumbing: an Observation whose URL and
// TrackingRef both match the GitRepository's current plumbing flows through
// unchanged — the baseline the two rejection tests above are contrasted
// against.
func TestBuildAcceptsObservationMatchingPlumbing(t *testing.T) {
	src := types.NamespacedName{Namespace: fluxNamespace, Name: "steady"}
	repoURL := "https://git.example.com/org/steady.git"
	repo := managedRepo(src.Name, repoURL, &sourcev1.GitRepositoryRef{Name: mainRef})

	reader := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(repo).Build()

	firstObserved := time.Unix(1000, 0)
	node := teamARef()
	nodes := &fakeAdapter{
		nodes:    map[adapter.NodeRef]adapter.Node{node: {Ref: node, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}}},
		selected: []adapter.NodeRef{node},
	}

	res := buildFor(t, reader, nodes, map[types.NamespacedName]gitpoll.Observation{
		src: {SHA: shaA, FirstObserved: firstObserved, ObservedAt: time.Unix(2000, 0), URL: repoURL, TrackingRef: mainRef},
	})

	state := res.Inputs[node].Source
	if state == nil {
		t.Fatal("Source is nil, want a resolved SourceState")
	}
	if state.ObservedSHA != shaA {
		t.Errorf("ObservedSHA = %q, want %q", state.ObservedSHA, shaA)
	}
	if !state.FirstObserved.Equal(firstObserved) {
		t.Errorf("FirstObserved = %v, want %v", state.FirstObserved, firstObserved)
	}
}

// --- holds (WP3): suspended sources unify with hand-pins -------------------

// TestBuildSuspendedSourceYieldsSuspendHold covers finding 7: a suspended
// source must land in Holds (kind Suspend, no manager), not just in the
// engine's Held/Blocked signal.
func TestBuildSuspendedSourceYieldsSuspendHold(t *testing.T) {
	src := types.NamespacedName{Namespace: fluxNamespace, Name: "suspended"}
	repo := managedRepo(src.Name, "https://git.example.com/org/suspended.git",
		&sourcev1.GitRepositoryRef{Name: mainRef})
	repo.Spec.Suspend = true

	reader := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(repo).Build()

	node := teamARef()
	nodes := &fakeAdapter{
		nodes:    map[adapter.NodeRef]adapter.Node{node: {Ref: node, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}}},
		selected: []adapter.NodeRef{node},
	}

	res := buildFor(t, reader, nodes, nil)

	got, ok := res.Holds[src]
	if !ok {
		t.Fatalf("Holds[%s] missing, want a Suspend hold", src)
	}
	if got.Kind != HoldSuspend || got.Manager != "" {
		t.Errorf("hold = %+v, want {Manager: \"\", Kind: Suspend}", got)
	}
}

// TestBuildHandPinAndSuspendYieldsHandPin: a source both hand-pinned and
// suspended reports HandPin — it names an actor, so it wins over the
// actor-less Suspend (decision D-B).
func TestBuildHandPinAndSuspendYieldsHandPin(t *testing.T) {
	src := types.NamespacedName{Namespace: fluxNamespace, Name: "both"}
	repo := managedRepo(src.Name, "https://git.example.com/org/both.git",
		&sourcev1.GitRepositoryRef{Name: mainRef})
	repo.Spec.Suspend = true

	fakeClient := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(repo).WithReturnManagedFields().Build()

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

	node := teamARef()
	nodes := &fakeAdapter{
		nodes:    map[adapter.NodeRef]adapter.Node{node: {Ref: node, SourceRef: &src, Readiness: adapter.Readiness{Ready: true}}},
		selected: []adapter.NodeRef{node},
	}

	res := buildFor(t, fakeClient, nodes, nil)

	got, ok := res.Holds[src]
	if !ok {
		t.Fatalf("Holds[%s] missing, want a HandPin hold", src)
	}
	if got.Kind != HoldHandPin || got.Manager != humanManager {
		t.Errorf("hold = %+v, want {Manager: %q, Kind: HandPin} even though the source is also suspended",
			got, humanManager)
	}
}

// --- the structural verdict --------------------------------------------------

// TestGraphVerdictOverlapOutranksCycles pins the precedence and the exact
// message strings: a Wavefront reporting a cycle while another Wavefront is
// double-managing its nodes would hide the more urgent, admission-suppressing
// configuration error (DESIGN §4.1).
func TestGraphVerdictOverlapOutranksCycles(t *testing.T) {
	cycle := [][]adapter.NodeRef{{teamARef(), teamBRef()}}

	cases := []struct {
		name       string
		res        *Result
		wantValid  bool
		wantReason string
		wantIn     string
	}{
		{
			name:       "clean",
			res:        &Result{},
			wantValid:  true,
			wantReason: wavefrontv1alpha1.GraphValidReasonValid,
			wantIn:     "no dependsOn cycles",
		},
		{
			name:       "cycle only",
			res:        &Result{Cycles: cycle},
			wantReason: wavefrontv1alpha1.GraphValidReasonCyclesDetected,
			wantIn:     "dependsOn cycle: Kustomization/flux-system/team-a -> Kustomization/flux-system/team-b",
		},
		{
			name:       "overlap only",
			res:        &Result{Overlap: "other"},
			wantReason: wavefrontv1alpha1.GraphValidReasonSelectorOverlap,
			wantIn:     `node selector overlaps Wavefront "other"; admissions suppressed`,
		},
		{
			name:       "both: overlap wins",
			res:        &Result{Overlap: "other", Cycles: cycle},
			wantReason: wavefrontv1alpha1.GraphValidReasonSelectorOverlap,
			wantIn:     `node selector overlaps Wavefront "other"; admissions suppressed`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verdict := tc.res.GraphVerdict()
			if verdict.Valid != tc.wantValid {
				t.Errorf("Valid = %v, want %v", verdict.Valid, tc.wantValid)
			}
			if verdict.Reason != tc.wantReason {
				t.Errorf("Reason = %q, want %q", verdict.Reason, tc.wantReason)
			}
			if !strings.Contains(verdict.Message, tc.wantIn) {
				t.Errorf("Message = %q, want it to contain %q", verdict.Message, tc.wantIn)
			}
			if got, want := tc.res.SkipAdmissions(), tc.res.Overlap != ""; got != want {
				t.Errorf("SkipAdmissions() = %v, want %v: only an overlap suppresses writes", got, want)
			}
		})
	}
}

// --- purity ------------------------------------------------------------------

// TestPackageStaysPure is the architectural constraint in test form: this
// package is the shared derivation, so it must stay free of everything that
// makes the reconciler a reconciler — metrics, events, the reconciler itself.
// A CLI links it and has none of those.
//
// The import list is read from the package's own sources rather than from
// `go list`, so the check is hermetic and needs no toolchain invocation.
func TestPackageStaysPure(t *testing.T) {
	banned := []string{
		"github.com/isometry/wavefront-controller/internal/metrics",
		"github.com/isometry/wavefront-controller/internal/controller",
		"k8s.io/client-go/tools/events",
	}
	// Everything this package is allowed to reach for. Anything else is a new
	// dependency that has to be argued for, not acquired by accident.
	allowed := []string{
		"github.com/isometry/wavefront-controller/api/v1alpha1",
		"github.com/isometry/wavefront-controller/internal/adapter",
		"github.com/isometry/wavefront-controller/internal/engine",
		"github.com/isometry/wavefront-controller/internal/gitpoll",
		"github.com/isometry/wavefront-controller/internal/graph",
		"github.com/isometry/wavefront-controller/internal/pin",
		"github.com/isometry/wavefront-controller/internal/selection",
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package directory: %v", err)
	}

	fset := token.NewFileSet()
	seen := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, spec := range file.Imports {
			seen[strings.Trim(spec.Path.Value, `"`)] = true
		}
	}
	if len(seen) == 0 {
		t.Fatal("no imports found, want the package's own sources to have been parsed")
	}

	for path := range seen {
		if slices.Contains(banned, path) {
			t.Errorf("imports %q: this package must stay free of the reconciler's collaborators", path)
		}
		if strings.HasPrefix(path, "github.com/isometry/wavefront-controller/") && !slices.Contains(allowed, path) {
			t.Errorf("imports %q, which is not on the allowlist: a new in-repo dependency needs arguing for", path)
		}
	}
}
