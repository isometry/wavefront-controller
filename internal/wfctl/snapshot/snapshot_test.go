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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
)

// Shared fixture literals, named so the same string in two files means the
// same thing.
const (
	nsApps      = "apps"
	nsFlux      = "flux-system"
	refMain     = "refs/heads/main"
	testRepoURL = "https://github.com/org/repo.git"
	testSHA     = "0123456789abcdef0123456789abcdef01234567"
	secretName  = "deploy-key"
	nodeWeb     = "web"
	wfName      = "fleet"
)

// ref is the test shorthand for a Kustomization node reference.
func ref(name string) adapter.NodeRef {
	return adapter.NodeRef{Kind: kustomizev1.KustomizationKind, Namespace: nsApps, Name: name}
}

// node builds a NodeView with the given dependsOn edges; Waves reads nothing
// else.
func node(name string, deps ...string) NodeView {
	view := NodeView{Ref: ref(name)}
	for _, dep := range deps {
		view.DependsOn = append(view.DependsOn, ref(dep))
	}
	return view
}

func TestWaves(t *testing.T) {
	tests := []struct {
		name  string
		nodes []NodeView
		want  map[adapter.NodeRef]int
	}{
		{
			name:  "isolated nodes are all wave 0",
			nodes: []NodeView{node("a"), node("b")},
			want:  map[adapter.NodeRef]int{ref("a"): 0, ref("b"): 0},
		},
		{
			name:  "a chain layers one per hop",
			nodes: []NodeView{node("a"), node("b", "a"), node("c", "b")},
			want:  map[adapter.NodeRef]int{ref("a"): 0, ref("b"): 1, ref("c"): 2},
		},
		{
			// d depends on two branches of different depths and must take
			// the deeper: a wave is 1 + the *deepest* dependency, not the
			// first one relaxed.
			name: "a diamond takes the deeper branch",
			nodes: []NodeView{
				node("a"),
				node("b", "a"),
				node("c", "b"),
				node("d", "a", "c"),
			},
			want: map[adapter.NodeRef]int{ref("a"): 0, ref("b"): 1, ref("c"): 2, ref("d"): 3},
		},
		{
			// Neither cycle member ever dequeues, and neither does anything
			// behind them: no depth is meaningful once ordering is undefined.
			name: "a cycle and everything behind it is unlayered",
			nodes: []NodeView{
				node("a"),
				node("b", "c"),
				node("c", "b"),
				node("d", "c"),
			},
			want: map[adapter.NodeRef]int{ref("a"): 0, ref("b"): -1, ref("c"): -1, ref("d"): -1},
		},
		{
			name:  "a self-loop is unlayered",
			nodes: []NodeView{node("a", "a")},
			want:  map[adapter.NodeRef]int{ref("a"): -1},
		},
		{
			// The dangling target is not in the node list, but it still
			// layers at 0 as the gate it behaves as, so its dependant layers
			// at 1 rather than being mistaken for a cycle.
			name:  "a missing dependency is a wave 0 gate",
			nodes: []NodeView{node("b", "gone")},
			want:  map[adapter.NodeRef]int{ref("gone"): 0, ref("b"): 1},
		},
		{
			name:  "a duplicated edge is counted once",
			nodes: []NodeView{node("a"), node("b", "a", "a")},
			want:  map[adapter.NodeRef]int{ref("a"): 0, ref("b"): 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Waves(tc.nodes)
			if len(got) != len(tc.want) {
				t.Fatalf("Waves() has %d entries, want %d: %v", len(got), len(tc.want), got)
			}
			for r, want := range tc.want {
				if got[r] != want {
					t.Errorf("Waves()[%s] = %d, want %d", r, got[r], want)
				}
			}
		})
	}
}

func TestApplyWavesStampsNodes(t *testing.T) {
	nodes := []NodeView{node("a"), node("b", "a")}
	applyWaves(nodes)

	if nodes[0].Wave != 0 || nodes[1].Wave != 1 {
		t.Fatalf("applyWaves() gave waves %d, %d; want 0, 1", nodes[0].Wave, nodes[1].Wave)
	}
}

func TestParseNodeRef(t *testing.T) {
	want := adapter.NodeRef{Kind: kustomizev1.KustomizationKind, Namespace: nsApps, Name: nodeWeb}

	tests := []struct {
		in      string
		want    adapter.NodeRef
		wantErr string
	}{
		{in: "apps/web", want: want},
		{in: "Kustomization/apps/web", want: want},
		{in: "kustomization/apps/web", want: want},
		{in: "web", wantErr: "want"},
		{in: "a/b/c/d", wantErr: "want"},
		{in: "HelmRelease/apps/web", wantErr: "unsupported kind"},
		{in: "/web", wantErr: "empty namespace or name"},
		{in: "apps/", wantErr: "empty namespace or name"},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseNodeRef(tc.in)
			switch {
			case tc.wantErr != "":
				if err == nil {
					t.Fatalf("ParseNodeRef(%q) = %v, want an error", tc.in, got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ParseNodeRef(%q) error = %v, want it to mention %q", tc.in, err, tc.wantErr)
				}
			case err != nil:
				t.Fatalf("ParseNodeRef(%q) returned %v", tc.in, err)
			case got != tc.want:
				t.Fatalf("ParseNodeRef(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseSource(t *testing.T) {
	got, err := ParseSource("flux-system/apps")
	if err != nil {
		t.Fatalf("ParseSource() returned %v", err)
	}
	if got.Namespace != nsFlux || got.Name != nsApps {
		t.Fatalf("ParseSource() = %v, want flux-system/apps", got)
	}

	for _, bad := range []string{"apps", "", "/apps", "flux-system/", "a/b/c"} {
		if _, err := ParseSource(bad); err == nil {
			t.Errorf("ParseSource(%q) succeeded, want an error", bad)
		}
	}
}

func TestStripUserinfo(t *testing.T) {
	tests := []struct{ in, want string }{
		{testRepoURL, testRepoURL},
		{"https://user:hunter2@github.com/org/repo.git", testRepoURL},
		{"https://token@github.com/org/repo.git", testRepoURL},
		{"ssh://git@github.com/org/repo.git", "ssh://github.com/org/repo.git"},
		{"", ""},
		// Unparseable: dropped rather than forwarded on the chance it embeds
		// a credential.
		{"://nonsense", ""},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got := stripUserinfo(tc.in)
			if got != tc.want {
				t.Fatalf("stripUserinfo(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if strings.Contains(got, "hunter2") || strings.Contains(got, "token@") {
				t.Fatalf("stripUserinfo(%q) leaked userinfo: %q", tc.in, got)
			}
		})
	}
}

// wavefront builds a Wavefront whose status was evaluated age ago, at the
// given poll interval.
func wavefront(interval time.Duration, age time.Duration, now time.Time) *wavefrontv1alpha1.Wavefront {
	evaluated := metav1.NewTime(now.Add(-age))
	return &wavefrontv1alpha1.Wavefront{
		Spec: wavefrontv1alpha1.WavefrontSpec{
			Poll: wavefrontv1alpha1.PollSpec{Interval: metav1.Duration{Duration: interval}},
		},
		Status: wavefrontv1alpha1.WavefrontStatus{LastEvaluated: &evaluated},
	}
}

func TestStaleDiagnostics(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

	t.Run("fresh status is silent", func(t *testing.T) {
		// Budget is 2 x (90s + 30s) = 4m.
		if diags := staleDiagnostics(wavefront(90*time.Second, 3*time.Minute, now), now); len(diags) != 0 {
			t.Fatalf("staleDiagnostics() = %v, want none", diags)
		}
	})

	t.Run("status past the budget warns", func(t *testing.T) {
		diags := staleDiagnostics(wavefront(90*time.Second, 5*time.Minute, now), now)
		if len(diags) != 1 || !strings.Contains(diags[0], "--derive") {
			t.Fatalf("staleDiagnostics() = %v, want one warning naming --derive", diags)
		}
	})

	t.Run("the budget follows the configured interval", func(t *testing.T) {
		// A 10m interval gives a 21m budget, so 5m is fresh where it was
		// stale at 90s.
		if diags := staleDiagnostics(wavefront(10*time.Minute, 5*time.Minute, now), now); len(diags) != 0 {
			t.Fatalf("staleDiagnostics() = %v, want none at a 10m interval", diags)
		}
	})

	t.Run("an unset interval falls back to the controller default", func(t *testing.T) {
		// The CRD default is 90s, so the budget is 4m again.
		wf := wavefront(0, 5*time.Minute, now)
		if diags := staleDiagnostics(wf, now); len(diags) != 1 {
			t.Fatalf("staleDiagnostics() = %v, want one warning", diags)
		}
	})

	t.Run("a never-evaluated status warns", func(t *testing.T) {
		wf := &wavefrontv1alpha1.Wavefront{}
		diags := staleDiagnostics(wf, now)
		if len(diags) != 1 || !strings.Contains(diags[0], "never been evaluated") {
			t.Fatalf("staleDiagnostics() = %v, want a never-evaluated warning", diags)
		}
	})

	t.Run("Ready False warns independently of age", func(t *testing.T) {
		wf := wavefront(90*time.Second, time.Second, now)
		wf.Status.Conditions = []metav1.Condition{{
			Type:    wavefrontv1alpha1.ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  wavefrontv1alpha1.ReadyReasonReconciliationFailed,
			Message: "listing Kustomizations: forbidden",
		}}
		diags := staleDiagnostics(wf, now)
		if len(diags) != 1 || !strings.Contains(diags[0], "Ready is False") {
			t.Fatalf("staleDiagnostics() = %v, want a Ready-False warning", diags)
		}
	})

	t.Run("Ready True warns not at all", func(t *testing.T) {
		wf := wavefront(90*time.Second, time.Second, now)
		wf.Status.Conditions = []metav1.Condition{{
			Type:   wavefrontv1alpha1.ConditionReady,
			Status: metav1.ConditionTrue,
			Reason: wavefrontv1alpha1.ReadyReasonSucceeded,
		}}
		if diags := staleDiagnostics(wf, now); len(diags) != 0 {
			t.Fatalf("staleDiagnostics() = %v, want none", diags)
		}
	})
}

// fixture is a small but complete snapshot: enough shape that a round trip
// exercises every optional field kind (pointer time, pointer string, nested
// slices, maps).
func fixture() *Snapshot {
	evaluated := time.Date(2026, 9, 7, 11, 59, 0, 0, time.UTC).UTC()
	source := "flux-system/apps"
	return &Snapshot{
		APIVersion: Version,
		Kind:       KindSnapshot,
		Origin:     OriginStatus,
		CapturedAt: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC).UTC(),
		Evaluated:  &evaluated,
		Cluster:    ClusterIdent{Context: "prod", Server: "api.example.com", WfctlVersion: "v0.1.0"},
		Observed:   true,
		Wavefront: WavefrontView{
			Name:       wfName,
			Generation: 3,
			SpecOwners: map[string]string{"spec.mode": "kustomize-controller"},
		},
		Nodes: []NodeView{{
			Ref:         ref("web"),
			Role:        "Pinned",
			State:       "Settled",
			Ready:       true,
			DependsOn:   []adapter.NodeRef{ref("base")},
			Source:      &source,
			Pin:         testSHA,
			ObservedSHA: testSHA,
			Wave:        1,
		}},
		Sources: []SourceView{{
			Name:        source,
			URL:         testRepoURL,
			TrackingRef: refMain,
			Pin:         testSHA,
			Provenance:  map[string]string{"wavefront.as-code.io/observed-ref": refMain},
			Nodes:       []adapter.NodeRef{ref("web")},
		}},
		Diagnostics: []string{"one degradation"},
	}
}

func TestFileSourceRoundTrip(t *testing.T) {
	want := fixture()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	writeJSON(t, path, want)

	got, err := FileSource{Path: path}.Capture(context.Background())
	if err != nil {
		t.Fatalf("Capture() returned %v", err)
	}

	// The captured origin survives; only Replayed is added.
	if got.Origin != want.Origin {
		t.Fatalf("Capture() origin = %q, want %q", got.Origin, want.Origin)
	}
	if !got.Replayed {
		t.Fatal("Capture() left Replayed false; a picture read from a file must say so")
	}

	want.Replayed = true
	gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want)
	if gotJSON != wantJSON {
		t.Fatalf("round trip changed the snapshot:\n got: %s\nwant: %s", gotJSON, wantJSON)
	}
}

// A replayed derive snapshot is still a derive snapshot: it kept everything
// only a live derivation could prove, so a renderer must go on showing the
// DERIVED columns rather than demoting it to a status view.
func TestFileSourceKeepsDeriveOrigin(t *testing.T) {
	want := fixture()
	want.Origin = OriginDerive
	want.Observed = true
	want.Derived = DerivedStatus{
		Phase:      wavefrontv1alpha1.PhaseAdvancing,
		GraphValid: true,
		Admissions: []AdmissionView{{Node: ref("web"), Source: "flux-system/apps", To: testSHA}},
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	writeJSON(t, path, want)

	got, err := FileSource{Path: path}.Capture(context.Background())
	if err != nil {
		t.Fatalf("Capture() returned %v", err)
	}

	if got.Origin != OriginDerive {
		t.Fatalf("Capture() origin = %q, want %q", got.Origin, OriginDerive)
	}
	if !got.Replayed {
		t.Fatal("Capture() left Replayed false")
	}
	if got.Derived.Phase != wavefrontv1alpha1.PhaseAdvancing || len(got.Derived.Admissions) != 1 {
		t.Fatalf("Capture() lost the derived detail: %+v", got.Derived)
	}
}

func TestFileSourceRejectsMismatch(t *testing.T) {
	tests := []struct {
		name       string
		apiVersion string
		kind       string
		wantErr    string
	}{
		{"a future apiVersion", "wfctl.wavefront.as-code.io/v9", KindSnapshot, "apiVersion"},
		{"an empty apiVersion", "", KindSnapshot, "apiVersion"},
		{"another kind entirely", Version, "Wavefront", "not a wfctl snapshot"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snap := fixture()
			snap.APIVersion, snap.Kind = tc.apiVersion, tc.kind
			path := filepath.Join(t.TempDir(), "snapshot.json")
			writeJSON(t, path, snap)

			_, err := FileSource{Path: path}.Capture(context.Background())
			if err == nil {
				t.Fatalf("Capture() succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Capture() error = %v, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestFileSourceReportsUnreadableAndMalformed(t *testing.T) {
	if _, err := (FileSource{Path: filepath.Join(t.TempDir(), "absent.json")}).Capture(context.Background()); err == nil {
		t.Fatal("Capture() on a missing file succeeded, want an error")
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}
	if _, err := (FileSource{Path: path}).Capture(context.Background()); err == nil {
		t.Fatal("Capture() on malformed JSON succeeded, want an error")
	}
}

func writeJSON(t *testing.T, path string, snap *Snapshot) {
	t.Helper()
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshalling fixture: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}
}

func mustMarshal(t *testing.T, snap *Snapshot) string {
	t.Helper()
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshalling snapshot: %v", err)
	}
	return string(data)
}

// credentialURL is a repository URL with a password embedded, the shape
// source-controller quotes verbatim in its failure messages.
const credentialURL = "https://bot:hunter2@git.example.com/org/apps.git"

func managedRepo(commit string) *sourcev1.GitRepository {
	return &sourcev1.GitRepository{
		Namespace: nsFlux,
		Name:      nsApps,
		Labels:    map[string]string{pin.ManagedLabel: "true"},
		Spec: sourcev1.GitRepositorySpec{
			URL:       credentialURL,
			Interval:  metav1.Duration{Duration: time.Minute},
			Reference: &sourcev1.GitRepositoryRef{Name: refMain, Commit: commit},
		},
	}
}

// A Snapshot is written to files and pasted into incident channels, and
// conditions are the one field copied wholesale off the object. If a
// credentialed URL can ride out in a FetchFailed message, "never contains auth
// material" is not true.
func TestDescribeRepoScrubsCredentialsFromConditions(t *testing.T) {
	repo := managedRepo(testSHA)
	repo.Status.Conditions = []metav1.Condition{
		{
			Type:    "FetchFailed",
			Status:  metav1.ConditionTrue,
			Reason:  "GitOperationFailed",
			Message: "failed to checkout and determine revision: unable to clone '" + credentialURL + "': authentication required",
		},
		{
			Type:   "Ready",
			Status: metav1.ConditionFalse,
			Reason: "GitOperationFailed",
			// A URL this package never resolved: only the general pattern can
			// catch it.
			Message: "redirected to https://mirror:s3cret@mirror.example.com/org/apps.git and failed",
		},
		{Type: "Reconciling", Status: metav1.ConditionTrue, Reason: "Progressing", Message: "no URL here"},
	}

	var view SourceView
	describeRepo(&view, repo, selection.TrackRef())

	if len(view.Conditions) != 3 {
		t.Fatalf("describeRepo() kept %d conditions, want 3", len(view.Conditions))
	}
	for _, condition := range view.Conditions {
		for _, secret := range []string{"hunter2", "s3cret", "bot:", "mirror:"} {
			if strings.Contains(condition.Message, secret) {
				t.Errorf("condition %s leaked %q: %s", condition.Type, secret, condition.Message)
			}
		}
		// Nothing between "//" and the next "@" may survive: that span is
		// exactly the userinfo.
		for _, part := range strings.Split(condition.Message, "//")[1:] {
			if host, _, found := strings.Cut(part, "@"); found && !strings.ContainsAny(host, " '") {
				t.Errorf("condition %s still carries userinfo %q: %s", condition.Type, host, condition.Message)
			}
		}
	}

	// The message must still identify the repository, scrubbed, or the
	// diagnosis is lost along with the credential.
	if !strings.Contains(view.Conditions[0].Message, "https://git.example.com/org/apps.git") {
		t.Errorf("scrubbing lost the repository identity: %s", view.Conditions[0].Message)
	}
	if !strings.Contains(view.Conditions[0].Message, "authentication required") {
		t.Errorf("scrubbing lost the diagnosis: %s", view.Conditions[0].Message)
	}
	if view.Conditions[2].Message != "no URL here" {
		t.Errorf("scrubbing altered an innocent message: %s", view.Conditions[2].Message)
	}
	// The scrub must not have edited the object it was handed.
	if !strings.Contains(repo.Status.Conditions[0].Message, "hunter2") {
		t.Error("describeRepo() mutated the GitRepository's own conditions")
	}
	if strings.Contains(view.URL, "@") {
		t.Errorf("SourceView.URL kept its userinfo: %s", view.URL)
	}
}

// statusReader builds a fake cluster holding one Wavefront whose published
// members name one source, plus that source at whatever pin the test wants.
func statusReader(t *testing.T, memberPin, livePin string) client.Reader {
	t.Helper()

	sch := runtime.NewScheme()
	if err := wavefrontv1alpha1.AddToScheme(sch); err != nil {
		t.Fatalf("building scheme: %v", err)
	}
	if err := sourcev1.AddToScheme(sch); err != nil {
		t.Fatalf("building scheme: %v", err)
	}

	evaluated := metav1.NewTime(time.Now())
	wf := &wavefrontv1alpha1.Wavefront{
		Name: wfName,
		Spec: wavefrontv1alpha1.WavefrontSpec{
			Nodes: wavefrontv1alpha1.NodesSpec{Kinds: []string{kustomizev1.KustomizationKind}},
			Poll:  wavefrontv1alpha1.PollSpec{Interval: metav1.Duration{Duration: time.Hour}},
		},
		Status: wavefrontv1alpha1.WavefrontStatus{
			LastEvaluated: &evaluated,
			Members: []wavefrontv1alpha1.Member{{
				Node:   wavefrontv1alpha1.NodeReference{Kind: kustomizev1.KustomizationKind, Namespace: nsApps, Name: nodeWeb},
				Role:   "Pinned",
				State:  "Settled",
				Ready:  true,
				Source: nsFlux + "/" + nsApps,
				Pin:    memberPin,
			}},
		},
	}

	return fake.NewClientBuilder().WithScheme(sch).WithObjects(wf, managedRepo(livePin)).Build()
}

func captureStatus(t *testing.T, reader client.Reader) *Snapshot {
	t.Helper()
	snap, err := (&StatusSource{Reader: reader, Wavefront: wfName}).Capture(context.Background())
	if err != nil {
		t.Fatalf("Capture() returned %v", err)
	}
	return snap
}

// SourceView is the live view of a source and NodeView.Pin is the published
// one; when they disagree the status is simply behind, and the snapshot has to
// say so rather than show two pins and leave the reader to spot it.
func TestStatusSourceReportsPinDrift(t *testing.T) {
	const livePin = "9999999999999999999999999999999999999999"

	t.Run("agreeing pins are silent", func(t *testing.T) {
		snap := captureStatus(t, statusReader(t, testSHA, testSHA))
		if len(snap.Diagnostics) != 0 {
			t.Fatalf("Capture() = %v diagnostics, want none", snap.Diagnostics)
		}
		if snap.Sources[0].Pin != testSHA || snap.Nodes[0].Pin != testSHA {
			t.Fatalf("pins = %q / %q, want both %q", snap.Sources[0].Pin, snap.Nodes[0].Pin, testSHA)
		}
	})

	t.Run("a drifted pin is diagnosed", func(t *testing.T) {
		snap := captureStatus(t, statusReader(t, testSHA, livePin))

		// SourceView keeps the live value; the member keeps the published one.
		if snap.Sources[0].Pin != livePin {
			t.Errorf("SourceView.Pin = %q, want the live %q", snap.Sources[0].Pin, livePin)
		}
		if snap.Nodes[0].Pin != testSHA {
			t.Errorf("NodeView.Pin = %q, want the published %q", snap.Nodes[0].Pin, testSHA)
		}

		if len(snap.Diagnostics) != 1 {
			t.Fatalf("Capture() = %v, want exactly one diagnostic", snap.Diagnostics)
		}
		for _, want := range []string{nsFlux + "/" + nsApps, livePin[:7], testSHA[:7], "status is behind"} {
			if !strings.Contains(snap.Diagnostics[0], want) {
				t.Errorf("diagnostic %q does not mention %q", snap.Diagnostics[0], want)
			}
		}
	})

	t.Run("an unpinned source names the unpinned case", func(t *testing.T) {
		snap := captureStatus(t, statusReader(t, testSHA, ""))
		if len(snap.Diagnostics) != 1 || !strings.Contains(snap.Diagnostics[0], "(unpinned)") {
			t.Fatalf("Capture() = %v, want one diagnostic naming the unpinned case", snap.Diagnostics)
		}
	})
}
