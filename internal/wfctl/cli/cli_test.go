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

package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// The two fixtures the command tree is driven against, copied from
// internal/wfctl/render/testdata. Everything here runs through --from, so no
// test in this package needs a cluster: what is under test is the wiring —
// flag validation, provider selection, payload selection and exit codes —
// not the renderers, which have their own goldens.
const (
	fxQuiescent = "quiescent"
	fxBlocked   = "blocked-ancestor-unhealthy"
)

// srcInfra is the quiescent fixture's only source; nodeLeaf is the deepest
// node of the blocked fixture's three-deep chain.
const (
	srcInfra = "sources/infra"
	nodeLeaf = "flotillas/leaf"
)

// Error fragments each validation failure must name.
const (
	errCombined = "cannot be combined"
	errFormat   = "invalid output format"
)

// fixture is the --from path of a copied snapshot.
func fixture(name string) string {
	return filepath.Join("testdata", name+".snapshot.json")
}

// from builds the --from argument pair.
func from(name string) []string {
	return []string{"--from", fixture(name)}
}

// run executes the command tree with args, capturing everything it writes.
func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := NewRootCommand(binaryName)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	return runCommand(root, &out)
}

func runCommand(root *cobra.Command, out *bytes.Buffer) (string, error) {
	err := root.Execute()
	return out.String(), err
}

// TestReadCommands drives every read command through a replayed snapshot and
// asserts on a marker each one alone can produce, so a command wired to the
// wrong renderer or the wrong payload cannot pass.
func TestReadCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{cmdStatus, []string{cmdStatus}, []string{"PHASE", "Quiescent", "DIAGNOSTICS"}},
		{cmdNodes, []string{cmdNodes}, []string{"NODE ", "ROLE", "STATE", "OBSERVED", "LAG"}},
		{"nodes wide", []string{cmdNodes, "-o", outputWide}, []string{"WAVE", "READY", "DEPENDS"}},
		{cmdSources, []string{cmdSources}, []string{"SOURCE", "PIN", "HOLD", "OWNERS", srcInfra}},
		{"sources wide", []string{cmdSources, "-o", outputWide}, []string{"PREVIOUS-PIN", "OBSERVED-REF", "URL"}},
		{cmdSource, []string{cmdSource, srcInfra}, []string{srcInfra}},
		{cmdGraph, []string{cmdGraph}, []string{"wave 0", "flotillas/infra"}},
		{"graph tree", []string{cmdGraph, "--tree"}, []string{"flotillas/"}},
		{"graph dot", []string{cmdGraph, "-o", outputDOT}, []string{"digraph"}},
		{"graph mermaid", []string{cmdGraph, "-o", outputMermaid}, []string{"graph LR"}},
		{cmdSnapshot, []string{cmdSnapshot}, []string{`"kind": "Snapshot"`, `"origin": "status"`}},
		{"snapshot yaml", []string{cmdSnapshot, "-o", outputYAML}, []string{"kind: Snapshot"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := run(t, append(tc.args, from(fxQuiescent)...)...)
			if err != nil {
				t.Fatalf("running %v: %v\n%s", tc.args, err, out)
			}
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Errorf("output does not contain %q:\n%s", want, out)
				}
			}
		})
	}
}

// TestExplain checks the one command that takes a node reference: it must
// reach the blocked node's root cause, and it must reject a reference the
// snapshot package would not accept either.
func TestExplain(t *testing.T) {
	out, err := run(t, append([]string{cmdExplain, nodeLeaf}, from(fxBlocked)...)...)
	if err != nil {
		t.Fatalf("explain: %v\n%s", err, out)
	}
	// The chain is leaf -> mid -> root, and root is the unhealthy cause.
	for _, want := range []string{nodeLeaf, "flotillas/mid", "flotillas/root"} {
		if !strings.Contains(out, want) {
			t.Errorf("explain did not walk to %q:\n%s", want, out)
		}
	}

	if _, err := run(t, append([]string{cmdExplain, "not-a-ref"}, from(fxBlocked)...)...); err == nil {
		t.Error("a malformed node reference was accepted")
	}
	if _, err := run(t, append([]string{cmdSource, "not-a-source"}, from(fxQuiescent)...)...); err == nil {
		t.Error("a malformed source reference was accepted")
	}
}

// TestEncodedPayloads pins which slice of the snapshot each command emits
// under -o json (plan B3): a command that encoded the whole document would
// look right and be wrong.
func TestEncodedPayloads(t *testing.T) {
	nodesJSON, err := run(t, append([]string{cmdNodes, "-o", outputJSON}, from(fxQuiescent)...)...)
	if err != nil {
		t.Fatalf("nodes -o json: %v", err)
	}
	var nodes []snapshot.NodeView
	if err := json.Unmarshal([]byte(nodesJSON), &nodes); err != nil {
		t.Fatalf("nodes -o json is not a NodeView list: %v\n%s", err, nodesJSON)
	}
	if len(nodes) == 0 {
		t.Error("nodes -o json emitted no nodes")
	}

	sourcesJSON, err := run(t, append([]string{cmdSources, "-o", outputJSON}, from(fxQuiescent)...)...)
	if err != nil {
		t.Fatalf("sources -o json: %v", err)
	}
	var sources []snapshot.SourceView
	if err := json.Unmarshal([]byte(sourcesJSON), &sources); err != nil {
		t.Fatalf("sources -o json is not a SourceView list: %v\n%s", err, sourcesJSON)
	}

	statusJSON, err := run(t, append([]string{cmdStatus, "-o", outputJSON}, from(fxQuiescent)...)...)
	if err != nil {
		t.Fatalf("status -o json: %v", err)
	}
	var payload statusPayload
	if err := json.Unmarshal([]byte(statusJSON), &payload); err != nil {
		t.Fatalf("status -o json is not a status payload: %v\n%s", err, statusJSON)
	}
	if payload.Wavefront.Name == "" {
		t.Error("status -o json carries no Wavefront")
	}
	// The whole snapshot must not leak out of the status payload: only the
	// three documented keys, and no NodeView (which every node carries a
	// "ref" in).
	var keys map[string]json.RawMessage
	if err := json.Unmarshal([]byte(statusJSON), &keys); err != nil {
		t.Fatalf("status -o json is not an object: %v", err)
	}
	for key := range keys {
		if key != "wavefront" && key != "derived" && key != "diagnostics" {
			t.Errorf("status -o json carries an undocumented key %q", key)
		}
	}
	if strings.Contains(statusJSON, `"ref":`) {
		t.Errorf("status -o json emitted the node list:\n%s", statusJSON)
	}
}

// TestSnapshotRoundTrip proves `wfctl snapshot -f` writes something --from
// will read back: it is the contract the whole file-replay story rests on.
func TestSnapshotRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.json")
	if _, err := run(t, append([]string{cmdSnapshot, "-f", path}, from(fxQuiescent)...)...); err != nil {
		t.Fatalf("snapshot -f: %v", err)
	}

	written, err := snapshot.FileSource{Path: path}.Capture(t.Context())
	if err != nil {
		t.Fatalf("replaying the captured snapshot: %v", err)
	}
	original, err := snapshot.FileSource{Path: fixture(fxQuiescent)}.Capture(t.Context())
	if err != nil {
		t.Fatalf("reading the fixture: %v", err)
	}
	if !reflect.DeepEqual(written, original) {
		t.Error("the captured snapshot does not round-trip back to the replayed one")
	}

	// And the rendered tables must agree, which is the property an operator
	// attaching a capture to a ticket is relying on.
	fromFixture, err := run(t, append([]string{cmdNodes}, from(fxQuiescent)...)...)
	if err != nil {
		t.Fatalf("rendering the fixture: %v", err)
	}
	fromCapture, err := run(t, cmdNodes, "--from", path)
	if err != nil {
		t.Fatalf("rendering the capture: %v\n%s", err, fromCapture)
	}
	if fromCapture != fromFixture {
		t.Errorf("the capture renders differently\n--- want ---\n%s\n--- got ---\n%s",
			fromFixture, fromCapture)
	}
}

// TestStatusExitsTwoWhenBlocked pins the one exit code a script depends on.
//
// Every format is covered, because the format is not what the exit code is
// about: a caller piping `-o json` into jq is the one most likely to check
// $?, and it must not be told 0 while the document it just read says Blocked.
func TestStatusExitsTwoWhenBlocked(t *testing.T) {
	formats := []struct {
		name string
		args []string
		want string
	}{
		{outputTable, nil, "Blocked"},
		{outputWide, []string{"-o", outputWide}, "Blocked"},
		{outputJSON, []string{"-o", outputJSON}, `"phase": "Blocked"`},
		{outputYAML, []string{"-o", outputYAML}, "phase: Blocked"},
	}

	for _, format := range formats {
		t.Run(format.name, func(t *testing.T) {
			args := append([]string{cmdStatus}, format.args...)
			out, err := run(t, append(args, from(fxBlocked)...)...)
			if err == nil {
				t.Fatalf("a blocked fleet exited 0:\n%s", out)
			}
			// The report is printed in full before the exit: the code says
			// "blocked", the output says what by.
			if !strings.Contains(out, format.want) {
				t.Errorf("the report was not printed before the exit:\n%s", out)
			}

			var stderr bytes.Buffer
			if code := Fail(&stderr, err); code != exitBlocked {
				t.Errorf("exit code is %d, want %d", code, exitBlocked)
			}
			// Nothing is printed: the report already said Blocked, and an
			// "Error:" line would corrupt a `-o json` pipeline that merged
			// the two streams.
			if stderr.Len() != 0 {
				t.Errorf("the blocked exit printed %q", stderr.String())
			}

			// A quiescent fleet is the control, in the same format.
			quiescent := append([]string{cmdStatus}, format.args...)
			if _, err := run(t, append(quiescent, from(fxQuiescent)...)...); err != nil {
				t.Errorf("a quiescent fleet exited non-zero: %v", err)
			}
		})
	}
}

// TestCaptureStampsTheCluster proves the Task-4 carry-over: no provider fills
// Snapshot.Cluster, so the CLI must, and only for the cluster-backed origins.
//
// The reader is a fake and the identity is injected, so the test depends on
// neither an apiserver nor whatever kubeconfig the machine running it happens
// to have.
func TestCaptureStampsTheCluster(t *testing.T) {
	ident := snapshot.ClusterIdent{
		Context:      "test-context",
		Server:       "api.test.example:6443",
		WfctlVersion: "v9.9.9",
	}
	reader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&wavefrontv1alpha1.Wavefront{
			Name: "fleet",
		}).
		Build()

	opts := &Options{
		Output:       outputTable,
		NewReader:    func() (client.Reader, error) { return reader, nil },
		ClusterIdent: func() snapshot.ClusterIdent { return ident },
	}

	snap, err := opts.capture(t.Context())
	if err != nil {
		t.Fatalf("capturing from the status provider: %v", err)
	}
	if snap.Origin != snapshot.OriginStatus {
		t.Errorf("origin is %q, want %q", snap.Origin, snapshot.OriginStatus)
	}
	if snap.Cluster != ident {
		t.Errorf("status capture was stamped %+v, want %+v", snap.Cluster, ident)
	}

	// The same for a live derivation, which is the other cluster-backed
	// origin. An empty fleet is enough: what is under test is the stamp.
	opts.Derive = true
	derived, err := opts.capture(t.Context())
	if err != nil {
		t.Fatalf("capturing from the derive provider: %v", err)
	}
	if derived.Origin != snapshot.OriginDerive {
		t.Errorf("origin is %q, want %q", derived.Origin, snapshot.OriginDerive)
	}
	if derived.Cluster != ident {
		t.Errorf("derive capture was stamped %+v, want %+v", derived.Cluster, ident)
	}

	// And never for a replayed file, whose identity is the cluster it was
	// captured against, not the one reading it.
	opts.Derive = false
	opts.From = fixture(fxQuiescent)
	replayed, err := opts.capture(t.Context())
	if err != nil {
		t.Fatalf("replaying: %v", err)
	}
	if replayed.Cluster == ident {
		t.Error("a replayed snapshot was re-stamped with this run's cluster")
	}
}

// TestFail covers the mapping main relies on.
func TestFail(t *testing.T) {
	var out bytes.Buffer
	if code := Fail(&out, nil); code != 0 {
		t.Errorf("no error exited %d", code)
	}

	out.Reset()
	if code := Fail(&out, errors.New("boom")); code != exitError {
		t.Errorf("a plain error exited %d, want %d", code, exitError)
	}
	if !strings.Contains(out.String(), "Error: boom") {
		t.Errorf("the error was not reported: %q", out.String())
	}

	out.Reset()
	wrapped := &ExitError{Code: exitBlocked, Err: errors.New("still blocked")}
	if code := Fail(&out, wrapped); code != exitBlocked {
		t.Errorf("an ExitError exited %d, want %d", code, exitBlocked)
	}
	if !errors.Is(wrapped.Unwrap(), wrapped.Err) {
		t.Error("ExitError does not unwrap to its cause")
	}
}

// TestFlagValidation covers the combinations that must fail before anything
// touches a cluster — none of these tests has one.
func TestFlagValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"poll without derive", []string{cmdNodes, "--poll"}, "requires --derive"},
		{
			"from with derive",
			append([]string{cmdNodes, "--derive"}, from(fxQuiescent)...),
			errCombined,
		},
		{
			"from with poll",
			append([]string{cmdNodes, "--poll"}, from(fxQuiescent)...),
			errCombined,
		},
		{
			"unknown format",
			append([]string{cmdNodes, "-o", "toml"}, from(fxQuiescent)...),
			errFormat,
		},
		{
			"explain cannot encode",
			append([]string{cmdExplain, nodeLeaf, "-o", outputJSON}, from(fxBlocked)...),
			errFormat,
		},
		{
			"graph cannot encode",
			append([]string{cmdGraph, "-o", outputJSON}, from(fxQuiescent)...),
			errFormat,
		},
		{
			"tree is not a graphviz layout",
			append([]string{cmdGraph, "--tree", "-o", outputDOT}, from(fxQuiescent)...),
			errCombined,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := run(t, tc.args...)
			if err == nil {
				t.Fatalf("%v was accepted:\n%s", tc.args, out)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// TestInvocationName covers the kubectl-plugin switch. What matters is the
// command path users are told to type, which is why it is asserted on the
// help text rather than on the Use field: cobra derives a command's name from
// the first word of Use, so the two-word plugin form travels as a
// display-name annotation.
func TestInvocationName(t *testing.T) {
	tests := []struct {
		argv0 string
		want  string
	}{
		{binaryName, binaryName},
		{"/usr/local/bin/" + binaryName, binaryName},
		{"/usr/local/bin/" + pluginName, pluginUse},
		{pluginName, pluginUse},
	}

	for _, tc := range tests {
		t.Run(tc.argv0, func(t *testing.T) {
			root := NewRootCommand(tc.argv0)
			if got := root.CommandPath(); got != tc.want {
				t.Errorf("root command path is %q, want %q", got, tc.want)
			}
			status, _, err := root.Find([]string{cmdStatus})
			if err != nil {
				t.Fatalf("finding the status command: %v", err)
			}
			if got, want := status.CommandPath(), tc.want+" "+cmdStatus; got != want {
				t.Errorf("subcommand path is %q, want %q", got, want)
			}
			if !strings.Contains(root.UsageString(), tc.want+" [command]") {
				t.Errorf("usage does not name %q:\n%s", tc.want, root.UsageString())
			}
		})
	}
}

// TestRenderOptionsAlwaysCarryAClock defends the invariant that no renderer
// is ever left to find its own: an unset Now would make `-o wide` ages
// depend on when the renderer happened to run.
func TestRenderOptionsAlwaysCarryAClock(t *testing.T) {
	opts := &Options{Output: outputWide, NoColor: true}
	rendered := opts.renderOptions()
	if rendered.Now.IsZero() {
		t.Error("render options carry no clock")
	}
	if !rendered.Wide {
		t.Error("-o wide did not select the wide columns")
	}
	if rendered.Palette.Enabled {
		t.Error("--no-color still enabled the palette")
	}
}

// TestColorEnabled covers the three-way rule of plan B1.
func TestColorEnabled(t *testing.T) {
	tty := func() bool { return true }

	t.Setenv("NO_COLOR", "1")
	if (&Options{ColorTTY: tty}).colorEnabled() {
		t.Error("NO_COLOR did not disable colour")
	}

	// An empty NO_COLOR does not count (no-color.org): it is how a wrapper
	// script clears an inherited one.
	t.Setenv("NO_COLOR", "")
	if !(&Options{ColorTTY: tty}).colorEnabled() {
		t.Error("an empty NO_COLOR disabled colour")
	}

	if err := os.Unsetenv("NO_COLOR"); err != nil {
		t.Fatalf("unsetting NO_COLOR: %v", err)
	}
	if !(&Options{ColorTTY: tty}).colorEnabled() {
		t.Error("an interactive terminal did not enable colour")
	}
	if (&Options{ColorTTY: tty, NoColor: true}).colorEnabled() {
		t.Error("--no-color did not disable colour")
	}
	if (&Options{ColorTTY: func() bool { return false }}).colorEnabled() {
		t.Error("a pipe enabled colour")
	}
}

// TestReplayedSnapshotKeepsItsCluster proves a replayed file is not
// re-stamped with the identity of whoever is reading it: the fixture was
// captured against "prod-eu", and it must still say so.
func TestReplayedSnapshotKeepsItsCluster(t *testing.T) {
	out, err := run(t, append([]string{cmdSnapshot}, from(fxQuiescent)...)...)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var snap snapshot.Snapshot
	if err := json.Unmarshal([]byte(out), &snap); err != nil {
		t.Fatalf("decoding the snapshot: %v", err)
	}
	if snap.Cluster.Context != "prod-eu" {
		t.Errorf("cluster context is %q, want the captured %q", snap.Cluster.Context, "prod-eu")
	}
	if snap.Cluster.WfctlVersion != "v0.1.0" {
		t.Errorf("wfctl version is %q, want the captured %q", snap.Cluster.WfctlVersion, "v0.1.0")
	}
}

// TestHostOnly proves the one field of ClusterIdent that could leak: the
// apiserver URL is reduced to a host, so neither a path nor embedded
// credentials reach a snapshot that gets pasted into a channel.
func TestHostOnly(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"", ""},
		{"https://api.example.test:6443", "api.example.test:6443"},
		{"https://api.example.test:6443/some/path", "api.example.test:6443"},
		{"https://user:hunter2@api.example.test", "api.example.test"},
		{"not a url", ""},
	}
	for _, tc := range tests {
		if got := hostOnly(tc.raw); got != tc.want {
			t.Errorf("hostOnly(%q) = %q, want %q", tc.raw, got, tc.want)
		}
		if strings.Contains(hostOnly(tc.raw), "hunter2") {
			t.Errorf("hostOnly(%q) leaked credentials", tc.raw)
		}
	}
}

// TestPersistentFlagsAreDeclared pins the flag surface Task 7's writes and
// Task 8's history will build on: they register commands, never flags.
func TestPersistentFlagsAreDeclared(t *testing.T) {
	root := NewRootCommand(binaryName)
	for _, name := range []string{
		"kubeconfig", "context", "namespace",
		"wavefront", "output", "no-color", "yes",
		"derive", "poll", "poll-timeout", "per-host-concurrency", "from",
	} {
		if root.PersistentFlags().Lookup(name) == nil {
			t.Errorf("--%s is not a persistent flag", name)
		}
	}
	if got := root.PersistentFlags().Lookup("poll-timeout").DefValue; got != snapshot.DefaultPollTimeout.String() {
		t.Errorf("--poll-timeout defaults to %q, want %q", got, snapshot.DefaultPollTimeout)
	}
	if got := root.PersistentFlags().Lookup("per-host-concurrency").DefValue; got != "0" {
		t.Errorf("--per-host-concurrency defaults to %q, want 0 (the Wavefront's value)", got)
	}
}

// TestMissingSnapshotFile checks the error a mistyped --from produces reaches
// the user intact rather than as a panic or an empty table.
func TestMissingSnapshotFile(t *testing.T) {
	_, err := run(t, cmdNodes, "--from", filepath.Join(t.TempDir(), "absent.json"))
	if err == nil {
		t.Fatal("a missing snapshot file was accepted")
	}
	if !strings.Contains(err.Error(), "absent.json") {
		t.Errorf("error %q does not name the file", err)
	}
}
