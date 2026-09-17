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
	"context"
	"errors"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/spf13/cobra"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
)

// The write commands are wired here and proved in internal/wfctl/actions,
// which is where a real apiserver is. What these tests own is the wiring: the
// flags, the consent rule at the command boundary, the audit event, and the
// refusals that happen before any client is asked for anything.

const (
	testWavefront = "fleet"
	testSource    = "default/infra"

	// The flags and arguments these tests type often enough to name.
	flagYes   = "--yes"
	flagPoll  = "--poll"
	badSource = "not-a-source"
)

// writeCluster is the fixture every write test drives against: one Wavefront
// and one managed source.
func writeCluster() client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			&wavefrontv1alpha1.Wavefront{
				Name: testWavefront,
				Spec: wavefrontv1alpha1.WavefrontSpec{
					Mode: wavefrontv1alpha1.ModeShadow,
				},
			},
			&sourcev1.GitRepository{
				Namespace: "default",
				Name:      "infra",
				Labels:    map[string]string{pin.ManagedLabel: "true"},
				Spec: sourcev1.GitRepositorySpec{
					Reference: &sourcev1.GitRepositoryRef{Name: "refs/heads/main", Commit: "abc123"},
				},
			},
		).
		Build()
}

// writeTree builds the real command tree with the cluster, the terminal and
// the kubeconfig replaced.
func writeTree(c client.Client, stdinTTY bool, answer string) (*cobra.Command, *bytes.Buffer) {
	opts := NewOptions()
	opts.NewClient = func() (client.Client, error) { return c, nil }
	opts.StdinTTY = func() bool { return stdinTTY }
	opts.Identity = func() string { return "tester@test-context" }

	root := newRootCommand(binaryName, opts)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetIn(strings.NewReader(answer))
	return root, &out
}

// runWriteTree drives the tree and returns everything it wrote.
func runWriteTree(t *testing.T, c client.Client, stdinTTY bool, answer string, args ...string) (string, error) {
	t.Helper()

	root, out := writeTree(c, stdinTTY, answer)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

// suspended reads the fleet's brake back.
func suspended(t *testing.T, c client.Client) bool {
	t.Helper()

	wf := &wavefrontv1alpha1.Wavefront{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: testWavefront}, wf); err != nil {
		t.Fatalf("reading back the Wavefront: %v", err)
	}
	return wf.Spec.Suspend
}

// auditEvents lists the audit trail.
func auditEvents(t *testing.T, c client.Client) []eventsv1.Event {
	t.Helper()

	list := &eventsv1.EventList{}
	if err := c.List(t.Context(), list); err != nil {
		t.Fatalf("listing events: %v", err)
	}
	return list.Items
}

func TestWriteCommandsAreRegistered(t *testing.T) {
	root := NewRootCommand(binaryName)

	for _, name := range []string{
		cmdSuspend, cmdResume, cmdMode, cmdPin, cmdRelease, cmdPinStrip, cmdForceAdmit,
	} {
		found := false
		for _, cmd := range root.Commands() {
			if cmd.Name() == name {
				found = true
				if !cmd.Flags().HasAvailableFlags() || cmd.Flags().Lookup("dry-run") == nil {
					t.Errorf("%s does not declare --dry-run", name)
				}
			}
		}
		if !found {
			t.Errorf("the tree has no %q command", name)
		}
	}
}

func TestWriteAppliesWithYes(t *testing.T) {
	c := writeCluster()

	out, err := runWriteTree(t, c, false, "", cmdSuspend, flagYes)
	if err != nil {
		t.Fatalf("suspend --yes: %v\n%s", err, out)
	}
	if !suspended(t, c) {
		t.Error("suspend --yes did not set spec.suspend")
	}

	// The plan is printed even when consent is not asked for: --yes waives the
	// question, not the explanation.
	for _, want := range []string{"spec.suspend", "false", "true", "FIELD"} {
		if !strings.Contains(out, want) {
			t.Errorf("the plan was not printed (%q missing):\n%s", want, out)
		}
	}

	events := auditEvents(t, c)
	if len(events) != 1 {
		t.Fatalf("got %d audit events, want 1", len(events))
	}
	if events[0].Reason != actions.ReasonSuspended {
		t.Errorf("audit reason is %q, want %q", events[0].Reason, actions.ReasonSuspended)
	}
	if !strings.HasPrefix(events[0].Note, "tester@test-context ran: ") {
		t.Errorf("audit note is %q, want it to name who ran what", events[0].Note)
	}
	if events[0].Regarding.Name != testWavefront || events[0].Namespace != "default" {
		t.Errorf("audit event regards %s in %s, want %s in default",
			events[0].Regarding.Name, events[0].Namespace, testWavefront)
	}
}

func TestWriteDryRunChangesNothing(t *testing.T) {
	c := writeCluster()

	out, err := runWriteTree(t, c, true, "y\n", cmdSuspend, "--dry-run")
	if err != nil {
		t.Fatalf("suspend --dry-run: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Dry run: nothing was written.") {
		t.Errorf("the dry run did not say so:\n%s", out)
	}
	if suspended(t, c) {
		t.Error("--dry-run wrote to the cluster")
	}
	if events := auditEvents(t, c); len(events) != 0 {
		t.Errorf("--dry-run recorded %d audit events", len(events))
	}
}

func TestWriteRefusesToPromptWithoutATerminal(t *testing.T) {
	c := writeCluster()

	out, err := runWriteTree(t, c, false, "y\n", cmdSuspend)
	if !errors.Is(err, actions.ErrNotATerminal) {
		t.Fatalf("error = %v, want %v\n%s", err, actions.ErrNotATerminal, out)
	}
	if suspended(t, c) {
		t.Error("a refused prompt still wrote to the cluster")
	}

	var stderr bytes.Buffer
	if code := Fail(&stderr, err); code != exitError {
		t.Errorf("exit code is %d, want %d", code, exitError)
	}
	if !strings.Contains(stderr.String(), flagYes) {
		t.Errorf("the refusal does not name the flag that fixes it: %q", stderr.String())
	}
}

func TestWritePromptsAtATerminal(t *testing.T) {
	tests := []struct {
		name    string
		answer  string
		applied bool
	}{
		{"y applies", "y\n", true},
		{"n declines", "n\n", false},
		{"return declines", "\n", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := writeCluster()

			out, err := runWriteTree(t, c, true, tc.answer, cmdSuspend)
			if err != nil {
				t.Fatalf("suspend: %v\n%s", err, out)
			}
			if !strings.Contains(out, "Proceed? [y/N]") {
				t.Errorf("no prompt was printed:\n%s", out)
			}
			if suspended(t, c) != tc.applied {
				t.Errorf("answering %q applied = %v, want %v", tc.answer, !tc.applied, tc.applied)
			}
			if got := len(auditEvents(t, c)); (got > 0) != tc.applied {
				t.Errorf("answering %q recorded %d audit events", tc.answer, got)
			}
		})
	}
}

func TestModeCommand(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		want    wavefrontv1alpha1.Mode
		wantErr string
	}{
		{"canonical", "Enforce", wavefrontv1alpha1.ModeEnforce, ""},
		{"any case", "enforce", wavefrontv1alpha1.ModeEnforce, ""},
		{"shadow", "Shadow", wavefrontv1alpha1.ModeShadow, ""},
		{"anything else", "Off", "", "invalid mode"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := writeCluster()

			out, err := runWriteTree(t, c, false, "", cmdMode, tc.arg, flagYes)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one naming %q\n%s", err, tc.wantErr, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("mode %s: %v\n%s", tc.arg, err, out)
			}

			wf := &wavefrontv1alpha1.Wavefront{}
			if err := c.Get(t.Context(), types.NamespacedName{Name: testWavefront}, wf); err != nil {
				t.Fatalf("reading back the Wavefront: %v", err)
			}
			if wf.Spec.Mode != tc.want {
				t.Errorf("spec.mode is %q, want %q", wf.Spec.Mode, tc.want)
			}
		})
	}
}

func TestWriteFlagValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "--from replays and cannot write",
			args: []string{cmdSuspend, flagYes, "--from", fixture(fxQuiescent)},
			want: "--from replays",
		},
		{
			name: "--derive is a read-side flag",
			args: []string{cmdSuspend, flagYes, "--derive"},
			want: "--derive only changes",
		},
		{
			name: "--poll belongs to pin alone",
			args: []string{cmdPinStrip, flagYes, flagPoll},
			want: "applies only to `pin`",
		},
		{
			name: "an unparseable source",
			args: []string{cmdRelease, badSource, flagYes},
			want: badSource,
		},
		{
			name: "pin without a SHA",
			args: []string{cmdPin, testSource, flagYes},
			want: `required flag(s) "sha"`,
		},
		{
			name: "pin with an unchecked SHA",
			args: []string{cmdPin, testSource, "--sha", "abc123", flagYes},
			want: "--unverified",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := writeCluster()

			out, err := runWriteTree(t, c, false, "", tc.args...)
			if err == nil {
				t.Fatalf("%v was accepted:\n%s", tc.args, out)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want one naming %q", err, tc.want)
			}
			if suspended(t, c) {
				t.Error("a rejected command still wrote to the cluster")
			}
		})
	}
}

// TestAuditFailureIsAWarning proves the contract that matters most to an
// operator: the write is done, and a missing audit event does not tell them
// otherwise.
func TestAuditFailureIsAWarning(t *testing.T) {
	c := &auditRefusingClient{Client: writeCluster()}

	out, err := runWriteTree(t, c, false, "", cmdSuspend, flagYes)
	if err != nil {
		t.Fatalf("a failed audit event failed the command: %v\n%s", err, out)
	}
	if !suspended(t, c) {
		t.Error("the write did not happen")
	}
	if !strings.Contains(out, "Warning:") {
		t.Errorf("the missing audit event was not reported:\n%s", out)
	}
}

// auditRefusingClient is an apiserver that will not take events — the RBAC
// tier that can patch a Wavefront but not create an Event.
type auditRefusingClient struct {
	client.Client
}

func (c *auditRefusingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*eventsv1.Event); ok {
		return errors.New("events is forbidden")
	}
	return c.Client.Create(ctx, obj, opts...)
}

// TestPartialWriteIsAudited is the case the audit trail exists for: half the
// command landed. `pin-strip --suspend` against a token that may suspend the
// fleet but not patch a GitRepository suspends it and then fails — and an
// operator reading the trail afterwards has to find that out from the trail.
func TestPartialWriteIsAudited(t *testing.T) {
	c := &sourcePatchRefusingClient{Client: writeCluster()}

	out, err := runWriteTree(t, c, false, "", cmdPinStrip, "--suspend", flagYes)
	if err == nil {
		t.Fatalf("a denied strip exited 0:\n%s", out)
	}
	if !suspended(t, c) {
		t.Fatal("the suspend step did not land, so this proves nothing about partial writes")
	}

	events := auditEvents(t, c)
	if len(events) != 1 {
		t.Fatalf("got %d audit events for a partial write, want 1", len(events))
	}
	if events[0].Reason != actions.ReasonPinStripped {
		t.Errorf("audit reason is %q, want %q", events[0].Reason, actions.ReasonPinStripped)
	}
	if !strings.Contains(events[0].Note, "(partial: ") {
		t.Errorf("the audit note does not say the write was partial: %q", events[0].Note)
	}
	if !strings.Contains(events[0].Note, "could not be stripped") {
		t.Errorf("the audit note does not name what failed: %q", events[0].Note)
	}
}

// sourcePatchRefusingClient is an apiserver that takes Wavefront patches and
// refuses GitRepository ones: the operator tier that stops at a namespace
// boundary.
type sourcePatchRefusingClient struct {
	client.Client
}

func (c *sourcePatchRefusingClient) Patch(
	ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption,
) error {
	if _, ok := obj.(*sourcev1.GitRepository); ok {
		return errors.New("gitrepositories is forbidden")
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}
