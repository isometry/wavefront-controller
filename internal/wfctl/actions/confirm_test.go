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

package actions_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
)

// The consent rule is pure logic over three inputs — --yes, --dry-run, and
// whether anyone is there to answer — so it is tested without a cluster, a
// terminal, or ginkgo. Every write command runs through it.

// testPlan builds a plan whose Apply records that it ran.
func testPlan(applied *bool) *actions.Plan {
	return &actions.Plan{
		Summary: "Hand-pin default/infra to abc123",
		Before:  map[string]string{fieldCommit: "old", "metadata.annotations[x]": ""},
		After:   map[string]string{fieldCommit: "abc123"},
		Warnings: []string{
			"the controller will report this as held",
		},
		Apply: func(context.Context) (bool, error) {
			*applied = true
			return true, nil
		},
	}
}

func TestConfirmerConsent(t *testing.T) {
	tests := []struct {
		name        string
		confirmer   actions.Confirmer
		answer      string
		wantApplied bool
		wantErr     error
		wantOutput  string
	}{
		{
			name:        "--yes applies without asking",
			confirmer:   actions.Confirmer{Yes: true},
			wantApplied: true,
		},
		{
			name:       "--dry-run prints and stops",
			confirmer:  actions.Confirmer{DryRun: true, Yes: true},
			wantOutput: "Dry run: nothing was written.",
		},
		{
			name:        "y at a terminal applies",
			confirmer:   actions.Confirmer{IsTTY: yes},
			answer:      "y\n",
			wantApplied: true,
			wantOutput:  "Proceed? [y/N]",
		},
		{
			name:        "yes at a terminal applies",
			confirmer:   actions.Confirmer{IsTTY: yes},
			answer:      "YES\n",
			wantApplied: true,
		},
		{
			name:       "an empty answer declines",
			confirmer:  actions.Confirmer{IsTTY: yes},
			answer:     "\n",
			wantOutput: "Aborted: nothing was written.",
		},
		{
			name:      "anything else declines",
			confirmer: actions.Confirmer{IsTTY: yes},
			answer:    "no\n",
		},
		{
			name:      "a closed stream is not consent",
			confirmer: actions.Confirmer{IsTTY: yes},
			answer:    "",
		},
		{
			name:      "no terminal and no --yes refuses to prompt",
			confirmer: actions.Confirmer{},
			wantErr:   actions.ErrNotATerminal,
		},
		{
			name:      "an explicitly non-interactive stdin refuses too",
			confirmer: actions.Confirmer{IsTTY: no},
			wantErr:   actions.ErrNotATerminal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				applied bool
				out     bytes.Buffer
			)
			confirmer := tc.confirmer
			confirmer.Out = &out
			confirmer.In = strings.NewReader(tc.answer)

			got, err := confirmer.Run(context.Background(), testPlan(&applied))

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Run() error = %v, want %v", err, tc.wantErr)
			}
			if got != tc.wantApplied || applied != tc.wantApplied {
				t.Errorf("Run() = %v (applied %v), want %v", got, applied, tc.wantApplied)
			}
			if tc.wantOutput != "" && !strings.Contains(out.String(), tc.wantOutput) {
				t.Errorf("output does not contain %q:\n%s", tc.wantOutput, out.String())
			}
		})
	}
}

func TestConfirmerPrintsThePlan(t *testing.T) {
	var (
		applied bool
		out     bytes.Buffer
	)

	confirmer := actions.Confirmer{Out: &out, In: strings.NewReader(""), DryRun: true}
	if _, err := confirmer.Run(context.Background(), testPlan(&applied)); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	for _, want := range []string{
		"Hand-pin default/infra to abc123",
		"FIELD", "BEFORE", "AFTER",
		fieldCommit, "old", "abc123",
		// A field named by one side alone reads as absent, not blank.
		"metadata.annotations[x]", "(unset)",
		"Warning: the controller will report this as held",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("plan output does not contain %q:\n%s", want, out.String())
		}
	}
	if applied {
		t.Error("a dry run applied the plan")
	}
}

func TestConfirmerReportsApplyFailure(t *testing.T) {
	boom := errors.New("boom")

	tests := []struct {
		name        string
		written     bool
		wantWritten bool
	}{
		// Nothing reached the cluster: the caller records no audit event.
		{"a write that never landed", false, false},
		// A partial write: the error stands, and the caller still has to
		// record what happened to the fleet.
		{"a write that partly landed", true, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan := &actions.Plan{
				Summary: "x",
				Apply:   func(context.Context) (bool, error) { return tc.written, boom },
			}

			written, err := actions.Confirmer{Out: &bytes.Buffer{}, Yes: true}.
				Run(context.Background(), plan)
			if !errors.Is(err, boom) {
				t.Fatalf("Run() error = %v, want %v", err, boom)
			}
			if written != tc.wantWritten {
				t.Errorf("Run() written = %v, want %v", written, tc.wantWritten)
			}
		})
	}
}

func TestPartialNote(t *testing.T) {
	note := actions.Partial(actions.Note("alice@prod", "wfctl pin-strip --yes"), errors.New("denied"))

	if want := "alice@prod ran: wfctl pin-strip --yes (partial: denied)"; note != want {
		t.Errorf("Partial() = %q, want %q", note, want)
	}
	if long := actions.Partial(strings.Repeat("x", 2000), errors.New("denied")); len(long) > 1024 {
		t.Errorf("Partial() returned %d bytes, want <= 1024", len(long))
	}
}

func TestPlanFields(t *testing.T) {
	plan := &actions.Plan{
		Before: map[string]string{fieldSuspend: "false", "a": "1"},
		After:  map[string]string{fieldSuspend: valueTrue, "z": "2"},
	}

	want := []string{"a", fieldSuspend, "z"}
	if got := plan.Fields(); !reflect.DeepEqual(got, want) {
		t.Errorf("Fields() = %v, want %v", got, want)
	}
}

func TestNote(t *testing.T) {
	if got, want := actions.Note("alice@prod", "wfctl pin a/b --sha c"),
		"alice@prod ran: wfctl pin a/b --sha c"; got != want {
		t.Errorf("Note() = %q, want %q", got, want)
	}
}

func yes() bool { return true }
func no() bool  { return false }
