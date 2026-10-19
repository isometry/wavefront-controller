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

package render_test

import (
	"bytes"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/wfctl/render"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// TestExplainUnknownNode: a reference that is not in the snapshot is an
// error, not an empty walk. Printing nothing would read as "nothing wrong".
func TestExplainUnknownNode(t *testing.T) {
	snap := loadFixture(t, "quiescent")
	ref := adapter.NodeRef{Kind: "Kustomization", Namespace: "flotillas", Name: "nope"}

	var out bytes.Buffer
	err := render.Explain(&out, snap, ref, options())
	if err == nil {
		t.Fatal("explaining an unknown node did not fail")
	}
	if !strings.Contains(err.Error(), "flotillas/nope") {
		t.Errorf("error does not name the node: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("wrote output for an unknown node: %q", out.String())
	}
}

// TestExplainFleetBrakes: an Admissible root cause is only as good as the
// fleet-wide brakes, so explain names the brake that will not let the
// controller write, in the controller's own precedence (overlap, suspend,
// Shadow), and never falls back to "wait for convergence".
func TestExplainFleetBrakes(t *testing.T) {
	overlapCond := func(s *snapshot.Snapshot) {
		s.Wavefront.Status.Conditions = []metav1.Condition{{
			Type: wavefrontv1alpha1.ConditionGraphValid, Status: metav1.ConditionFalse,
			Reason: wavefrontv1alpha1.GraphValidReasonSourceOverlap, Message: "source flotillas/s is shared",
		}}
	}
	suspend := func(s *snapshot.Snapshot) { s.Wavefront.Spec.Suspend = true }
	enforce := func(s *snapshot.Snapshot) { s.Wavefront.Spec.Mode = wavefrontv1alpha1.ModeEnforce }

	const waitFix = "wait for convergence"
	tests := []struct {
		name   string
		mutate []func(*snapshot.Snapshot)
		want   []string
		absent []string
	}{
		{
			name:   "suspend",
			mutate: []func(*snapshot.Snapshot){enforce, suspend},
			want:   []string{"fix: resume the Wavefront", "never admitted. Run `wfctl resume`; the controller then admits", "wavefront: fleet is suspended"},
			absent: []string{waitFix, "in git"},
		},
		{
			name: "suspend owned by GitOps",
			mutate: []func(*snapshot.Snapshot){enforce, suspend, func(s *snapshot.Snapshot) {
				s.Wavefront.SpecOwners = map[string]string{snapshot.FieldSuspend: "kustomize-controller"}
			}},
			want: []string{"spec.suspend is owned by kustomize-controller, so `wfctl resume` would be reverted: set it to false in git. The controller then admits",
				"(spec.suspend owned by kustomize-controller)"},
			absent: []string{waitFix, "Run `wfctl resume`"},
		},
		{
			name:   "overlap, status origin",
			mutate: []func(*snapshot.Snapshot){enforce, overlapCond},
			want:   []string{"fix: resolve the overlap: source flotillas/s is shared", "wavefront: admissions suppressed: source flotillas/s is shared"},
			absent: []string{waitFix},
		},
		{
			name: "overlap, derive origin",
			mutate: []func(*snapshot.Snapshot){enforce, func(s *snapshot.Snapshot) {
				s.Origin = snapshot.OriginDerive
				s.Derived.GraphReason = wavefrontv1alpha1.GraphValidReasonSelectorOverlap
				s.Derived.GraphMessage = "selectors overlap"
			}},
			want:   []string{"fix: resolve the overlap: selectors overlap"},
			absent: []string{waitFix},
		},
		{
			name:   "overlap wins over suspend",
			mutate: []func(*snapshot.Snapshot){enforce, suspend, overlapCond},
			want:   []string{"fix: resolve the overlap", "wavefront: fleet is suspended", "wavefront: admissions suppressed"},
			absent: []string{"fix: resume"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snap := loadFixture(t, "shadow-pending")
			for _, mutate := range tc.mutate {
				mutate(snap)
			}
			ref := adapter.NodeRef{Kind: "Kustomization", Namespace: "flotillas", Name: "beta"}

			var out bytes.Buffer
			if err := render.Explain(&out, snap, ref, options()); err != nil {
				t.Fatalf("explaining: %v", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(out.String(), want) {
					t.Errorf("output lacks %q:\n%s", want, out.String())
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(out.String(), absent) {
					t.Errorf("output contains %q:\n%s", absent, out.String())
				}
			}
		})
	}
}

// TestBrakeNotes: `status` and `nodes` say that the fleet-wide brakes are on,
// because the engine still reports Admissible nodes under them.
func TestBrakeNotes(t *testing.T) {
	snap := loadFixture(t, "shadow-pending")
	var nodes, status bytes.Buffer
	if err := render.Nodes(&nodes, snap, options()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(nodes.String(), "note:") {
		t.Errorf("unbraked fleet got a note:\n%s", nodes.String())
	}

	snap.Wavefront.Spec.Suspend = true
	snap.Wavefront.Status.Conditions = []metav1.Condition{{
		Type: wavefrontv1alpha1.ConditionGraphValid, Status: metav1.ConditionFalse,
		Reason: wavefrontv1alpha1.GraphValidReasonSelectorOverlap, Message: "selectors overlap",
	}}
	nodes.Reset()
	if err := render.Nodes(&nodes, snap, options()); err != nil {
		t.Fatal(err)
	}
	if err := render.Status(&status, snap, options()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"note: admissions are suppressed (selectors overlap)",
		"note: pin writes are frozen (spec.suspend)",
	} {
		if !strings.Contains(nodes.String(), want) {
			t.Errorf("nodes output lacks %q:\n%s", want, nodes.String())
		}
	}
	if want := "SUSPEND:"; !strings.Contains(status.String(), want) ||
		!strings.Contains(status.String(), "true (pin writes frozen; Admissible nodes will not advance)") {
		t.Errorf("status output does not annotate SUSPEND:\n%s", status.String())
	}
}

// TestSourceUnknown: likewise for a source the snapshot does not carry.
func TestSourceUnknown(t *testing.T) {
	snap := loadFixture(t, "quiescent")

	var out bytes.Buffer
	err := render.Source(&out, snap, "sources/nope", options())
	if err == nil {
		t.Fatal("rendering an unknown source did not fail")
	}
	if !strings.Contains(err.Error(), "sources/nope") {
		t.Errorf("error does not name the source: %v", err)
	}
}

// TestEmptySnapshot: every renderer has to survive a Wavefront that selects
// nothing — a fresh install, or a selector that matches no labels. Headers
// only, no panics.
func TestEmptySnapshot(t *testing.T) {
	empty := &snapshot.Snapshot{
		APIVersion: snapshot.Version,
		Kind:       snapshot.KindSnapshot,
		Origin:     snapshot.OriginStatus,
		Observed:   true,
	}
	opts := options()

	renderers := map[string]func() error{
		cmdNodes:   func() error { return render.Nodes(discard(t), empty, opts) },
		cmdSources: func() error { return render.Sources(discard(t), empty, opts) },
		cmdStatus:  func() error { return render.Status(discard(t), empty, opts) },
		cmdGraph:   func() error { return render.Graph(discard(t), empty, opts) },
		"tree":     func() error { return render.Tree(discard(t), empty, opts) },
		"dot":      func() error { return render.DOT(discard(t), empty) },
		"mermaid":  func() error { return render.Mermaid(discard(t), empty) },
		"json":     func() error { return render.JSON(discard(t), empty) },
		"yaml":     func() error { return render.YAML(discard(t), empty) },
	}
	for name, run := range renderers {
		t.Run(name, func(t *testing.T) {
			if err := run(); err != nil {
				t.Fatalf("rendering an empty snapshot: %v", err)
			}
		})
	}
}

// TestPaletteDisabled: the zero Palette must not emit a single escape byte,
// because that is what every non-terminal caller gets.
func TestPaletteDisabled(t *testing.T) {
	snap := loadFixture(t, fxHandPin)

	var out bytes.Buffer
	if err := render.Nodes(&out, snap, options()); err != nil {
		t.Fatalf("rendering nodes: %v", err)
	}
	if bytes.ContainsAny(out.Bytes(), "\x1b\xff") {
		t.Errorf("colourless output contains escapes: %q", out.String())
	}
}

// discard returns a writer whose output the test does not inspect.
func discard(t *testing.T) *bytes.Buffer {
	t.Helper()
	return &bytes.Buffer{}
}
