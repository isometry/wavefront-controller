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
