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
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/isometry/wavefront-controller/internal/wfctl/render"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// update rewrites every golden file from the current renderers. Reviewing the
// resulting diff is how a deliberate output change is signed off.
var update = flag.Bool("update", false, "rewrite the golden files")

// fixtureNow anchors every rendered age, so a golden is a function of its
// fixture alone and not of the day the suite runs.
const fixtureNow = "2026-09-01T12:00:00Z"

// testdataDir holds both the snapshot fixtures and the goldens they produce.
const testdataDir = "testdata"

// Command names, as they appear in golden filenames.
const (
	cmdNodes   = "nodes"
	cmdSources = "sources"
	cmdStatus  = "status"
	cmdGraph   = "graph"
)

// fmtTable is the default output format's golden suffix.
const fmtTable = "table"

// fxHandPin is the fixture the coloured renderings are taken from: it has a
// held source, so it exercises the dim treatment as well as the state
// colours.
const fxHandPin = "hand-pin-hold"

// fixtures are the snapshots every renderer is exercised against, plus the
// arguments the two node- and source-scoped commands need. Between them they
// cover a quiescent fleet, a blocked subtree, a hand-pin, Shadow mode, a
// cycle, an unobservable derive run, stale status with an unreadable source,
// and a shared source.
var fixtures = []struct {
	name string
	// explain names the nodes `wfctl explain` is run against.
	explain []string
	// sources names the sources `wfctl source` is run against.
	sources []string
	// encode adds the -o json / -o yaml goldens. One fixture is enough:
	// the encoders are content-agnostic, and eight copies of the same
	// serialisation would test the schema, not the renderer.
	encode bool
}{
	{name: "quiescent", sources: []string{"sources/infra"}, encode: true},
	{name: "blocked-ancestor-unhealthy", explain: []string{"flotillas/leaf"}},
	{
		name:    fxHandPin,
		explain: []string{"flotillas/grandchild", "flotillas/held"},
		sources: []string{"sources/held"},
	},
	{name: "shadow-pending"},
	{name: "cycle", explain: []string{"flotillas/a"}},
	{name: "unobserved-derive", explain: []string{"flotillas/app"}},
	{name: "stale-status", sources: []string{"sources/two"}},
	{
		name:    "shared-source",
		explain: []string{"flotillas/api"},
		sources: []string{"sources/mono"},
	},
}

// renderer is one renderer bound to its options.
type renderer func(w io.Writer, s *snapshot.Snapshot) error

// goldenCase names the file a renderer's output is compared against:
// "<fixture>.<cmd>.<format>.golden".
type goldenCase struct {
	cmd    string
	format string
	render renderer
}

func TestGolden(t *testing.T) {
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			snap := loadFixture(t, fixture.name)

			cases := tableCases()
			for _, ref := range fixture.explain {
				cases = append(cases, explainCase(t, ref))
			}
			for _, name := range fixture.sources {
				cases = append(cases, sourceCase(name))
			}
			if fixture.encode {
				cases = append(cases, encodeCases()...)
			}

			for _, tc := range cases {
				t.Run(tc.cmd+"."+tc.format, func(t *testing.T) {
					var got bytes.Buffer
					if err := tc.render(&got, snap); err != nil {
						t.Fatalf("rendering %s %s: %v", tc.cmd, tc.format, err)
					}
					compare(t, goldenPath(fixture.name, tc.cmd, tc.format), got.Bytes())
				})
			}
		})
	}
}

// colourCases are the coloured renderers, each paired with the uncoloured
// golden its layout must match exactly.
var colourCases = []struct {
	fixture string
	cmd     string
	// plain names the format of the uncoloured golden this must reduce to.
	plain  string
	render func(w io.Writer, s *snapshot.Snapshot, o render.Options) error
}{
	{fxHandPin, cmdNodes, fmtTable, render.Nodes},
	{fxHandPin, cmdSources, fmtTable, render.Sources},
	{fxHandPin, cmdGraph, "waves", render.Graph},
}

// TestGoldenColour is the only place the palette is switched on: ANSI in
// every golden would make every other diff unreadable.
//
// The assertion that matters is structural, not byte-for-byte. Colour is
// applied after layout precisely because text/tabwriter counts escape
// sequences as width, so the property to defend is that stripping the ANSI
// back out returns the uncoloured golden *exactly* — same cells, same column
// positions, same padding. That is strictly stronger than comparing the
// visible start position of each cell, and it cannot be satisfied by a table
// that is merely consistently skewed.
func TestGoldenColour(t *testing.T) {
	for _, tc := range colourCases {
		t.Run(tc.fixture+"."+tc.cmd, func(t *testing.T) {
			snap := loadFixture(t, tc.fixture)
			opts := options()
			opts.Palette = render.NewPalette(true)

			var got bytes.Buffer
			if err := tc.render(&got, snap, opts); err != nil {
				t.Fatalf("rendering coloured %s: %v", tc.cmd, err)
			}

			if !bytes.Contains(got.Bytes(), []byte("\x1b[")) {
				t.Fatal("no ANSI sequence in coloured output")
			}
			if bytes.ContainsRune(got.Bytes(), '\xff') {
				t.Error("tabwriter escape bytes leaked into the output")
			}

			plain, err := os.ReadFile(goldenPath(tc.fixture, tc.cmd, tc.plain))
			if err != nil {
				t.Fatalf("reading uncoloured golden: %v", err)
			}
			if stripped := stripANSI(got.String()); stripped != string(plain) {
				t.Errorf("coloured layout differs from the uncoloured one\n--- want ---\n%s\n--- got (ANSI stripped) ---\n%s",
					plain, stripped)
			}

			compare(t, goldenPath(tc.fixture, tc.cmd, "color"), got.Bytes())
		})
	}
}

// TestColumnHeaders pins the expected column lists independently of the
// goldens, so `-update` cannot quietly absorb a renamed, dropped or reordered
// column.
func TestColumnHeaders(t *testing.T) {
	snap := loadFixture(t, "quiescent")
	wide := options()
	wide.Wide = true

	tests := []struct {
		name   string
		render func(w io.Writer, s *snapshot.Snapshot, o render.Options) error
		opts   render.Options
		want   string
	}{
		{
			"nodes", render.Nodes, options(),
			"NODE ROLE STATE HELD BLOCKED ANCESTOR SOURCE PIN OBSERVED LAG",
		},
		{
			"nodes wide", render.Nodes, wide,
			"NODE ROLE STATE HELD BLOCKED ANCESTOR SOURCE PIN OBSERVED LAG WAVE READY DEPENDS",
		},
		{
			"sources", render.Sources, options(),
			"SOURCE PIN OBSERVED PENDING HOLD OWNERS ARTIFACT ADMITTED NODES",
		},
		{
			"sources wide", render.Sources, wide,
			"SOURCE PIN OBSERVED PENDING HOLD OWNERS ARTIFACT ADMITTED NODES PREVIOUS-PIN OBSERVED-REF FETCH URL",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := tc.render(&out, snap, tc.opts); err != nil {
				t.Fatalf("rendering: %v", err)
			}
			header, _, _ := strings.Cut(out.String(), "\n")
			if got := strings.Join(strings.Fields(header), " "); got != tc.want {
				t.Errorf("columns changed unexpectedly\n want: %s\n  got: %s", tc.want, got)
			}
		})
	}
}

// stripANSI removes CSI escape sequences, so a coloured rendering can be
// compared with the uncoloured layout it must reduce to. Deliberately a
// second implementation, independent of the package's own: a shared helper
// would let one bug hide the other.
func stripANSI(text string) string {
	var b strings.Builder
	for i := 0; i < len(text); {
		if text[i] != 0x1b || i+1 >= len(text) || text[i+1] != '[' {
			b.WriteByte(text[i])
			i++
			continue
		}
		i += 2
		for i < len(text) && (text[i] < 0x40 || text[i] > 0x7e) {
			i++
		}
		if i < len(text) {
			i++
		}
	}
	return b.String()
}

// tableCases are the renderers every fixture is put through, so that no
// fixture can reach a renderer that has never seen it.
func tableCases() []goldenCase {
	opts := options()
	wide := options()
	wide.Wide = true

	return []goldenCase{
		{cmdNodes, fmtTable, func(w io.Writer, s *snapshot.Snapshot) error { return render.Nodes(w, s, opts) }},
		{cmdNodes, "wide", func(w io.Writer, s *snapshot.Snapshot) error { return render.Nodes(w, s, wide) }},
		{cmdSources, fmtTable, func(w io.Writer, s *snapshot.Snapshot) error { return render.Sources(w, s, opts) }},
		{cmdSources, "wide", func(w io.Writer, s *snapshot.Snapshot) error { return render.Sources(w, s, wide) }},
		{cmdStatus, fmtTable, func(w io.Writer, s *snapshot.Snapshot) error { return render.Status(w, s, opts) }},
		{cmdGraph, "waves", func(w io.Writer, s *snapshot.Snapshot) error { return render.Graph(w, s, opts) }},
		{cmdGraph, "tree", func(w io.Writer, s *snapshot.Snapshot) error { return render.Tree(w, s, opts) }},
		{cmdGraph, "dot", func(w io.Writer, s *snapshot.Snapshot) error { return render.DOT(w, s) }},
		{cmdGraph, "mermaid", func(w io.Writer, s *snapshot.Snapshot) error { return render.Mermaid(w, s) }},
	}
}

// explainCase builds the case for one `wfctl explain <node>`.
func explainCase(t *testing.T, name string) goldenCase {
	t.Helper()
	ref, err := snapshot.ParseNodeRef(name)
	if err != nil {
		t.Fatalf("parsing node reference %q: %v", name, err)
	}
	opts := options()
	return goldenCase{
		cmd:    "explain." + fileToken(name),
		format: fmtTable,
		render: func(w io.Writer, s *snapshot.Snapshot) error { return render.Explain(w, s, ref, opts) },
	}
}

// sourceCase builds the case for one `wfctl source <ns/name>`.
func sourceCase(name string) goldenCase {
	opts := options()
	return goldenCase{
		cmd:    "source." + fileToken(name),
		format: fmtTable,
		render: func(w io.Writer, s *snapshot.Snapshot) error { return render.Source(w, s, name, opts) },
	}
}

// encodeCases render the whole snapshot through the machine-readable
// formats.
func encodeCases() []goldenCase {
	return []goldenCase{
		{"snapshot", "json", func(w io.Writer, s *snapshot.Snapshot) error { return render.JSON(w, s) }},
		{"snapshot", "yaml", func(w io.Writer, s *snapshot.Snapshot) error { return render.YAML(w, s) }},
	}
}

// options are the shared render options: fixed clock, no colour.
func options() render.Options {
	return render.Options{Now: mustParse(fixtureNow)}
}

func mustParse(stamp string) time.Time {
	at, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		panic(err)
	}
	return at
}

// loadFixture reads a fixture through FileSource rather than encoding/json,
// so a fixture that does not match the schema — a wrong apiVersion, a field
// the renderers would silently see as zero — fails loudly here instead of
// quietly producing a plausible golden.
func loadFixture(t *testing.T, name string) *snapshot.Snapshot {
	t.Helper()
	path := filepath.Join(testdataDir, name+".snapshot.json")
	snap, err := snapshot.FileSource{Path: path}.Capture(context.Background())
	if err != nil {
		t.Fatalf("loading fixture %s: %v", path, err)
	}
	return snap
}

func goldenPath(fixture, cmd, format string) string {
	return filepath.Join(testdataDir, fixture+"."+cmd+"."+format+".golden")
}

// fileToken makes an argument safe to put in a filename.
func fileToken(name string) string {
	return filepath.Base(name)
}

// compare checks output against its golden file, or rewrites it under
// -update.
func compare(t *testing.T, path string, got []byte) {
	t.Helper()

	if *update {
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatalf("writing golden %s: %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden %s (re-run with -update to create it): %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("output does not match %s\n--- want ---\n%s\n--- got ---\n%s", path, want, got)
	}
}
