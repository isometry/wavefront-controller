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
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/wfctl/render"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// Command names, shared with the tests that drive them.
const (
	cmdStatus   = "status"
	cmdNodes    = "nodes"
	cmdSources  = "sources"
	cmdSource   = "source"
	cmdExplain  = "explain"
	cmdGraph    = "graph"
	cmdSnapshot = "snapshot"
)

// Format sets, per command. Each command declares what it can actually
// produce, so `wfctl explain -o dot` fails at the flag rather than silently
// rendering a table.
var (
	tableFormats = []string{outputTable, outputWide}
	readFormats  = []string{outputTable, outputWide, outputJSON, outputYAML}
	graphFormats = []string{outputTable, outputWide, outputDOT, outputMermaid}
)

// addReadCommands registers the commands that only read.
//
// The write commands and `history` register the same way, against the
// same Options: every flag they share is already persistent on the root, so
// neither has to touch the plumbing here.
func addReadCommands(root *cobra.Command, o *Options) {
	root.AddCommand(
		newStatusCommand(o),
		newNodesCommand(o),
		newSourcesCommand(o),
		newSourceCommand(o),
		newExplainCommand(o),
		newGraphCommand(o),
		newSnapshotCommand(o),
	)
}

// newStatusCommand reports the fleet's headline.
func newStatusCommand(o *Options) *cobra.Command {
	return &cobra.Command{
		Use:   cmdStatus,
		Short: "Summarise the Wavefront: phase, counts, and what is blocked or held",
		Long: `Summarise the Wavefront: phase, counts, and what is blocked or held.

With --derive the reported and freshly derived pictures are printed side by
side, and disagreement between them is evidence about the controller.

Exits 2 when the fleet is Blocked (the derived phase under --derive, the
reported one otherwise), 1 on error, 0 otherwise.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			snap, err := o.prepare(cmd, readFormats)
			if err != nil {
				return err
			}
			if err := o.reportStatus(cmd.OutOrStdout(), snap); err != nil {
				return err
			}
			// The exit code answers the same question in every format. A
			// script that pipes `-o json` into jq is exactly the caller most
			// likely to check $?, and telling it 0 while the document says
			// Blocked would be the worst of both.
			if phase(snap) == wavefrontv1alpha1.PhaseBlocked {
				return &ExitError{Code: exitBlocked}
			}
			return nil
		},
	}
}

// reportStatus writes the status report in whichever format was asked for.
func (o *Options) reportStatus(w io.Writer, snap *snapshot.Snapshot) error {
	if isEncoded(o.Output) {
		return o.encode(w, statusPayload{
			Wavefront:   snap.Wavefront,
			Derived:     snap.Derived,
			Diagnostics: snap.Diagnostics,
		})
	}
	return render.Status(w, snap, o.renderOptions())
}

// statusPayload is `status -o json|yaml`: the Wavefront as the cluster holds
// it, what a derivation proved, and how the picture is degraded.
type statusPayload struct {
	Wavefront   snapshot.WavefrontView `json:"wavefront"`
	Derived     snapshot.DerivedStatus `json:"derived"`
	Diagnostics []string               `json:"diagnostics,omitempty"`
}

// phase is the phase the exit code answers for: a derivation's own verdict
// when there is one, the controller's published one otherwise. Under --derive
// the published phase may be exactly what is stale.
func phase(snap *snapshot.Snapshot) wavefrontv1alpha1.Phase {
	if snap.Origin == snapshot.OriginDerive {
		return snap.Derived.Phase
	}
	return snap.Wavefront.Status.Phase
}

// newNodesCommand lists every evaluated node.
func newNodesCommand(o *Options) *cobra.Command {
	return &cobra.Command{
		Use:   cmdNodes,
		Short: "List every evaluated node with its state, pin and observed SHA",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			snap, err := o.prepare(cmd, readFormats)
			if err != nil {
				return err
			}
			if isEncoded(o.Output) {
				return o.encode(cmd.OutOrStdout(), snap.Nodes)
			}
			return render.Nodes(cmd.OutOrStdout(), snap, o.renderOptions())
		},
	}
}

// newSourcesCommand lists every managed source.
func newSourcesCommand(o *Options) *cobra.Command {
	return &cobra.Command{
		Use:   cmdSources,
		Short: "List every managed GitRepository with its pin, hold and owners",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			snap, err := o.prepare(cmd, readFormats)
			if err != nil {
				return err
			}
			if isEncoded(o.Output) {
				return o.encode(cmd.OutOrStdout(), snap.Sources)
			}
			return render.Sources(cmd.OutOrStdout(), snap, o.renderOptions())
		},
	}
}

// newSourceCommand details one source.
func newSourceCommand(o *Options) *cobra.Command {
	return &cobra.Command{
		Use:   cmdSource + " NAMESPACE/NAME",
		Short: "Show one source in full: pin, provenance, owners, conditions and nodes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref, err := snapshot.ParseSource(args[0])
			if err != nil {
				return err
			}
			snap, err := o.prepare(cmd, readFormats)
			if err != nil {
				return err
			}
			if isEncoded(o.Output) {
				view := findSource(snap, ref.String())
				if view == nil {
					return fmt.Errorf("no source %s in this snapshot", ref)
				}
				return o.encode(cmd.OutOrStdout(), view)
			}
			return render.Source(cmd.OutOrStdout(), snap, ref.String(), o.renderOptions())
		},
	}
}

// findSource locates a source by its "ns/name".
func findSource(snap *snapshot.Snapshot, name string) *snapshot.SourceView {
	for i := range snap.Sources {
		if snap.Sources[i].Name == name {
			return &snap.Sources[i]
		}
	}
	return nil
}

// newExplainCommand walks a node's blocked chain to its root cause.
func newExplainCommand(o *Options) *cobra.Command {
	return &cobra.Command{
		Use:   cmdExplain + " NAMESPACE/NAME | KIND/NAMESPACE/NAME",
		Short: "Walk a node's blocked chain to its root cause and name the fix",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref, err := snapshot.ParseNodeRef(args[0])
			if err != nil {
				return err
			}
			snap, err := o.prepare(cmd, tableFormats)
			if err != nil {
				return err
			}
			return render.Explain(cmd.OutOrStdout(), snap, ref, o.renderOptions())
		},
	}
}

// newGraphCommand renders the dependency graph.
func newGraphCommand(o *Options) *cobra.Command {
	var tree bool

	cmd := &cobra.Command{
		Use:   cmdGraph,
		Short: "Render the dependency graph as waves, a tree, DOT or Mermaid",
		Long: `Render the dependency graph.

The default layout is waves: dependency-free nodes first, then everything one
edge deeper, with anything in or behind a cycle listed unlayered at the end.
--tree walks the same edges depth-first from the wave-0 roots instead.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if tree && (o.Output == outputDOT || o.Output == outputMermaid) {
				return fmt.Errorf("--tree is a text layout and cannot be combined with -o %s", o.Output)
			}
			snap, err := o.prepare(cmd, graphFormats)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			switch o.Output {
			case outputDOT:
				return render.DOT(out, snap)
			case outputMermaid:
				return render.Mermaid(out, snap)
			}
			if tree {
				return render.Tree(out, snap, o.renderOptions())
			}
			return render.Graph(out, snap, o.renderOptions())
		},
	}
	cmd.Flags().BoolVar(&tree, "tree", false, "Render a dependency tree instead of waves")

	return cmd
}

// newSnapshotCommand captures the whole picture for replay.
func newSnapshotCommand(o *Options) *cobra.Command {
	var path string

	cmd := &cobra.Command{
		Use:   cmdSnapshot,
		Short: "Capture the whole snapshot for replay with --from",
		Long: `Capture the whole snapshot for replay with --from.

The document is JSON unless -o yaml is given, and carries no secret data,
credentials, kubeconfig or URL userinfo — it is meant to be attached to a
ticket.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			snap, err := o.prepare(cmd, readFormats)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			if path != "" {
				file, err := os.Create(path)
				if err != nil {
					return fmt.Errorf("creating %s: %w", path, err)
				}
				defer file.Close() //nolint:errcheck // the write error below is the one that matters
				out = file
			}

			if o.Output == outputYAML {
				return render.YAML(out, snap)
			}
			return render.JSON(out, snap)
		},
	}
	cmd.Flags().StringVarP(&path, "file", "f", "", "Write the snapshot to this file instead of stdout")

	return cmd
}

// prepare validates the flags this command can honour and captures the
// picture it renders. Validation happens before any cluster is contacted, so
// a mistyped flag fails immediately rather than after a poll.
func (o *Options) prepare(cmd *cobra.Command, formats []string) (*snapshot.Snapshot, error) {
	if err := o.validate(formats); err != nil {
		return nil, err
	}
	return o.capture(cmd.Context())
}

// isEncoded reports whether the format is one of the machine-readable pair.
func isEncoded(format string) bool {
	return format == outputJSON || format == outputYAML
}

// encode writes v in whichever machine-readable format was asked for.
func (o *Options) encode(w io.Writer, v any) error {
	switch o.Output {
	case outputJSON:
		return render.JSON(w, v)
	case outputYAML:
		return render.YAML(w, v)
	}
	return fmt.Errorf("%w: %q", errNotEncoded, o.Output)
}

// errNotEncoded guards encode against a caller that reached it with a table
// format: a silent fallthrough would print nothing at all.
var errNotEncoded = errors.New("not a machine-readable output format")
