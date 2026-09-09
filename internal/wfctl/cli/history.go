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
	"time"

	"github.com/spf13/cobra"
	eventsv1 "k8s.io/api/events/v1"

	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/wfctl/render"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// cmdHistory is the command name, shared with the tests that drive it.
const cmdHistory = "history"

// historyCaveat is B3's history row, printed verbatim on every invocation: an
// event stream is a convenience trail, not the durable ledger, and this is
// the one place that has to say so every time.
const historyCaveat = "Events are at-least-once (DESIGN §4.2) and retained only for the API " +
	"server's --event-ttl (default 1h); the durable ledger is provenance annotations " +
	"(`wfctl sources -o wide`) and log aggregation."

// addHistoryCommand registers `history` the same way Task 7's writes did:
// against the existing Options, touching none of the persistent-flag wiring.
func addHistoryCommand(root *cobra.Command, o *Options) {
	root.AddCommand(newHistoryCommand(o))
}

// newHistoryCommand lists the admission ledger: the controller's own events
// and wfctl's audit trail, merged, because both are recorded regarding the
// same Wavefront (plan B3).
func newHistoryCommand(o *Options) *cobra.Command {
	var (
		source   string
		node     string
		reason   string
		warnings bool
		since    time.Duration
	)

	cmd := &cobra.Command{
		Use:   cmdHistory,
		Short: "List the controller's and wfctl's own events for this Wavefront",
		Long: `List the controller's and wfctl's own events for this Wavefront.

Both streams are recorded regarding the Wavefront, so one list sees the
controller's admissions, holds and failures beside every wfctl write's audit
event. --source token-matches the note against one "namespace/name"; --node
resolves that node's current source through the selected snapshot provider
(status by default) and filters the same way.

Events are a convenience trail, not the durable record: the caveat printed
after every listing says where the durable one lives.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			events, err := o.listHistory(cmd, source, node, reason, warnings, since)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			if isEncoded(o.Output) {
				if err := o.encode(out, events); err != nil {
					return err
				}
				// The caveat is not part of the document: a `-o json` pipeline
				// into jq must see nothing but the events on stdout.
				_, err := fmt.Fprintln(cmd.ErrOrStderr(), historyCaveat)
				return err
			}

			if err := render.History(out, events, o.renderOptions()); err != nil {
				return err
			}
			_, err = fmt.Fprintln(out, historyCaveat)
			return err
		},
	}

	cmd.Flags().StringVar(&source, "source", "",
		"Filter to events whose note names this source (NAMESPACE/NAME)")
	cmd.Flags().StringVar(&node, "node", "",
		"Filter to events for the source this node currently resolves to (NAMESPACE/NAME or Kind/NAMESPACE/NAME)")
	cmd.Flags().StringVar(&reason, "reason", "", "Filter to one event reason (e.g. PinAdvanced, HoldDetected)")
	cmd.Flags().BoolVar(&warnings, "warnings", false, "Only Warning-type events")
	cmd.Flags().DurationVar(&since, "since", 0, "Only events at or after this long ago (e.g. 1h)")

	return cmd
}

// listHistory resolves the flags into an EventFilter and lists.
//
// history always reads live: a Snapshot file carries no event data, so
// --from is refused before anything is contacted, the same way the write
// commands refuse it (plan B4).
func (o *Options) listHistory(
	cmd *cobra.Command, source, node, reason string, warnings bool, since time.Duration,
) ([]eventsv1.Event, error) {
	if err := o.validate(readFormats); err != nil {
		return nil, err
	}
	if source != "" && node != "" {
		return nil, errors.New("--source and --node are mutually exclusive")
	}
	if o.From != "" {
		return nil, errors.New("--from replays a captured snapshot; `history` reads events from a live cluster")
	}

	ctx := cmd.Context()

	filter := snapshot.EventFilter{Reason: reason, Warnings: warnings}
	if since > 0 {
		filter.Since = o.now().Add(-since)
	}

	// wavefront is resolved once, from whichever path also resolves --node,
	// so a node lookup does not cost a second "get Wavefront" beyond the one
	// capture() already makes.
	var wavefront string
	switch {
	case node != "":
		ref, err := snapshot.ParseNodeRef(node)
		if err != nil {
			return nil, err
		}
		snap, err := o.capture(ctx)
		if err != nil {
			return nil, err
		}
		wavefront = snap.Wavefront.Name
		resolved := findNode(snap, ref)
		if resolved == nil {
			return nil, fmt.Errorf("no node %s in this snapshot", node)
		}
		if resolved.Source == nil {
			return nil, fmt.Errorf("node %s has no source (a gate); nothing to filter history by", node)
		}
		filter.Source = *resolved.Source
	default:
		reader, err := o.reader()
		if err != nil {
			return nil, err
		}
		wf, err := snapshot.SelectWavefront(ctx, reader, o.Wavefront)
		if err != nil {
			return nil, err
		}
		wavefront = wf.Name
		if source != "" {
			src, err := snapshot.ParseSource(source)
			if err != nil {
				return nil, err
			}
			filter.Source = src.String()
		}
	}

	reader, err := o.reader()
	if err != nil {
		return nil, err
	}
	return snapshot.ListEvents(ctx, reader, wavefront, filter)
}

// findNode locates a node by reference, the same small linear lookup
// findSource (read.go) does for sources.
func findNode(snap *snapshot.Snapshot, ref adapter.NodeRef) *snapshot.NodeView {
	for i := range snap.Nodes {
		if snap.Nodes[i].Ref == ref {
			return &snap.Nodes[i]
		}
	}
	return nil
}
