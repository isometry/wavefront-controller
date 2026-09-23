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

package render

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// Source renders one managed GitRepository in detail.
//
// Unlike the table, this view prints full SHAs and the provenance
// annotations verbatim: it is what an operator reads before writing to the
// object, and before a write the exact value matters.
func Source(w io.Writer, s *snapshot.Snapshot, name string, o Options) error {
	source := sourceView(s, name)
	if source == nil {
		return fmt.Errorf("no source %q in this snapshot: try `wfctl sources`", name)
	}

	now := o.now()
	out := newSink(w)

	table := newPlainTable(w)
	table.Row("NAME:", source.Name)
	if source.Partial {
		table.Row("PIN:", orAbsent(source.Pin))
		table.Row("NODES:", nodeCount(len(source.Nodes)))
		if err := table.Flush(); err != nil {
			return err
		}
		out.line(o.Palette.Warn(footnoteUnproven))
		if err := nodesSection(w, source); err != nil {
			return err
		}
		return out.err
	}

	observed := unknown
	if s.Observed && source.ObservedSHA != "" {
		observed = source.ObservedSHA
	}

	table.Row("URL:", orAbsent(source.URL))
	table.Row("SECRET:", orAbsent(source.SecretRefName))
	table.Row("TRACKING REF:", orAbsent(source.TrackingRef))
	table.Row("PIN:", orAbsent(source.Pin))
	table.Row("OBSERVED:", observed)
	table.Row("PENDING:", sourcePending(s, source, now))
	table.Row("ARTIFACT:", orAbsent(source.ArtifactSHA))
	table.Row("SUSPENDED:", yesNo(source.Suspended))
	table.Row("FETCH:", fetchCell(source.FetchFailing))
	table.Row("HOLD:", o.Palette.stateCell(holdCell(source.Hold), source.Hold != nil))
	table.Row("NODES:", nodeCount(len(source.Nodes)))
	if err := table.Flush(); err != nil {
		return err
	}

	if observed == unknown {
		out.line(o.Palette.Dim(unknownFootnote(s)))
		if out.err != nil {
			return out.err
		}
	}

	if err := ownersSection(w, source); err != nil {
		return err
	}
	if err := provenanceSection(w, source); err != nil {
		return err
	}
	if err := conditionsSection(w, source, now); err != nil {
		return err
	}
	return nodesSection(w, source)
}

// nodeCount renders the referencing-node count for the summary block.
func nodeCount(count int) string {
	return fmt.Sprintf("%d", count)
}

// sourcePending says whether a newer commit is waiting, and for how long.
func sourcePending(s *snapshot.Snapshot, source *snapshot.SourceView, now time.Time) string {
	if !s.Observed || source.ObservedSHA == "" {
		return unknown
	}
	if source.ObservedSHA == source.Pin {
		return "no"
	}
	return "yes (" + pendingAge(s, now, source.FirstObserved) + ")"
}

// ownersSection lists every field manager owning spec.ref.commit, with the
// operation it owns under: `release` unpicks an Apply share and an Update
// share by entirely different means.
func ownersSection(w io.Writer, source *snapshot.SourceView) error {
	if len(source.CommitOwners) == 0 {
		return nil
	}
	out := newSink(w)
	out.blank()
	out.printf("OWNERS (%d)\n", len(source.CommitOwners))
	if out.err != nil {
		return out.err
	}

	table := NewTable(w, "MANAGER", "OPERATION")
	for _, owner := range source.CommitOwners {
		table.Row(orAbsent(owner.Manager), orAbsent(string(owner.Operation)))
	}
	return table.Flush()
}

// provenanceSection prints the pin annotations — the durable ledger, unlike
// events, which the apiserver eventually expires.
func provenanceSection(w io.Writer, source *snapshot.SourceView) error {
	if len(source.Provenance) == 0 {
		return nil
	}
	out := newSink(w)
	out.blank()
	out.printf("PROVENANCE (%d)\n", len(source.Provenance))
	if out.err != nil {
		return out.err
	}

	table := NewTable(w, "ANNOTATION", "VALUE")
	for _, key := range slices.Sorted(maps.Keys(source.Provenance)) {
		table.Row(sanitize(key), orAbsent(source.Provenance[key]))
	}
	return table.Flush()
}

// conditionsSection prints the GitRepository's own conditions: a pin that
// will not fetch is a source-controller problem, and this is where it shows.
func conditionsSection(w io.Writer, source *snapshot.SourceView, now time.Time) error {
	if len(source.Conditions) == 0 {
		return nil
	}
	out := newSink(w)
	out.blank()
	out.printf("CONDITIONS (%d)\n", len(source.Conditions))
	if out.err != nil {
		return out.err
	}

	table := NewTable(w, "TYPE", "STATUS", "REASON", "AGE", "MESSAGE")
	for i := range source.Conditions {
		cond := &source.Conditions[i]
		at := cond.LastTransitionTime.Time
		table.Row(
			sanitize(cond.Type),
			string(cond.Status),
			orAbsent(cond.Reason),
			age(now, &at),
			orAbsent(cond.Message),
		)
	}
	return table.Flush()
}

// nodesSection lists the nodes this source backs — every one of which shares
// its single pin (the shared-source dedup, gateSharedSources).
func nodesSection(w io.Writer, source *snapshot.SourceView) error {
	if len(source.Nodes) == 0 {
		return nil
	}
	out := newSink(w)
	out.blank()
	out.printf("NODES (%d)\n", len(source.Nodes))
	for _, ref := range source.Nodes {
		out.printf("  %s\n", nodeName(ref))
	}
	return out.err
}
