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
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// Fetch column values. A GitRepository that cannot fetch is a source-
// controller problem, not an admission one, so it is reported rather than
// coloured like a node state.
const (
	fetchOK      = "ok"
	fetchFailing = "failing"
)

// Sources renders one row per managed GitRepository.
//
// This is the pin ledger: the pin, what the ref actually advertises, and who
// owns the field. `-o wide` adds the provenance of the last advance, which
// is the durable record, unlike events, which the apiserver eventually
// expires.
func Sources(w io.Writer, s *snapshot.Snapshot, o Options) error {
	header := []string{"SOURCE", "PIN", colObserved, "PENDING", "HOLD", "OWNERS", "ARTIFACT", "ADMITTED", "NODES"}
	if o.Wide {
		header = append(header, "PREVIOUS-PIN", "OBSERVED-REF", "FETCH", "URL")
	}

	now := o.now()
	table := NewTable(w, header...)
	unknownSeen, partialSeen := false, false

	for i := range s.Sources {
		source := &s.Sources[i]

		// A partial row is one whose GitRepository could not be read: every
		// field beyond Name, Pin and Nodes came from the GitRepository, so
		// none of them is a fact here (SourceView.Partial).
		if source.Partial {
			partialSeen = true
			cells := []string{
				source.Name, shortSHA(source.Pin),
				unproven, unproven, unproven, unproven, unproven, unproven,
				strconv.Itoa(len(source.Nodes)),
			}
			if o.Wide {
				cells = append(cells, unproven, unproven, unproven, unproven)
			}
			table.Row(cells...)
			continue
		}

		observed, pending := unknown, unknown
		if s.Observed && source.ObservedSHA != "" {
			observed = shortSHA(source.ObservedSHA)
			pending = absent
			if source.ObservedSHA != source.Pin {
				pending = pendingAge(s, now, source.FirstObserved)
			}
		} else {
			unknownSeen = true
		}

		cells := []string{
			source.Name,
			shortSHA(source.Pin),
			observed,
			pending,
			o.Palette.stateCell(holdCell(source.Hold), source.Hold != nil),
			ownersCell(source.CommitOwners),
			shortSHA(source.ArtifactSHA),
			admittedCell(now, source.Provenance),
			strconv.Itoa(len(source.Nodes)),
		}
		if o.Wide {
			cells = append(cells,
				shortSHA(source.Provenance[pin.AnnotPreviousPin]),
				orAbsent(source.Provenance[pin.AnnotObservedRef]),
				fetchCell(source.FetchFailing),
				orAbsent(source.URL),
			)
		}
		table.Row(cells...)
	}

	if err := table.Flush(); err != nil {
		return err
	}

	out := newSink(w)
	if unknownSeen {
		out.line(o.Palette.Dim(unknownFootnote(s)))
	}
	if partialSeen {
		out.line(o.Palette.Dim(footnoteUnproven))
	}
	return out.err
}

// ownersCell names every field manager owning spec.ref.commit. The operation
// each holds it under decides how `release` unpicks it, so it belongs in the
// detail view rather than this column.
func ownersCell(owners []pin.Owner) string {
	if len(owners) == 0 {
		return absent
	}
	names := make([]string, 0, len(owners))
	for _, owner := range owners {
		names = append(names, sanitize(owner.Manager))
	}
	return strings.Join(names, ",")
}

// admittedCell renders how long ago the controller last advanced this pin,
// from the provenance annotation that is the durable record of it.
func admittedCell(now time.Time, provenance map[string]string) string {
	at, ok := provenance[pin.AnnotAdmittedAt]
	if !ok {
		return absent
	}
	parsed, err := time.Parse(time.RFC3339, at)
	if err != nil {
		// An annotation is a string a human may have edited. Showing it
		// verbatim beats both a silent "-" (which would claim the pin was
		// never advanced) and a "?" (which promises a footnote about ref
		// observation that has nothing to do with this).
		return orAbsent(at)
	}
	return age(now, &parsed)
}

// fetchCell reports source-controller's own health for the repository.
func fetchCell(failing bool) string {
	if failing {
		return fetchFailing
	}
	return fetchOK
}
