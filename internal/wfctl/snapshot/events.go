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

package snapshot

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EventNamespace is where every events.k8s.io/v1 Event regarding a Wavefront
// lands.
//
// Wavefronts are cluster-scoped; client-go's own event recorder defaults a
// cluster-scoped regarding object's namespace to "default" — the same place
// actions.Audit writes wfctl's own audit trail — which the e2e suite's
// fleetEventNamespace constant confirms against a real apiserver
// (test/e2e/wavefront_test.go).
const EventNamespace = "default"

// regardingKindWavefront is the only regarding.kind `history` selects on. The
// controller also records PinAdvanced/InitialPin on the GitRepository
// (wavefront_controller.go's pinEvent), but it mirrors the same event onto
// the Wavefront in the same call, so one selector on the Wavefront already
// sees both streams (plan B3, `history`).
const regardingKindWavefront = "Wavefront"

// EventFilter narrows ListEvents (plan B3, `history`).
type EventFilter struct {
	// Reason restricts to one event reason; empty means every reason.
	Reason string
	// Warnings restricts to type=Warning events.
	Warnings bool
	// Source restricts to events whose note names this source (as returned
	// by ParseSource's NamespacedName.String, "namespace/name") as a
	// standalone token; empty means every source.
	Source string
	// Since restricts to events at or after this instant; the zero value
	// means no lower bound.
	Since time.Time
}

// ListEvents lists the controller's and wfctl's own events.k8s.io/v1 events
// regarding wavefront, oldest first (kubectl's own --sort-by=.lastTimestamp
// convention).
//
// The controller's Eventf calls (wavefront_controller.go's event helper) and
// wfctl's own audit trail (actions.Audit) both record regarding the
// Wavefront in EventNamespace, distinguished only by ReportingController
// ("wavefront-controller" vs "wfctl"), so one selector sees the merged
// stream `history` promises.
func ListEvents(ctx context.Context, reader client.Reader, wavefront string, filter EventFilter) ([]eventsv1.Event, error) {
	selector := client.MatchingFields{
		"regarding.kind": regardingKindWavefront,
		"regarding.name": wavefront,
	}
	if filter.Reason != "" {
		selector["reason"] = filter.Reason
	}
	if filter.Warnings {
		selector["type"] = corev1.EventTypeWarning
	}

	list := &eventsv1.EventList{}
	if err := reader.List(ctx, list, client.InNamespace(EventNamespace), selector); err != nil {
		return nil, fmt.Errorf("listing events for Wavefront %q: %w", wavefront, err)
	}

	events := list.Items
	if filter.Source != "" {
		events = filterBySource(events, filter.Source)
	}
	if !filter.Since.IsZero() {
		events = filterSince(events, filter.Since)
	}
	slices.SortFunc(events, func(a, b eventsv1.Event) int {
		return EventTime(a).Compare(EventTime(b))
	})
	return events, nil
}

// EventTime resolves the instant an event is ordered and filtered by (plan
// B3, `history`): its own eventTime when the recorder set one, falling back
// to the series' last-observed heartbeat, falling back to the deprecated
// core/v1 firstTimestamp a converted event carries instead of eventTime.
func EventTime(e eventsv1.Event) time.Time {
	if !e.EventTime.IsZero() {
		return e.EventTime.Time
	}
	if e.Series != nil && !e.Series.LastObservedTime.IsZero() {
		return e.Series.LastObservedTime.Time
	}
	return e.DeprecatedFirstTimestamp.Time
}

// EventCount is the number of occurrences one row represents: the recorder
// aggregates repeats onto a single Event with a Series rather than creating a
// new object every time, and a converted core/v1 event carries the same
// aggregate in its deprecated count instead.
func EventCount(e eventsv1.Event) int32 {
	if e.Series != nil {
		return e.Series.Count
	}
	if e.DeprecatedCount > 0 {
		return e.DeprecatedCount
	}
	return 1
}

// filterBySource keeps events whose note names source as a standalone token.
//
// Every note that carries a source writes it as exactly "namespace/name"
// (wavefront_controller.go's event messages, actions.Audit's Note), so token
// equality — not substring containment — is the honest match: it will not
// mistake "infra" for "other-infra".
func filterBySource(events []eventsv1.Event, source string) []eventsv1.Event {
	out := make([]eventsv1.Event, 0, len(events))
	for _, e := range events {
		if noteNamesSource(e.Note, source) {
			out = append(out, e)
		}
	}
	return out
}

// noteNamesSource reports whether note contains source as a whole word.
func noteNamesSource(note, source string) bool {
	for token := range strings.FieldsSeq(note) {
		if strings.Trim(token, ".,;:()") == source {
			return true
		}
	}
	return false
}

// filterSince keeps events at or after at.
func filterSince(events []eventsv1.Event, at time.Time) []eventsv1.Event {
	out := make([]eventsv1.Event, 0, len(events))
	for _, e := range events {
		if !EventTime(e).Before(at) {
			out = append(out, e)
		}
	}
	return out
}
