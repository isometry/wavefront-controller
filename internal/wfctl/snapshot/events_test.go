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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// testEvent builds a minimal events.k8s.io/v1 Event regarding the Wavefront
// named wfName, the way both the controller's recorder and actions.Audit
// produce one.
func testEvent(name, reason, eventType, note string, at time.Time, seriesCount int32) eventsv1.Event {
	e := eventsv1.Event{
		Name: name, Namespace: EventNamespace,
		EventTime: metav1.NewMicroTime(at),
		Reason:    reason,
		Type:      eventType,
		Note:      note,
		Regarding: corev1.ObjectReference{
			Kind: regardingKindWavefront,
			Name: wfName,
		},
	}
	if seriesCount > 1 {
		e.Series = &eventsv1.EventSeries{Count: seriesCount, LastObservedTime: metav1.NewMicroTime(at)}
	}
	return e
}

// eventFieldIndexes registers the same selectable fields a real apiserver
// honours for events.k8s.io/v1 (regarding.kind, regarding.name, reason,
// type): the fake client only filters a field selector through an index
// (sigs.k8s.io/controller-runtime/pkg/client/fake), so a test has to stand
// one up to exercise ListEvents' selector at all.
func eventFieldIndexes(b *fake.ClientBuilder) *fake.ClientBuilder {
	field := func(extract func(*eventsv1.Event) string) client.IndexerFunc {
		return func(o client.Object) []string {
			return []string{extract(o.(*eventsv1.Event))}
		}
	}
	return b.
		WithIndex(&eventsv1.Event{}, "regarding.kind", field(func(e *eventsv1.Event) string { return e.Regarding.Kind })).
		WithIndex(&eventsv1.Event{}, "regarding.name", field(func(e *eventsv1.Event) string { return e.Regarding.Name })).
		WithIndex(&eventsv1.Event{}, "reason", field(func(e *eventsv1.Event) string { return e.Reason })).
		WithIndex(&eventsv1.Event{}, "type", field(func(e *eventsv1.Event) string { return e.Type }))
}

// eventsReader builds a fake reader seeded with events, indexed exactly as a
// real apiserver's events.k8s.io/v1 registry selects.
func eventsReader(events ...eventsv1.Event) client.Reader {
	objs := make([]client.Object, 0, len(events))
	for i := range events {
		e := events[i]
		objs = append(objs, &e)
	}
	return eventFieldIndexes(fake.NewClientBuilder().WithScheme(scheme.Scheme)).WithObjects(objs...).Build()
}

// TestListEventsFiltersByReasonAndWarnings proves the two field-selector
// filters plan B3 names.
func TestListEventsFiltersByReasonAndWarnings(t *testing.T) {
	now := time.Now()
	reader := eventsReader(
		testEvent("a", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/infra to abc1234", now, 0),
		testEvent("b", "HoldDetected", corev1.EventTypeWarning, "pin of apps/infra is held", now.Add(time.Minute), 0),
		testEvent("c", "PinFailed", corev1.EventTypeWarning, "failed to pin apps/infra", now.Add(2*time.Minute), 0),
	)

	byReason, err := ListEvents(t.Context(), reader, wfName, EventFilter{Reason: "HoldDetected"})
	if err != nil {
		t.Fatalf("listing by reason: %v", err)
	}
	if len(byReason) != 1 || byReason[0].Reason != "HoldDetected" {
		t.Errorf("--reason HoldDetected returned %+v", byReason)
	}

	warnings, err := ListEvents(t.Context(), reader, wfName, EventFilter{Warnings: true})
	if err != nil {
		t.Fatalf("listing warnings: %v", err)
	}
	if len(warnings) != 2 {
		t.Errorf("--warnings returned %d events, want 2", len(warnings))
	}
	for _, e := range warnings {
		if e.Type != corev1.EventTypeWarning {
			t.Errorf("--warnings returned a %s event", e.Type)
		}
	}
}

// TestListEventsFiltersByRegarding proves the selector is scoped to this one
// Wavefront and to regarding.kind=Wavefront, so a GitRepository-regarding
// mirror of the same PinAdvanced event or another Wavefront's event is not
// double-counted.
func TestListEventsFiltersByRegarding(t *testing.T) {
	now := time.Now()
	own := testEvent("own", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/infra to abc1234", now, 0)

	other := testEvent("other-wf", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/infra to abc1234", now, 0)
	other.Regarding.Name = "some-other-fleet"

	repo := testEvent("repo-mirror", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/infra to abc1234", now, 0)
	repo.Regarding = corev1.ObjectReference{Kind: "GitRepository", Namespace: "apps", Name: "infra"}

	reader := eventsReader(own, other, repo)

	events, err := ListEvents(t.Context(), reader, wfName, EventFilter{})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	if len(events) != 1 || events[0].Name != "own" {
		t.Errorf("regarding selector returned %+v, want only the Wavefront-regarding event", events)
	}
}

// TestListEventsOrdersByEventTime proves the sort is oldest-first (kubectl's
// own --sort-by=.lastTimestamp convention) regardless of insertion order.
func TestListEventsOrdersByEventTime(t *testing.T) {
	base := time.Now()
	reader := eventsReader(
		testEvent("third", "PinAdvanced", corev1.EventTypeNormal, "third", base.Add(2*time.Minute), 0),
		testEvent("first", "PinAdvanced", corev1.EventTypeNormal, "first", base, 0),
		testEvent("second", "PinAdvanced", corev1.EventTypeNormal, "second", base.Add(time.Minute), 0),
	)

	events, err := ListEvents(t.Context(), reader, wfName, EventFilter{})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	for i, want := range []string{"first", "second", "third"} {
		if events[i].Name != want {
			t.Errorf("event %d is %q, want %q (out of order)", i, events[i].Name, want)
		}
	}
}

// TestListEventsFiltersBySource proves the note token match: it must find
// "apps/infra" and must not be fooled by "apps/infra2" or a substring
// elsewhere in the note.
func TestListEventsFiltersBySource(t *testing.T) {
	now := time.Now()
	reader := eventsReader(
		testEvent("match", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/infra to abc1234", now, 0),
		testEvent("near-miss", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/infra2 to abc1234", now, 0),
		testEvent("unrelated", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/other to abc1234", now, 0),
	)

	events, err := ListEvents(t.Context(), reader, wfName, EventFilter{Source: "apps/infra"})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	if len(events) != 1 || events[0].Name != "match" {
		t.Errorf("--source apps/infra returned %+v", events)
	}
}

// TestListEventsFiltersBySince proves the lower time bound is inclusive and
// applied client-side (events.k8s.io/v1 has no selectable time field).
func TestListEventsFiltersBySince(t *testing.T) {
	// eventTime is a metav1.MicroTime, so anything an apiserver (or the fake
	// client's JSON round trip) hands back is truncated to microseconds; a
	// nanosecond-precision base (Linux time.Now()) would put the boundary
	// event just before Since and break the inclusive-bound assertion.
	base := time.Now().Truncate(time.Microsecond)
	reader := eventsReader(
		testEvent("old", "PinAdvanced", corev1.EventTypeNormal, "old", base, 0),
		testEvent("boundary", "PinAdvanced", corev1.EventTypeNormal, "boundary", base.Add(time.Hour), 0),
		testEvent("new", "PinAdvanced", corev1.EventTypeNormal, "new", base.Add(2*time.Hour), 0),
	)

	events, err := ListEvents(t.Context(), reader, wfName, EventFilter{Since: base.Add(time.Hour)})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (boundary and new)", len(events))
	}
	if events[0].Name != "boundary" || events[1].Name != "new" {
		t.Errorf("--since returned %+v", events)
	}
}

// TestEventCountFromSeries proves COUNT reads the recorder's own
// aggregation: a singleton event is 1, a repeated one is the series' count.
func TestEventCountFromSeries(t *testing.T) {
	now := time.Now()
	singleton := testEvent("singleton", "PinAdvanced", corev1.EventTypeNormal, "n/a", now, 0)
	if got := EventCount(singleton); got != 1 {
		t.Errorf("a singleton event counted %d, want 1", got)
	}

	repeated := testEvent("repeated", "HoldDetected", corev1.EventTypeWarning, "n/a", now, 5)
	if got := EventCount(repeated); got != 5 {
		t.Errorf("a series event counted %d, want 5", got)
	}

	deprecated := testEvent("deprecated", "PinAdvanced", corev1.EventTypeNormal, "n/a", now, 0)
	deprecated.DeprecatedCount = 3
	if got := EventCount(deprecated); got != 3 {
		t.Errorf("a deprecated-count event counted %d, want 3", got)
	}
}

// TestEventTimeFallback proves the three-way precedence plan B3 specifies:
// eventTime, then the series' last-observed heartbeat, then the deprecated
// firstTimestamp a converted core/v1 event carries instead of eventTime.
func TestEventTimeFallback(t *testing.T) {
	at := time.Now().Truncate(time.Second)

	withEventTime := testEvent("a", "PinAdvanced", corev1.EventTypeNormal, "n/a", at, 0)
	if got := EventTime(withEventTime); !got.Equal(at) {
		t.Errorf("eventTime = %v, want %v", got, at)
	}

	seriesOnly := eventsv1.Event{
		Series: &eventsv1.EventSeries{Count: 2, LastObservedTime: metav1.NewMicroTime(at)},
	}
	if got := EventTime(seriesOnly); !got.Equal(at) {
		t.Errorf("series fallback = %v, want %v", got, at)
	}

	deprecatedOnly := eventsv1.Event{
		DeprecatedFirstTimestamp: metav1.NewTime(at),
	}
	if got := EventTime(deprecatedOnly); !got.Equal(at) {
		t.Errorf("deprecated fallback = %v, want %v", got, at)
	}
}
