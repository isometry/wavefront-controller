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
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// nodeWithSource is the node `--node` resolution is proved against: it
// carries testSource (write_test.go), so `history --node` and
// `history --source` on that source must agree. nodeGate is a second node in
// the same fixture with no source, proving --node refuses a gate cleanly
// rather than silently listing everything.
const (
	nodeWithSource = "apps/web"
	nodeGate       = "apps/gate"
)

// historyEvent builds a minimal events.k8s.io/v1 Event regarding
// testWavefront, the shape both the controller's recorder and actions.Audit
// produce.
func historyEvent(name, reason, eventType, note string, at time.Time) eventsv1.Event {
	return eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: snapshot.EventNamespace},
		EventTime:  metav1.NewMicroTime(at),
		Reason:     reason,
		Type:       eventType,
		Note:       note,
		Regarding: corev1.ObjectReference{
			Kind: "Wavefront",
			Name: testWavefront,
		},
	}
}

// historyIndexes registers the selectable fields ListEvents relies on: the
// fake client only honours a field selector through a registered index.
func historyIndexes(b *fake.ClientBuilder) *fake.ClientBuilder {
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

// historyCluster builds a Wavefront with one node resolving to testSource
// (so --node has something to resolve), seeded with events.
func historyCluster(events ...eventsv1.Event) client.Client {
	wf := &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{Name: testWavefront},
		Status: wavefrontv1alpha1.WavefrontStatus{
			Members: []wavefrontv1alpha1.Member{
				{
					Node:   wavefrontv1alpha1.NodeReference{Kind: kustomizev1.KustomizationKind, Namespace: "apps", Name: "web"},
					Role:   "Pinned",
					State:  "Settled",
					Source: testSource,
				},
				{
					// A gate: no source at all, so --node resolution against it
					// has nothing to filter history by (TestHistoryNodeGateErrors).
					Node:  wavefrontv1alpha1.NodeReference{Kind: kustomizev1.KustomizationKind, Namespace: "apps", Name: "gate"},
					Role:  "Gate",
					State: "Settled",
				},
			},
		},
	}

	objs := make([]client.Object, 0, 1+len(events))
	objs = append(objs, wf)
	for i := range events {
		e := events[i]
		objs = append(objs, &e)
	}
	return historyIndexes(fake.NewClientBuilder().WithScheme(scheme)).WithObjects(objs...).Build()
}

// historyTree builds the real command tree with the cluster and clock
// replaced, mirroring writeTree (write_test.go) for the read side.
func historyTree(c client.Reader, now time.Time) (*cobra.Command, *bytes.Buffer) {
	opts := NewOptions()
	opts.NewReader = func() (client.Reader, error) { return c, nil }
	opts.Now = func() time.Time { return now }

	root := newRootCommand(binaryName, opts)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	return root, &out
}

func runHistory(t *testing.T, c client.Reader, args ...string) (string, error) {
	t.Helper()
	root, out := historyTree(c, time.Now())
	root.SetArgs(append([]string{cmdHistory}, args...))
	err := root.Execute()
	return out.String(), err
}

// TestHistoryCommandIsRegistered proves history is wired without touching
// the persistent-flag plumbing (plan B3): the flags it declares belong to it
// alone.
func TestHistoryCommandIsRegistered(t *testing.T) {
	root := NewRootCommand(binaryName)
	cmd, _, err := root.Find([]string{cmdHistory})
	if err != nil {
		t.Fatalf("the tree has no %q command: %v", cmdHistory, err)
	}
	for _, name := range []string{"source", "node", "reason", "warnings", "since"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("history does not declare --%s", name)
		}
	}
}

// TestHistoryListsBothStreams proves the controller's own reason and
// wfctl's own audit reason both come back from one selector, because both
// are recorded regarding the Wavefront (plan B3).
func TestHistoryListsBothStreams(t *testing.T) {
	now := time.Now()
	c := historyCluster(
		historyEvent("a", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/infra to abc1234", now),
		historyEvent("b", "HandPinned", corev1.EventTypeNormal, "tester ran: wfctl pin apps/infra --sha abc1234", now.Add(time.Minute)),
	)

	out, err := runHistory(t, c)
	if err != nil {
		t.Fatalf("history: %v\n%s", err, out)
	}
	for _, want := range []string{"PinAdvanced", "HandPinned", "TIME", "TYPE", "REASON", "COUNT", "NOTE"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output does not contain %q:\n%s", want, out)
		}
	}
}

// TestHistoryFiltersByReasonAndWarnings covers the two field-selector flags.
func TestHistoryFiltersByReasonAndWarnings(t *testing.T) {
	now := time.Now()
	c := historyCluster(
		historyEvent("advance", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/infra to abc1234", now),
		historyEvent("hold", "HoldDetected", corev1.EventTypeWarning, "pin of apps/infra is held", now.Add(time.Minute)),
	)

	byReason, err := runHistory(t, c, "--reason", "HoldDetected")
	if err != nil {
		t.Fatalf("--reason: %v\n%s", err, byReason)
	}
	if strings.Contains(byReason, "PinAdvanced") || !strings.Contains(byReason, "HoldDetected") {
		t.Errorf("--reason HoldDetected did not isolate the reason:\n%s", byReason)
	}

	warnings, err := runHistory(t, c, "--warnings")
	if err != nil {
		t.Fatalf("--warnings: %v\n%s", err, warnings)
	}
	if strings.Contains(warnings, "PinAdvanced") || !strings.Contains(warnings, "HoldDetected") {
		t.Errorf("--warnings did not isolate the Warning event:\n%s", warnings)
	}
}

// TestHistoryFiltersBySource proves the note token match, and
// TestHistoryFiltersByNode proves --node resolves to the same source through
// the snapshot and filters identically.
func TestHistoryFiltersBySource(t *testing.T) {
	now := time.Now()
	c := historyCluster(
		historyEvent("match", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of "+testSource+" to abc1234", now),
		historyEvent("other", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/other to abc1234", now),
	)

	out, err := runHistory(t, c, "--source", testSource)
	if err != nil {
		t.Fatalf("--source: %v\n%s", err, out)
	}
	if !strings.Contains(out, "abc1234") {
		t.Errorf("--source %s dropped the matching event:\n%s", testSource, out)
	}
	if strings.Count(out, "advanced pin of") != 1 {
		t.Errorf("--source %s did not exclude the other source:\n%s", testSource, out)
	}
}

func TestHistoryFiltersByNode(t *testing.T) {
	now := time.Now()
	c := historyCluster(
		historyEvent("match", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of "+testSource+" to abc1234", now),
		historyEvent("other", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/other to abc1234", now),
	)

	out, err := runHistory(t, c, "--node", nodeWithSource)
	if err != nil {
		t.Fatalf("--node: %v\n%s", err, out)
	}
	if strings.Count(out, "advanced pin of") != 1 || !strings.Contains(out, "abc1234") {
		t.Errorf("--node %s did not filter to its resolved source:\n%s", nodeWithSource, out)
	}

	if _, err := runHistory(t, c, "--node", "apps/does-not-exist"); err == nil {
		t.Error("--node for an absent node was accepted")
	}
}

// TestHistoryNodeGateErrors proves --node against a gate (no source at all)
// is refused with an explanation, rather than silently falling through to an
// unfiltered listing.
func TestHistoryNodeGateErrors(t *testing.T) {
	c := historyCluster(historyEvent("a", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of "+testSource+" to abc1234", time.Now()))

	out, err := runHistory(t, c, "--node", nodeGate)
	if err == nil {
		t.Fatalf("--node %s (a gate) was accepted:\n%s", nodeGate, out)
	}
	if !strings.Contains(err.Error(), "gate") {
		t.Errorf("error %q does not explain that the node is a gate", err)
	}
}

// TestHistorySourceAndNodeAreMutuallyExclusive covers the flag-combination
// refusal that happens before any cluster is contacted.
func TestHistorySourceAndNodeAreMutuallyExclusive(t *testing.T) {
	c := historyCluster()
	out, err := runHistory(t, c, "--source", testSource, "--node", nodeWithSource)
	if err == nil {
		t.Fatalf("--source and --node together were accepted:\n%s", out)
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("error %q does not name the conflict", err)
	}
}

// TestHistoryRejectsFrom proves history never tries to read events out of a
// replayed snapshot file, which carries none.
func TestHistoryRejectsFrom(t *testing.T) {
	out, err := runHistory(t, historyCluster(), "--from", "testdata/quiescent.snapshot.json")
	if err == nil {
		t.Fatalf("--from was accepted:\n%s", out)
	}
	if !strings.Contains(err.Error(), "live cluster") {
		t.Errorf("error %q does not explain the refusal", err)
	}
}

// TestHistoryOrdersOldestFirstAndCountsSeries proves the sort and the COUNT
// column reads the recorder's series aggregation.
func TestHistoryOrdersOldestFirstAndCountsSeries(t *testing.T) {
	base := time.Now()
	later := historyEvent("later", "PinAdvanced", corev1.EventTypeNormal, "later event", base.Add(time.Hour))
	earlier := historyEvent("earlier", "PinAdvanced", corev1.EventTypeNormal, "earlier event", base)
	earlier.Series = &eventsv1.EventSeries{Count: 4, LastObservedTime: metav1.NewMicroTime(base)}

	c := historyCluster(later, earlier)

	out, err := runHistory(t, c)
	if err != nil {
		t.Fatalf("history: %v\n%s", err, out)
	}
	earlierIdx := strings.Index(out, "earlier event")
	laterIdx := strings.Index(out, "later event")
	if earlierIdx < 0 || laterIdx < 0 || earlierIdx > laterIdx {
		t.Errorf("events are not ordered oldest first:\n%s", out)
	}

	// COUNT sits right before the earlier event's row starts.
	line := out[:earlierIdx]
	lastLine := line[strings.LastIndex(line, "\n")+1:]
	if !strings.Contains(lastLine, "4") {
		t.Errorf("the earlier row does not show COUNT=4:\n%q", lastLine)
	}
}

// TestHistoryCaveatPlacement proves the retention caveat prints every time,
// on stdout beside a table and on stderr beside a machine-readable format so
// a `-o json | jq` pipeline never sees it mixed into the document.
func TestHistoryCaveatPlacement(t *testing.T) {
	const caveatFragment = "at-least-once"

	c := historyCluster(historyEvent("a", "PinAdvanced", corev1.EventTypeNormal, "advanced pin of apps/infra to abc1234", time.Now()))

	tableOut, err := runHistory(t, c)
	if err != nil {
		t.Fatalf("table: %v\n%s", err, tableOut)
	}
	if !strings.Contains(tableOut, caveatFragment) {
		t.Errorf("the table output does not carry the caveat:\n%s", tableOut)
	}

	root, out := historyTree(c, time.Now())
	root.SetArgs([]string{cmdHistory, "-o", outputJSON})
	if err := root.Execute(); err != nil {
		t.Fatalf("json: %v\n%s", err, out.String())
	}
	combined := out.String()
	if !strings.Contains(combined, caveatFragment) {
		t.Errorf("the json run does not carry the caveat at all:\n%s", combined)
	}

	// The caveat has to be separable from the document: split it off the end
	// and the remainder must still parse as a JSON array of events.
	jsonPart := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(combined), historyCaveat))
	var events []eventsv1.Event
	if err := json.Unmarshal([]byte(jsonPart), &events); err != nil {
		t.Fatalf("the json stream is not a clean events document once the caveat is removed: %v\n%s", err, jsonPart)
	}
	if len(events) != 1 {
		t.Errorf("decoded %d events, want 1", len(events))
	}
}

// TestHistorySinceFilters proves --since bounds the listing relative to the
// injected clock.
func TestHistorySinceFilters(t *testing.T) {
	now := time.Now()
	c := historyCluster(
		historyEvent("old", "PinAdvanced", corev1.EventTypeNormal, "an old event", now.Add(-2*time.Hour)),
		historyEvent("recent", "PinAdvanced", corev1.EventTypeNormal, "a recent event", now.Add(-time.Minute)),
	)

	root, out := historyTree(c, now)
	root.SetArgs([]string{cmdHistory, "--since", "10m"})
	if err := root.Execute(); err != nil {
		t.Fatalf("--since: %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "an old event") || !strings.Contains(out.String(), "a recent event") {
		t.Errorf("--since 10m did not bound the listing:\n%s", out.String())
	}
}
