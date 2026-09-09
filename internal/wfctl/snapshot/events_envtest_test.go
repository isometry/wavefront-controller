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

package snapshot_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// ListEvents' field selectors (regarding.kind, regarding.name, reason, type)
// are checked here against envtest's real kube-apiserver, not the fake
// client: the fake client only honours a field selector through a
// hand-registered index, which proves nothing about whether a real
// events.k8s.io/v1 registry actually accepts and applies those selector
// keys. Nothing in this package's other suites exercises that, so this is
// the one place it is proven.
var _ = Describe("ListEvents", func() {
	const (
		eventsWavefront      = "events-fleet"
		eventsOtherWavefront = "events-other-fleet"
		wavefrontKind        = "Wavefront"
	)

	var (
		pinAdvanced    *eventsv1.Event
		holdDetected   *eventsv1.Event
		repoMirror     *eventsv1.Event
		otherWavefront *eventsv1.Event
	)

	BeforeEach(func() {
		GinkgoHelper()
		now := time.Now()

		// Regarding the Wavefront under test, Normal, reason PinAdvanced: the
		// baseline row every filter test either keeps or excludes.
		pinAdvanced = newTestEvent("events-pin-advanced", "PinAdvanced", corev1.EventTypeNormal,
			corev1.ObjectReference{Kind: wavefrontKind, Name: eventsWavefront}, now)
		Expect(k8sClient.Create(ctx, pinAdvanced)).To(Succeed())

		// Same Wavefront, Warning, a different reason: what --reason and
		// --warnings each have to isolate.
		holdDetected = newTestEvent("events-hold-detected", "HoldDetected", corev1.EventTypeWarning,
			corev1.ObjectReference{Kind: wavefrontKind, Name: eventsWavefront}, now.Add(time.Minute))
		Expect(k8sClient.Create(ctx, holdDetected)).To(Succeed())

		// Same reason and type as pinAdvanced, but regarding.kind=GitRepository:
		// the controller's own mirrored PinAdvanced event (wavefront_controller.go's
		// pinEvent), which regarding.kind=Wavefront must exclude.
		repoMirror = newTestEvent("events-repo-mirror", "PinAdvanced", corev1.EventTypeNormal,
			corev1.ObjectReference{Kind: "GitRepository", Namespace: "apps", Name: "infra"}, now)
		Expect(k8sClient.Create(ctx, repoMirror)).To(Succeed())

		// Same reason and type as pinAdvanced, regarding a different
		// Wavefront: what regarding.name must exclude.
		otherWavefront = newTestEvent("events-other-wavefront", "PinAdvanced", corev1.EventTypeNormal,
			corev1.ObjectReference{Kind: wavefrontKind, Name: eventsOtherWavefront}, now)
		Expect(k8sClient.Create(ctx, otherWavefront)).To(Succeed())

		DeferCleanup(func() {
			for _, e := range []*eventsv1.Event{pinAdvanced, holdDetected, repoMirror, otherWavefront} {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, e))).To(Succeed())
			}
		})
	})

	It("selects only the events regarding this Wavefront (regarding.kind, regarding.name)", func() {
		events, err := snapshot.ListEvents(ctx, k8sClient, eventsWavefront, snapshot.EventFilter{})
		Expect(err).NotTo(HaveOccurred())
		Expect(eventNames(events)).To(ConsistOf(pinAdvanced.Name, holdDetected.Name))
	})

	It("narrows further by reason", func() {
		events, err := snapshot.ListEvents(ctx, k8sClient, eventsWavefront, snapshot.EventFilter{Reason: "HoldDetected"})
		Expect(err).NotTo(HaveOccurred())
		Expect(eventNames(events)).To(ConsistOf(holdDetected.Name))
	})

	It("narrows further by type=Warning", func() {
		events, err := snapshot.ListEvents(ctx, k8sClient, eventsWavefront, snapshot.EventFilter{Warnings: true})
		Expect(err).NotTo(HaveOccurred())
		Expect(eventNames(events)).To(ConsistOf(holdDetected.Name))
	})

	It("combines reason and warnings", func() {
		matching, err := snapshot.ListEvents(ctx, k8sClient, eventsWavefront,
			snapshot.EventFilter{Reason: "HoldDetected", Warnings: true})
		Expect(err).NotTo(HaveOccurred())
		Expect(eventNames(matching)).To(ConsistOf(holdDetected.Name))

		mismatched, err := snapshot.ListEvents(ctx, k8sClient, eventsWavefront,
			snapshot.EventFilter{Reason: "PinAdvanced", Warnings: true})
		Expect(err).NotTo(HaveOccurred())
		Expect(mismatched).To(BeEmpty())
	})
})

// newTestEvent builds a minimal events.k8s.io/v1 Event in EventNamespace,
// the shape both the controller's recorder and actions.Audit produce.
func newTestEvent(name, reason, eventType string, regarding corev1.ObjectReference, at time.Time) *eventsv1.Event {
	return &eventsv1.Event{
		ObjectMeta:          metav1.ObjectMeta{Name: name, Namespace: snapshot.EventNamespace},
		EventTime:           metav1.NewMicroTime(at),
		ReportingController: "wavefront-controller",
		ReportingInstance:   "wavefront-controller",
		Action:              reason,
		Reason:              reason,
		Regarding:           regarding,
		Note:                "advanced pin of apps/infra to abc1234",
		Type:                eventType,
	}
}

// eventNames extracts a comparable set of event names.
func eventNames(events []eventsv1.Event) []string {
	names := make([]string, 0, len(events))
	for _, e := range events {
		names = append(names, e.Name)
	}
	return names
}
