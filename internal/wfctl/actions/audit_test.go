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

package actions_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	eventsv1 "k8s.io/api/events/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
)

var _ = Describe("audit", func() {
	It("records the write on the Wavefront, in the default namespace", func() {
		wf := makeWavefront("audit")
		note := actions.Note("alice@prod", "wfctl suspend --yes")

		Expect(actions.Audit(ctx, k8sClient, wf, actions.ReasonSuspended, note, releasedAt)).To(Succeed())

		list := &eventsv1.EventList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace("default"))).To(Succeed())

		var found *eventsv1.Event
		for i := range list.Items {
			if list.Items[i].Regarding.Name == wf.Name {
				found = &list.Items[i]
			}
		}
		Expect(found).NotTo(BeNil(), "no event regarding Wavefront %s", wf.Name)

		Expect(found.Reason).To(Equal(actions.ReasonSuspended))
		Expect(found.Action).To(Equal(actions.ReasonSuspended))
		Expect(found.ReportingController).To(Equal("wfctl"))
		Expect(found.Type).To(Equal("Normal"))
		Expect(found.Note).To(Equal("alice@prod ran: wfctl suspend --yes"))
		Expect(found.Regarding.Kind).To(Equal("Wavefront"))
		Expect(found.Regarding.UID).To(Equal(wf.UID))
	})

	It("truncates a note the apiserver would reject", func() {
		note := actions.Note("alice@prod", strings.Repeat("0123456789", 200))
		Expect(len(note)).To(BeNumerically("<=", 1024))
		Expect(note).To(HaveSuffix("..."))

		wf := makeWavefront("audit-long")
		Expect(actions.Audit(ctx, k8sClient, wf, actions.ReasonPinStripped, note, releasedAt)).To(Succeed())
	})
})
