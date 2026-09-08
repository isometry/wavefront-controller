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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/types"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
)

var _ = Describe("force-admit", func() {
	var (
		wf   *wavefrontv1alpha1.Wavefront
		key  types.NamespacedName
		refs actions.Advertisement
	)

	BeforeEach(func() {
		wf = makeWavefront("force-admit")
		key = renderSource("force-admit")
		refs = actions.Advertisement{Lister: fakeLister{refs: map[string]string{
			trackingRef:         shaC,
			"refs/heads/hotfix": shaB,
		}}}
	})

	admit := func(sha string, unverified bool) *actions.ForceAdmit {
		return &actions.ForceAdmit{
			Client:        k8sClient,
			Source:        key,
			Wavefront:     wf,
			SHA:           sha,
			Unverified:    unverified,
			Now:           fixedClock,
			Advertisement: refs,
		}
	}

	It("writes the advertised SHA as the controller would", func() {
		advance(key, shaB)

		plan := planOf(admit("", false))
		Expect(plan.Summary).To(ContainSubstring(shaC))
		Expect(plan.Summary).To(ContainSubstring(trackingRef))
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("bypasses ancestor gating")))
		run(plan)

		repo := get(key)
		Expect(repo.Spec.Reference.Commit).To(Equal(shaC))

		By("under the controller's own field manager, so nothing reads as a hold")
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.FieldManager}))
		_, held := pin.Hold(repo)
		Expect(held).To(BeFalse())

		By("with the same provenance a real admission writes")
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotPreviousPin, shaB))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotObservedRef, trackingRef))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotAdmittedAt, releasedRFC))
	})

	It("initial-pins an unpinned source with an empty previous pin", func() {
		run(planOf(admit("", false)))

		repo := get(key)
		Expect(repo.Spec.Reference.Commit).To(Equal(shaC))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotPreviousPin, ""))
	})

	It("refuses a source held by hand", func() {
		run(planOf(&actions.Pin{Client: k8sClient, Source: key, SHA: shaB, Unverified: true}))

		_, err := admit("", false).Plan(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("held by field manager"))
		Expect(err.Error()).To(ContainSubstring("wfctl release"))
	})

	It("refuses a suspended source", func() {
		suspended := renderSource("force-admit-suspended", func(spec map[string]any) {
			spec["suspend"] = true
		})
		action := admit("", false)
		action.Source = suspended

		_, err := action.Plan(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("suspended"))
	})

	It("verifies an overriding --sha against the advertisement", func() {
		By("accepting one the remote advertises")
		run(planOf(admit(shaB, false)))
		Expect(get(key).Spec.Reference.Commit).To(Equal(shaB))

		By("refusing one it does not")
		_, err := admit(shaZ, false).Plan(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("advertises no ref"))

		By("taking it unchecked when --unverified says so")
		run(planOf(admit(shaZ, true)))
		Expect(get(key).Spec.Reference.Commit).To(Equal(shaZ))
	})

	It("warns, but still writes, when the fleet is suspended or shadowed", func() {
		suspend := true
		run(planOf(&actions.WavefrontChange{Client: k8sClient, Wavefront: wf, Suspend: &suspend}))
		wf = getWavefront(wf.Name)

		plan := planOf(admit("", false))
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("is suspended")))
		run(plan)

		Expect(get(key).Spec.Reference.Commit).To(Equal(shaC))
	})

	It("reports a tracking ref the remote does not advertise", func() {
		action := admit("", false)
		action.Advertisement = actions.Advertisement{Lister: fakeLister{refs: map[string]string{
			"refs/heads/other": shaB,
		}}}

		_, err := action.Plan(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("does not advertise " + trackingRef))
	})
})
