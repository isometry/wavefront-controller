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

	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
)

var _ = Describe("pin", func() {
	var key types.NamespacedName

	BeforeEach(func() {
		key = renderSource("pin")
	})

	It("takes the pin from the controller and records what it displaced", func() {
		advance(key, shaA)

		plan := planOf(&actions.Pin{Client: k8sClient, Source: key, SHA: shaB, Unverified: true})

		By("describing the transfer before performing it")
		Expect(plan.Before).To(HaveKeyWithValue(fieldCommit, shaA))
		Expect(plan.After).To(HaveKeyWithValue(fieldCommit, shaB))
		Expect(plan.After).To(HaveKeyWithValue(displacedField, shaA))
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("HoldDetected")))

		run(plan)

		repo := get(key)
		Expect(repo.Spec.Reference.Commit).To(Equal(shaB))

		By("owning the commit alone, under the wfctl field manager")
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.WfctlFieldManager}))

		By("presenting to the controller as a hand-pin hold")
		manager, held := pin.Hold(repo)
		Expect(held).To(BeTrue())
		Expect(manager).To(Equal(pin.WfctlFieldManager))

		By("recording the displaced pin, and forging no provenance")
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotDisplacedPin, shaA))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotPreviousPin, ""))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotAdmittedAt, admittedA))

		By("leaving the catalog's own fields to the catalog")
		Expect(fieldOwners(repo, "spec", "ref", "name")).To(Equal([]string{catalogManager}))
		Expect(fieldOwners(repo, "spec", "url")).To(Equal([]string{catalogManager}))
	})

	It("records an empty displaced pin when the source was never pinned", func() {
		run(planOf(&actions.Pin{Client: k8sClient, Source: key, SHA: shaB, Unverified: true}))

		repo := get(key)
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotDisplacedPin, ""))
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.WfctlFieldManager}))
	})

	It("refuses a third-party owner without --force, and displaces it with", func() {
		handPin(key, shaX, humanManager)

		_, err := (&actions.Pin{Client: k8sClient, Source: key, SHA: shaB, Unverified: true}).Plan(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(humanManager))
		Expect(err.Error()).To(ContainSubstring("--force"))

		By("leaving the hand-pin exactly as it was")
		Expect(get(key).Spec.Reference.Commit).To(Equal(shaX))

		By("taking the field when --force says so")
		plan := planOf(&actions.Pin{Client: k8sClient, Source: key, SHA: shaB, Unverified: true, Force: true})
		Expect(plan.Warnings).To(ContainElement(ContainSubstring(humanManager)))
		run(plan)

		repo := get(key)
		Expect(repo.Spec.Reference.Commit).To(Equal(shaB))
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.WfctlFieldManager}))
	})

	It("re-pins its own hold without asking for --force", func() {
		run(planOf(&actions.Pin{Client: k8sClient, Source: key, SHA: shaA, Unverified: true}))
		run(planOf(&actions.Pin{Client: k8sClient, Source: key, SHA: shaB, Unverified: true}))

		repo := get(key)
		Expect(repo.Spec.Reference.Commit).To(Equal(shaB))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotDisplacedPin, shaA))
	})

	It("refuses an unchecked SHA unless verification is asked for or waived", func() {
		_, err := (&actions.Pin{Client: k8sClient, Source: key, SHA: shaB}).Plan(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("--poll"))
		Expect(err.Error()).To(ContainSubstring("--unverified"))
	})

	It("verifies a SHA against the advertisement under --poll", func() {
		advertised := actions.Advertisement{Lister: fakeLister{refs: map[string]string{
			trackingRef:                shaA,
			"refs/heads/hotfix":        shaB,
			"refs/tags/v1.0.0" + "^{}": shaC,
		}}}

		By("accepting a SHA the remote advertises, on any ref")
		run(planOf(&actions.Pin{
			Client: k8sClient, Source: key, SHA: shaB, Verify: true, Advertisement: advertised,
		}))
		Expect(get(key).Spec.Reference.Commit).To(Equal(shaB))

		By("refusing one it does not")
		_, err := (&actions.Pin{
			Client: k8sClient, Source: key, SHA: shaZ, Verify: true, Advertisement: advertised,
		}).Plan(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("advertises no ref"))
	})

	It("warns that an unverified SHA was nobody's to check", func() {
		plan := planOf(&actions.Pin{Client: k8sClient, Source: key, SHA: shaB, Unverified: true})
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("--unverified")))
	})
})
