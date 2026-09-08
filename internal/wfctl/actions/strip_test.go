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

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
)

var _ = Describe("pin-strip", func() {
	var (
		wf              *wavefrontv1alpha1.Wavefront
		pinned          types.NamespacedName
		held            types.NamespacedName
		suspendedSource types.NamespacedName
		clean           types.NamespacedName
	)

	BeforeEach(func() {
		// A strip is fleet-wide by definition, so the fleet has to be exactly
		// what this spec built: anything another spec left behind would be
		// stripped too, and the counts would mean nothing.
		Expect(k8sClient.DeleteAllOf(ctx, &sourcev1.GitRepository{},
			client.InNamespace(testNamespace))).To(Succeed())
		Eventually(func() int {
			list := &sourcev1.GitRepositoryList{}
			Expect(k8sClient.List(ctx, list, client.InNamespace(testNamespace))).To(Succeed())
			return len(list.Items)
		}).Should(BeZero())

		wf = makeWavefront("strip")

		pinned = renderSource("strip-pinned")
		advance(pinned, shaA)

		held = renderSource("strip-held")
		advance(held, shaA)
		run(planOf(&actions.Pin{Client: k8sClient, Source: held, SHA: shaB, Unverified: true}))

		suspendedSource = renderSource("strip-suspended", func(spec map[string]any) {
			spec["suspend"] = true
		})
		advance(suspendedSource, shaA)

		clean = renderSource("strip-clean")
	})

	It("strips controller pins and skips held sources", func() {
		plan := planOf(&actions.Strip{Client: k8sClient, Wavefront: wf})

		Expect(plan.Summary).To(ContainSubstring("1 managed source"))
		Expect(plan.Before).To(HaveKeyWithValue(pinned.String(), shaA))
		Expect(plan.Before).NotTo(HaveKey(held.String()))
		Expect(plan.Before).NotTo(HaveKey(clean.String()))
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("skipping " + held.String())))
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("re-pins")))

		By("treating a suspended source as held too, the way the controller does")
		Expect(plan.Before).NotTo(HaveKey(suspendedSource.String()))
		Expect(plan.Warnings).To(ContainElement(And(
			ContainSubstring("skipping "+suspendedSource.String()),
			ContainSubstring("suspended"))))

		run(plan)

		Expect(get(pinned).Spec.Reference.Commit).To(BeEmpty())
		Expect(get(suspendedSource).Spec.Reference.Commit).To(Equal(shaA))

		By("leaving the hand-pinned source exactly as its holder left it")
		Expect(get(held).Spec.Reference.Commit).To(Equal(shaB))
		manager, stillHeld := pin.Hold(get(held))
		Expect(stillHeld).To(BeTrue())
		Expect(manager).To(Equal(pin.WfctlFieldManager))
	})

	It("strips held sources too under --include-held", func() {
		plan := planOf(&actions.Strip{Client: k8sClient, Wavefront: wf, IncludeHeld: true})
		Expect(plan.Before).To(HaveKeyWithValue(held.String(), shaB))
		Expect(plan.Before).To(HaveKeyWithValue(suspendedSource.String(), shaA))
		run(plan)

		Expect(get(held).Spec.Reference.Commit).To(BeEmpty())
		_, stillHeld := pin.Hold(get(held))
		Expect(stillHeld).To(BeFalse())
		Expect(get(suspendedSource).Spec.Reference.Commit).To(BeEmpty())
	})

	It("suspends the fleet first under --suspend", func() {
		plan := planOf(&actions.Strip{Client: k8sClient, Wavefront: wf, Suspend: true})
		Expect(plan.Summary).To(ContainSubstring("suspend Wavefront " + wf.Name))
		Expect(plan.After).To(HaveKeyWithValue("Wavefront spec.suspend", valueTrue))
		run(plan)

		Expect(getWavefront(wf.Name).Spec.Suspend).To(BeTrue())
		Expect(get(pinned).Spec.Reference.Commit).To(BeEmpty())
	})

	It("carries the suspend step's own warnings, GitOps ownership included", func() {
		// The strip is planned against a fleet whose spec.suspend a GitOps
		// applier owns: the brake this command is about to pull is one Flux
		// will release again, and the plan has to say so before anyone
		// consents to the strip.
		owned := makeWavefront("strip-gitops", func(spec map[string]any) {
			spec["suspend"] = false
		})

		plan := planOf(&actions.Strip{Client: k8sClient, Wavefront: owned, Suspend: true})
		Expect(plan.Warnings).To(ContainElement(And(
			ContainSubstring("GitOps-owned"),
			ContainSubstring("change it in git"))))
	})

	It("says so when there is nothing to strip", func() {
		run(planOf(&actions.Strip{Client: k8sClient, Wavefront: wf, IncludeHeld: true}))

		plan := planOf(&actions.Strip{Client: k8sClient, Wavefront: wf, IncludeHeld: true})
		Expect(plan.Summary).To(ContainSubstring("0 managed sources"))
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("strips nothing")))

		By("writing nothing, so nothing is audited either")
		runWritingNothing(plan)
	})
})
