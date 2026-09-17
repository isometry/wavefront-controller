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

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

var _ = Describe("suspend, resume and mode", func() {
	var wf *wavefrontv1alpha1.Wavefront

	BeforeEach(func() {
		// Applied by a GitOps applier owning both spec fields: the case the
		// warnings exist for, and the ownership a merge patch must not disturb.
		wf = makeWavefront("brakes", func(spec map[string]any) {
			spec["suspend"] = false
		})
		Expect(snapshot.SpecOwners(wf)).To(HaveKeyWithValue(snapshot.FieldSuspend, catalogManager))
		Expect(snapshot.SpecOwners(wf)).To(HaveKeyWithValue(snapshot.FieldMode, catalogManager))
	})

	It("suspends with a merge patch, leaving the other field's owner alone", func() {
		suspend := true
		plan := planOf(&actions.WavefrontChange{Client: k8sClient, Wavefront: wf, Suspend: &suspend})

		Expect(plan.Before).To(HaveKeyWithValue(snapshot.FieldSuspend, "false"))
		Expect(plan.After).To(HaveKeyWithValue(snapshot.FieldSuspend, valueTrue))
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("GitOps-owned")))
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("change it in git")))
		run(plan)

		updated := getWavefront(wf.Name)
		Expect(updated.Spec.Suspend).To(BeTrue())

		By("leaving spec.mode where it was, owner and all")
		Expect(updated.Spec.Mode).To(Equal(wavefrontv1alpha1.ModeEnforce))
		Expect(fieldOwners(updated, "spec", "mode")).To(Equal([]string{catalogManager}))

		By("leaving the applier's whole entry in place")
		Expect(fieldOwners(updated, "spec", "nodes", "kinds")).To(Equal([]string{catalogManager}))
	})

	It("resumes by clearing the field", func() {
		suspend := true
		run(planOf(&actions.WavefrontChange{Client: k8sClient, Wavefront: wf, Suspend: &suspend}))

		resumed := getWavefront(wf.Name)
		off := false
		run(planOf(&actions.WavefrontChange{Client: k8sClient, Wavefront: resumed, Suspend: &off}))

		Expect(getWavefront(wf.Name).Spec.Suspend).To(BeFalse())
	})

	It("changes the mode", func() {
		shadow := wavefrontv1alpha1.ModeShadow
		plan := planOf(&actions.WavefrontChange{Client: k8sClient, Wavefront: wf, Mode: &shadow})
		Expect(plan.Before).To(HaveKeyWithValue(snapshot.FieldMode, string(wavefrontv1alpha1.ModeEnforce)))
		Expect(plan.After).To(HaveKeyWithValue(snapshot.FieldMode, string(wavefrontv1alpha1.ModeShadow)))
		run(plan)

		updated := getWavefront(wf.Name)
		Expect(updated.Spec.Mode).To(Equal(wavefrontv1alpha1.ModeShadow))

		By("leaving spec.suspend to its applier")
		Expect(fieldOwners(updated, "spec", "suspend")).To(Equal([]string{catalogManager}))
	})

	It("says when there is nothing to change", func() {
		mode := wavefrontv1alpha1.ModeEnforce
		plan := planOf(&actions.WavefrontChange{Client: k8sClient, Wavefront: wf, Mode: &mode})
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("already")))
		run(plan)
	})

	It("refuses to overwrite a concurrent change", func() {
		stale := wf.DeepCopy()

		By("somebody else moving first")
		shadow := wavefrontv1alpha1.ModeShadow
		run(planOf(&actions.WavefrontChange{Client: k8sClient, Wavefront: wf, Mode: &shadow}))

		suspend := true
		err := runExpectingError(planOf(
			&actions.WavefrontChange{Client: k8sClient, Wavefront: stale, Suspend: &suspend}))
		Expect(err.Error()).To(ContainSubstring("re-run"))

		By("leaving the other change standing")
		Expect(getWavefront(wf.Name).Spec.Mode).To(Equal(wavefrontv1alpha1.ModeShadow))
		Expect(getWavefront(wf.Name).Spec.Suspend).To(BeFalse())
	})

	It("does not warn when nobody GitOps-owns the field", func() {
		// Created by an operator's own apply, not a GitOps applier.
		own := &wavefrontv1alpha1.Wavefront{}
		own.Name = uniqueName("brakes-own")
		own.Spec.Nodes = wavefrontv1alpha1.NodesSpec{Kinds: []string{"Kustomization"}}
		own.Spec.Nodes.Selector.MatchLabels = map[string]string{pin.ManagedLabel: valueTrue}
		Expect(k8sClient.Create(ctx, own)).To(Succeed())

		suspend := true
		plan := planOf(&actions.WavefrontChange{
			Client: k8sClient, Wavefront: getWavefront(own.Name), Suspend: &suspend})
		Expect(plan.Warnings).NotTo(ContainElement(ContainSubstring("GitOps-owned")))
	})
})
