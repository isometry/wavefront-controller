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

	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
)

var _ = Describe("release", func() {
	var key types.NamespacedName

	BeforeEach(func() {
		key = renderSource("release")
	})

	It("transfers a wfctl hold to the controller, value and provenance intact", func() {
		By("a controller pin, then a hand-pin over it")
		advance(key, shaA)
		run(planOf(&actions.Pin{Client: k8sClient, Source: key, SHA: shaB, Unverified: true}))
		Expect(ownersOf(get(key))).To(ConsistOf(pin.Owner{Manager: pin.WfctlFieldManager, Operation: applyOperation}))

		plan := planOf(&actions.Release{Client: k8sClient, Source: key, Now: fixedClock})
		Expect(plan.Before).To(HaveKeyWithValue(fieldCommit, shaB))
		Expect(plan.After).To(HaveKeyWithValue(fieldCommit, shaB))
		Expect(plan.After).To(HaveKeyWithValue("spec.ref.commit owners", ContainSubstring(pin.FieldManager)))
		run(plan)

		repo := get(key)

		By("keeping the value the hand-pin chose")
		Expect(repo.Spec.Reference.Commit).To(Equal(shaB))

		By("leaving the controller sole owner, so the hold is over")
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.FieldManager}))
		manager, held := pin.Hold(repo)
		Expect(held).To(BeFalse())
		Expect(manager).To(BeEmpty())

		By("restoring provenance from the displaced pin")
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotPreviousPin, shaA))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotObservedRef, trackingRef))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotAdmittedAt, releasedRFC))

		By("taking the displaced-pin annotation with the hold it belonged to")
		Expect(repo.GetAnnotations()).NotTo(HaveKey(pin.AnnotDisplacedPin))

		By("leaving the catalog's fields alone throughout")
		Expect(fieldOwners(repo, "spec", "ref", "name")).To(Equal([]string{catalogManager}))
		Expect(repo.Spec.URL).To(Equal(repoURL))
	})

	It("releases an Update-op hold by rewriting managedFields", func() {
		advance(key, shaA)
		handPin(key, shaX, humanManager)
		Expect(ownersOf(get(key))).To(ConsistOf(pin.Owner{Manager: humanManager, Operation: updateOperation}))

		plan := planOf(&actions.Release{Client: k8sClient, Source: key, Now: fixedClock})
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("managedFields")))
		run(plan)

		repo := get(key)

		By("keeping the hand-pinned value")
		Expect(repo.Spec.Reference.Commit).To(Equal(shaX))

		By("leaving the controller sole owner")
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.FieldManager}))
		_, held := pin.Hold(repo)
		Expect(held).To(BeFalse())

		By("recording provenance, with an empty previous pin: kubectl left no note")
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotPreviousPin, ""))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotObservedRef, trackingRef))

		By("leaving the rest of the holder's entry intact")
		Expect(fieldOwners(repo, "spec", "url")).To(Equal([]string{catalogManager}))
	})

	It("releases a third-party apply-op hold the same surgical way", func() {
		const otherApplier = "flux-cli"

		advance(key, shaA)
		By("a second server-side applier taking the commit")
		foreign := object(sourcev1.GroupVersion.String(), sourcev1.GitRepositoryKind,
			map[string]any{fieldName: key.Name, fieldNS: key.Namespace},
			map[string]any{fieldRef: map[string]any{"commit": shaX}})
		Expect(k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(foreign),
			client.FieldOwner(otherApplier), client.ForceOwnership)).To(Succeed())
		Expect(ownersOf(get(key))).To(ContainElement(pin.Owner{Manager: otherApplier, Operation: applyOperation}))

		run(planOf(&actions.Release{Client: k8sClient, Source: key, Now: fixedClock}))

		repo := get(key)
		Expect(repo.Spec.Reference.Commit).To(Equal(shaX))
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.FieldManager}))
		_, held := pin.Hold(repo)
		Expect(held).To(BeFalse())
	})

	// The regression this guards is invisible to every other spec: releasing an
	// Update-op hold is the one place wfctl PUTs a whole GitRepository, and a
	// PUT read through the vendored Go type deletes whatever that build does
	// not know about. The CRD's pruning is relaxed for the length of the spec
	// so that "a field this build does not know about" can exist at all.
	It("preserves fields the vendored Go type has never heard of", func() {
		const unknownField = "unknownToThisBuild"

		preserveUnknownFields()

		key := types.NamespacedName{Namespace: testNamespace, Name: uniqueName("release-skew")}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Create(ctx, object(
				sourcev1.GroupVersion.String(), sourcev1.GitRepositoryKind,
				map[string]any{fieldName: key.Name, fieldNS: key.Namespace},
				map[string]any{
					"url":      repoURL,
					"interval": "1m",
					fieldRef:   map[string]any{fieldName: trackingRef},
					// The field a newer source-controller would render and
					// this build's Go type would drop on the floor.
					unknownField: "kept",
				}))).To(Succeed())
			g.Expect(unknownValue(key)).To(Equal("kept"))
		}).Should(Succeed())

		advance(key, shaA)

		By("a kubectl-style hold, set without a typed round trip of its own")
		Expect(k8sClient.Patch(ctx, gitRepository(key), client.RawPatch(types.JSONPatchType,
			[]byte(`[{"op":"add","path":"/spec/ref/commit","value":"`+shaX+`"}]`)),
			client.FieldOwner(humanManager))).To(Succeed())
		Expect(ownersOf(get(key))).To(ConsistOf(pin.Owner{Manager: humanManager, Operation: updateOperation}))

		run(planOf(&actions.Release{Client: k8sClient, Source: key, Now: fixedClock}))

		By("releasing the hold without deleting what it could not parse")
		Expect(unknownValue(key)).To(Equal("kept"))
		repo := get(key)
		Expect(repo.Spec.Reference.Commit).To(Equal(shaX))
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.FieldManager}))
	})

	It("removes the pin under --float", func() {
		advance(key, shaA)
		run(planOf(&actions.Pin{Client: k8sClient, Source: key, SHA: shaB, Unverified: true}))

		plan := planOf(&actions.Release{Client: k8sClient, Source: key, Float: true, Now: fixedClock})
		Expect(plan.After).To(HaveKeyWithValue(fieldCommit, valueUnset))
		Expect(plan.Warnings).To(ContainElement(ContainSubstring("floats on " + trackingRef)))
		run(plan)

		repo := get(key)
		Expect(repo.Spec.Reference.Commit).To(BeEmpty())

		By("leaving nobody owning a field that no longer exists")
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(BeEmpty())
		_, held := pin.Hold(repo)
		Expect(held).To(BeFalse())

		By("leaving the tracking ref to float on")
		Expect(repo.Spec.Reference.Name).To(Equal(trackingRef))
	})

	It("refuses to transfer a pin nobody is holding", func() {
		advance(key, shaA)

		_, err := (&actions.Release{Client: k8sClient, Source: key, Now: fixedClock}).Plan(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not held"))
		Expect(err.Error()).To(ContainSubstring("--float"))
	})

	It("refuses to release a source with no pin at all", func() {
		_, err := (&actions.Release{Client: k8sClient, Source: key, Now: fixedClock}).Plan(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no spec.ref.commit"))
	})
})
