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

package pin_test

import (
	"errors"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"github.com/isometry/wavefront-controller/internal/pin"
)

const (
	// Field managers other than the controller's own.
	catalogManager = "kustomize-controller"
	humanManager   = "kubectl-edit"

	shaA = "1111111111111111111111111111111111111111"
	shaB = "2222222222222222222222222222222222222222"
	shaX = "3333333333333333333333333333333333333333"
	shaC = "4444444444444444444444444444444444444444"

	trackingRef = "refs/heads/main"
	repoURL     = "https://example.com/org/repo.git"
)

// fieldOwners returns the sorted field managers that own path in the object's
// managedFields (spec/metadata only — status-subresource entries are skipped).
func fieldOwners(repo *sourcev1.GitRepository, path ...string) []string {
	parts := make([]any, 0, len(path))
	for _, p := range path {
		parts = append(parts, p)
	}
	target := fieldpath.MakePathOrDie(parts...)

	owners := []string{}
	for _, entry := range repo.GetManagedFields() {
		if entry.Subresource != "" || entry.FieldsV1 == nil {
			continue
		}
		set := &fieldpath.Set{}
		ExpectWithOffset(1, set.FromJSON(entry.FieldsV1.GetRawReader())).To(Succeed())
		if set.Has(target) {
			owners = append(owners, entry.Manager)
		}
	}
	slices.Sort(owners)
	return slices.Compact(owners)
}

var _ = Describe("Writer", Ordered, func() {
	var (
		writer *pin.Writer
		key    types.NamespacedName
		admitA = time.Date(2026, 8, 27, 10, 14, 3, 0, time.FixedZone("CEST", 2*60*60))
		admitB = time.Date(2026, 8, 27, 11, 30, 0, 0, time.UTC)
	)

	// get returns the live GitRepository, managedFields included.
	get := func() *sourcev1.GitRepository {
		repo := &sourcev1.GitRepository{}
		ExpectWithOffset(1, k8sClient.Get(ctx, key, repo)).To(Succeed())
		return repo
	}

	BeforeAll(func() {
		writer = &pin.Writer{Client: k8sClient}
		key = types.NamespacedName{Namespace: "default", Name: "flotilla-team-a"}

		By("rendering the catalog's GitRepository as kustomize-controller (no commit)")
		rendered := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": sourcev1.GroupVersion.String(),
			"kind":       sourcev1.GitRepositoryKind,
			"metadata": map[string]any{
				"name":      key.Name,
				"namespace": key.Namespace,
				"labels": map[string]any{
					pin.ManagedLabel: "true",
				},
			},
			"spec": map[string]any{
				"url":      repoURL,
				"interval": "1m",
				"ref": map[string]any{
					"name": trackingRef,
				},
			},
		}}
		Expect(k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(rendered),
			client.FieldOwner(catalogManager))).To(Succeed())
	})

	It("takes clean co-ownership of spec.ref.commit on the initial pin", func() {
		Expect(writer.Advance(ctx, key, "", shaA, trackingRef, admitA)).To(Succeed())

		repo := get()
		Expect(repo.Spec.Reference).NotTo(BeNil())
		Expect(repo.Spec.Reference.Commit).To(Equal(shaA))

		By("recording provenance annotations, with an explicit empty previous pin")
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotAdmittedAt, "2026-08-27T08:14:03Z"))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotPreviousPin, ""))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotObservedRef, trackingRef))

		By("leaving the catalog-rendered fields untouched")
		Expect(repo.Spec.Reference.Name).To(Equal(trackingRef))
		Expect(repo.Spec.URL).To(Equal(repoURL))
		Expect(repo.Spec.Interval.Duration).To(Equal(time.Minute))
		Expect(repo.GetLabels()).To(HaveKeyWithValue(pin.ManagedLabel, "true"))

		By("co-owning only the commit field")
		Expect(fieldOwners(repo, "spec", "ref", "name")).To(Equal([]string{catalogManager}))
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.FieldManager}))
		Expect(fieldOwners(repo, "spec", "url")).To(Equal([]string{catalogManager}))

		By("reporting no hold")
		manager, held := pin.Hold(repo)
		Expect(held).To(BeFalse())
		Expect(manager).To(BeEmpty())
	})

	It("records the outgoing pin on a second advance", func() {
		Expect(writer.Advance(ctx, key, shaA, shaB, trackingRef, admitB)).To(Succeed())

		repo := get()
		Expect(repo.Spec.Reference.Commit).To(Equal(shaB))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotPreviousPin, shaA))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotAdmittedAt, "2026-08-27T11:30:00Z"))
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.FieldManager}))
	})

	It("detects a hand-pin and refuses to advance past it", func() {
		By("hand-pinning spec.ref.commit under a foreign field manager")
		repo := get()
		repo.Spec.Reference.Commit = shaX
		Expect(k8sClient.Update(ctx, repo, client.FieldOwner(humanManager))).To(Succeed())

		repo = get()
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{humanManager}))

		manager, held := pin.Hold(repo)
		Expect(held).To(BeTrue())
		Expect(manager).To(Equal(humanManager))

		By("returning ErrHeld rather than forcing ownership")
		err := writer.Advance(ctx, key, shaB, shaC, trackingRef, admitB)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, pin.ErrHeld)).To(BeTrue())

		By("leaving the hand-pin and its provenance untouched")
		repo = get()
		Expect(repo.Spec.Reference.Commit).To(Equal(shaX))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotPreviousPin, shaA))
	})

	It("advances again once the hold is released", func() {
		By("removing spec.ref.commit as the foreign field manager")
		repo := get()
		Expect(k8sClient.Patch(ctx, repo,
			client.RawPatch(types.JSONPatchType, []byte(`[{"op":"remove","path":"/spec/ref/commit"}]`)),
			client.FieldOwner(humanManager))).To(Succeed())

		repo = get()
		Expect(repo.Spec.Reference.Commit).To(BeEmpty())
		manager, held := pin.Hold(repo)
		Expect(held).To(BeFalse())
		Expect(manager).To(BeEmpty())

		Expect(writer.Advance(ctx, key, shaX, shaC, trackingRef, admitB)).To(Succeed())

		repo = get()
		Expect(repo.Spec.Reference.Commit).To(Equal(shaC))
		Expect(repo.GetAnnotations()).To(HaveKeyWithValue(pin.AnnotPreviousPin, shaX))
		Expect(fieldOwners(repo, "spec", "ref", "commit")).To(Equal([]string{pin.FieldManager}))
	})

	It("never writes the managed label", func() {
		repo := get()
		Expect(repo.GetLabels()).To(HaveKeyWithValue(pin.ManagedLabel, "true"))
		Expect(fieldOwners(repo, "metadata", "labels", pin.ManagedLabel)).To(Equal([]string{catalogManager}))
		Expect(fieldOwners(repo, "spec", "ref", "name")).To(Equal([]string{catalogManager}))
	})
})
