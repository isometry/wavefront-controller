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
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5/plumbing/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/actions"
)

// The fixture: one catalog-rendered source in the default namespace, tracking
// a branch, with the SHAs an incident moves it between.
const (
	testNamespace = "default"
	repoURL       = "https://git.example.com/org/infra.git"
	trackingRef   = "refs/heads/main"

	shaA = "1111111111111111111111111111111111111111"
	shaB = "2222222222222222222222222222222222222222"
	shaC = "3333333333333333333333333333333333333333"
	shaX = "4444444444444444444444444444444444444444"
	shaZ = "9999999999999999999999999999999999999999"

	// catalogManager renders the GitRepository, and must keep owning every
	// field it renders through all of this.
	catalogManager = "kustomize-controller"
	// humanManager is `kubectl patch`: a third party, holding the pin through
	// an Update operation, which is the case plan B4 calls experimental.
	humanManager = "kubectl-patch"

	// The literals the specs assert on often enough that the linter, rightly,
	// wants them named.
	fieldCommit  = "spec.ref.commit"
	fieldRef     = "ref"
	fieldNS      = "namespace"
	fieldSuspend = "spec.suspend"
	fieldName    = "name"
	valueTrue    = "true"
	valueUnset   = "(unset)"
)

// The timestamps a controller pin and a wfctl write are stamped with. Fixed so
// that a provenance assertion is an equality, not an approximation.
var (
	admittedAt  = time.Date(2026, 8, 27, 8, 14, 3, 0, time.UTC)
	releasedAt  = time.Date(2026, 8, 27, 11, 30, 0, 0, time.UTC)
	admittedA   = "2026-08-27T08:14:03Z"
	releasedRFC = "2026-08-27T11:30:00Z"
)

// fixedClock is the clock every write is stamped with, so a provenance
// assertion is an equality rather than a window.
func fixedClock() time.Time { return releasedAt }

// displacedField is how a plan names the displaced-pin annotation.
var displacedField = "metadata.annotations[" + pin.AnnotDisplacedPin + "]"

// names makes each spec's objects its own: envtest is one apiserver for the
// whole suite, and a shared name would make the specs order-dependent.
var names = map[string]int{}

func uniqueName(prefix string) string {
	names[prefix]++
	return fmt.Sprintf("%s-%d", prefix, names[prefix])
}

// sourceSpec is the minimal GitRepository spec the catalog renders: the
// tracking ref and no commit (DESIGN §3.5.1), with mutate applied on top.
func sourceSpec(mutate ...func(spec map[string]any)) map[string]any {
	spec := map[string]any{
		"url":      repoURL,
		"interval": "1m",
		fieldRef:   map[string]any{fieldName: trackingRef},
	}
	for _, m := range mutate {
		m(spec)
	}
	return spec
}

// renderSource applies a GitRepository the way the catalog does: sourceSpec
// and the participation label.
func renderSource(prefix string, mutate ...func(spec map[string]any)) types.NamespacedName {
	GinkgoHelper()

	key := types.NamespacedName{Namespace: testNamespace, Name: uniqueName(prefix)}
	spec := sourceSpec(mutate...)

	rendered := object(sourcev1.GroupVersion.String(), sourcev1.GitRepositoryKind, map[string]any{
		fieldName: key.Name,
		fieldNS:   key.Namespace,
		"labels":  map[string]any{pin.ManagedLabel: valueTrue},
	}, spec)
	Expect(k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(rendered),
		client.FieldOwner(catalogManager))).To(Succeed())

	return key
}

// object builds an apply configuration's envelope. Every spec that applies
// something needs the same four keys, and spelling them out each time is how a
// typo in one of them becomes a puzzling apiserver error.
func object(apiVersion, kind string, meta, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   meta,
		"spec":       spec,
	}}
}

// gitRepository is an empty typed source carrying only its identity, for the
// patches that need a target and nothing else.
func gitRepository(key types.NamespacedName) *sourcev1.GitRepository {
	repo := &sourcev1.GitRepository{}
	repo.Namespace, repo.Name = key.Namespace, key.Name
	return repo
}

// unstructuredSource reads a source with nothing thrown away, which is the
// only way to see a field the vendored Go type does not declare.
func unstructuredSource(key types.NamespacedName) *unstructured.Unstructured {
	GinkgoHelper()

	repo := &unstructured.Unstructured{}
	repo.SetGroupVersionKind(sourcev1.GroupVersion.WithKind(sourcev1.GitRepositoryKind))
	Expect(k8sClient.Get(ctx, key, repo)).To(Succeed())
	return repo
}

// unknownToThisBuild is the spec field this build's Go type does not declare:
// the version skew a release has to survive, staged by preserveUnknownFields.
const unknownToThisBuild = "unknownToThisBuild"

// unknownValue reads the spec field this build's Go type does not declare.
func unknownValue(key types.NamespacedName) string {
	GinkgoHelper()

	value, _, err := unstructured.NestedString(unstructuredSource(key).Object, "spec", unknownToThisBuild)
	Expect(err).NotTo(HaveOccurred())
	return value
}

// preserveUnknownFields relaxes the GitRepository CRD's pruning for the length
// of one spec, and puts it back afterwards. Without it the apiserver deletes an
// unknown field on the way in, and the version skew a release has to survive
// cannot be staged at all.
func preserveUnknownFields() {
	GinkgoHelper()

	const crdName = "gitrepositories.source.toolkit.fluxcd.io"

	// Read as unstructured: the apiextensions types are nobody's dependency
	// here, and the RESTMapper needs no scheme entry to find a CRD.
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: crdName}, crd)).To(Succeed())

	versions, found, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	original := runtime.DeepCopyJSON(map[string]any{"versions": versions})["versions"].([]any)

	for _, version := range versions {
		Expect(unstructured.SetNestedField(version.(map[string]any), true,
			"schema", "openAPIV3Schema", "properties", "spec",
			"x-kubernetes-preserve-unknown-fields")).To(Succeed())
	}
	Expect(unstructured.SetNestedSlice(crd.Object, versions, "spec", "versions")).To(Succeed())
	Expect(k8sClient.Update(ctx, crd)).To(Succeed())

	// The apiserver picks up a CRD schema change asynchronously, so a create
	// racing the update above is still pruned on a slow runner. A server-side
	// dry run persists nothing and returns the object as it would be stored,
	// so it is the honest readiness probe: wait until it hands the unknown
	// field back before letting the spec stage one for real.
	Eventually(func(g Gomega) {
		probe := object(sourcev1.GroupVersion.String(), sourcev1.GitRepositoryKind,
			map[string]any{fieldName: uniqueName("preserve-probe"), fieldNS: testNamespace},
			sourceSpec(func(spec map[string]any) { spec[unknownToThisBuild] = "probe" }))
		g.Expect(k8sClient.Create(ctx, probe, client.DryRunAll)).To(Succeed())
		value, _, err := unstructured.NestedString(probe.Object, "spec", unknownToThisBuild)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(value).To(Equal("probe"))
	}, 30*time.Second).Should(Succeed())

	DeferCleanup(func() {
		restored := &unstructured.Unstructured{}
		restored.SetGroupVersionKind(crd.GroupVersionKind())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: crdName}, restored)).To(Succeed())
		Expect(unstructured.SetNestedSlice(restored.Object, original, "spec", "versions")).To(Succeed())
		Expect(k8sClient.Update(ctx, restored)).To(Succeed())
	})
}

// get reads a source back.
func get(key types.NamespacedName) *sourcev1.GitRepository {
	GinkgoHelper()

	repo := &sourcev1.GitRepository{}
	Expect(k8sClient.Get(ctx, key, repo)).To(Succeed())
	return repo
}

// advance pins a source the way the controller does, as an initial pin: the
// outgoing pin every spec here starts from is no pin at all.
func advance(key types.NamespacedName, sha string) {
	GinkgoHelper()

	writer := &pin.Writer{Client: k8sClient}
	Expect(writer.Advance(ctx, key, "", sha, trackingRef, admittedAt)).To(Succeed())
}

// handPin sets the commit the way `kubectl patch` does: an Update operation
// under a foreign manager, which the apiserver keys separately from any apply.
func handPin(key types.NamespacedName, sha, manager string) {
	GinkgoHelper()

	repo := get(key)
	if repo.Spec.Reference == nil {
		repo.Spec.Reference = &sourcev1.GitRepositoryRef{Name: trackingRef}
	}
	repo.Spec.Reference.Commit = sha
	Expect(k8sClient.Update(ctx, repo, client.FieldOwner(manager))).To(Succeed())
}

// fieldOwners lists the managers owning one field, in managedFields order.
func fieldOwners(obj client.Object, path ...string) []string {
	GinkgoHelper()

	target := fieldpath.MakePathOrDie(pathElements(path)...)
	var owners []string
	for _, entry := range obj.GetManagedFields() {
		if entry.Subresource != "" || entry.FieldsV1 == nil {
			continue
		}
		set := &fieldpath.Set{}
		Expect(set.FromJSON(entry.FieldsV1.GetRawReader())).To(Succeed())
		if set.Has(target) {
			owners = append(owners, entry.Manager)
		}
	}
	return owners
}

// pathElements converts a field path to what fieldpath.MakePathOrDie wants.
func pathElements(path []string) []any {
	elements := make([]any, 0, len(path))
	for _, element := range path {
		elements = append(elements, element)
	}
	return elements
}

// ownersOf renders the managedFields entries owning the pin, operation
// included: which operation a manager holds through is the whole question a
// release has to answer.
func ownersOf(repo *sourcev1.GitRepository) []pin.Owner {
	return pin.Owners(repo)
}

// planOf builds a plan and fails the spec if it cannot.
func planOf(action actions.Action) *actions.Plan {
	GinkgoHelper()

	plan, err := action.Plan(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(plan).NotTo(BeNil())
	return plan
}

// run applies a plan through the same confirmation gate the CLI uses, with
// consent already given: --yes is the path every spec here is asserting on.
func run(plan *actions.Plan) {
	GinkgoHelper()

	written, err := actions.Confirmer{Out: GinkgoWriter, Yes: true}.Run(ctx, plan)
	Expect(err).NotTo(HaveOccurred())
	Expect(written).To(BeTrue())
}

// runWritingNothing applies a plan that succeeds having written nothing —
// which is what a strip with no targets is, and why it records no audit event.
func runWritingNothing(plan *actions.Plan) {
	GinkgoHelper()

	written, err := actions.Confirmer{Out: GinkgoWriter, Yes: true}.Run(ctx, plan)
	Expect(err).NotTo(HaveOccurred())
	Expect(written).To(BeFalse())
}

// runExpectingError applies a plan that is expected to fail outright, having
// written nothing.
func runExpectingError(plan *actions.Plan) error {
	GinkgoHelper()

	written, err := actions.Confirmer{Out: GinkgoWriter, Yes: true}.Run(ctx, plan)
	Expect(err).To(HaveOccurred())
	Expect(written).To(BeFalse())
	return err
}

// makeWavefront creates a Wavefront as a GitOps applier would, so that the
// spec-ownership warnings have something real to read.
func makeWavefront(prefix string, mutate ...func(spec map[string]any)) *wavefrontv1alpha1.Wavefront {
	GinkgoHelper()

	name := uniqueName(prefix)
	spec := map[string]any{
		"nodes": map[string]any{
			"kinds":    []any{"Kustomization"},
			"selector": map[string]any{"matchLabels": map[string]any{pin.ManagedLabel: "true"}},
		},
		"mode": string(wavefrontv1alpha1.ModeEnforce),
	}
	for _, m := range mutate {
		m(spec)
	}

	desired := object(wavefrontv1alpha1.GroupVersion.String(), "Wavefront",
		map[string]any{fieldName: name}, spec)
	Expect(k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(desired),
		client.FieldOwner(catalogManager))).To(Succeed())

	return getWavefront(name)
}

// getWavefront reads a Wavefront back.
func getWavefront(name string) *wavefrontv1alpha1.Wavefront {
	GinkgoHelper()

	wf := &wavefrontv1alpha1.Wavefront{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, wf)).To(Succeed())
	return wf
}

// fakeLister is the ref advertisement, without a git host. The production
// lister speaks to a remote over the network, which no unit of behaviour
// asserted here has anything to do with.
type fakeLister struct {
	refs map[string]string
	err  error
}

func (f fakeLister) List(_ context.Context, _ string, _ transport.AuthMethod) (map[string]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.refs, nil
}

// applyOperation is metav1's name for an apply-op managedFields entry, spelled
// once so the specs read as English.
var (
	applyOperation  = metav1.ManagedFieldsOperationApply
	updateOperation = metav1.ManagedFieldsOperationUpdate
)
