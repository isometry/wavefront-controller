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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/inputs"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

const (
	parityNamespace = "parity"
	parityWavefront = "parity"
	scenarioLabel   = "wavefront.test/scenario"

	shaA  = "1111111111111111111111111111111111111111"
	shaB  = "2222222222222222222222222222222222222222"
	shaA2 = "3333333333333333333333333333333333333333"
	shaB2 = "4444444444444444444444444444444444444444"

	refMain = "refs/heads/main"
)

// firstObserved is the moment the injected observations were made, and
// firstObservedStamp the one form both providers must reduce it to.
//
// The fixture deliberately carries sub-second precision in the local zone,
// because that is exactly what the two origins disagree about: status.members
// round-trips through RFC3339 (whole seconds) and comes back in the local
// zone, while the engine hands derive the original instant untouched. Whole
// seconds in UTC would let the parity assertion pass without either
// normalisation.
var (
	firstObserved      = time.Now().Add(-2 * time.Minute).Truncate(time.Second).Add(750 * time.Millisecond)
	firstObservedStamp = firstObserved.UTC().Truncate(time.Second)
)

// The two providers are one truth model or they are two, and only a fixture
// that has been through the apiserver can tell the difference: status.members
// is written under the CRD's own schema and read back with its own timestamp
// precision, so parity has to be proven on the far side of a round trip.
var _ = Describe("Snapshot provider parity", Ordered, func() {
	var (
		nodeA, nodeB adapter.NodeRef
		repoA, repoB types.NamespacedName
	)

	BeforeAll(func() {
		makeNamespace(parityNamespace)

		repoA = types.NamespacedName{Namespace: parityNamespace, Name: "a"}
		repoB = types.NamespacedName{Namespace: parityNamespace, Name: "b"}
		nodeA = kustomizationRef("a")
		nodeB = kustomizationRef("b")

		labels := map[string]string{scenarioLabel: parityWavefront}

		By("creating two pinned sources and the nodes that depend on them")
		makeGitRepo(repoA, refMain, shaA)
		makeGitRepo(repoB, refMain, shaB)
		ksA := makeKustomization(nodeA, repoA, nil, labels)
		ksB := makeKustomization(nodeB, repoB, []string{nodeA.Name}, labels)

		// envtest runs no kustomize-controller, so the health signal a real
		// one would publish is hand-set here.
		setKustomizationReady(ksA, revisionOf(shaA))
		setKustomizationReady(ksB, revisionOf(shaB))

		makeWavefront(parityWavefront, parityWavefront)
	})

	AfterAll(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx,
			&wavefrontv1alpha1.Wavefront{Name: parityWavefront}))).To(Succeed())
	})

	It("publishes the status a controller would", func() {
		Expect(publishStatus(nil)).To(HaveLen(2))
	})

	It("describes the same nodes from status and from a live derivation", func() {
		statusSnap := capture(&snapshot.StatusSource{Reader: k8sClient, Wavefront: parityWavefront})
		deriveSnap := capture(&snapshot.DeriveSource{Reader: k8sClient, Wavefront: parityWavefront})

		Expect(statusSnap.Origin).To(Equal(snapshot.OriginStatus))
		Expect(deriveSnap.Origin).To(Equal(snapshot.OriginDerive))

		// The controller wrote every observed SHA it had; a derivation without
		// --poll observed nothing and must say so rather than present its
		// empty observations as fact.
		Expect(statusSnap.Observed).To(BeTrue())
		Expect(deriveSnap.Observed).To(BeFalse())

		// ReadyMessage and AppliedSHA are the fields only a live read carries;
		// everything else must agree node for node.
		Expect(statusSnap.Nodes).To(Equal(withoutDeriveOnly(deriveSnap.Nodes)))
	})

	It("layers both origins into the same waves", func() {
		statusSnap := capture(&snapshot.StatusSource{Reader: k8sClient, Wavefront: parityWavefront})
		deriveSnap := capture(&snapshot.DeriveSource{Reader: k8sClient, Wavefront: parityWavefront})

		Expect(snapshot.Waves(statusSnap.Nodes)).To(Equal(map[adapter.NodeRef]int{nodeA: 0, nodeB: 1}))
		Expect(snapshot.Waves(deriveSnap.Nodes)).To(Equal(snapshot.Waves(statusSnap.Nodes)))

		Expect(waveOf(statusSnap.Nodes, nodeB)).To(Equal(1), "the stamped wave must match the computed one")
		Expect(waveOf(deriveSnap.Nodes, nodeB)).To(Equal(1))
	})

	It("names the same sources under both origins, with the pins intact", func() {
		statusSnap := capture(&snapshot.StatusSource{Reader: k8sClient, Wavefront: parityWavefront})
		deriveSnap := capture(&snapshot.DeriveSource{Reader: k8sClient, Wavefront: parityWavefront})

		Expect(sourceNames(statusSnap.Sources)).To(Equal([]string{repoA.String(), repoB.String()}))
		Expect(sourceNames(deriveSnap.Sources)).To(Equal(sourceNames(statusSnap.Sources)))

		for _, snap := range []*snapshot.Snapshot{statusSnap, deriveSnap} {
			Expect(snap.Sources[0].Pin).To(Equal(shaA))
			Expect(snap.Sources[0].TrackingRef).To(Equal(refMain))
			Expect(snap.Sources[0].Nodes).To(Equal([]adapter.NodeRef{nodeA}))
			Expect(snap.Sources[0].Partial).To(BeFalse())
			// A source's URL is reported without any userinfo it may carry.
			Expect(snap.Sources[0].URL).NotTo(ContainSubstring("@"))
			// Pinned by the controller's own field manager, so nothing holds
			// it — and the provenance annotations are the durable ledger the
			// `source` command prints.
			Expect(snap.Sources[0].Hold).To(BeNil())
			Expect(snap.Sources[0].CommitOwners).To(ConsistOf(pin.Owner{
				Manager: pin.FieldManager, Operation: metav1.ManagedFieldsOperationApply,
			}))
			Expect(snap.Sources[0].Provenance).To(HaveKeyWithValue(pin.AnnotObservedRef, refMain))
			Expect(snap.Sources[0].Provenance).To(HaveKeyWithValue(pin.AnnotPreviousPin, ""))
			Expect(snap.Sources[0].Provenance).To(HaveKey(pin.AnnotAdmittedAt))
		}
	})

	It("carries no diagnostics for a fresh, fully readable fleet", func() {
		Expect(capture(&snapshot.StatusSource{Reader: k8sClient, Wavefront: parityWavefront}).Diagnostics).To(BeEmpty())
		Expect(capture(&snapshot.DeriveSource{Reader: k8sClient, Wavefront: parityWavefront}).Diagnostics).To(BeEmpty())
	})

	It("derives the numbers the controller published", func() {
		wf := &wavefrontv1alpha1.Wavefront{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: parityWavefront}, wf)).To(Succeed())

		derived := capture(&snapshot.DeriveSource{Reader: k8sClient, Wavefront: parityWavefront}).Derived
		Expect(derived.Phase).To(Equal(wf.Status.Phase))
		Expect(derived.Counts).To(Equal(wf.Status.Nodes))
		Expect(derived.GraphValid).To(BeTrue())
		Expect(derived.GraphReason).To(Equal(wavefrontv1alpha1.GraphValidReasonValid))
	})

	It("reports who owns the spec fields the write commands patch", func() {
		snap := capture(&snapshot.StatusSource{Reader: k8sClient, Wavefront: parityWavefront})

		// The suite created the Wavefront with the default client, whose
		// field manager therefore owns spec.mode and spec.suspend.
		Expect(snap.Wavefront.SpecOwners).To(HaveKey("spec.mode"))
		Expect(snap.Wavefront.SpecOwners["spec.mode"]).NotTo(BeEmpty())
		// Never the raw managedFields, only the manager names.
		Expect(snap.Wavefront.Status.Members).To(HaveLen(2))
	})

	// Quiescence is the easy case: every optional field is zero, so two
	// providers can agree by both saying nothing. The states that matter in an
	// incident are the ones that fill those fields in — and PendingSince is
	// the field the two origins reach by genuinely different routes (an
	// RFC3339 round trip versus the engine's own clock).
	It("agrees on a pending node and the descendant it blocks", func() {
		observed := observations(firstObserved, map[types.NamespacedName]string{repoA: shaA2, repoB: shaB2})
		publishStatus(observed)

		statusSnap := capture(&snapshot.StatusSource{Reader: k8sClient, Wavefront: parityWavefront})
		deriveSnap := capture(&snapshot.DeriveSource{
			Reader: k8sClient, Wavefront: parityWavefront, Observations: observed,
		})

		// A is pending with nothing above it, so it is the admissible head of
		// the wave; B is pending behind it and therefore blocked on it.
		a := nodeIn(statusSnap, nodeA)
		Expect(a.State).To(Equal(string(engine.StateAdmissible)))
		Expect(a.ObservedSHA).To(Equal(shaA2))
		Expect(a.Pin).To(Equal(shaA))
		Expect(a.Held).To(BeFalse())
		Expect(a.Blocked).To(BeNil())
		Expect(a.PendingSince).NotTo(BeNil())
		Expect(*a.PendingSince).To(Equal(firstObservedStamp),
			"both origins must reduce the observation instant to one form")

		b := nodeIn(statusSnap, nodeB)
		Expect(b.State).To(Equal(string(engine.StatePending)))
		Expect(b.ObservedSHA).To(Equal(shaB2))
		Expect(b.Blocked).NotTo(BeNil())
		Expect(b.Blocked.Reason).To(Equal(string(engine.ReasonAncestorPending)))
		Expect(b.Blocked.Ancestor).To(Equal(&wavefrontv1alpha1.NodeReference{
			Kind: nodeA.Kind, Namespace: nodeA.Namespace, Name: nodeA.Name,
		}))

		// And the derivation must reach every one of those by itself.
		Expect(statusSnap.Nodes).To(Equal(withoutDeriveOnly(deriveSnap.Nodes)))
		Expect(deriveSnap.Observed).To(BeTrue(), "an injected observation set is an observation set")
	})

	It("agrees on a self-held source and the descendant it holds up", func() {
		By("suspending the ancestor's source, the incident action a hold models")
		suspendRepo(repoA, true)
		DeferCleanup(func() { suspendRepo(repoA, false) })

		observed := observations(firstObserved, map[types.NamespacedName]string{repoA: shaA2, repoB: shaB2})
		publishStatus(observed)

		statusSnap := capture(&snapshot.StatusSource{Reader: k8sClient, Wavefront: parityWavefront})
		deriveSnap := capture(&snapshot.DeriveSource{
			Reader: k8sClient, Wavefront: parityWavefront, Observations: observed,
		})

		a := nodeIn(statusSnap, nodeA)
		Expect(a.Held).To(BeTrue())
		Expect(a.State).To(Equal(string(engine.StatePending)))
		Expect(a.Blocked).NotTo(BeNil())
		Expect(a.Blocked.Reason).To(Equal(string(engine.ReasonSelfHeld)))
		Expect(a.PendingSince).NotTo(BeNil())

		b := nodeIn(statusSnap, nodeB)
		Expect(b.Held).To(BeFalse(), "the descendant is not itself held")
		Expect(b.Blocked.Reason).To(Equal(string(engine.ReasonAncestorHeld)))
		Expect(b.Blocked.Ancestor.Name).To(Equal(nodeA.Name))

		Expect(statusSnap.Nodes).To(Equal(withoutDeriveOnly(deriveSnap.Nodes)))

		// The hold is a property of the source, and both origins report it —
		// status from the live GitRepository, derive from the engine's ledger.
		for _, snap := range []*snapshot.Snapshot{statusSnap, deriveSnap} {
			Expect(snap.Sources[0].Hold).To(Equal(&snapshot.HoldView{
				Kind: wavefrontv1alpha1.HoldReasonSuspend,
			}))
			Expect(snap.Sources[0].Suspended).To(BeTrue())
		}
	})

	It("diagnoses a pin the published status has not caught up with", func() {
		By("advancing a pin without republishing status")
		writer := &pin.Writer{Client: k8sClient}
		Expect(writer.Advance(ctx, repoA, shaA, shaA2, refMain, time.Now())).To(Succeed())
		DeferCleanup(func() {
			Expect(writer.Advance(ctx, repoA, shaA2, shaA, refMain, time.Now())).To(Succeed())
		})

		snap := capture(&snapshot.StatusSource{Reader: k8sClient, Wavefront: parityWavefront})

		// The live view keeps the live pin; the member keeps the published one.
		Expect(snap.Sources[0].Pin).To(Equal(shaA2))
		Expect(nodeIn(snap, nodeA).Pin).To(Equal(shaA))
		Expect(snap.Diagnostics).To(ContainElement(SatisfyAll(
			ContainSubstring(repoA.String()),
			ContainSubstring(shaA2[:7]),
			ContainSubstring(shaA[:7]),
			ContainSubstring("status is behind"),
		)))
	})

	It("degrades to a partial source when the GitRepository cannot be read", func() {
		By("deleting one source out from under the published status")
		Expect(k8sClient.Delete(ctx, &sourcev1.GitRepository{
			Namespace: repoB.Namespace, Name: repoB.Name,
		})).To(Succeed())
		DeferCleanup(func() {
			makeGitRepo(repoB, refMain, shaB)
		})

		snap := capture(&snapshot.StatusSource{Reader: k8sClient, Wavefront: parityWavefront})

		// The status still proves the source's name, pin and referencing
		// nodes; only the GitRepository's own detail is lost.
		Expect(snap.Sources).To(HaveLen(2))
		Expect(snap.Sources[1].Name).To(Equal(repoB.String()))
		Expect(snap.Sources[1].Partial).To(BeTrue())
		Expect(snap.Sources[1].Pin).To(Equal(shaB))
		Expect(snap.Sources[1].Nodes).To(Equal([]adapter.NodeRef{nodeB}))
		Expect(snap.Sources[1].URL).To(BeEmpty())
		Expect(snap.Diagnostics).To(ContainElement(ContainSubstring(repoB.String())))
	})
})

var _ = Describe("SelectWavefront", func() {
	It("auto-selects the only Wavefront", func() {
		makeWavefront("solo", "solo")
		DeferCleanup(deleteWavefront, "solo")

		wf, err := snapshot.SelectWavefront(ctx, k8sClient, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(wf.Name).To(Equal("solo"))
	})

	It("refuses to guess between several, and names them", func() {
		makeWavefront("alpha", "alpha")
		makeWavefront("beta", "beta")
		DeferCleanup(deleteWavefront, "alpha")
		DeferCleanup(deleteWavefront, "beta")

		_, err := snapshot.SelectWavefront(ctx, k8sClient, "")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("alpha"))
		Expect(err.Error()).To(ContainSubstring("beta"))
		Expect(err.Error()).To(ContainSubstring("--wavefront"))
	})

	It("gets the named one", func() {
		makeWavefront("alpha", "alpha")
		makeWavefront("beta", "beta")
		DeferCleanup(deleteWavefront, "alpha")
		DeferCleanup(deleteWavefront, "beta")

		wf, err := snapshot.SelectWavefront(ctx, k8sClient, "beta")
		Expect(err).NotTo(HaveOccurred())
		Expect(wf.Name).To(Equal("beta"))
	})

	It("reports an absent Wavefront rather than an empty one", func() {
		_, err := snapshot.SelectWavefront(ctx, k8sClient, "nonesuch")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nonesuch"))
	})
})

// publishStatus runs the reconciler's own path — inputs.Build then
// inputs.Summarise, whose Members are exactly what StatusSource must invert —
// and writes the result to the Wavefront, standing in for the controller this
// suite deliberately does not run. It returns the members it published.
func publishStatus(observed map[types.NamespacedName]gitpoll.Observation) []wavefrontv1alpha1.Member {
	GinkgoHelper()
	params := buildParams(parityWavefront)
	params.Observations = observed

	res, err := inputs.Build(ctx, k8sClient, params)
	Expect(err).NotTo(HaveOccurred())
	Expect(res.Resolved).To(BeTrue())

	summary := inputs.Summarise(res, time.Now())

	wf := &wavefrontv1alpha1.Wavefront{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: parityWavefront}, wf)).To(Succeed())
	now := metav1.NewTime(time.Now())
	wf.Status.Phase = summary.Phase
	wf.Status.Nodes = summary.Counts
	wf.Status.Blocked = summary.Blocked
	wf.Status.Held = summary.Held
	wf.Status.Members = summary.Members
	wf.Status.MembersOmitted = summary.MembersOmitted
	wf.Status.LastEvaluated = &now
	wf.Status.ObservedGeneration = wf.Generation
	apimeta.SetStatusCondition(&wf.Status.Conditions, metav1.Condition{
		Type:               wavefrontv1alpha1.ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             wavefrontv1alpha1.ReadyReasonSucceeded,
		ObservedGeneration: wf.Generation,
	})
	Expect(k8sClient.Status().Update(ctx, wf)).To(Succeed())
	return summary.Members
}

// observations builds the coherent sweep a poller would have published,
// stamped with each source's *live* plumbing — inputs.Build rejects an
// observation whose URL or tracking ref no longer matches the GitRepository.
func observations(at time.Time, shas map[types.NamespacedName]string) map[types.NamespacedName]gitpoll.Observation {
	GinkgoHelper()
	observed := make(map[types.NamespacedName]gitpoll.Observation, len(shas))
	for src, sha := range shas {
		repo := &sourcev1.GitRepository{}
		Expect(k8sClient.Get(ctx, src, repo)).To(Succeed())
		observed[src] = gitpoll.Observation{
			SHA:           sha,
			ObservedAt:    at,
			FirstObserved: at,
			URL:           repo.Spec.URL,
			TrackingRef:   refMain,
		}
	}
	return observed
}

// suspendRepo flips spec.suspend, the incident action a Suspend hold models.
func suspendRepo(src types.NamespacedName, suspend bool) {
	GinkgoHelper()
	Eventually(func() error {
		repo := &sourcev1.GitRepository{}
		if err := k8sClient.Get(ctx, src, repo); err != nil {
			return err
		}
		repo.Spec.Suspend = suspend
		return k8sClient.Update(ctx, repo)
	}).Should(Succeed())
}

// nodeIn returns one node's view, failing the spec rather than panicking when
// the snapshot does not carry it.
func nodeIn(snap *snapshot.Snapshot, ref adapter.NodeRef) snapshot.NodeView {
	GinkgoHelper()
	for _, node := range snap.Nodes {
		if node.Ref == ref {
			return node
		}
	}
	Fail(fmt.Sprintf("node %s is absent from the snapshot", ref))
	return snapshot.NodeView{}
}

// capture runs a provider and fails the spec on error.
func capture(source snapshot.Source) *snapshot.Snapshot {
	GinkgoHelper()
	snap, err := source.Capture(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(snap).NotTo(BeNil())
	return snap
}

// withoutDeriveOnly zeroes the two fields status.members cannot carry, so the
// remaining comparison is of what both origins genuinely claim to know.
func withoutDeriveOnly(nodes []snapshot.NodeView) []snapshot.NodeView {
	out := make([]snapshot.NodeView, len(nodes))
	copy(out, nodes)
	for i := range out {
		Expect(out[i].ReadyMessage).NotTo(BeEmpty(), "a live derivation must carry the Ready message")
		Expect(out[i].AppliedSHA).NotTo(BeEmpty(), "a live derivation must carry the applied SHA")
		out[i].ReadyMessage, out[i].AppliedSHA = "", ""
	}
	return out
}

func waveOf(nodes []snapshot.NodeView, ref adapter.NodeRef) int {
	GinkgoHelper()
	for _, node := range nodes {
		if node.Ref == ref {
			return node.Wave
		}
	}
	Fail(fmt.Sprintf("node %s is absent from the snapshot", ref))
	return 0
}

func sourceNames(sources []snapshot.SourceView) []string {
	names := make([]string, 0, len(sources))
	for _, source := range sources {
		names = append(names, source.Name)
	}
	return names
}

func buildParams(name string) inputs.Params {
	GinkgoHelper()
	wf := &wavefrontv1alpha1.Wavefront{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, wf)).To(Succeed())
	return inputs.Params{
		Wavefront: wf,
		Adapter:   adapter.NewKustomizationAdapter(),
		Strategy:  selection.TrackRef(),
	}
}

func kustomizationRef(name string) adapter.NodeRef {
	return adapter.NodeRef{Kind: kustomizev1.KustomizationKind, Namespace: parityNamespace, Name: name}
}

func revisionOf(sha string) string { return "main@sha1:" + sha }

func makeNamespace(name string) {
	GinkgoHelper()
	err := k8sClient.Create(ctx, &corev1.Namespace{Name: name})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}
}

// makeGitRepo creates a managed source and pins it the way the controller
// does — through pin.Writer, under the controller's own field manager — so
// the fixture is a normal pin rather than a hand-pin: the catalog renders the
// tracking ref and never the commit.
//
// The URL deliberately embeds a credential, so every spec that reads a URL
// back proves it was stripped.
func makeGitRepo(src types.NamespacedName, refName, commit string) {
	GinkgoHelper()
	repo := &sourcev1.GitRepository{
		Namespace: src.Namespace,
		Name:      src.Name,
		Labels:    map[string]string{pin.ManagedLabel: "true"},
		Spec: sourcev1.GitRepositorySpec{
			URL:       fmt.Sprintf("https://bot:hunter2@git.example.com/%s/%s.git", src.Namespace, src.Name),
			Interval:  metav1.Duration{Duration: time.Minute},
			Reference: &sourcev1.GitRepositoryRef{Name: refName},
		},
	}
	Expect(k8sClient.Create(ctx, repo)).To(Succeed())

	writer := &pin.Writer{Client: k8sClient}
	Expect(writer.Advance(ctx, src, "", commit, refName, time.Now())).To(Succeed())
}

func makeKustomization(
	ref adapter.NodeRef,
	source types.NamespacedName,
	dependsOn []string,
	labels map[string]string,
) *kustomizev1.Kustomization {
	GinkgoHelper()
	deps := make([]kustomizev1.DependencyReference, 0, len(dependsOn))
	for _, dep := range dependsOn {
		deps = append(deps, kustomizev1.DependencyReference{Name: dep, Namespace: ref.Namespace})
	}

	ks := &kustomizev1.Kustomization{
		Namespace: ref.Namespace, Name: ref.Name, Labels: labels,
		Spec: kustomizev1.KustomizationSpec{
			Interval:  metav1.Duration{Duration: time.Minute},
			DependsOn: deps,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind:      sourcev1.GitRepositoryKind,
				Name:      source.Name,
				Namespace: source.Namespace,
			},
		},
	}
	Expect(k8sClient.Create(ctx, ks)).To(Succeed())
	return ks
}

// setKustomizationReady hand-sets the health signal envtest's absent
// kustomize-controller would otherwise supply. The message is non-empty on
// purpose: it is one of the two fields status.members cannot carry, so parity
// is only meaningful if the derived side actually has one.
func setKustomizationReady(obj *kustomizev1.Kustomization, revision string) {
	GinkgoHelper()
	Eventually(func() error {
		fresh := &kustomizev1.Kustomization{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), fresh); err != nil {
			return err
		}
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               fluxmeta.ReadyCondition,
			Status:             metav1.ConditionTrue,
			Reason:             "ReconciliationSucceeded",
			Message:            "Applied revision: " + revision,
			ObservedGeneration: fresh.Generation,
		})
		fresh.Status.ObservedGeneration = fresh.Generation
		fresh.Status.LastAppliedRevision = revision
		return k8sClient.Status().Update(ctx, fresh)
	}).Should(Succeed())
}

func makeWavefront(name, scenario string) {
	GinkgoHelper()
	wf := &wavefrontv1alpha1.Wavefront{
		Name: name,
		Spec: wavefrontv1alpha1.WavefrontSpec{
			Nodes: wavefrontv1alpha1.NodesSpec{
				Kinds:    []string{kustomizev1.KustomizationKind},
				Selector: metav1.LabelSelector{MatchLabels: map[string]string{scenarioLabel: scenario}},
			},
			Mode: wavefrontv1alpha1.ModeShadow,
			Poll: wavefrontv1alpha1.PollSpec{Interval: metav1.Duration{Duration: time.Hour}},
		},
	}
	Expect(k8sClient.Create(ctx, wf)).To(Succeed())
}

func deleteWavefront(name string) {
	GinkgoHelper()
	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx,
		&wavefrontv1alpha1.Wavefront{Name: name}))).To(Succeed())
}
