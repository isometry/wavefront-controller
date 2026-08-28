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

package controller

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/pin"
)

const (
	// Scenario-scoped participation label. Deliberately NOT the production
	// wavefront.as-code.io/managed label, so that the CRD-validation specs'
	// Wavefronts (which select on the production label) can never select a
	// scenario's Kustomizations, whatever order Ginkgo runs the containers in.
	scenarioLabel = "wavefront.test/scenario"

	// Field manager standing in for a human hand-pin (DESIGN §3.5.3).
	humanManager = "kubectl-edit"

	mainRef    = "refs/heads/main"
	releaseRef = "refs/heads/release"

	// flotilla is the conventional pinned-node name across scenarios.
	flotilla = "flotilla"

	shaA    = "1111111111111111111111111111111111111111"
	shaB    = "2222222222222222222222222222222222222222"
	shaHand = "3333333333333333333333333333333333333333"

	testPollInterval = 100 * time.Millisecond
)

// revisionOf renders a Flux git revision for a SHA on the main branch.
func revisionOf(sha string) string { return "main@sha1:" + sha }

func repoURLFor(ns, name string) string {
	return fmt.Sprintf("https://git.example.com/%s/%s.git", ns, name)
}

// makeNamespace creates (idempotently) the scenario's namespace.
func makeNamespace(name string) {
	GinkgoHelper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := k8sClient.Create(ctx, ns)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}
}

// makeGitRepo creates a GitRepository. managed stamps the participation label
// the controller keys the Pinned role off; the commit is deliberately never
// rendered (DESIGN §3.5.1).
func makeGitRepo(ns, name, url, refName string, managed bool) *sourcev1.GitRepository {
	GinkgoHelper()
	repo := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: sourcev1.GitRepositorySpec{
			URL:       url,
			Interval:  metav1.Duration{Duration: time.Minute},
			Reference: &sourcev1.GitRepositoryRef{Name: refName},
		},
	}
	if managed {
		repo.Labels = map[string]string{pin.ManagedLabel: "true"}
	}
	Expect(k8sClient.Create(ctx, repo)).To(Succeed())
	return repo
}

// makeKustomization creates a Kustomization node. dependsOn names are
// same-namespace; labels select it into a Wavefront.
func makeKustomization(
	ns, name string,
	sourceRef types.NamespacedName,
	dependsOn []string,
	labels map[string]string,
) *kustomizev1.Kustomization {
	GinkgoHelper()
	deps := make([]kustomizev1.DependencyReference, 0, len(dependsOn))
	for _, dep := range dependsOn {
		deps = append(deps, kustomizev1.DependencyReference{Name: dep, Namespace: ns})
	}

	ks := &kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: labels},
		Spec: kustomizev1.KustomizationSpec{
			Interval:  metav1.Duration{Duration: time.Minute},
			Prune:     false,
			DependsOn: deps,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind:      sourcev1.GitRepositoryKind,
				Name:      sourceRef.Name,
				Namespace: sourceRef.Namespace,
			},
		},
	}
	Expect(k8sClient.Create(ctx, ks)).To(Succeed())
	return ks
}

// setKustomizationReady hand-sets the health signal envtest's absent
// kustomize-controller would otherwise supply.
func setKustomizationReady(obj *kustomizev1.Kustomization, revision string, ready bool) {
	GinkgoHelper()
	Eventually(func() error {
		fresh := &kustomizev1.Kustomization{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), fresh); err != nil {
			return err
		}
		status, reason := metav1.ConditionTrue, "ReconciliationSucceeded"
		if !ready {
			status, reason = metav1.ConditionFalse, "ReconciliationFailed"
		}
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               fluxmeta.ReadyCondition,
			Status:             status,
			Reason:             reason,
			Message:            "set by the wavefront controller test suite",
			ObservedGeneration: fresh.Generation,
		})
		fresh.Status.ObservedGeneration = fresh.Generation
		fresh.Status.LastAppliedRevision = revision
		return k8sClient.Status().Update(ctx, fresh)
	}).Should(Succeed())
}

// setArtifact hand-sets the GitRepository artifact source-controller would
// publish, which is what an initial pin bootstraps from (DESIGN §3.5.4).
func setArtifact(repo *sourcev1.GitRepository, revision string) {
	GinkgoHelper()
	Eventually(func() error {
		fresh := &sourcev1.GitRepository{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(repo), fresh); err != nil {
			return err
		}
		fresh.Status.Artifact = &fluxmeta.Artifact{
			Revision:       revision,
			Path:           "gitrepository/artifact.tar.gz",
			URL:            "http://source-controller/artifact.tar.gz",
			Digest:         "sha256:" + strings.Repeat("a", 64),
			LastUpdateTime: metav1.Now(),
		}
		return k8sClient.Status().Update(ctx, fresh)
	}).Should(Succeed())
}

// makeWavefront creates a Wavefront selecting one scenario's nodes, polling
// fast enough for a test to observe advancement.
func makeWavefront(name, scenario string, mode wavefrontv1alpha1.Mode) {
	GinkgoHelper()
	wf := &wavefrontv1alpha1.Wavefront{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: wavefrontv1alpha1.WavefrontSpec{
			Nodes: wavefrontv1alpha1.NodesSpec{
				Kinds:    []string{kindKustomization},
				Selector: metav1.LabelSelector{MatchLabels: map[string]string{scenarioLabel: scenario}},
			},
			Mode: mode,
			Poll: wavefrontv1alpha1.PollSpec{
				Interval:           metav1.Duration{Duration: testPollInterval},
				PerHostConcurrency: 4,
			},
		},
	}
	Expect(k8sClient.Create(ctx, wf)).To(Succeed())
	DeferCleanup(func() {
		_ = k8sClient.Delete(ctx, wf)
	})
}

func getWavefront(name string) *wavefrontv1alpha1.Wavefront {
	GinkgoHelper()
	wf := &wavefrontv1alpha1.Wavefront{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, wf)).To(Succeed())
	return wf
}

func getRepo(ns, name string) *sourcev1.GitRepository {
	GinkgoHelper()
	repo := &sourcev1.GitRepository{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, repo)).To(Succeed())
	return repo
}

// pinOf returns the live spec.ref.commit, "" when unpinned.
func pinOf(ns, name string) string {
	GinkgoHelper()
	repo := getRepo(ns, name)
	if repo.Spec.Reference == nil {
		return ""
	}
	return repo.Spec.Reference.Commit
}

func conditionOf(wf *wavefrontv1alpha1.Wavefront, conditionType string) *metav1.Condition {
	return apimeta.FindStatusCondition(wf.Status.Conditions, conditionType)
}

// recordedEvents returns every events.k8s.io/v1 Event with the given reason
// for the named regarding object, together with the summed occurrence count
// (the recorder aggregates repeats onto a single Event with a Series).
func recordedEvents(reason, regardingName string) (messages []string, occurrences int32) {
	GinkgoHelper()
	var list eventsv1.EventList
	Expect(k8sClient.List(ctx, &list)).To(Succeed())
	for i := range list.Items {
		e := &list.Items[i]
		if e.Reason != reason || e.Regarding.Name != regardingName {
			continue
		}
		messages = append(messages, e.Note)
		count := int32(1)
		if e.Series != nil {
			count = e.Series.Count
		}
		occurrences += count
	}
	return messages, occurrences
}

var _ = Describe("Wavefront reconciler", func() {
	Describe("initial pin on discovery", func() {
		It("pins a freshly discovered managed source to its artifact commit", func() {
			const ns, scenario = "initial-pin", "initial-pin"
			makeNamespace(ns)

			url := repoURLFor(ns, flotilla)
			lister.advertise(url, mainRef, shaA)
			repo := makeGitRepo(ns, flotilla, url, mainRef, true)
			setArtifact(repo, revisionOf(shaA))
			makeKustomization(ns, flotilla,
				types.NamespacedName{Namespace: ns, Name: flotilla}, nil,
				map[string]string{scenarioLabel: scenario})

			makeWavefront("wf-initial-pin", scenario, wavefrontv1alpha1.ModeEnforce)

			By("advancing spec.ref.commit to the artifact's commit")
			Eventually(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaA))

			By("recording provenance with an empty previous pin")
			annotations := getRepo(ns, flotilla).GetAnnotations()
			Expect(annotations).To(HaveKeyWithValue(pin.AnnotPreviousPin, ""))
			Expect(annotations).To(HaveKeyWithValue(pin.AnnotObservedRef, mainRef))
			Expect(annotations).To(HaveKey(pin.AnnotAdmittedAt))

			By("emitting an InitialPin event on the GitRepository")
			Eventually(func() int32 {
				_, occurrences := recordedEvents("InitialPin", flotilla)
				return occurrences
			}).Should(BeNumerically(">=", 1))

			By("incrementing wavefront_admissions_total{result=\"initial\"}")
			Eventually(func() float64 {
				return testutil.ToFloat64(instruments.AdmissionsTotal.WithLabelValues("initial"))
			}).Should(BeNumerically(">=", 1))

			By("reporting Ready")
			Eventually(func() *metav1.Condition {
				return conditionOf(getWavefront("wf-initial-pin"), wavefrontv1alpha1.ConditionReady)
			}).Should(HaveField("Status", metav1.ConditionTrue))

			By("counting the node as pinned")
			Eventually(func() wavefrontv1alpha1.NodeCounts {
				return getWavefront("wf-initial-pin").Status.Nodes
			}).Should(And(
				HaveField("Observed", 1),
				HaveField("Pinned", 1),
				HaveField("Gates", 0),
			))
		})
	})

	Describe("rolling admission with a gate", func() {
		It("holds a pending flotilla behind an unhealthy gate, then admits it", func() {
			const ns, scenario = "rolling-gate", "rolling-gate"
			makeNamespace(ns)

			gateURL := repoURLFor(ns, "gate")
			makeGitRepo(ns, "gate", gateURL, mainRef, false)
			// The gate is deliberately unlabelled: it must be discovered by the
			// dependsOn transitive closure, not by the selector.
			gate := makeKustomization(ns, "gate",
				types.NamespacedName{Namespace: ns, Name: "gate"}, nil, nil)

			url := repoURLFor(ns, flotilla)
			lister.advertise(url, mainRef, shaA)
			repo := makeGitRepo(ns, flotilla, url, mainRef, true)
			setArtifact(repo, revisionOf(shaA))
			flotillaKS := makeKustomization(ns, flotilla,
				types.NamespacedName{Namespace: ns, Name: flotilla}, []string{"gate"},
				map[string]string{scenarioLabel: scenario})

			makeWavefront("wf-rolling-gate", scenario, wavefrontv1alpha1.ModeEnforce)

			By("taking the initial pin")
			Eventually(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaA))

			By("blocking the pending revision on an unhealthy gate")
			setKustomizationReady(gate, "", false)
			lister.advertise(url, mainRef, shaB)
			Eventually(func() []wavefrontv1alpha1.BlockedNode {
				return getWavefront("wf-rolling-gate").Status.Blocked
			}).Should(HaveLen(1))

			wf := getWavefront("wf-rolling-gate")
			Expect(wf.Status.Blocked[0].Node.Name).To(Equal(flotilla))
			Expect(wf.Status.Blocked[0].Reason).To(Equal(string(engine.ReasonAncestorUnhealthy)))
			Expect(wf.Status.Blocked[0].Ancestor).NotTo(BeNil())
			Expect(wf.Status.Blocked[0].Ancestor.Name).To(Equal("gate"))
			Expect(wf.Status.Phase).To(Equal(wavefrontv1alpha1.PhaseBlocked))

			Consistently(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaA))

			By("admitting once the gate reports Ready")
			setKustomizationReady(gate, "", true)
			Eventually(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaB))

			annotations := getRepo(ns, flotilla).GetAnnotations()
			Expect(annotations).To(HaveKeyWithValue(pin.AnnotPreviousPin, shaA))
			Expect(annotations).To(HaveKeyWithValue(pin.AnnotObservedRef, mainRef))

			Eventually(func() int32 {
				_, occurrences := recordedEvents("PinAdvanced", flotilla)
				return occurrences
			}).Should(BeNumerically(">=", 1))

			By("quiescing once the flotilla is Ready at its new pin")
			setKustomizationReady(flotillaKS, revisionOf(shaB), true)
			Eventually(func() wavefrontv1alpha1.Phase {
				return getWavefront("wf-rolling-gate").Status.Phase
			}).Should(Equal(wavefrontv1alpha1.PhaseQuiescent))
		})
	})

	Describe("strict co-arrival ordering", func() {
		It("advances the downstream node only after the upstream has settled", func() {
			const ns, scenario = "co-arrival", "co-arrival"
			makeNamespace(ns)

			upURL, downURL := repoURLFor(ns, "up"), repoURLFor(ns, "down")
			lister.advertise(upURL, mainRef, shaA)
			lister.advertise(downURL, mainRef, shaA)

			upRepo := makeGitRepo(ns, "up", upURL, mainRef, true)
			setArtifact(upRepo, revisionOf(shaA))
			up := makeKustomization(ns, "up",
				types.NamespacedName{Namespace: ns, Name: "up"}, nil,
				map[string]string{scenarioLabel: scenario})

			downRepo := makeGitRepo(ns, "down", downURL, mainRef, true)
			setArtifact(downRepo, revisionOf(shaA))
			down := makeKustomization(ns, "down",
				types.NamespacedName{Namespace: ns, Name: "down"}, []string{"up"},
				map[string]string{scenarioLabel: scenario})

			makeWavefront("wf-co-arrival", scenario, wavefrontv1alpha1.ModeEnforce)

			By("taking both initial pins and settling both nodes")
			Eventually(func() string { return pinOf(ns, "up") }).Should(Equal(shaA))
			Eventually(func() string { return pinOf(ns, "down") }).Should(Equal(shaA))
			setKustomizationReady(up, revisionOf(shaA), true)
			setKustomizationReady(down, revisionOf(shaA), true)
			Eventually(func() wavefrontv1alpha1.Phase {
				return getWavefront("wf-co-arrival").Status.Phase
			}).Should(Equal(wavefrontv1alpha1.PhaseQuiescent))

			By("advertising new commits on both repositories at once")
			lister.advertise(upURL, mainRef, shaB)
			lister.advertise(downURL, mainRef, shaB)

			Eventually(func() string { return pinOf(ns, "up") }).Should(Equal(shaB))
			Consistently(func() string { return pinOf(ns, "down") }).Should(Equal(shaA))

			By("admitting downstream once upstream is Ready at its new pin")
			setKustomizationReady(up, revisionOf(shaB), true)
			Eventually(func() string { return pinOf(ns, "down") }).Should(Equal(shaB))
		})
	})

	Describe("Shadow mode", func() {
		It("reports would-be admissions without writing anything", func() {
			const ns, scenario = "shadow", "shadow"
			makeNamespace(ns)

			url := repoURLFor(ns, flotilla)
			lister.advertise(url, mainRef, shaA)
			repo := makeGitRepo(ns, flotilla, url, mainRef, true)
			setArtifact(repo, revisionOf(shaA))
			makeKustomization(ns, flotilla,
				types.NamespacedName{Namespace: ns, Name: flotilla}, nil,
				map[string]string{scenarioLabel: scenario})

			makeWavefront("wf-shadow", scenario, wavefrontv1alpha1.ModeShadow)

			By("emitting a ShadowAdmission carrying the would-be SHA")
			Eventually(func() []string {
				messages, _ := recordedEvents("ShadowAdmission", "wf-shadow")
				return messages
			}).Should(ContainElement(ContainSubstring(shaA)))

			By("never writing a pin, initial pins included")
			Consistently(func() string { return pinOf(ns, flotilla) }).Should(BeEmpty())

			By("still publishing live status")
			Eventually(func() wavefrontv1alpha1.NodeCounts {
				return getWavefront("wf-shadow").Status.Nodes
			}).Should(And(HaveField("Observed", 1), HaveField("Pinned", 1)))
		})
	})

	Describe("suspend", func() {
		It("freezes writes mid-pending while status keeps updating", func() {
			const ns, scenario = "suspend", "suspend"
			makeNamespace(ns)

			url := repoURLFor(ns, flotilla)
			lister.advertise(url, mainRef, shaA)
			repo := makeGitRepo(ns, flotilla, url, mainRef, true)
			setArtifact(repo, revisionOf(shaA))
			makeKustomization(ns, flotilla,
				types.NamespacedName{Namespace: ns, Name: flotilla}, nil,
				map[string]string{scenarioLabel: scenario})

			makeWavefront("wf-suspend", scenario, wavefrontv1alpha1.ModeEnforce)
			Eventually(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaA))

			By("suspending the Wavefront and waiting for the controller to observe it")
			Eventually(func() error {
				wf := getWavefront("wf-suspend")
				wf.Spec.Suspend = true
				return k8sClient.Update(ctx, wf)
			}).Should(Succeed())
			generation := getWavefront("wf-suspend").Generation
			Eventually(func() int64 {
				return getWavefront("wf-suspend").Status.ObservedGeneration
			}).Should(Equal(generation))

			By("leaving a newly advertised revision unadmitted")
			lister.advertise(url, mainRef, shaB)
			Consistently(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaA))

			By("still reporting the pending revision")
			Eventually(func() int {
				return getWavefront("wf-suspend").Status.Nodes.Pending
			}).Should(Equal(1))
		})
	})

	Describe("hand-pin holds", func() {
		It("reports a foreign field manager once and resumes when released", func() {
			const ns, scenario = "hand-pin", "hand-pin"
			makeNamespace(ns)

			url := repoURLFor(ns, flotilla)
			// A non-default tracking ref, so the whole chain (selection ->
			// poll target -> provenance) is proven for more than main.
			lister.advertise(url, releaseRef, shaA)
			// Deliberately artifact-free: once the hand-pin is removed the pin
			// bootstraps from the latest observation rather than a stale artifact.
			makeGitRepo(ns, flotilla, url, releaseRef, true)
			makeKustomization(ns, flotilla,
				types.NamespacedName{Namespace: ns, Name: flotilla}, nil,
				map[string]string{scenarioLabel: scenario})

			makeWavefront("wf-hand-pin", scenario, wavefrontv1alpha1.ModeEnforce)
			Eventually(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaA))
			Expect(getRepo(ns, flotilla).GetAnnotations()).To(
				HaveKeyWithValue(pin.AnnotObservedRef, releaseRef))

			By("hand-pinning spec.ref.commit under a foreign field manager")
			Eventually(func() error {
				repo := getRepo(ns, flotilla)
				repo.Spec.Reference.Commit = shaHand
				return k8sClient.Update(ctx, repo, client.FieldOwner(humanManager))
			}).Should(Succeed())

			By("naming the holder in status.held")
			Eventually(func() []wavefrontv1alpha1.HeldNode {
				return getWavefront("wf-hand-pin").Status.Held
			}).Should(HaveLen(1))
			wf := getWavefront("wf-hand-pin")
			Expect(wf.Status.Held[0].Manager).To(Equal(humanManager))
			Expect(wf.Status.Held[0].Source).To(Equal(ns + "/" + flotilla))
			Expect(wf.Status.Held[0].Node.Name).To(Equal(flotilla))

			By("never advancing past the hand-pin")
			lister.advertise(url, releaseRef, shaB)
			Consistently(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaHand))

			By("emitting HoldDetected exactly once")
			Eventually(func() int32 {
				_, occurrences := recordedEvents("HoldDetected", "wf-hand-pin")
				return occurrences
			}).Should(Equal(int32(1)))
			Consistently(func() int32 {
				_, occurrences := recordedEvents("HoldDetected", "wf-hand-pin")
				return occurrences
			}).Should(Equal(int32(1)))

			By("releasing the hold")
			Eventually(func() error {
				repo := getRepo(ns, flotilla)
				return k8sClient.Patch(ctx, repo,
					client.RawPatch(types.JSONPatchType, []byte(`[{"op":"remove","path":"/spec/ref/commit"}]`)),
					client.FieldOwner(humanManager))
			}).Should(Succeed())

			Eventually(func() int32 {
				_, occurrences := recordedEvents("HoldReleased", "wf-hand-pin")
				return occurrences
			}).Should(BeNumerically(">=", 1))
			Eventually(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaB))
			Eventually(func() []wavefrontv1alpha1.HeldNode {
				return getWavefront("wf-hand-pin").Status.Held
			}).Should(BeEmpty())
		})
	})

	Describe("selector overlap", func() {
		It("marks both Wavefronts invalid and admits nothing", func() {
			const ns, scenario = "overlap", "overlap"
			makeNamespace(ns)

			// Both Wavefronts are created before any node exists: overlap is a
			// property of a selected node, so a lone Wavefront that reconciles
			// first would legitimately admit before the second one appears.
			makeWavefront("wf-overlap-a", scenario, wavefrontv1alpha1.ModeEnforce)
			makeWavefront("wf-overlap-b", scenario, wavefrontv1alpha1.ModeEnforce)
			for _, name := range []string{"wf-overlap-a", "wf-overlap-b"} {
				Eventually(func() int64 {
					return getWavefront(name).Status.ObservedGeneration
				}).Should(Equal(getWavefront(name).Generation))
			}

			url := repoURLFor(ns, flotilla)
			lister.advertise(url, mainRef, shaA)
			repo := makeGitRepo(ns, flotilla, url, mainRef, true)
			setArtifact(repo, revisionOf(shaA))
			makeKustomization(ns, flotilla,
				types.NamespacedName{Namespace: ns, Name: flotilla}, nil,
				map[string]string{scenarioLabel: scenario})

			for _, name := range []string{"wf-overlap-a", "wf-overlap-b"} {
				Eventually(func() *metav1.Condition {
					return conditionOf(getWavefront(name), wavefrontv1alpha1.ConditionGraphValid)
				}).Should(And(
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", "SelectorOverlap"),
				))
			}
			Expect(conditionOf(getWavefront("wf-overlap-a"),
				wavefrontv1alpha1.ConditionGraphValid).Message).To(ContainSubstring("wf-overlap-b"))

			Consistently(func() string { return pinOf(ns, flotilla) }).Should(BeEmpty())
		})
	})

	Describe("dependsOn cycles", func() {
		It("reports the cycle while unrelated nodes still admit", func() {
			const ns, scenario = "cycle", "cycle"
			makeNamespace(ns)

			gateURL := repoURLFor(ns, "unmanaged")
			makeGitRepo(ns, "unmanaged", gateURL, mainRef, false)
			unmanaged := types.NamespacedName{Namespace: ns, Name: "unmanaged"}
			makeKustomization(ns, "cycle-a", unmanaged, []string{"cycle-b"},
				map[string]string{scenarioLabel: scenario})
			makeKustomization(ns, "cycle-b", unmanaged, []string{"cycle-a"},
				map[string]string{scenarioLabel: scenario})

			url := repoURLFor(ns, "solo")
			lister.advertise(url, mainRef, shaA)
			repo := makeGitRepo(ns, "solo", url, mainRef, true)
			setArtifact(repo, revisionOf(shaA))
			makeKustomization(ns, "solo",
				types.NamespacedName{Namespace: ns, Name: "solo"}, nil,
				map[string]string{scenarioLabel: scenario})

			makeWavefront("wf-cycle", scenario, wavefrontv1alpha1.ModeEnforce)

			Eventually(func() *metav1.Condition {
				return conditionOf(getWavefront("wf-cycle"), wavefrontv1alpha1.ConditionGraphValid)
			}).Should(And(
				HaveField("Status", metav1.ConditionFalse),
				HaveField("Reason", "CyclesDetected"),
			))
			Expect(conditionOf(getWavefront("wf-cycle"),
				wavefrontv1alpha1.ConditionGraphValid).Message).To(ContainSubstring("cycle-a"))

			By("still admitting the unrelated flotilla")
			Eventually(func() string { return pinOf(ns, "solo") }).Should(Equal(shaA))
		})
	})
})
