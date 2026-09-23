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

	// Field manager standing in for a human hand-pin.
	humanManager = "kubectl-edit"

	mainRef    = "refs/heads/main"
	releaseRef = "refs/heads/release"

	// flotilla is the conventional pinned-node name across scenarios, gateNode
	// the conventional health-only one.
	flotilla = "flotilla"
	gateNode = "gate"

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
	ns := &corev1.Namespace{Name: name}
	err := k8sClient.Create(ctx, ns)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}
}

// makeGitRepo creates a GitRepository. managed stamps the participation label
// the controller keys the Pinned role off; the commit is deliberately never
// rendered, since the catalog renders only the tracking ref and leaves
// spec.ref.commit for the controller to own.
func makeGitRepo(ns, name, url, refName string, managed bool) *sourcev1.GitRepository {
	GinkgoHelper()
	repo := &sourcev1.GitRepository{
		Namespace: ns, Name: name,
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
		Namespace: ns, Name: name, Labels: labels,
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
// publish, which is what an initial pin bootstraps from.
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
	makeWavefrontPolling(name, scenario, mode, testPollInterval)
}

// makeWavefrontPolling is makeWavefront with an explicit poll interval, for the
// specs that care about what the interval rate-limits.
func makeWavefrontPolling(name, scenario string, mode wavefrontv1alpha1.Mode, interval time.Duration) {
	GinkgoHelper()
	wf := &wavefrontv1alpha1.Wavefront{
		Name: name,
		Spec: wavefrontv1alpha1.WavefrontSpec{
			Nodes: wavefrontv1alpha1.NodesSpec{
				Kinds:    []string{kindKustomization},
				Selector: metav1.LabelSelector{MatchLabels: map[string]string{scenarioLabel: scenario}},
			},
			Mode: mode,
			Poll: wavefrontv1alpha1.PollSpec{
				Interval:           metav1.Duration{Duration: interval},
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

// memberOf returns the named node's entry in status.members, nil when absent.
func memberOf(wf *wavefrontv1alpha1.Wavefront, name string) *wavefrontv1alpha1.Member {
	for i := range wf.Status.Members {
		if wf.Status.Members[i].Node.Name == name {
			return &wf.Status.Members[i]
		}
	}
	return nil
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

// rawEvents returns every events.k8s.io/v1 Event with the given reason,
// regarding the named object of the given kind, unaggregated — for asserting
// per-Event fields (Action, Related) that recordedEvents' summary throws
// away by collapsing across the whole reason+regardingName set.
func rawEvents(reason, regardingKind, regardingName string) []eventsv1.Event {
	GinkgoHelper()
	var list eventsv1.EventList
	Expect(k8sClient.List(ctx, &list)).To(Succeed())
	var out []eventsv1.Event
	for i := range list.Items {
		e := &list.Items[i]
		if e.Reason != reason || e.Regarding.Kind != regardingKind || e.Regarding.Name != regardingName {
			continue
		}
		out = append(out, *e)
	}
	return out
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
				return testutil.ToFloat64(instruments.AdmissionsTotal.WithLabelValues("wf-initial-pin", "initial"))
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

	Describe("initial pin fan-out", func() {
		It("records one InitialPin event per source when several pin in the same pass", func() {
			const ns, scenario = "initial-pin-fanout", "initial-pin-fanout"
			makeNamespace(ns)

			sources := []string{"fanout-a", "fanout-b", "fanout-c"}
			for _, name := range sources {
				url := repoURLFor(ns, name)
				lister.advertise(url, mainRef, shaA)
				repo := makeGitRepo(ns, name, url, mainRef, true)
				setArtifact(repo, revisionOf(shaA))
				makeKustomization(ns, name,
					types.NamespacedName{Namespace: ns, Name: name}, nil,
					map[string]string{scenarioLabel: scenario})
			}

			makeWavefront("wf-initial-fanout", scenario, wavefrontv1alpha1.ModeEnforce)

			By("pinning every source")
			for _, name := range sources {
				Eventually(func() string { return pinOf(ns, name) }).Should(Equal(shaA))
			}

			By("recording one InitialPin event per source on the Wavefront, each naming its source as related")
			var wfEvents []eventsv1.Event
			Eventually(func() []eventsv1.Event {
				wfEvents = rawEvents("InitialPin", "Wavefront", "wf-initial-fanout")
				return wfEvents
			}).Should(HaveLen(len(sources)), "one InitialPin per source, not one collapsed across all of them")

			seen := map[string]bool{}
			for _, e := range wfEvents {
				Expect(e.Action).To(Equal("Pin"))
				Expect(e.Related).NotTo(BeNil())
				Expect(e.Related.Kind).To(Equal("GitRepository"))
				Expect(sources).To(ContainElement(e.Related.Name))
				seen[e.Related.Name] = true
			}
			Expect(seen).To(HaveLen(len(sources)), "every source must be named exactly once, not one source's message overwriting another's")

			By("mirroring the pin on each GitRepository, related back to the Wavefront")
			for _, name := range sources {
				var repoEvents []eventsv1.Event
				Eventually(func() []eventsv1.Event {
					repoEvents = rawEvents("InitialPin", "GitRepository", name)
					return repoEvents
				}).Should(HaveLen(1))
				Expect(repoEvents[0].Action).To(Equal("Pin"))
				Expect(repoEvents[0].Related).NotTo(BeNil())
				Expect(repoEvents[0].Related.Kind).To(Equal("Wavefront"))
				Expect(repoEvents[0].Related.Name).To(Equal("wf-initial-fanout"))
			}
		})
	})

	Describe("Shadow to Enforce fan-out", func() {
		It("keeps per-source events distinct across a Shadow -> Enforce transition", func() {
			// Reproduces the reported bug directly: a Wavefront with several
			// managed sources flipped from Shadow to Enforce fired one
			// ShadowAdmission (and later one InitialPin) log line per source,
			// but only a single Event landed on the Wavefront — because
			// every per-source event shared the same regarding object and
			// reason with related left nil, so the events.k8s.io/v1
			// recorder's dedup key collapsed them onto one Event and
			// discarded every source's message but the first's.
			const ns, scenario = "shadow-enforce-fanout", "shadow-enforce-fanout"
			makeNamespace(ns)

			sources := []string{"se-a", "se-b"}
			for _, name := range sources {
				url := repoURLFor(ns, name)
				lister.advertise(url, mainRef, shaA)
				repo := makeGitRepo(ns, name, url, mainRef, true)
				setArtifact(repo, revisionOf(shaA))
				makeKustomization(ns, name,
					types.NamespacedName{Namespace: ns, Name: name}, nil,
					map[string]string{scenarioLabel: scenario})
			}

			makeWavefront("wf-shadow-enforce-fanout", scenario, wavefrontv1alpha1.ModeShadow)

			By("recording one ShadowAdmission event per source")
			var shadowEvents []eventsv1.Event
			Eventually(func() []eventsv1.Event {
				shadowEvents = rawEvents("ShadowAdmission", "Wavefront", "wf-shadow-enforce-fanout")
				return shadowEvents
			}).Should(HaveLen(len(sources)))

			seenShadow := map[string]bool{}
			for _, e := range shadowEvents {
				Expect(e.Action).To(Equal("ShadowPin"))
				Expect(e.Related).NotTo(BeNil())
				Expect(e.Related.Kind).To(Equal("GitRepository"))
				seenShadow[e.Related.Name] = true
			}
			Expect(seenShadow).To(HaveLen(len(sources)))

			By("flipping to Enforce")
			Eventually(func() error {
				wf := getWavefront("wf-shadow-enforce-fanout")
				wf.Spec.Mode = wavefrontv1alpha1.ModeEnforce
				return k8sClient.Update(ctx, wf)
			}).Should(Succeed())

			By("recording one InitialPin event per source")
			var pinEvents []eventsv1.Event
			Eventually(func() []eventsv1.Event {
				pinEvents = rawEvents("InitialPin", "Wavefront", "wf-shadow-enforce-fanout")
				return pinEvents
			}).Should(HaveLen(len(sources)), "one InitialPin per source across the Shadow -> Enforce transition, not one collapsed across all of them")

			seenPin := map[string]bool{}
			for _, e := range pinEvents {
				Expect(e.Action).To(Equal("Pin"))
				Expect(e.Related).NotTo(BeNil())
				Expect(e.Related.Kind).To(Equal("GitRepository"))
				seenPin[e.Related.Name] = true
			}
			Expect(seenPin).To(HaveLen(len(sources)))

			for _, name := range sources {
				Eventually(func() string { return pinOf(ns, name) }).Should(Equal(shaA))
			}
		})
	})

	Describe("rolling admission with a gate", func() {
		It("holds a pending flotilla behind an unhealthy gate, then admits it", func() {
			const ns, scenario = "rolling-gate", "rolling-gate"
			makeNamespace(ns)

			gateURL := repoURLFor(ns, gateNode)
			makeGitRepo(ns, gateNode, gateURL, mainRef, false)
			// The gate is deliberately unlabelled: it must be discovered by the
			// dependsOn transitive closure, not by the selector.
			gate := makeKustomization(ns, gateNode,
				types.NamespacedName{Namespace: ns, Name: gateNode}, nil, nil)

			url := repoURLFor(ns, flotilla)
			lister.advertise(url, mainRef, shaA)
			repo := makeGitRepo(ns, flotilla, url, mainRef, true)
			setArtifact(repo, revisionOf(shaA))
			flotillaKS := makeKustomization(ns, flotilla,
				types.NamespacedName{Namespace: ns, Name: flotilla}, []string{gateNode},
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
			Expect(wf.Status.Blocked[0].Ancestor.Name).To(Equal(gateNode))
			Expect(wf.Status.Phase).To(Equal(wavefrontv1alpha1.PhaseBlocked))

			By("explaining the whole blocked subtree in status.members")
			// status.members is written in the same patch as status.blocked, so
			// the fleet picture above is the one being read here.
			Expect(wf.Status.Members).To(HaveLen(2), "the pinned node and the gate it waits on")
			Expect(wf.Status.MembersOmitted).To(BeZero())

			member := memberOf(wf, flotilla)
			Expect(member).NotTo(BeNil())
			Expect(member.Role).To(Equal(string(engine.RolePinned)))
			Expect(member.State).To(Equal(string(engine.StatePending)))
			Expect(member.Source).To(Equal(ns + "/" + flotilla))
			Expect(member.Pin).To(Equal(shaA))
			Expect(member.ObservedSHA).To(Equal(shaB))
			Expect(member.Held).To(BeFalse())
			Expect(member.PendingSince).NotTo(BeNil())
			Expect(member.Blocked).NotTo(BeNil())
			Expect(member.Blocked.Reason).To(Equal(string(engine.ReasonAncestorUnhealthy)))
			Expect(member.Blocked.Ancestor).NotTo(BeNil())
			Expect(member.Blocked.Ancestor.Name).To(Equal(gateNode))
			// The graph edges travel with status, so a reader needs no second
			// pass over the cluster to explain the block.
			Expect(member.DependsOn).To(ConsistOf(wavefrontv1alpha1.NodeReference{
				Kind: kindKustomization, Namespace: ns, Name: gateNode,
			}))

			gateMember := memberOf(wf, gateNode)
			Expect(gateMember).NotTo(BeNil())
			Expect(gateMember.Role).To(Equal(string(engine.RoleGate)))
			Expect(gateMember.State).To(Equal(string(engine.StateUnhealthy)))
			Expect(gateMember.Ready).To(BeFalse())
			Expect(gateMember.Source).To(BeEmpty(), "a gate carries no managed source")
			Expect(gateMember.Blocked).To(BeNil())

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

			// The engine re-derives the identical would-be admission every
			// reconcile, so the ShadowAdmission event and
			// wavefront_admissions_total{result="shadow"} are edge-triggered
			// against status.Shadow rather than fired once per pass (40+/hour
			// on a pending change).
			// Events are at-least-once, not exactly-once: the next reconcile
			// can read the informer cache before it has absorbed this pass's
			// status patch (client.MergeFrom carries no optimistic lock) and
			// re-fire the edge-trigger once more.
			// Assert >=1 then bound at <=2 so a regression that re-fires the
			// event on every pass (10+ occurrences in this window) still
			// fails.
			By("announcing the would-be admission at least once across repeated reconciles")
			Eventually(func() int32 {
				_, occurrences := recordedEvents("ShadowAdmission", "wf-shadow")
				return occurrences
			}).Should(BeNumerically(">=", 1))
			Consistently(func() int32 {
				_, occurrences := recordedEvents("ShadowAdmission", "wf-shadow")
				return occurrences
			}).Should(BeNumerically("<=", 2))

			By("incrementing wavefront_admissions_total{result=\"shadow\"} once or twice")
			Eventually(func() float64 {
				return testutil.ToFloat64(instruments.AdmissionsTotal.WithLabelValues("wf-shadow", resultShadow))
			}).Should(BeNumerically(">=", 1))
			Consistently(func() float64 {
				return testutil.ToFloat64(instruments.AdmissionsTotal.WithLabelValues("wf-shadow", resultShadow))
			}).Should(BeNumerically("<=", 2))
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

			By("reporting the hold on the node's own status.members entry")
			member := memberOf(wf, flotilla)
			Expect(member).NotTo(BeNil())
			Expect(member.Role).To(Equal(string(engine.RolePinned)))
			Expect(member.State).To(Equal(string(engine.StatePending)))
			Expect(member.Held).To(BeTrue())
			Expect(member.Pin).To(Equal(shaHand))
			Expect(member.ObservedSHA).To(Equal(shaA))
			Expect(member.Source).To(Equal(ns + "/" + flotilla))
			Expect(member.DependsOn).To(BeEmpty())
			Expect(member.Blocked).NotTo(BeNil())
			Expect(member.Blocked.Reason).To(Equal(string(engine.ReasonSelfHeld)))
			Expect(member.Blocked.Ancestor).To(BeNil(), "SelfHeld attributes no ancestor")

			By("never advancing past the hand-pin")
			lister.advertise(url, releaseRef, shaB)
			Consistently(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaHand))

			// Events are at-least-once, not exactly-once: a stale
			// informer-cache read of this controller's own status patch
			// (client.MergeFrom, no optimistic lock) can re-fire the
			// edge-trigger once more on the next reconcile.
			By("emitting HoldDetected at least once")
			Eventually(func() int32 {
				_, occurrences := recordedEvents("HoldDetected", "wf-hand-pin")
				return occurrences
			}).Should(BeNumerically(">=", 1))
			Consistently(func() int32 {
				_, occurrences := recordedEvents("HoldDetected", "wf-hand-pin")
				return occurrences
			}).Should(BeNumerically("<=", 2))

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
			Consistently(func() int32 {
				_, occurrences := recordedEvents("HoldReleased", "wf-hand-pin")
				return occurrences
			}).Should(BeNumerically("<=", 2))
			Eventually(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaB))
			Eventually(func() []wavefrontv1alpha1.HeldNode {
				return getWavefront("wf-hand-pin").Status.Held
			}).Should(BeEmpty())
		})
	})

	Describe("suspended source holds", func() {
		It("treats a suspended GitRepository as a hold, distinct from a hand-pin", func() {
			const ns, scenario = "suspend-hold", "suspend-hold"
			makeNamespace(ns)

			url := repoURLFor(ns, flotilla)
			lister.advertise(url, mainRef, shaA)
			repo := makeGitRepo(ns, flotilla, url, mainRef, true)
			setArtifact(repo, revisionOf(shaA))
			makeKustomization(ns, flotilla,
				types.NamespacedName{Namespace: ns, Name: flotilla}, nil,
				map[string]string{scenarioLabel: scenario})

			makeWavefront("wf-suspend-hold", scenario, wavefrontv1alpha1.ModeEnforce)
			Eventually(func() string { return pinOf(ns, flotilla) }).Should(Equal(shaA))

			By("suspending the GitRepository")
			Eventually(func() error {
				repo := getRepo(ns, flotilla)
				repo.Spec.Suspend = true
				return k8sClient.Update(ctx, repo)
			}).Should(Succeed())

			By("naming the source as a Suspend hold in status.held, with no manager")
			Eventually(func() []wavefrontv1alpha1.HeldNode {
				return getWavefront("wf-suspend-hold").Status.Held
			}).Should(HaveLen(1))
			wf := getWavefront("wf-suspend-hold")
			Expect(wf.Status.Held[0].Reason).To(Equal(wavefrontv1alpha1.HoldReasonSuspend))
			Expect(wf.Status.Held[0].Manager).To(BeEmpty())
			Expect(wf.Status.Held[0].Source).To(Equal(ns + "/" + flotilla))
			Expect(wf.Status.Held[0].Node.Name).To(Equal(flotilla))
			Expect(wf.Status.Nodes.Held).To(Equal(1))

			// Events are at-least-once, not exactly-once: a stale
			// informer-cache read of this controller's own status patch
			// (client.MergeFrom, no optimistic lock) can re-fire the
			// edge-trigger once more on the next reconcile.
			By("emitting HoldDetected at least once")
			Eventually(func() int32 {
				_, occurrences := recordedEvents("HoldDetected", "wf-suspend-hold")
				return occurrences
			}).Should(BeNumerically(">=", 1))
			Consistently(func() int32 {
				_, occurrences := recordedEvents("HoldDetected", "wf-suspend-hold")
				return occurrences
			}).Should(BeNumerically("<=", 2))

			By("unsuspending the GitRepository")
			Eventually(func() error {
				repo := getRepo(ns, flotilla)
				repo.Spec.Suspend = false
				return k8sClient.Update(ctx, repo)
			}).Should(Succeed())

			By("releasing the hold at least once")
			Eventually(func() int32 {
				_, occurrences := recordedEvents("HoldReleased", "wf-suspend-hold")
				return occurrences
			}).Should(BeNumerically(">=", 1))
			Consistently(func() int32 {
				_, occurrences := recordedEvents("HoldReleased", "wf-suspend-hold")
				return occurrences
			}).Should(BeNumerically("<=", 2))
			Eventually(func() []wavefrontv1alpha1.HeldNode {
				return getWavefront("wf-suspend-hold").Status.Held
			}).Should(BeEmpty())
		})
	})

	Describe("evaluation timestamp", func() {
		It("advances status.lastEvaluated at most once per poll interval", func() {
			const ns, scenario = "last-evaluated", "last-evaluated"
			// A poll interval far longer than the spec, so every reconcile
			// below falls inside it.
			const interval = 10 * time.Minute
			makeNamespace(ns)

			url := repoURLFor(ns, flotilla)
			lister.advertise(url, mainRef, shaA)
			repo := makeGitRepo(ns, flotilla, url, mainRef, true)
			setArtifact(repo, revisionOf(shaA))
			makeKustomization(ns, flotilla,
				types.NamespacedName{Namespace: ns, Name: flotilla}, nil,
				map[string]string{scenarioLabel: scenario})

			// A gate, so its readiness can be flipped to witness a fresh pass
			// without disturbing the pinned node.
			makeGitRepo(ns, gateNode, repoURLFor(ns, gateNode), mainRef, false)
			gate := makeKustomization(ns, gateNode,
				types.NamespacedName{Namespace: ns, Name: gateNode}, nil,
				map[string]string{scenarioLabel: scenario})

			makeWavefrontPolling("wf-last-evaluated", scenario, wavefrontv1alpha1.ModeEnforce, interval)
			// A co-resident Wavefront selecting nothing, at the suite's fast
			// cadence: the Poller is shared and takes the tightest interval, and
			// its post-sweep notify enqueues *every* Wavefront — so the slow one
			// above reconciles many times a second throughout the window below.
			makeWavefront("wf-last-evaluated-ticker", "last-evaluated-idle", wavefrontv1alpha1.ModeShadow)

			By("stamping it on the first resolved pass")
			Eventually(func() *metav1.Time {
				return getWavefront("wf-last-evaluated").Status.LastEvaluated
			}).ShouldNot(BeNil())
			stamped := *getWavefront("wf-last-evaluated").Status.LastEvaluated

			By("holding it steady across many reconciles inside the interval")
			Consistently(func() metav1.Time {
				return *getWavefront("wf-last-evaluated").Status.LastEvaluated
			}).WithTimeout(3 * time.Second).Should(Equal(stamped))

			By("proving those passes were re-deriving status all along")
			setKustomizationReady(gate, "", true)
			Eventually(func() bool {
				member := memberOf(getWavefront("wf-last-evaluated"), gateNode)
				return member != nil && member.Ready
			}).Should(BeTrue())
			Expect(*getWavefront("wf-last-evaluated").Status.LastEvaluated).To(Equal(stamped))
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
