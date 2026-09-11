//go:build e2e
// +build e2e

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

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxgit "github.com/fluxcd/pkg/git"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
	"github.com/isometry/wavefront-controller/test/utils"
)

const (
	fleetNamespace = utils.GitServerNamespace
	fleetName      = "fleet"

	infraNode = "infra"
	teamNode  = "team-a"
	gateNode  = "wave-gate"
	gateRepo  = "gate"

	trackedRef  = "refs/heads/" + utils.GitBranch
	handManager = "e2e-hand-pin"

	// Events for the cluster-scoped Wavefront land in the default namespace:
	// client-go's events.k8s.io recorder substitutes it for an empty
	// regarding.namespace.
	fleetEventNamespace = "default"

	kustomizePath  = "kustomize/kustomization.yaml"
	configMapPath  = "kustomize/configmap.yaml"
	fixturesFile   = "test/e2e/fixtures.yaml"
	kustomizeIndex = `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - configmap.yaml
`
	// brokenManifest is not parseable YAML, so the real kustomize-controller
	// fails the build rather than applying anything.
	brokenManifest = `apiVersion: v1
kind: ConfigMap
metadata:
  name: infra-config
data: [ unbalanced: "bracket"
`

	// A ConfigMap-only flotilla converges in about eighty milliseconds
	// ("Reconciliation finished in 80.894709ms"), so a whole two-node rollout
	// lands inside a single second — too fast for any external observer to
	// witness the ordering it is supposed to prove. Scenarios that must *see*
	// a node converge therefore push a payload with deliberately delayed
	// readiness: minReadySeconds keeps the Deployment — and so the waiting
	// Kustomization — Progressing for a guaranteed ten seconds after the pod
	// is up. That is not a stall invented for the test so much as what an
	// infrastructure flotilla actually looks like; the ConfigMap was always
	// the unrealistic part.
	workloadPath     = "kustomize/workload.yaml"
	slowKustomizeIdx = `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - configmap.yaml
  - workload.yaml
`
)

// Timeouts. Every wait crosses at least one poll sweep (15s), one source
// reconcile and one kustomize apply, so they are deliberately generous.
const (
	waitShort    = 3 * time.Minute
	waitConverge = 5 * time.Minute
	pollFast     = 500 * time.Millisecond

	// holdWindow must comfortably exceed the fleet's poll interval so that a
	// "did not advance" assertion has seen the controller decline at least
	// twice.
	holdWindow = 45 * time.Second

	// minSequencingGap is the smallest infra→team-a admission gap consistent
	// with team-a having actually waited. The delayed-readiness payload cannot
	// go Ready for ten seconds, so a real wait lands around eleven; the
	// same-pass advance this discriminates against lands on zero.
	minSequencingGap = 5 * time.Second
)

var (
	ctx = context.Background()

	repos map[string]*utils.Repo
	// revision makes every push change file content, and therefore produce a
	// new commit SHA.
	revision int

	// kubeContext is the kubeconfig context the suite's own client resolved to
	// — the kind cluster `make test-e2e` prepared. The wfctl scenario passes it
	// explicitly rather than letting the binary inherit whatever the ambient
	// default happens to be.
	kubeContext string
)

var _ = Describe("Wavefront fleet", Ordered, func() {
	BeforeAll(func() {
		By("seeding the fixture repositories on the in-cluster git server")
		workdir, err := os.MkdirTemp("", "wavefront-e2e-repos-")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = os.RemoveAll(workdir) })

		repos = map[string]*utils.Repo{}
		for _, name := range []string{infraNode, teamNode, gateRepo} {
			repo, err := utils.NewRepo(workdir, name)
			Expect(err).NotTo(HaveOccurred(), "failed to initialise %s", name)
			repos[name] = repo
			pushRevision(repo)
		}

		By("applying the fixture fleet")
		cmd := exec.Command("kubectl", "apply", "-f", fixturesFile)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "failed to apply the fixture fleet")
	})

	AfterAll(func() {
		By("removing the fixture fleet")
		cmd := exec.Command("kubectl", "delete", "-f", fixturesFile,
			"--ignore-not-found", "--timeout=3m")
		if _, err := utils.Run(cmd); err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "fixture teardown: %v\n", err)
		}
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			dumpFleet()
		}
	})

	It("bootstraps: initial pins at the artifact revision, fleet quiescent", func() {
		By("waiting for both flotillas to be pinned at their current revisions")
		Eventually(func(g Gomega) {
			g.Expect(pinOf(g, infraNode)).To(Equal(repos[infraNode].Head()))
			g.Expect(pinOf(g, teamNode)).To(Equal(repos[teamNode].Head()))
		}, waitConverge, pollFast).Should(Succeed())

		By("checking the initial pins carry initial-pin provenance")
		for _, node := range []string{infraNode, teamNode} {
			repo := getRepo(Default, node)
			Expect(repo.Annotations).To(HaveKeyWithValue(pin.AnnotPreviousPin, ""))
			Expect(repo.Annotations).To(HaveKeyWithValue(pin.AnnotObservedRef, trackedRef))
			Expect(repo.Annotations).To(HaveKey(pin.AnnotAdmittedAt))
			expectEvent(fleetNamespace, sourcev1.GitRepositoryKind, node, "InitialPin", "")
		}

		By("waiting for the whole fleet to converge and quiesce")
		expectQuiescent()

		By("checking the fleet counts")
		fleet := getFleet(Default)
		Expect(fleet.Status.Nodes.Observed).To(Equal(3))
		Expect(fleet.Status.Nodes.Pinned).To(Equal(2))
		Expect(fleet.Status.Nodes.Gates).To(Equal(1))
	})

	It("sequences co-arriving pushes: team-a waits for infra and the gate", func() {
		teamBefore := pinOf(Default, teamNode)
		infraBefore := pinOf(Default, infraNode)

		By("pushing to infra and team-a back to back")
		// infra's payload converges slowly on purpose: with a ConfigMap the
		// whole two-node rollout completes inside one second, and no external
		// observer can then witness that team-a actually waited.
		shaI := pushSlowRevision(repos[infraNode])
		shaT := pushRevision(repos[teamNode])
		Expect(shaI).NotTo(Equal(infraBefore))
		Expect(shaT).NotTo(Equal(teamBefore))

		// The ordering has to be proved from evidence that cannot slip between
		// two polls. Sampling the settled *state* — as earlier revisions of
		// this spec did, latched or not — cannot work: the controller watches
		// Kustomizations, so it admits team-a within milliseconds of infra
		// going Ready, and no external poll is reliably inside that window.
		//
		// lastAppliedRevision is the durable substitute. Flux sets it only
		// after an apply that also cleared the wait:true health gate, and
		// never rolls it back, so it is monotone: "team-a moved, therefore
		// infra must already have applied shaI" is safe to evaluate at any
		// sample, and an out-of-order admission cannot hide in a status dip.
		// That monotonicity holds only up to shaI: this scenario's spec never
		// pushes infra past shaI, so LastAppliedRevision cannot advance again
		// mid-test and invalidate an earlier positive read.
		infraApplied := func(g Gomega) bool {
			ks := getKustomization(g, infraNode)
			return fluxgit.ExtractHashFromRevision(ks.Status.LastAppliedRevision).String() == shaI
		}

		// This guard covers only infra: an unpinned gate node (wave-gate) has
		// no durable revision evidence to sample the same way (no pin, no
		// LastAppliedRevision tied to an admitted SHA), so it cannot be
		// guarded here. Scenario 3 covers gate-mediated blocking instead.
		By("checking team-a does not advance before infra has applied shaI")
		Consistently(func(g Gomega) {
			if pinOf(g, teamNode) != teamBefore {
				g.Expect(infraApplied(g)).To(BeTrue(),
					"team-a advanced before infra applied shaI")
			}
		}, holdWindow, 100*time.Millisecond).Should(Succeed())

		// The Consistently closes long before the chain settles — with the
		// slow payload infra needs a further ~13s — and team-a is admitted
		// milliseconds after it does. So the guard stays live across that whole
		// stretch, in the same block that waits for the outcome: splitting
		// "wait for infra" from "wait for team-a" would leave team-a's actual
		// admission, the only instant the ordering can be violated, inside the
		// seam between them.
		By("waiting for infra to converge and team-a to follow, guarded throughout")
		Eventually(func(g Gomega) {
			if pinOf(g, teamNode) != teamBefore {
				// A hard failure: an out-of-order admission is not something
				// to retry until it looks right.
				Expect(infraApplied(g)).To(BeTrue(),
					"team-a advanced before infra applied shaI")
			}
			g.Expect(pinOf(g, infraNode)).To(Equal(shaI))
			g.Expect(infraApplied(g)).To(BeTrue())
			g.Expect(pinOf(g, teamNode)).To(Equal(shaT))
		}, waitConverge, 100*time.Millisecond).Should(Succeed())

		// A second proof that needs no sampling at all: the admission times the
		// controller stamped into the cluster. infra's payload cannot go Ready
		// for at least minReadySeconds, so a correctly sequenced team-a
		// admission cannot follow infra's closely — whereas the same-pass
		// advance this scenario exists to rule out shows a gap of zero.
		By("checking the recorded admission times show a real wait")
		infraAdmitted := admittedAt(Default, infraNode)
		teamAdmitted := admittedAt(Default, teamNode)
		_, _ = fmt.Fprintf(GinkgoWriter, "infra->team-a admission gap: %s\n",
			teamAdmitted.Sub(infraAdmitted))
		Expect(teamAdmitted).To(BeTemporally(">=", infraAdmitted.Add(minSequencingGap)),
			"team-a was admitted only %s after infra; infra's payload cannot converge that fast",
			teamAdmitted.Sub(infraAdmitted))
		Expect(readyStatus(Default, gateNode)).To(Equal("True"))

		By("checking team-a's provenance records the outgoing pin")
		repo := getRepo(Default, teamNode)
		Expect(repo.Annotations).To(HaveKeyWithValue(pin.AnnotPreviousPin, teamBefore))
		Expect(repo.Annotations).To(HaveKeyWithValue(pin.AnnotObservedRef, trackedRef))

		By("checking PinAdvanced events exist for both flotillas")
		expectEvent(fleetNamespace, sourcev1.GitRepositoryKind, infraNode, "PinAdvanced", shaI)
		expectEvent(fleetNamespace, sourcev1.GitRepositoryKind, teamNode, "PinAdvanced", shaT)

		expectQuiescent()
	})

	It("blocks a subtree behind an unhealthy ancestor and recovers via a fix", func() {
		teamBefore := pinOf(Default, teamNode)

		By("pushing a manifest that the kustomize-controller cannot build")
		shaBroken, err := repos[infraNode].PushFile(configMapPath, brokenManifest, "break infra")
		Expect(err).NotTo(HaveOccurred())

		By("waiting for infra to be admitted and to go Ready=False for real")
		Eventually(func(g Gomega) {
			g.Expect(pinOf(g, infraNode)).To(Equal(shaBroken))
			g.Expect(readyStatus(g, infraNode)).To(Equal("False"))
		}, waitConverge, pollFast).Should(Succeed())

		By("pushing to team-a behind the broken ancestor")
		shaT := pushRevision(repos[teamNode])

		By("checking team-a stays pinned and the fleet reports Blocked")
		Eventually(func(g Gomega) {
			fleet := getFleet(g)
			g.Expect(fleet.Status.Phase).To(Equal(wavefrontv1alpha1.PhaseBlocked))
			g.Expect(blockedEntry(fleet, teamNode)).NotTo(BeNil())
		}, waitShort, pollFast).Should(Succeed())

		fleet := getFleet(Default)
		entry := blockedEntry(fleet, teamNode)
		Expect(entry.Reason).To(Equal("AncestorUnhealthy"))
		// Attribution is the *nearest* unsettled ancestor (DESIGN §3.3 rule 7).
		// infra is the origin of the breakage, but Flux propagates
		// DependencyNotReady to wave-gate on its next reconcile, at which
		// point wave-gate becomes the nearer unsettled ancestor. Both are
		// correct attributions of the same stall; which one is observed is a
		// matter of Flux's reconcile timing.
		Expect(entry.Ancestor).NotTo(BeNil())
		Expect(entry.Ancestor.Name).To(BeElementOf(infraNode, gateNode))
		Expect(readyStatus(Default, infraNode)).To(Equal("False"))

		Consistently(func(g Gomega) {
			g.Expect(pinOf(g, teamNode)).To(Equal(teamBefore))
		}, holdWindow, pollFast).Should(Succeed())

		By("pushing a fix to infra — the normal recovery path")
		shaFix := pushRevision(repos[infraNode])

		By("waiting for infra to re-admit and the subtree to drain")
		Eventually(func(g Gomega) {
			g.Expect(pinOf(g, infraNode)).To(Equal(shaFix))
			g.Expect(pinOf(g, teamNode)).To(Equal(shaT))
		}, waitConverge, pollFast).Should(Succeed())

		expectQuiescent()
	})

	It("coexists with a hand-pin: reports the hold, resumes when it is lifted", func() {
		older := getRepo(Default, teamNode).Annotations[pin.AnnotPreviousPin]
		Expect(older).NotTo(BeEmpty(), "expected a recorded previous pin to hand-pin back to")

		By("hand-pinning team-a's source under a foreign field manager")
		handPin(older)

		By("checking the hold is detected and attributed")
		Eventually(func(g Gomega) {
			fleet := getFleet(g)
			g.Expect(fleet.Status.Held).To(HaveLen(1))
			g.Expect(fleet.Status.Held[0].Manager).To(Equal(handManager))
			g.Expect(fleet.Status.Held[0].Node.Name).To(Equal(teamNode))
		}, waitShort, pollFast).Should(Succeed())
		expectEvent(fleetEventNamespace, "Wavefront", fleetName, "HoldDetected", teamNode)

		By("pushing to team-a and checking the hand-pin is never forced past")
		held := pushRevision(repos[teamNode])
		Consistently(func(g Gomega) {
			g.Expect(pinOf(g, teamNode)).To(Equal(older))
		}, holdWindow, pollFast).Should(Succeed())

		By("releasing the hand-pin")
		releaseHandPin()

		By("checking the hold is released and the pending push admitted")
		expectEvent(fleetEventNamespace, "Wavefront", fleetName, "HoldReleased", teamNode)
		Eventually(func(g Gomega) {
			g.Expect(getFleet(g).Status.Held).To(BeEmpty())
			g.Expect(pinOf(g, teamNode)).To(Equal(held))
		}, waitConverge, pollFast).Should(Succeed())

		expectQuiescent()
	})

	// The CLI is the operator's half of the same contract the scenarios above
	// prove from the cluster side, so it is exercised the same way: against the
	// live fleet, with the controller's own reaction as the assertion. Nothing
	// here re-tests rendering — the golden suite owns that — only that a real
	// `wfctl` run against a real fleet moves the fleet, and reads back what the
	// controller published about it.
	It("drives the live fleet through wfctl: pin, nodes, release, status and explain", func() {
		By("pointing wfctl at the cluster the suite itself reads")
		out, err := utils.Run(exec.Command("kubectl", "config", "current-context"))
		Expect(err).NotTo(HaveOccurred(), "failed to read the current kubeconfig context")
		kubeContext = strings.TrimSpace(out)
		Expect(kubeContext).NotTo(BeEmpty())

		// Two references that happen to spell the same thing in this fixture:
		// the GitRepository the write commands address, and the Kustomization
		// `explain` walks from. Keeping them apart keeps the calls readable.
		teamSource := fleetNamespace + "/" + teamNode
		teamNodeRef := fleetNamespace + "/" + teamNode
		pinned := pinOf(Default, teamNode)
		Expect(pinned).To(Equal(repos[teamNode].Head()))

		// Pinning the value that is already there is deliberate: it isolates
		// the ownership transfer, which is the whole of what makes a hand-pin
		// a hold, from any change of commit.
		By("hand-pinning team-a's source with `wfctl pin`")
		wfctlOK("pin", teamSource, "--sha", pinned, "--yes", "--unverified")

		By("checking the controller detects the hold and attributes it to wfctl")
		Eventually(func(g Gomega) {
			fleet := getFleet(g)
			g.Expect(fleet.Status.Held).To(HaveLen(1))
			g.Expect(fleet.Status.Held[0].Manager).To(Equal(pin.WfctlFieldManager))
			g.Expect(fleet.Status.Held[0].Node.Name).To(Equal(teamNode))
		}, waitShort, pollFast).Should(Succeed())

		// The note has to name the manager. The hand-pin scenario above fired
		// HoldDetected for this very node under a different one, and matching
		// on the node alone would be satisfied by that stale event.
		expectEvent(fleetEventNamespace, "Wavefront", fleetName, "HoldDetected",
			fmt.Sprintf("field manager %q", pin.WfctlFieldManager))

		By("checking `wfctl nodes` reports the hold in the state the controller published")
		Eventually(func(g Gomega) {
			node := nodeViewOf(wfctlNodes(g), teamNode)
			g.Expect(node).NotTo(BeNil(), "team-a is missing from `wfctl nodes`")
			g.Expect(node.Held).To(BeTrue())
			g.Expect(node.Pin).To(Equal(pinned))

			// The status origin must render status, not a second opinion:
			// what wfctl prints has to be what status.members says.
			member := memberOf(getFleet(g), teamNode)
			g.Expect(member).NotTo(BeNil(), "team-a is missing from status.members")
			g.Expect(node.State).To(Equal(member.State))
			g.Expect(node.Held).To(Equal(member.Held))
		}, waitShort, pollFast).Should(Succeed())

		By("handing the pin back with `wfctl release`")
		wfctlOK("release", teamSource, "--yes")

		expectEvent(fleetEventNamespace, "Wavefront", fleetName, "HoldReleased",
			fmt.Sprintf("released by %q", pin.WfctlFieldManager))
		Eventually(func(g Gomega) {
			g.Expect(getFleet(g).Status.Held).To(BeEmpty())
		}, waitShort, pollFast).Should(Succeed())

		// A release is a transfer, not an unpin: the fleet carries on running
		// exactly the commit the hold held it at.
		By("checking the commit is unchanged and the controller is its sole owner")
		Expect(pinOf(Default, teamNode)).To(Equal(pinned))
		owners := pin.Owners(getRepo(Default, teamNode))
		Expect(owners).To(HaveLen(1), "spec.ref.commit still has a foreign owner: %+v", owners)
		Expect(owners[0].Manager).To(Equal(pin.FieldManager))

		expectQuiescent()

		// `status --derive` prints the controller's picture beside a live
		// re-derivation of it. The two agree on everything the derivation can
		// see — and on a quiescent fleet that is the whole graph — but they
		// cannot agree on the *phase*, and it would be wrong to assert that
		// they do: without --poll a derivation has no ref observations at all,
		// and DESIGN §3.3 rule 2 makes an unobserved pinned node conservatively
		// unsettled rather than let it pass for quiescent. --poll is no help
		// from here either: the fixture sources are addressed by the git
		// server's in-cluster Service name, which the host running wfctl
		// cannot resolve. So the assertion is the honest one — same graph,
		// same counts, nothing blocked, nothing held, and the blind
		// derivation declining to claim a quiescence it cannot prove.
		By("checking `wfctl status --derive` re-derives the controller's picture")
		var report wfctlStatusPayload
		Eventually(func(g Gomega) {
			report = wfctlStatusDerive(g)
			g.Expect(report.Wavefront.Status.Phase).To(Equal(wavefrontv1alpha1.PhaseQuiescent))
		}, waitShort, time.Second).Should(Succeed())

		reported := report.Wavefront.Status
		Expect(report.Diagnostics).To(BeEmpty(), "the derivation reported a degraded picture")
		Expect(report.Derived.GraphValid).To(BeTrue())
		Expect(report.Derived.Counts.Observed).To(Equal(reported.Nodes.Observed))
		Expect(report.Derived.Counts.Pinned).To(Equal(reported.Nodes.Pinned))
		Expect(report.Derived.Counts.Gates).To(Equal(reported.Nodes.Gates))
		Expect(report.Derived.Blocked).To(BeEmpty())
		Expect(report.Derived.Held).To(BeEmpty())
		Expect(report.Derived.Counts.Converging).To(Equal(reported.Nodes.Pinned),
			"a blind derivation should hold every pinned node unsettled: %+v", report.Derived)
		Expect(report.Derived.Phase).To(Equal(wavefrontv1alpha1.PhaseAdvancing),
			"derived picture: %+v", report.Derived)

		By("reproducing the blocked subtree so `wfctl explain` has a chain to walk")
		shaT := blockTeamBehindInfra()

		By("checking `wfctl explain` names the reason and the unhealthy ancestor")
		Eventually(func(g Gomega) {
			explained, stderr, code := runWfctl("explain", teamNodeRef)
			g.Expect(code).To(BeZero(), "wfctl explain exited %d: %s", code, stderr)
			g.Expect(explained).To(ContainSubstring(teamNodeRef))
			g.Expect(explained).To(ContainSubstring("AncestorUnhealthy"))
			// Attribution is the nearest unsettled ancestor, so which of the
			// two it names is Flux's reconcile timing (see the blocked-subtree
			// scenario above); naming neither is the failure.
			g.Expect(explained).To(SatisfyAny(
				ContainSubstring(fleetNamespace+"/"+infraNode),
				ContainSubstring(fleetNamespace+"/"+gateNode)))
			g.Expect(explained).To(ContainSubstring("root cause:"))
		}, waitShort, time.Second).Should(Succeed())

		By("handing the fleet back as it was found: infra fixed, the subtree drained")
		shaFix := pushRevision(repos[infraNode])
		Eventually(func(g Gomega) {
			g.Expect(pinOf(g, infraNode)).To(Equal(shaFix))
			g.Expect(pinOf(g, teamNode)).To(Equal(shaT))
		}, waitConverge, pollFast).Should(Succeed())

		expectQuiescent()
	})

	It("resumes statelessly when the controller is restarted mid-rollout", func() {
		infraBefore := pinOf(Default, infraNode)
		teamBefore := pinOf(Default, teamNode)

		By("pushing to both flotillas — infra's payload converges slowly, on purpose")
		shaI := pushSlowRevision(repos[infraNode])
		shaT := pushRevision(repos[teamNode])

		// The premise of this scenario is that the restart interrupts an
		// *incomplete* rollout. Waiting only for infra's pin would not
		// establish that: if the whole chain had already drained, the spec
		// would still pass while proving nothing more than "the controller
		// survives a restart". So the kill is anchored to a witnessed
		// mid-rollout state instead — infra pinned at shaI but not yet
		// settled, which is precisely the window in which the engine may not
		// admit team-a (DESIGN §3.3 rule 4). Polling starts before the pin is
		// written, so the window cannot be missed, and minReadySeconds holds
		// it open for ten seconds — two orders of magnitude more than the
		// ~250ms it takes to observe the state and issue the kill.
		By("waiting for infra to be admitted but not yet settled — the mid-rollout window")
		Eventually(func(g Gomega) {
			g.Expect(pinOf(g, infraNode)).To(Equal(shaI))
			g.Expect(nodeSettled(g, infraNode)).To(BeFalse(),
				"infra settled before the kill — no rollout left to interrupt")
			g.Expect(pinOf(g, teamNode)).To(Equal(teamBefore))
		}, waitConverge, 100*time.Millisecond).Should(Succeed())

		By("killing the controller mid-rollout")
		cmd := exec.Command("kubectl", "delete", "pod",
			"-l", "control-plane=controller-manager", "-n", namespace, "--wait=false")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "failed to delete the controller pod")

		By("confirming the rollout really was incomplete when the controller died")
		Expect(pinOf(Default, teamNode)).To(Equal(teamBefore),
			"restart was not mid-rollout: team-a had already advanced")

		By("waiting for the rollout to complete after the restart")
		Eventually(func(g Gomega) {
			g.Expect(pinOf(g, infraNode)).To(Equal(shaI))
			g.Expect(pinOf(g, teamNode)).To(Equal(shaT))
		}, waitConverge, pollFast).Should(Succeed())

		By("checking the pin chain has no duplicate or out-of-order step")
		Expect(getRepo(Default, infraNode).Annotations).
			To(HaveKeyWithValue(pin.AnnotPreviousPin, infraBefore))
		Expect(getRepo(Default, teamNode).Annotations).
			To(HaveKeyWithValue(pin.AnnotPreviousPin, teamBefore))

		expectQuiescent()
	})

	It("writes nothing in Shadow mode, and admits again in Enforce", func() {
		infraBefore := pinOf(Default, infraNode)

		By("switching the fleet to Shadow")
		setMode(wavefrontv1alpha1.ModeShadow)

		By("pushing to infra")
		shaS := pushRevision(repos[infraNode])

		By("checking no pin is written")
		Consistently(func(g Gomega) {
			g.Expect(pinOf(g, infraNode)).To(Equal(infraBefore))
		}, holdWindow, pollFast).Should(Succeed())

		By("checking the would-be admission was reported")
		expectEvent(fleetEventNamespace, "Wavefront", fleetName, "ShadowAdmission", shaS)

		By("switching back to Enforce and checking the admission lands")
		setMode(wavefrontv1alpha1.ModeEnforce)
		Eventually(func(g Gomega) {
			g.Expect(pinOf(g, infraNode)).To(Equal(shaS))
		}, waitConverge, pollFast).Should(Succeed())

		expectQuiescent()
	})

	It("self-heals after a force-push over the pinned commit", func() {
		pinned := pinOf(Default, infraNode)

		By("rewriting infra's history and force-pushing over the pinned commit")
		revision++
		rewritten, err := repos[infraNode].Rewrite(map[string]string{
			kustomizePath: kustomizeIndex,
			configMapPath: configMapYAML(infraNode, revision),
		}, "rewrite infra history")
		Expect(err).NotTo(HaveOccurred())
		Expect(rewritten).NotTo(Equal(pinned))

		By("expiring the rewritten-away objects on the server")
		pruneServerHistory(infraNode)

		By("checking the next poll re-pins infra at the rewritten SHA")
		Eventually(func(g Gomega) {
			g.Expect(pinOf(g, infraNode)).To(Equal(rewritten))
		}, waitConverge, pollFast).Should(Succeed())

		expectQuiescent()
	})
})

// --- fixture content -------------------------------------------------------

func configMapYAML(node string, rev int) string {
	return fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: %s-config
data:
  revision: "%d"
`, node, rev)
}

// workloadYAML is a Deployment whose minReadySeconds holds the owning
// Kustomization Progressing for ten seconds after its pod is up. The infra
// Kustomization's timeout (fixtures.yaml, 1m) must stay comfortably above
// this value plus pod start time, or wait:true times out before Ready.
func workloadYAML(node string) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s-workload
spec:
  replicas: 1
  minReadySeconds: 10
  selector:
    matchLabels:
      app: %[1]s-workload
  template:
    metadata:
      labels:
        app: %[1]s-workload
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: idle
          image: example.com/wavefront-gitserver:v0.0.1
          imagePullPolicy: IfNotPresent
          command: ["/bin/sh", "-c", "while true; do sleep 3600; done"]
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
`, node)
}

// pushRevision commits a fresh, valid manifest set and pushes it, returning
// the new SHA (the brief's pushCommit).
func pushRevision(repo *utils.Repo) string {
	GinkgoHelper()
	revision++
	sha, err := repo.CommitPush(map[string]string{
		kustomizePath: kustomizeIndex,
		configMapPath: configMapYAML(repo.Name, revision),
	}, fmt.Sprintf("%s revision %d", repo.Name, revision))
	Expect(err).NotTo(HaveOccurred(), "failed to push to %s", repo.Name)
	return sha
}

// pushSlowRevision is pushRevision with a payload whose readiness is delayed,
// giving the node a convergence window wide enough to observe from outside.
func pushSlowRevision(repo *utils.Repo) string {
	GinkgoHelper()
	revision++
	sha, err := repo.CommitPush(map[string]string{
		kustomizePath: slowKustomizeIdx,
		configMapPath: configMapYAML(repo.Name, revision),
		workloadPath:  workloadYAML(repo.Name),
	}, fmt.Sprintf("%s revision %d (delayed readiness)", repo.Name, revision))
	Expect(err).NotTo(HaveOccurred(), "failed to push to %s", repo.Name)
	return sha
}

// --- cluster reads ---------------------------------------------------------

func key(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: fleetNamespace, Name: name}
}

func getRepo(g Gomega, name string) *sourcev1.GitRepository {
	repo := &sourcev1.GitRepository{}
	g.Expect(k8sClient.Get(ctx, key(name), repo)).To(Succeed())
	return repo
}

func getKustomization(g Gomega, name string) *kustomizev1.Kustomization {
	ks := &kustomizev1.Kustomization{}
	g.Expect(k8sClient.Get(ctx, key(name), ks)).To(Succeed())
	return ks
}

func getFleet(g Gomega) *wavefrontv1alpha1.Wavefront {
	fleet := &wavefrontv1alpha1.Wavefront{}
	g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: fleetName}, fleet)).To(Succeed())
	return fleet
}

// pinOf reads a managed source's spec.ref.commit.
func pinOf(g Gomega, name string) string {
	repo := getRepo(g, name)
	if repo.Spec.Reference == nil {
		return ""
	}
	return repo.Spec.Reference.Commit
}

func readyStatus(g Gomega, name string) string {
	ks := getKustomization(g, name)
	for _, condition := range ks.Status.Conditions {
		if condition.Type == "Ready" {
			return string(condition.Status)
		}
	}
	return "Unknown"
}

// nodeSettled is the suite's read of the engine's settledness rule: Ready at
// the node's own generation, having applied exactly what its source is pinned
// to (DESIGN §3.3 rule 1).
func nodeSettled(g Gomega, name string) bool {
	ks := getKustomization(g, name)
	if ks.Status.ObservedGeneration != ks.Generation || readyStatus(g, name) != "True" {
		return false
	}
	if ks.Spec.SourceRef.Name == "" {
		return true
	}
	repo := getRepo(g, ks.Spec.SourceRef.Name)
	if repo.Spec.Reference == nil || repo.Spec.Reference.Commit == "" {
		// A gate's source is never pinned; readiness is all it contributes.
		return true
	}
	applied := fluxgit.ExtractHashFromRevision(ks.Status.LastAppliedRevision).String()
	return applied == repo.Spec.Reference.Commit
}

// admittedAt reads the admission time the controller stamped on a source
// (DESIGN §4.2). RFC3339, so second resolution — fine against the ten-second
// separations the sequencing assertions rely on.
func admittedAt(g Gomega, node string) time.Time {
	repo := getRepo(g, node)
	at, err := time.Parse(time.RFC3339, repo.Annotations[pin.AnnotAdmittedAt])
	g.Expect(err).NotTo(HaveOccurred(), "unparseable admitted-at on %s", node)
	return at
}

func blockedEntry(fleet *wavefrontv1alpha1.Wavefront, node string) *wavefrontv1alpha1.BlockedNode {
	for i := range fleet.Status.Blocked {
		if fleet.Status.Blocked[i].Node.Name == node {
			return &fleet.Status.Blocked[i]
		}
	}
	return nil
}

// expectQuiescent waits for the whole fixture fleet to settle: the invariant
// every scenario hands to the next.
func expectQuiescent() {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		for _, node := range []string{infraNode, gateNode, teamNode} {
			g.Expect(nodeSettled(g, node)).To(BeTrue(), "%s is not settled", node)
		}
		g.Expect(pinOf(g, infraNode)).To(Equal(repos[infraNode].Head()))
		g.Expect(pinOf(g, teamNode)).To(Equal(repos[teamNode].Head()))
		g.Expect(getFleet(g).Status.Phase).To(Equal(wavefrontv1alpha1.PhaseQuiescent))
	}, waitConverge, pollFast).Should(Succeed())
}

// --- events ----------------------------------------------------------------

// expectEvent waits for an events.k8s.io/v1 Event with the given reason
// regarding the named object, optionally containing note.
func expectEvent(ns, kind, name, reason, note string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		var list eventsv1.EventList
		g.Expect(k8sClient.List(ctx, &list, client.InNamespace(ns))).To(Succeed())

		for i := range list.Items {
			e := &list.Items[i]
			if e.Reason != reason || e.Regarding.Kind != kind || e.Regarding.Name != name {
				continue
			}
			if note == "" || strings.Contains(e.Note, note) {
				return
			}
		}
		g.Expect(fmt.Errorf("no %s event for %s/%s containing %q", reason, kind, name, note)).
			NotTo(HaveOccurred())
	}, waitShort, time.Second).Should(Succeed())
}

// --- cluster writes --------------------------------------------------------

// handPin writes spec.ref.commit under a foreign field manager: the SSA
// co-ownership a human `kubectl patch` creates (DESIGN §3.5.3).
func handPin(sha string) {
	GinkgoHelper()
	cmd := exec.Command("kubectl", "patch", "gitrepository", teamNode,
		"-n", fleetNamespace, "--type=merge",
		"--field-manager="+handManager,
		"-p", fmt.Sprintf(`{"spec":{"ref":{"commit":%q}}}`, sha))
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "failed to hand-pin %s", teamNode)
}

// releaseHandPin removes the field, and with it the foreign manager's claim.
func releaseHandPin() {
	GinkgoHelper()
	cmd := exec.Command("kubectl", "patch", "gitrepository", teamNode,
		"-n", fleetNamespace, "--type=json",
		"--field-manager="+handManager,
		"-p", `[{"op":"remove","path":"/spec/ref/commit"}]`)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "failed to release the hand-pin on %s", teamNode)
}

func setMode(mode wavefrontv1alpha1.Mode) {
	GinkgoHelper()
	cmd := exec.Command("kubectl", "patch", "wavefront", fleetName, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"mode":%q}}`, mode))
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "failed to set mode %s", mode)
}

// pruneServerHistory expires the rewritten-away objects so the pinned commit
// is genuinely unreachable — the exposure DESIGN §10 describes. Best effort:
// the scenario's assertion is self-healing, not the fetch failure itself.
func pruneServerHistory(repo string) {
	script := fmt.Sprintf(
		"cd /srv/git/%s.git && git reflog expire --expire=now --all && git gc --prune=now --quiet",
		repo)
	cmd := exec.Command("kubectl", "exec", "-n", utils.GitServerNamespace,
		"deploy/"+utils.GitServerService, "--", "sh", "-c", script)
	if out, err := utils.Run(cmd); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "history prune (best effort) failed: %v\n%s", err, out)
	}
}

// --- wfctl -----------------------------------------------------------------

// wfctlBin is the binary the `test-e2e` target builds before the suite starts
// (Makefile, build-wfctl). Running the built artefact rather than the package
// is the point: the scenario is about the command an operator actually types.
func wfctlBin() string {
	GinkgoHelper()
	dir, err := utils.GetProjectDir()
	Expect(err).NotTo(HaveOccurred())
	return filepath.Join(dir, "bin", "wfctl")
}

// wfctlFlags are the connection flags every invocation carries: the same
// cluster the assertions read, and the fixture Wavefront named outright so no
// command has to auto-select one.
func wfctlFlags() []string {
	flags := []string{"--wavefront", fleetName, "--no-color"}
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		flags = append(flags, "--kubeconfig", kubeconfig)
	}
	if kubeContext != "" {
		flags = append(flags, "--context", kubeContext)
	}
	return flags
}

// runWfctl runs wfctl and returns its streams separately with the exit status.
//
// The status is returned rather than failed on, because a non-zero one is part
// of the contract: `status` exits 2 on a Blocked fleet, which is precisely the
// state `explain` is read in. stdout is kept clean of stderr so that `-o json`
// stays machine-readable even when the command also warns.
func runWfctl(args ...string) (stdout, stderr string, code int) {
	GinkgoHelper()
	full := append(wfctlFlags(), args...)
	cmd := exec.Command(wfctlBin(), full...)

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &outBuf, &errBuf
	_, _ = fmt.Fprintf(GinkgoWriter, "running: wfctl %s\n", strings.Join(full, " "))

	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		code = exit.ExitCode()
	default:
		// Not a command failure: the binary is missing or unrunnable.
		Expect(err).NotTo(HaveOccurred(), "failed to execute %s", wfctlBin())
	}

	if errBuf.Len() > 0 {
		_, _ = fmt.Fprintf(GinkgoWriter, "wfctl stderr:\n%s", errBuf.String())
	}
	return outBuf.String(), errBuf.String(), code
}

// wfctlOK runs a write command and requires a clean exit.
func wfctlOK(args ...string) string {
	GinkgoHelper()
	out, stderr, code := runWfctl(args...)
	Expect(code).To(BeZero(), "wfctl %s exited %d:\n%s%s",
		strings.Join(args, " "), code, out, stderr)
	return out
}

// wfctlNodes decodes `wfctl nodes -o json`: the snapshot's node views, read
// from the status the controller published rather than re-derived.
func wfctlNodes(g Gomega) []snapshot.NodeView {
	out, stderr, code := runWfctl("nodes", "-o", "json")
	g.Expect(code).To(BeZero(), "wfctl nodes exited %d: %s", code, stderr)

	var nodes []snapshot.NodeView
	g.Expect(json.Unmarshal([]byte(out), &nodes)).To(Succeed(), "unparseable `wfctl nodes` output: %s", out)
	return nodes
}

// wfctlStatusPayload is the document `wfctl status -o json` prints. cli's own
// type is unexported, and restating the shape here is the honest test: what is
// asserted is the JSON contract a script sees, not an internal struct.
type wfctlStatusPayload struct {
	Wavefront   snapshot.WavefrontView `json:"wavefront"`
	Derived     snapshot.DerivedStatus `json:"derived"`
	Diagnostics []string               `json:"diagnostics,omitempty"`
}

// wfctlStatusDerive decodes `wfctl status --derive -o json`: what the
// controller reported beside what a live derivation just proved.
func wfctlStatusDerive(g Gomega) wfctlStatusPayload {
	out, stderr, code := runWfctl("status", "--derive", "-o", "json")
	// 2 is "the fleet is Blocked", which is a verdict, not a failure.
	g.Expect(code).To(BeElementOf(0, 2), "wfctl status exited %d: %s", code, stderr)

	var payload wfctlStatusPayload
	g.Expect(json.Unmarshal([]byte(out), &payload)).To(Succeed(), "unparseable `wfctl status` output: %s", out)
	return payload
}

// nodeViewOf finds one fixture node in a decoded `wfctl nodes` listing.
func nodeViewOf(nodes []snapshot.NodeView, name string) *snapshot.NodeView {
	for i := range nodes {
		if nodes[i].Ref.Namespace == fleetNamespace && nodes[i].Ref.Name == name {
			return &nodes[i]
		}
	}
	return nil
}

// memberOf reads one node's published state out of status.members — the record
// the status origin renders, and so the thing `wfctl nodes` has to agree with.
func memberOf(fleet *wavefrontv1alpha1.Wavefront, name string) *wavefrontv1alpha1.Member {
	for i := range fleet.Status.Members {
		if fleet.Status.Members[i].Node.Name == name {
			return &fleet.Status.Members[i]
		}
	}
	return nil
}

// blockTeamBehindInfra reproduces the blocked subtree: infra unhealthy, team-a
// with an advance it cannot have. It returns the SHA team-a is waiting on, so
// the caller can wait for the subtree to drain once infra is fixed.
func blockTeamBehindInfra() string {
	GinkgoHelper()
	shaBroken, err := repos[infraNode].PushFile(configMapPath, brokenManifest, "break infra for explain")
	Expect(err).NotTo(HaveOccurred())

	Eventually(func(g Gomega) {
		g.Expect(pinOf(g, infraNode)).To(Equal(shaBroken))
		g.Expect(readyStatus(g, infraNode)).To(Equal("False"))
	}, waitConverge, pollFast).Should(Succeed())

	pending := pushRevision(repos[teamNode])
	Eventually(func(g Gomega) {
		fleet := getFleet(g)
		g.Expect(fleet.Status.Phase).To(Equal(wavefrontv1alpha1.PhaseBlocked))
		entry := blockedEntry(fleet, teamNode)
		g.Expect(entry).NotTo(BeNil())
		g.Expect(entry.Reason).To(Equal("AncestorUnhealthy"))
	}, waitShort, pollFast).Should(Succeed())

	return pending
}

// --- diagnostics -----------------------------------------------------------

func dumpFleet() {
	const get = "get"
	for _, args := range [][]string{
		{get, "gitrepositories,kustomizations", "-n", fleetNamespace, "-o", "wide"},
		{get, "wavefront", fleetName, "-o", "yaml"},
		{get, "events", "-n", fleetNamespace},
		{get, "events", "-n", fleetEventNamespace},
		{"logs", "-l", "control-plane=controller-manager", "-n", namespace, "--tail=200"},
	} {
		out, err := utils.Run(exec.Command("kubectl", args...))
		_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
	}
}
