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
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/test/utils"
)

var (
	// managerImage is the manager image built and loaded for testing. The
	// Makefile builds and `kind load`s it before the suite starts; keep the
	// two in step through E2E_IMG.
	managerImage = "example.com/wavefront-controller:v0.0.1"
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false

	// inst is the installation path under test — kustomize overlays or the
	// Helm chart, per E2E_INSTALL. It also supplies the resource names the
	// specs assert on, which differ between the two.
	inst installer

	// k8sClient reads (and occasionally patches) the fixture fleet. kubectl
	// jsonpath would do, but typed reads of Kustomization, GitRepository and
	// Wavefront keep the scenario assertions about admission rather than about
	// string wrangling.
	k8sClient client.Client

	// gitForward carries pushes from the host into the in-cluster git server.
	gitForward *utils.PortForward
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To enable kubectl kuberc (use custom kubectl configurations), set: KUBECTL_KUBERC=true
// By default, kuberc is disabled to ensure consistent test behavior across different environments.
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
//
// The suite expects `make test-e2e` to have prepared the cluster: kind up,
// both images loaded, Flux installed, git server running. Running `go test
// -tags=e2e` by hand against an unprepared cluster fails fast in BeforeSuite.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting wavefront-controller e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	if img := os.Getenv("E2E_IMG"); img != "" {
		managerImage = img
	}

	By("selecting the installation path from E2E_INSTALL")
	inst = newInstaller()

	configureKubectlKubeRC()

	By("refusing any target other than the e2e kind cluster")
	verifyTargetIsKind()

	setupCertManager()

	By("verifying the cluster was prepared by `make test-e2e`")
	verifyClusterPrepared()

	By("building a typed client for the test cluster")
	buildK8sClient()

	By("installing the controller-manager")
	inst.Install(managerImage)

	By("forwarding the in-cluster git server to the host")
	gitForward = utils.StartPortForward(utils.GitServerNamespace, utils.GitServerService,
		utils.GitServerLocalPort, utils.GitServerPort)
	Expect(gitForward.WaitReady(3*time.Minute)).To(Succeed(), "git server port-forward never came up")
})

var _ = AfterSuite(func() {
	if gitForward != nil {
		gitForward.Stop()
	}

	// BeforeSuite can fail before it ever picks an installer (an unusable
	// kubeconfig, say), and AfterSuite still runs.
	if inst != nil {
		inst.Uninstall()
	}

	teardownCertManager()
})

// buildK8sClient wires a typed client for the Flux and Wavefront APIs.
func buildK8sClient() {
	scheme := clientgoscheme.Scheme
	Expect(wavefrontv1alpha1.AddToScheme(scheme)).To(Succeed())
	Expect(sourcev1.AddToScheme(scheme)).To(Succeed())
	Expect(kustomizev1.AddToScheme(scheme)).To(Succeed())

	cfg, err := ctrl.GetConfig()
	Expect(err).NotTo(HaveOccurred(), "Failed to load kubeconfig")

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred(), "Failed to build a client for the test cluster")
}

// verifyClusterPrepared checks the Makefile-side prerequisites, so that a
// missing step reads as one clear message rather than as a scenario failure
// five minutes later.
func verifyClusterPrepared() {
	for _, crd := range []string{
		"gitrepositories.source.toolkit.fluxcd.io",
		"kustomizations.kustomize.toolkit.fluxcd.io",
	} {
		cmd := exec.Command("kubectl", "get", "crd", crd)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(),
			"Flux CRD %s missing — run `make test-e2e` rather than `go test -tags=e2e` directly", crd)
	}

	cmd := exec.Command("kubectl", "get", "deployment", utils.GitServerService,
		"-n", utils.GitServerNamespace)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(),
		"git server missing — run `make test-e2e` rather than `go test -tags=e2e` directly")
}

// verifyTargetIsKind aborts the suite unless the client it is about to use
// resolves to the kind cluster `make test-e2e` provisioned, on loopback.
//
// The suite installs CRDs, deploys the manager, and creates and deletes
// namespaces. Whichever context ~/.kube/config happens to name is not an
// acceptable target for that — least of all when another kind cluster being
// created or deleted on the same machine has just rewritten it — so this is
// checked before the first mutating step, and independently of the Makefile's
// own guard, to cover a hand-run `go test -tags=e2e`.
func verifyTargetIsKind() {
	cfg, err := ctrl.GetConfig()
	Expect(err).NotTo(HaveOccurred(), "Failed to load kubeconfig")

	host, err := url.Parse(cfg.Host)
	Expect(err).NotTo(HaveOccurred(), "unparseable API server address %q", cfg.Host)

	out, err := utils.Run(exec.Command("kubectl", "config", "current-context"))
	Expect(err).NotTo(HaveOccurred(), "failed to read the current kubeconfig context")
	ctx := strings.TrimSpace(out)

	want := "kind-" + utils.KindClusterName()
	refusal := fmt.Sprintf("refusing to run e2e against context %q at %q (expected %s on loopback) — "+
		"the suite installs CRDs, deploys the manager and deletes namespaces; "+
		"run `make test-e2e`, which provisions and pins its own kind cluster", ctx, cfg.Host, want)
	Expect(ctx).To(Equal(want), refusal)
	Expect(host.Hostname()).To(BeElementOf("127.0.0.1", "localhost", "::1"), refusal)
}

// Disable kubectl kuberc by default for test isolation.
// This prevents local kubectl configurations from affecting test behavior.
// To enable kuberc, set: KUBECTL_KUBERC=true
func configureKubectlKubeRC() {
	if os.Getenv("KUBECTL_KUBERC") != "true" {
		By("disabling kubectl kuberc for test isolation")
		err := os.Setenv("KUBECTL_KUBERC", "false")
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to disable kubectl kuberc")
		_, _ = fmt.Fprintf(GinkgoWriter,
			"kubectl kuberc disabled for consistent test behavior (override with KUBECTL_KUBERC=true)\n")
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "kubectl kuberc enabled (KUBECTL_KUBERC=true)\n")
	}
}

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager()
}
