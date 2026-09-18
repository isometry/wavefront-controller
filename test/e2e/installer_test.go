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
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/isometry/wavefront-controller/test/utils"
)

// The project ships two supported ways of installing the controller — the
// kustomize overlays under config/ and the Helm chart under deploy/charts —
// and they disagree about resource names, labels and the manager's own
// lifecycle. The specs care about neither: they want "the manager", "its
// namespace", "its metrics service". installer is that seam, so one suite
// covers both installation paths and a rename on either side is caught here
// rather than in a scenario five minutes in.
type installer interface {
	// Install installs the CRDs and the manager from the given image and
	// returns once the manager Deployment reports Available.
	Install(image string)
	// Uninstall removes the manager, the CRDs and the namespace. It is
	// idempotent and never fails the suite: it runs in AfterSuite, where a
	// half-finished BeforeSuite is a normal thing to clean up after.
	Uninstall()
	// Namespace is the namespace the manager runs in.
	Namespace() string
	// ServiceAccount is the manager's ServiceAccount, in Namespace.
	ServiceAccount() string
	// MetricsService is the Service fronting the manager's metrics endpoint.
	MetricsService() string
	// MetricsReaderRole is the ClusterRole granting GET on /metrics.
	MetricsReaderRole() string
	// PodSelector is a `kubectl -l` selector matching the manager pod.
	PodSelector() string
	// DeploymentName is the manager Deployment, and hence the prefix its pods
	// carry.
	DeploymentName() string
}

const (
	// managerNamespace is the namespace both installers deploy into: the
	// kustomize overlay hardcodes it, and the chart is installed there.
	managerNamespace = "wavefront-controller-system"
	// helmRelease is the chart release name, which drives every chart-side
	// resource name below.
	helmRelease = "wavefront-controller"
	// wavefrontCRD is the one CRD this project owns. The chart keeps its CRDs
	// on uninstall (crds.keep defaults to true), so the helm installer deletes
	// it explicitly.
	wavefrontCRD = "wavefronts.wavefront.as-code.io"
)

// newInstaller picks the installation path from E2E_INSTALL, which `make
// test-e2e` passes through. An unrecognised value is a typo in a CI matrix or
// a shell, and silently testing the wrong installer would be worse than not
// running at all.
func newInstaller() installer {
	mode := os.Getenv("E2E_INSTALL")
	switch mode {
	case "", "kustomize":
		return kustomizeInstaller{}
	case "helm":
		return helmInstaller{}
	default:
		Fail(fmt.Sprintf("unsupported E2E_INSTALL=%q: expected \"kustomize\" (the default) or \"helm\"", mode))
		return nil
	}
}

// createManagerNamespace creates the manager namespace and labels it for the
// restricted Pod Security Standard. Both installers do this themselves rather
// than leaving it to their manifests: the restricted label is what proves the
// manager's securityContext is actually admissible, and it has to be in place
// before the first pod is scheduled either way.
func createManagerNamespace() {
	By("creating manager namespace")
	cmd := exec.Command("kubectl", "create", "ns", managerNamespace)
	_, _ = utils.Run(cmd)

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", managerNamespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")
}

// deleteManagerNamespace removes the manager namespace, tolerating its absence.
func deleteManagerNamespace() {
	By("removing manager namespace")
	cmd := exec.Command("kubectl", "delete", "ns", managerNamespace, "--ignore-not-found")
	_, _ = utils.Run(cmd)
}

// waitForManagerAvailable blocks until the manager Deployment is Available, so
// that a manifest the cluster rejects surfaces here — with the deployment's own
// message — instead of as a spec timing out on a pod that never existed.
func waitForManagerAvailable(deployment string) {
	By("waiting for the controller-manager deployment to become available")
	cmd := exec.Command("kubectl", "wait", "deployment/"+deployment,
		"--for=condition=Available", "-n", managerNamespace, "--timeout=3m")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "controller-manager deployment never became available")
}

// kustomizeInstaller drives the config/ overlays through the Makefile, exactly
// as `make deploy` does for a human. Names come from config/default's
// `namePrefix: wavefront-controller-` over the scaffolded `controller-manager`.
type kustomizeInstaller struct{}

func (kustomizeInstaller) Install(image string) {
	By("installing CRDs")
	cmd := exec.Command("make", "install")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

	createManagerNamespace()

	By("deploying the controller-manager")
	cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", image))
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

	waitForManagerAvailable(kustomizeInstaller{}.DeploymentName())
}

func (kustomizeInstaller) Uninstall() {
	By("undeploying the controller-manager")
	cmd := exec.Command("make", "undeploy")
	_, _ = utils.Run(cmd)

	By("uninstalling CRDs")
	cmd = exec.Command("make", "uninstall")
	_, _ = utils.Run(cmd)

	deleteManagerNamespace()
}

func (kustomizeInstaller) Namespace() string      { return managerNamespace }
func (kustomizeInstaller) ServiceAccount() string { return "wavefront-controller-controller-manager" }
func (kustomizeInstaller) MetricsService() string {
	return "wavefront-controller-controller-manager-metrics-service"
}
func (kustomizeInstaller) MetricsReaderRole() string { return "wavefront-controller-metrics-reader" }
func (kustomizeInstaller) PodSelector() string       { return "control-plane=controller-manager" }
func (kustomizeInstaller) DeploymentName() string    { return "wavefront-controller-controller-manager" }

// helmInstaller drives the chart under deploy/charts/wavefront-controller.
// Every name below is the chart's `chart.fullname` — release name and chart
// name being equal, that collapses to just the release name.
type helmInstaller struct{}

func (helmInstaller) Install(image string) {
	createManagerNamespace()

	projectDir, err := utils.GetProjectDir()
	Expect(err).NotTo(HaveOccurred(), "Failed to locate the project directory")

	repository, tag := splitImage(image)

	By("installing the chart")
	cmd := exec.Command(helmBinary(), "upgrade", "--install", helmRelease,
		filepath.Join(projectDir, "deploy", "charts", "wavefront-controller"),
		"--namespace", managerNamespace,
		"--set", "manager.repository="+repository,
		"--set", "manager.tag="+tag,
		"--wait", "--timeout", "3m")
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to install the Helm chart")

	// `helm --wait` already blocked on readiness, but the Available condition
	// is what the kustomize path promises, so assert it in both.
	waitForManagerAvailable(helmInstaller{}.DeploymentName())
}

func (helmInstaller) Uninstall() {
	By("uninstalling the chart")
	cmd := exec.Command(helmBinary(), "uninstall", helmRelease,
		"-n", managerNamespace, "--ignore-not-found")
	_, _ = utils.Run(cmd)

	// crds.keep defaults to true, so `helm uninstall` deliberately leaves the
	// CRD (and any Wavefront resources) behind. A test cluster wants it gone.
	By("uninstalling CRDs")
	cmd = exec.Command("kubectl", "delete", "crd", wavefrontCRD, "--ignore-not-found")
	_, _ = utils.Run(cmd)

	deleteManagerNamespace()
}

func (helmInstaller) Namespace() string      { return managerNamespace }
func (helmInstaller) ServiceAccount() string { return helmRelease }
func (helmInstaller) MetricsService() string { return helmRelease + "-metrics-service" }
func (helmInstaller) MetricsReaderRole() string {
	return helmRelease + "-metrics-reader-role"
}
func (helmInstaller) PodSelector() string {
	return fmt.Sprintf("app.kubernetes.io/name=%s,app.kubernetes.io/instance=%s",
		helmRelease, helmRelease)
}
func (helmInstaller) DeploymentName() string { return helmRelease }

// helmBinary is the helm to invoke: $HELM as the Makefile passes it (and as
// `make helm-lint` and friends use it), or plain `helm` from PATH.
func helmBinary() string {
	if v, ok := os.LookupEnv("HELM"); ok && v != "" {
		return v
	}
	return "helm"
}

// splitImage splits a repo:tag reference at its last colon. As in the
// Makefile's ko targets, a registry:port host would defeat this — acceptable
// because E2E_IMG is always a plain registry host.
func splitImage(image string) (repository, tag string) {
	i := strings.LastIndex(image, ":")
	if i < 0 {
		Fail(fmt.Sprintf("manager image %q has no tag: expected repository:tag", image))
		return "", ""
	}
	return image[:i], image[i+1:]
}
