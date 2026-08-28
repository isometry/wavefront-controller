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
	"context"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/metrics"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.
//
// The controller suite runs the *real* manager against envtest: the
// reconciler, a real gitpoll.Poller (behind a scripted fake Lister) and the
// real pin.Writer. No Flux controllers run and there is no garbage
// collection, so specs hand-set Kustomization/GitRepository status themselves
// and never rely on pruning.

var (
	ctx       context.Context
	cancel    context.CancelFunc
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
	lister    *fakeLister
	poller    *gitpoll.Poller
	// instruments is the suite's own isolated metric registry: envtest
	// scenarios that care about a metric (e.g. wavefront_admissions_total)
	// read it directly rather than scraping an HTTP endpoint.
	instruments *metrics.Instruments
)

// fakeLister is the scripted stand-in for git ref advertisements: specs call
// advertise() to make a URL's tracking ref resolve to a SHA, and the real
// Poller then drives the reconciler exactly as production does.
type fakeLister struct {
	mu   sync.RWMutex
	refs map[string]map[string]string // url -> (ref name -> SHA)
}

func newFakeLister() *fakeLister {
	return &fakeLister{refs: map[string]map[string]string{}}
}

func (f *fakeLister) advertise(repoURL, refName, sha string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refs[repoURL] == nil {
		f.refs[repoURL] = map[string]string{}
	}
	f.refs[repoURL][refName] = sha
}

// List implements gitpoll.Lister.
func (f *fakeLister) List(_ context.Context, repoURL string, _ transport.AuthMethod) (map[string]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	out := make(map[string]string, len(f.refs[repoURL]))
	maps.Copy(out, f.refs[repoURL])
	return out, nil
}

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	SetDefaultEventuallyTimeout(30 * time.Second)
	SetDefaultEventuallyPollingInterval(100 * time.Millisecond)
	SetDefaultConsistentlyDuration(2 * time.Second)
	SetDefaultConsistentlyPollingInterval(100 * time.Millisecond)

	ctx, cancel = context.WithCancel(context.TODO())

	Expect(wavefrontv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())
	Expect(sourcev1.AddToScheme(scheme.Scheme)).To(Succeed())
	Expect(kustomizev1.AddToScheme(scheme.Scheme)).To(Succeed())

	// +kubebuilder:scaffold:scheme

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"),
			filepath.Join("..", "..", "test", "crds", "flux"),
		},
		ErrorIfCRDPathMissing: true,
	}

	// Retrieve the first found binary directory to allow running tests from IDEs
	if getFirstFoundEnvTestBinaryDir() != "" {
		testEnv.BinaryAssetsDirectory = getFirstFoundEnvTestBinaryDir()
	}

	// cfg is defined in this file globally.
	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	By("starting the manager with the Wavefront reconciler and a real poller")
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	Expect(err).NotTo(HaveOccurred())

	events := make(chan event.GenericEvent, 128)
	notify := func() {
		var list wavefrontv1alpha1.WavefrontList
		if err := mgr.GetClient().List(ctx, &list); err != nil {
			return
		}
		for i := range list.Items {
			select {
			case events <- event.GenericEvent{Object: &list.Items[i]}:
			default:
			}
		}
	}

	instruments = metrics.New(prometheus.NewRegistry())

	lister = newFakeLister()
	// Non-caching reader, exactly as main wires it: the poller must never
	// start a cluster-wide Secret informer.
	poller = gitpoll.NewPoller(mgr.GetAPIReader(), lister, notify, instruments.RefListFailures)
	poller.Configure(100*time.Millisecond, 4)
	Expect(mgr.Add(poller)).To(Succeed())

	reconciler := &WavefrontReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorder("wavefront-controller"),
		Adapter:   adapter.NewKustomizationAdapter(),
		Strategy:  selection.TrackRef(),
		Poller:    poller,
		PinWriter: &pin.Writer{Client: mgr.GetClient()},
		Clock:     time.Now,
		Metrics:   instruments,
	}
	Expect(reconciler.SetupWithManager(mgr, events)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()

	Expect(mgr.GetCache().WaitForCacheSync(ctx)).To(BeTrue())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	Eventually(func() error {
		return testEnv.Stop()
	}, time.Minute, time.Second).Should(Succeed())
})

// getFirstFoundEnvTestBinaryDir locates the first binary in the specified path.
// ENVTEST-based tests depend on specific binaries, usually located in paths set by
// controller-runtime. When running tests directly (e.g., via an IDE) without using
// Makefile targets, the 'BinaryAssetsDirectory' must be explicitly configured.
//
// This function streamlines the process by finding the required binaries, similar to
// setting the 'KUBEBUILDER_ASSETS' environment variable. To ensure the binaries are
// properly set up, run 'make setup-envtest' beforehand.
func getFirstFoundEnvTestBinaryDir() string {
	basePath := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		logf.Log.Error(err, "Failed to read directory", "path", basePath)
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}
