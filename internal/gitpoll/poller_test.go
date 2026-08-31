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

package gitpoll_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/metrics"
)

const (
	alphaURL = "https://git.example.com/org/alpha.git"
	betaURL  = "https://git.example.com/org/beta.git"
	otherURL = "https://git.other.com/org/gamma.git"

	exampleHost = "git.example.com"
	otherHost   = "git.other.com"

	trackedRef    = "refs/heads/main"
	tagRefV2      = "refs/tags/v2"
	testNamespace = "flux-system"

	shaA = "1111111111111111111111111111111111111111"
	shaB = "2222222222222222222222222222222222222222"

	failuresMetric = "wavefront_ref_list_failures_total"

	// listDelay is the fake lister's simulated round-trip; inside a synctest
	// bubble it is virtual time, so it costs the test nothing.
	listDelay = time.Second
)

var errListFailed = errors.New("simulated ref listing failure")

// --- fakes ------------------------------------------------------------------

// fakeLister serves scripted advertisements and records, per git host, the
// peak number of concurrently in-flight listings.
type fakeLister struct {
	mu sync.Mutex

	advertised  map[string]map[string]string // repo URL → advertisement
	errs        map[string]error             // repo URL → error to return
	calls       map[string]int               // repo URL → call count
	auth        map[string]transport.AuthMethod
	gates       map[string]chan struct{} // repo URL → listing is held until closed
	inFlight    map[string]int           // host → currently in flight
	maxInFlight map[string]int           // host → peak in flight
	delay       time.Duration
}

func newFakeLister() *fakeLister {
	return &fakeLister{
		advertised:  map[string]map[string]string{},
		errs:        map[string]error{},
		calls:       map[string]int{},
		auth:        map[string]transport.AuthMethod{},
		gates:       map[string]chan struct{}{},
		inFlight:    map[string]int{},
		maxInFlight: map[string]int{},
	}
}

func (f *fakeLister) List(ctx context.Context, repoURL string, auth transport.AuthMethod) (map[string]string, error) {
	host := hostOf(repoURL)

	f.mu.Lock()
	f.calls[repoURL]++
	f.auth[repoURL] = auth
	f.inFlight[host]++
	f.maxInFlight[host] = max(f.maxInFlight[host], f.inFlight[host])
	delay, gate := f.delay, f.gates[repoURL]
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.inFlight[host]--

	if err := f.errs[repoURL]; err != nil {
		return nil, err
	}
	return maps.Clone(f.advertised[repoURL]), nil
}

func (f *fakeLister) setAdvertised(repoURL string, refs map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advertised[repoURL] = refs
}

func (f *fakeLister) setErr(repoURL string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs[repoURL] = err
}

// gate makes listings of repoURL block until the returned channel is closed.
func (f *fakeLister) gate(repoURL string) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan struct{})
	f.gates[repoURL] = ch
	return ch
}

func (f *fakeLister) callCount(repoURL string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[repoURL]
}

func (f *fakeLister) peak(host string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInFlight[host]
}

func (f *fakeLister) authSeen() map[string]transport.AuthMethod {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.auth)
}

func hostOf(repoURL string) string {
	u, err := url.Parse(repoURL)
	if err != nil {
		return ""
	}
	return u.Host
}

// fakeSecrets is a client.Reader serving in-memory Secrets and counting Gets.
type fakeSecrets struct {
	mu   sync.Mutex
	data map[types.NamespacedName]map[string][]byte
	gets map[types.NamespacedName]int
}

func newFakeSecrets() *fakeSecrets {
	return &fakeSecrets{
		data: map[types.NamespacedName]map[string][]byte{},
		gets: map[types.NamespacedName]int{},
	}
}

func (f *fakeSecrets) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.gets[key]++
	data, ok := f.data[key]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, key.Name)
	}

	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return errors.New("fakeSecrets: object is not a Secret")
	}
	secret.Name, secret.Namespace = key.Name, key.Namespace
	secret.Data = maps.Clone(data)
	return nil
}

func (f *fakeSecrets) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return errors.New("fakeSecrets: List is not implemented")
}

func (f *fakeSecrets) set(key types.NamespacedName, data map[string][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = data
}

func (f *fakeSecrets) getCount(key types.NamespacedName) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets[key]
}

// counter is a notify() that records how many times it was called.
type counter struct {
	mu sync.Mutex
	n  int
}

func (c *counter) inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
}

func (c *counter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// --- helpers ----------------------------------------------------------------

func source(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: testNamespace, Name: name}
}

func target(name, repoURL string) gitpoll.Target {
	return gitpoll.Target{Source: source(name), URL: repoURL, TrackingRef: trackedRef}
}

// runPoller starts p inside the current synctest bubble and returns a stop
// function that cancels it and waits for Start to return.
func runPoller(t *testing.T, p *gitpoll.Poller) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() { errCh <- p.Start(ctx) }()

	return func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Start returned %v, want nil", err)
			}
		case <-time.After(time.Minute):
			t.Error("Start did not return after context cancellation")
		}
	}
}

// failureCount reads wavefront_ref_list_failures_total for one host label.
func failureCount(t *testing.T, reg *prometheus.Registry, host string) float64 {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != failuresMetric {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "host" && label.GetValue() == host {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// --- tests ------------------------------------------------------------------

func TestPollerIsManagerRunnable(t *testing.T) {
	var _ manager.Runnable = (*gitpoll.Poller)(nil)
}

func TestPollerSweepsOnInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
		notify := &counter{}

		p := gitpoll.NewPoller(newFakeSecrets(), lister, notify.inc, nil)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

		stop := runPoller(t, p)
		defer stop()

		// Nothing before the first tick.
		time.Sleep(9 * time.Second)
		synctest.Wait()
		if got := lister.callCount(alphaURL); got != 0 {
			t.Errorf("listings before the interval elapsed = %d, want 0", got)
		}
		if _, ok := p.Observation(source("alpha")); ok {
			t.Error("Observation exists before the first sweep, want none")
		}

		time.Sleep(2 * time.Second)
		synctest.Wait()
		if got := lister.callCount(alphaURL); got != 1 {
			t.Errorf("listings after one interval = %d, want 1", got)
		}

		obs, ok := p.Observation(source("alpha"))
		if !ok {
			t.Fatal("Observation missing after the first sweep")
		}
		if obs.SHA != shaA {
			t.Errorf("SHA = %q, want %q", obs.SHA, shaA)
		}
		if obs.Err != nil {
			t.Errorf("Err = %v, want nil", obs.Err)
		}
		if obs.ObservedAt.IsZero() || obs.FirstObserved.IsZero() {
			t.Errorf("ObservedAt/FirstObserved = %v/%v, want both set", obs.ObservedAt, obs.FirstObserved)
		}

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := lister.callCount(alphaURL); got != 2 {
			t.Errorf("listings after two intervals = %d, want 2", got)
		}
	})
}

func TestPollerNotifiesOncePerSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		for _, u := range []string{alphaURL, betaURL, otherURL} {
			lister.setAdvertised(u, map[string]string{trackedRef: shaA})
		}
		notify := &counter{}

		p := gitpoll.NewPoller(newFakeSecrets(), lister, notify.inc, nil)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{
			target("alpha", alphaURL),
			target("beta", betaURL),
			target("gamma", otherURL),
		})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := notify.count(); got != 1 {
			t.Errorf("notify calls after one sweep of 3 targets on 2 hosts = %d, want 1", got)
		}

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := notify.count(); got != 2 {
			t.Errorf("notify calls after two sweeps = %d, want 2", got)
		}
	})
}

func TestPollerBoundsPerHostConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			perHost         = 2
			exampleTargets  = 6
			otherHostTarget = "gamma"
		)

		lister := newFakeLister()
		lister.delay = listDelay

		targets := make([]gitpoll.Target, 0, exampleTargets+1)
		for i := range exampleTargets {
			u := "https://" + exampleHost + "/org/repo" + string(rune('a'+i)) + ".git"
			lister.setAdvertised(u, map[string]string{trackedRef: shaA})
			targets = append(targets, target("repo"+string(rune('a'+i)), u))
		}
		lister.setAdvertised(otherURL, map[string]string{trackedRef: shaA})
		targets = append(targets, target(otherHostTarget, otherURL))

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(time.Minute, perHost)
		p.SetTargets(targets)

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(time.Minute + 10*time.Second)
		synctest.Wait()

		if got := lister.peak(exampleHost); got > perHost {
			t.Errorf("peak concurrent listings for %s = %d, want <= %d", exampleHost, got, perHost)
		}
		if got := lister.peak(exampleHost); got != perHost {
			t.Errorf("peak concurrent listings for %s = %d, want exactly %d (the bound should be saturated)", exampleHost, got, perHost)
		}
		// Hosts run in parallel: the lone other-host target must not have
		// queued behind the busy host.
		if got := lister.peak(otherHost); got != 1 {
			t.Errorf("peak concurrent listings for %s = %d, want 1", otherHost, got)
		}
	})
}

func TestPollerFirstObservedStableUntilSHAChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		first, _ := p.Observation(source("alpha"))

		time.Sleep(10 * time.Second)
		synctest.Wait()
		second, _ := p.Observation(source("alpha"))

		if !second.FirstObserved.Equal(first.FirstObserved) {
			t.Errorf("FirstObserved moved from %v to %v across an unchanged SHA", first.FirstObserved, second.FirstObserved)
		}
		if !second.ObservedAt.After(first.ObservedAt) {
			t.Errorf("ObservedAt = %v, want later than %v", second.ObservedAt, first.ObservedAt)
		}

		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaB})
		time.Sleep(10 * time.Second)
		synctest.Wait()

		third, _ := p.Observation(source("alpha"))
		if third.SHA != shaB {
			t.Errorf("SHA = %q, want %q", third.SHA, shaB)
		}
		if !third.FirstObserved.After(second.FirstObserved) {
			t.Errorf("FirstObserved = %v, want reset to a later time than %v", third.FirstObserved, second.FirstObserved)
		}
	})
}

func TestPollerFailureRetainsLastGoodSHA(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
		reg := prometheus.NewRegistry()

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, metrics.New(reg).RefListFailures)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		good, _ := p.Observation(source("alpha"))

		lister.setErr(alphaURL, errListFailed)
		time.Sleep(10 * time.Second)
		synctest.Wait()

		failed, ok := p.Observation(source("alpha"))
		if !ok {
			t.Fatal("Observation dropped after a listing failure")
		}
		if failed.SHA != shaA {
			t.Errorf("SHA = %q, want the last good %q retained across a failure", failed.SHA, shaA)
		}
		if !failed.FirstObserved.Equal(good.FirstObserved) {
			t.Errorf("FirstObserved = %v, want %v preserved across a failure", failed.FirstObserved, good.FirstObserved)
		}
		if failed.Err == nil {
			t.Error("Err = nil, want the listing failure surfaced")
		} else if !errors.Is(failed.Err, errListFailed) {
			t.Errorf("Err = %v, want it to wrap %v", failed.Err, errListFailed)
		}
		if got := failureCount(t, reg, exampleHost); got != 1 {
			t.Errorf("%s{host=%q} = %v, want 1", failuresMetric, exampleHost, got)
		}

		// Recovery clears Err and resumes normal observation.
		lister.setErr(alphaURL, nil)
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaB})
		time.Sleep(10 * time.Second)
		synctest.Wait()

		recovered, _ := p.Observation(source("alpha"))
		if recovered.Err != nil {
			t.Errorf("Err = %v, want nil after recovery", recovered.Err)
		}
		if recovered.SHA != shaB {
			t.Errorf("SHA = %q, want %q after recovery", recovered.SHA, shaB)
		}
	})
}

// TestPollerWrappedCancelledErrorIsCountedFailure: an error chain containing
// context.Canceled is not proof of a shutdown — ctx.Err() is the only
// authority for that (see poll's doc comment) — so while the sweep context is
// healthy, an error that merely happens to wrap context.Canceled (e.g. an
// HTTP/2 stream reset) must be treated as an ordinary listing failure: it
// blips wavefront_ref_list_failures_total and freezes the last good
// observation with Err stamped, exactly like any other failure (DESIGN §6,
// D4).
func TestPollerWrappedCancelledErrorIsCountedFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
		reg := prometheus.NewRegistry()

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, metrics.New(reg).RefListFailures)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		good, ok := p.Observation(source("alpha"))
		if !ok || good.SHA != shaA {
			t.Fatalf("Observation = %+v, want %q observed on the first sweep", good, shaA)
		}

		// The context stays healthy; only the error happens to wrap
		// context.Canceled, as an HTTP/2 stream reset might.
		wrapped := fmt.Errorf("http2 stream reset: %w", context.Canceled)
		lister.setErr(alphaURL, wrapped)
		time.Sleep(10 * time.Second)
		synctest.Wait()

		after, ok := p.Observation(source("alpha"))
		if !ok {
			t.Fatal("Observation dropped after a listing failure")
		}
		if after.SHA != good.SHA {
			t.Errorf("SHA = %q, want the last good %q retained across a failure", after.SHA, good.SHA)
		}
		if after.Err == nil || !errors.Is(after.Err, context.Canceled) {
			t.Errorf("Err = %v, want it to wrap context.Canceled", after.Err)
		}
		if got := failureCount(t, reg, exampleHost); got != 1 {
			t.Errorf("%s{host=%q} = %v, want 1", failuresMetric, exampleHost, got)
		}
	})
}

// TestPollerShutdownDiscardsInFlightListing: at manager shutdown, go-git
// transports surface cancellation as plain EOF/closed-connection errors that
// never wrap context.Canceled. The poller must still recognise the shutdown —
// by ctx.Err(), not the error chain — and discard the in-flight listing
// outright: it must neither blip wavefront_ref_list_failures_total (a §6
// safety alarm operators rate-alert on, which a rolling restart would
// otherwise page) nor stamp a shutdown artefact over a good observation.
func TestPollerShutdownDiscardsInFlightListing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
		reg := prometheus.NewRegistry()

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, metrics.New(reg).RefListFailures)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() { errCh <- p.Start(ctx) }()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		good, ok := p.Observation(source("alpha"))
		if !ok || good.SHA != shaA {
			t.Fatalf("Observation = %+v, want %q observed before shutdown", good, shaA)
		}

		// Arm the next sweep to hang mid-listing, then have it eventually
		// surface a plain transport error that shares no chain with
		// context.Canceled — exactly what go-git's own EOF/closed-connection
		// errors look like.
		lister.gate(alphaURL)
		lister.setErr(alphaURL, errors.New("EOF"))

		time.Sleep(10 * time.Second)
		synctest.Wait() // the listing is now durably blocked on the gate

		cancel() // the manager shuts the poller down mid-listing
		synctest.Wait()

		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Start returned %v, want nil", err)
			}
		case <-time.After(time.Minute):
			t.Fatal("Start did not return after context cancellation")
		}

		after, ok := p.Observation(source("alpha"))
		if !ok {
			t.Fatal("Observation dropped by a discarded listing")
		}
		if after.Err != nil || after.SHA != good.SHA || !after.ObservedAt.Equal(good.ObservedAt) {
			t.Errorf("Observation = %+v, want %+v left untouched by the discarded listing", after, good)
		}
		if got := failureCount(t, reg, exampleHost); got != 0 {
			t.Errorf("%s{host=%q} = %v, want 0", failuresMetric, exampleHost, got)
		}
	})
}

func TestPollerSetTargetsReplacesAndDropsObservations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
		lister.setAdvertised(betaURL, map[string]string{trackedRef: shaB})

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL), target("beta", betaURL)})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if _, ok := p.Observation(source("beta")); !ok {
			t.Fatal("Observation for beta missing after the first sweep")
		}

		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})
		if _, ok := p.Observation(source("beta")); ok {
			t.Error("Observation for beta survived its removal from the target set")
		}
		if _, ok := p.Observation(source("alpha")); !ok {
			t.Error("Observation for alpha dropped although it is still targeted")
		}

		callsBefore := lister.callCount(betaURL)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := lister.callCount(betaURL); got != callsBefore {
			t.Errorf("beta listed %d times after removal, want no further listings (was %d)", got, callsBefore)
		}
	})
}

// TestPollerSetTargetsInvalidatesChangedPlumbing covers a catalog change that
// repoints a GitRepository: the observation made against the old ref or URL
// must not be served as current, because the reconciler would pin it.
func TestPollerSetTargetsInvalidatesChangedPlumbing(t *testing.T) {
	cases := []struct {
		name string
		next gitpoll.Target
	}{
		{
			name: "tracking ref changed",
			next: gitpoll.Target{Source: source("alpha"), URL: alphaURL, TrackingRef: tagRefV2},
		},
		{
			name: "url changed",
			next: gitpoll.Target{Source: source("alpha"), URL: betaURL, TrackingRef: trackedRef},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				lister := newFakeLister()
				lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA, tagRefV2: shaB})
				lister.setAdvertised(betaURL, map[string]string{trackedRef: shaB})

				p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
				p.Configure(10*time.Second, 2)
				p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

				stop := runPoller(t, p)
				defer stop()

				time.Sleep(10 * time.Second)
				synctest.Wait()
				if obs, ok := p.Observation(source("alpha")); !ok || obs.SHA != shaA {
					t.Fatalf("observation before the change = %+v (present=%t), want SHA %q", obs, ok, shaA)
				}

				p.SetTargets([]gitpoll.Target{tc.next})
				if obs, ok := p.Observation(source("alpha")); ok {
					t.Errorf("observation %+v survived a plumbing change, want it dropped until re-observed", obs)
				}

				time.Sleep(10 * time.Second)
				synctest.Wait()
				obs, ok := p.Observation(source("alpha"))
				if !ok {
					t.Fatal("no observation after the first sweep of the new plumbing")
				}
				if obs.SHA != shaB {
					t.Errorf("SHA = %q, want the new plumbing's %q", obs.SHA, shaB)
				}
			})
		})
	}
}

// TestPollerMidSweepPlumbingSwapDiscardsInFlightResult covers the same class of
// bug in flight: a listing started against the old URL and ref must not land in
// the repointed target's observation.
func TestPollerMidSweepPlumbingSwapDiscardsInFlightResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
		lister.setAdvertised(betaURL, map[string]string{tagRefV2: shaB})
		gate := lister.gate(alphaURL)

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := lister.callCount(alphaURL); got != 1 {
			t.Fatalf("listings = %d, want one in flight against the old plumbing", got)
		}
		if _, ok := p.Observation(source("alpha")); ok {
			t.Fatal("observation recorded while the listing is still in flight")
		}

		// The catalog repoints the source mid-sweep, then the old listing lands.
		p.SetTargets([]gitpoll.Target{{Source: source("alpha"), URL: betaURL, TrackingRef: tagRefV2}})
		close(gate)
		synctest.Wait()

		if obs, ok := p.Observation(source("alpha")); ok {
			t.Errorf("in-flight result for superseded plumbing landed as %+v, want no observation", obs)
		}

		time.Sleep(10 * time.Second)
		synctest.Wait()
		obs, ok := p.Observation(source("alpha"))
		if !ok {
			t.Fatal("no observation after the sweep following the swap")
		}
		if obs.SHA != shaB {
			t.Errorf("SHA = %q, want the new plumbing's %q", obs.SHA, shaB)
		}
	})
}

func TestPollerReadsSecretFreshEachSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		secretRef := types.NamespacedName{Namespace: testNamespace, Name: "alpha-auth"}
		secrets := newFakeSecrets()
		secrets.set(secretRef, map[string][]byte{
			keyUsername: []byte("alice"),
			keyPassword: []byte("old"),
		})

		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})

		p := gitpoll.NewPoller(secrets, lister, func() {}, nil)
		p.Configure(10*time.Second, 2)

		tgt := target("alpha", alphaURL)
		tgt.SecretRef = &secretRef
		p.SetTargets([]gitpoll.Target{tgt})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := secrets.getCount(secretRef); got != 1 {
			t.Errorf("secret Gets after one sweep = %d, want 1", got)
		}
		if basic, ok := lister.authSeen()[alphaURL].(*githttp.BasicAuth); !ok || basic.Password != "old" {
			t.Errorf("auth = %#v, want basic auth with password %q", lister.authSeen()[alphaURL], "old")
		}

		// Credentials rotate: the next sweep must pick them up.
		secrets.set(secretRef, map[string][]byte{
			keyUsername: []byte("alice"),
			keyPassword: []byte("new"),
		})
		time.Sleep(10 * time.Second)
		synctest.Wait()

		if got := secrets.getCount(secretRef); got != 2 {
			t.Errorf("secret Gets after two sweeps = %d, want 2 (credentials must be re-read each sweep)", got)
		}
		if basic, ok := lister.authSeen()[alphaURL].(*githttp.BasicAuth); !ok || basic.Password != "new" {
			t.Errorf("auth = %#v, want basic auth with rotated password %q", lister.authSeen()[alphaURL], "new")
		}
	})
}

func TestPollerMissingSecretIsAFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		secretRef := types.NamespacedName{Namespace: testNamespace, Name: "absent"}
		reg := prometheus.NewRegistry()
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, metrics.New(reg).RefListFailures)
		p.Configure(10*time.Second, 2)

		tgt := target("alpha", alphaURL)
		tgt.SecretRef = &secretRef
		p.SetTargets([]gitpoll.Target{tgt})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()

		obs, ok := p.Observation(source("alpha"))
		if !ok {
			t.Fatal("Observation missing after a failed sweep")
		}
		if obs.Err == nil {
			t.Error("Err = nil, want the missing-secret failure surfaced")
		}
		if obs.SHA != "" {
			t.Errorf("SHA = %q, want empty for a never-observed target", obs.SHA)
		}
		if got := lister.callCount(alphaURL); got != 0 {
			t.Errorf("listings = %d, want 0 when the secret cannot be read", got)
		}
		if got := failureCount(t, reg, exampleHost); got != 1 {
			t.Errorf("%s{host=%q} = %v, want 1", failuresMetric, exampleHost, got)
		}
	})
}

func TestPollerUnadvertisedTrackingRefIsAFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{"refs/heads/other": shaA})

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()

		obs, _ := p.Observation(source("alpha"))
		if obs.Err == nil {
			t.Error("Err = nil, want a failure when the tracking ref is not advertised")
		}
		if obs.SHA != "" {
			t.Errorf("SHA = %q, want empty", obs.SHA)
		}
	})
}

func TestPollerPrefersPeeledTag(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const tag = "refs/tags/v1"

		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{tag: shaA, tag + "^{}": shaB})

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(10*time.Second, 2)
		p.SetTargets([]gitpoll.Target{{Source: source("alpha"), URL: alphaURL, TrackingRef: tag}})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()

		obs, _ := p.Observation(source("alpha"))
		if obs.SHA != shaB {
			t.Errorf("SHA = %q, want the peeled commit %q rather than the tag object", obs.SHA, shaB)
		}
	})
}

// TestPollerConfigureClampsNonPositiveValues covers the defensive clamp: a
// typed client can send explicit zeros past CRD defaulting, and a zero
// interval must never tight-loop.
func TestPollerConfigureClampsNonPositiveValues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const defaultInterval = 90 * time.Second
		const defaultPerHost = 4
		const targetCount = 8

		lister := newFakeLister()
		lister.delay = listDelay

		targets := make([]gitpoll.Target, 0, targetCount)
		for i := range targetCount {
			u := "https://" + exampleHost + "/org/clamp" + string(rune('a'+i)) + ".git"
			lister.setAdvertised(u, map[string]string{trackedRef: shaA})
			targets = append(targets, target("clamp"+string(rune('a'+i)), u))
		}

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(0, 0)
		p.SetTargets(targets)

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(defaultInterval - time.Second)
		synctest.Wait()
		if got := lister.callCount(targets[0].URL); got != 0 {
			t.Fatalf("listings before the clamped %v interval = %d, want 0", defaultInterval, got)
		}

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := lister.callCount(targets[0].URL); got != 1 {
			t.Errorf("listings after the clamped %v interval = %d, want 1", defaultInterval, got)
		}
		if got := lister.peak(exampleHost); got != defaultPerHost {
			t.Errorf("peak concurrent listings = %d, want the clamped default %d", got, defaultPerHost)
		}
	})
}

// TestPollerObservationsNeverMixSweeps is the coherence contract that makes
// strict ordering for co-arriving changes structural rather than probabilistic
// (DESIGN §3.3). Two repositories receive new commits at the same moment, but
// their listings complete far apart. Every snapshot taken in between must be
// entirely the old sweep or entirely the new one: a mixture would present the
// slow repository at its previous SHA — equal to its pin, and therefore
// apparently settled — while the fast one already shows a pending revision,
// which is exactly how a descendant gets admitted ahead of its ancestor.
func TestPollerObservationsNeverMixSweeps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		alpha, beta := source("alpha"), source("beta")

		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
		lister.setAdvertised(betaURL, map[string]string{trackedRef: shaA})

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(10*time.Second, 4)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL), target("beta", betaURL)})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(10 * time.Second)
		synctest.Wait()

		snapshot := p.Observations()
		if snapshot[alpha].SHA != shaA || snapshot[beta].SHA != shaA {
			t.Fatalf("after the first sweep = alpha %q / beta %q, want both %q",
				snapshot[alpha].SHA, snapshot[beta].SHA, shaA)
		}

		// Co-arriving commits, with beta's listing held open so that alpha's
		// result is complete long before beta's.
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaB})
		lister.setAdvertised(betaURL, map[string]string{trackedRef: shaB})
		gate := lister.gate(betaURL)

		time.Sleep(10 * time.Second)
		synctest.Wait()

		if got := lister.callCount(alphaURL); got != 2 {
			t.Fatalf("alpha listings = %d, want the second sweep to have listed it", got)
		}

		snapshot = p.Observations()
		if snapshot[alpha].SHA != shaA || snapshot[beta].SHA != shaA {
			t.Errorf("mid-sweep snapshot = alpha %q / beta %q, want both still %q: "+
				"a snapshot must never mix one sweep's results with another's",
				snapshot[alpha].SHA, snapshot[beta].SHA, shaA)
		}

		close(gate)
		synctest.Wait()

		snapshot = p.Observations()
		if snapshot[alpha].SHA != shaB || snapshot[beta].SHA != shaB {
			t.Errorf("after the sweep completed = alpha %q / beta %q, want both %q",
				snapshot[alpha].SHA, snapshot[beta].SHA, shaB)
		}
	})
}

// TestPollerObservationsSnapshotIsIndependent covers the other half of the
// contract: the returned map is the caller's own, and taking one concurrently
// with live sweeps is safe (exercised under -race).
func TestPollerObservationsSnapshotIsIndependent(t *testing.T) {
	lister := newFakeLister()
	lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
	lister.setAdvertised(betaURL, map[string]string{trackedRef: shaB})

	p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
	p.Configure(time.Millisecond, 4)
	p.SetTargets([]gitpoll.Target{target("alpha", alphaURL), target("beta", betaURL)})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	var readers sync.WaitGroup
	deadline := time.Now().Add(250 * time.Millisecond)
	for range 4 {
		readers.Go(func() {
			for time.Now().Before(deadline) {
				// Mutating the snapshot must not corrupt the poller's store.
				snapshot := p.Observations()
				snapshot[source("alpha")] = gitpoll.Observation{SHA: "mutated"}
				delete(snapshot, source("beta"))
			}
		})
	}
	readers.Wait()

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Start returned %v, want nil", err)
	}

	final := p.Observations()
	if got := final[source("alpha")].SHA; got != shaA {
		t.Errorf("alpha SHA = %q, want %q: the snapshot must be an independent copy", got, shaA)
	}
	if _, ok := final[source("beta")]; !ok {
		t.Error("beta missing from the store: deleting from a snapshot must not delete from the poller")
	}
}

// TestPollerConfigureAppliesWithoutWaitingOutTheOldInterval: the reconciler
// re-Configures whenever the fleet's merged cadence changes, and a shortened
// interval has to take effect now. Waiting for the outgoing interval to elapse
// first would let a single 90s Wavefront stall a 30s one's observations for a
// full 90s — the exact stall the merged cadence exists to prevent.
func TestPollerConfigureAppliesWithoutWaitingOutTheOldInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(90*time.Second, 4)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

		stop := runPoller(t, p)
		defer stop()

		time.Sleep(time.Second)
		synctest.Wait()
		if got := lister.callCount(alphaURL); got != 0 {
			t.Fatalf("listings one second in = %d, want 0 on a 90s cadence", got)
		}

		// A faster Wavefront joins the fleet.
		p.Configure(10*time.Second, 4)

		time.Sleep(11 * time.Second)
		synctest.Wait()
		if got := lister.callCount(alphaURL); got != 1 {
			t.Errorf("listings %v after shortening the interval to 10s = %d, want 1: "+
				"a cadence change must not wait out the old interval", 12*time.Second, got)
		}
	})
}

// TestPollerRepeatedConfigureDoesNotStarveSweeps: the reconciler calls
// Configure on every pass. Re-arming the wait each time would postpone sweeps
// for ever, so an unchanged cadence must be a no-op and a changed one must
// still measure from the last sweep.
func TestPollerRepeatedConfigureDoesNotStarveSweeps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})

		p := gitpoll.NewPoller(newFakeSecrets(), lister, func() {}, nil)
		p.Configure(10*time.Second, 4)
		p.SetTargets([]gitpoll.Target{target("alpha", alphaURL)})

		stop := runPoller(t, p)
		defer stop()

		// Reconciles land far more often than sweeps do.
		for range 20 {
			time.Sleep(time.Second)
			p.Configure(10*time.Second, 4)
		}
		synctest.Wait()

		if got := lister.callCount(alphaURL); got != 2 {
			t.Errorf("listings over 20s of repeated Configure = %d, want 2: "+
				"an unchanged cadence must not re-arm the wait", got)
		}
	})
}

func TestPollerStartBlocksUntilContextDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := gitpoll.NewPoller(newFakeSecrets(), newFakeLister(), func() {}, nil)
		p.Configure(10*time.Second, 2)

		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() { errCh <- p.Start(ctx) }()

		time.Sleep(time.Hour)
		synctest.Wait()
		select {
		case err := <-errCh:
			t.Fatalf("Start returned %v while the context was live, want it to block", err)
		default:
		}

		cancel()
		synctest.Wait()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Start returned %v, want nil", err)
			}
		default:
			t.Error("Start did not return after context cancellation")
		}
	})
}
