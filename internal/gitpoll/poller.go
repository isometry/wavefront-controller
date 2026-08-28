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

package gitpoll

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/isometry/wavefront-controller/internal/selection"
)

const (
	// DefaultInterval matches the Wavefront CRD default for spec.poll.interval.
	DefaultInterval = 90 * time.Second
	// DefaultPerHostConcurrency matches the CRD default for
	// spec.poll.perHostConcurrency.
	DefaultPerHostConcurrency = 4

	// unknownHost labels failures for targets whose URL yields no host.
	unknownHost = "unknown"
)

// Target is one source to poll.
type Target struct {
	Source      types.NamespacedName  // the GitRepository
	URL         string                // spec.url
	SecretRef   *types.NamespacedName // secret holding credentials, if any
	TrackingRef string
}

// Observation is the latest advertisement result for a target's tracking ref.
type Observation struct {
	SHA           string    // "" while unobserved or on persistent failure
	ObservedAt    time.Time // when the last sweep touched this target
	FirstObserved time.Time // when this SHA value was first seen (reset on change)
	Err           error     // last listing error, nil on success
}

// Poller periodically sweeps all targets, batched per git host with bounded
// per-host concurrency (DESIGN §3.1, §4.1 poll.*). Implements manager.Runnable.
type Poller struct {
	secrets  client.Reader
	lister   Lister
	notify   func()
	strategy selection.Strategy

	// failures is nil when no Registerer was supplied.
	failures *prometheus.CounterVec

	mu                 sync.RWMutex
	interval           time.Duration
	perHostConcurrency int
	targets            []Target
	// live maps each targeted source to the plumbing it is currently polled
	// with, so that a listing in flight against superseded plumbing cannot
	// land in the new target's observation.
	live map[types.NamespacedName]targetRef
	// observations are keyed by source and tagged with the plumbing they were
	// made against; an observation whose plumbing no longer matches the target
	// set is stale by construction and is dropped rather than served.
	observations map[types.NamespacedName]record
}

// targetRef is an observation's identity beyond its source: the same
// GitRepository polled at a different URL or tracking ref yields an
// observation that says something different, so the two must never be
// confused (DESIGN §3.1, "no stale candidate can survive a plumbing change").
type targetRef struct {
	url         string
	trackingRef string
}

func targetRefOf(t Target) targetRef {
	return targetRef{url: t.URL, trackingRef: t.TrackingRef}
}

// record is a stored observation plus the plumbing it was observed against.
type record struct {
	ref targetRef
	obs Observation
}

var _ manager.Runnable = (*Poller)(nil)

// NewPoller returns a Poller with the CRD's default cadence, ready for
// Configure and SetTargets. reg may be nil, in which case no metric is
// registered.
func NewPoller(secrets client.Reader, lister Lister, notify func(), reg prometheus.Registerer) *Poller {
	return &Poller{
		secrets:            secrets,
		lister:             lister,
		notify:             notify,
		strategy:           selection.TrackRef(),
		failures:           registerFailureCounter(reg),
		interval:           DefaultInterval,
		perHostConcurrency: DefaultPerHostConcurrency,
		live:               map[types.NamespacedName]targetRef{},
		observations:       map[types.NamespacedName]record{},
	}
}

// registerFailureCounter registers wavefront_ref_list_failures_total (DESIGN
// §6), tolerating a registry that already holds it.
func registerFailureCounter(reg prometheus.Registerer) *prometheus.CounterVec {
	if reg == nil {
		return nil
	}

	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wavefront_ref_list_failures_total",
		Help: "Total ref-advertisement listing failures, by git host.",
	}, []string{"host"})

	err := reg.Register(counter)
	if err == nil {
		return counter
	}

	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		if existing, ok := already.ExistingCollector.(*prometheus.CounterVec); ok {
			return existing
		}
	}
	return nil
}

// Configure sets the sweep cadence. Non-positive values are clamped to the
// CRD defaults: a typed client can send explicit zeros past CRD defaulting,
// and a zero interval must never tight-loop.
func (p *Poller) Configure(interval time.Duration, perHostConcurrency int) {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if perHostConcurrency <= 0 {
		perHostConcurrency = DefaultPerHostConcurrency
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.interval, p.perHostConcurrency = interval, perHostConcurrency
}

// SetTargets replaces the poll set (the reconciler calls this). An observation
// is dropped when its source is no longer targeted *or* when the target's
// plumbing changed: a GitRepository flipped from refs/heads/main to
// refs/tags/v2 (or repointed at another URL) must not present main's HEAD as a
// current observation, because that SHA would be pinned under the
// observed-SHAs-only invariant (DESIGN §3.1).
func (p *Poller) SetTargets(targets []Target) {
	live := make(map[types.NamespacedName]targetRef, len(targets))
	for _, t := range targets {
		live[t.Source] = targetRefOf(t)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.targets = slices.Clone(targets)
	p.live = live
	maps.DeleteFunc(p.observations, func(src types.NamespacedName, rec record) bool {
		current, ok := live[src]
		return !ok || current != rec.ref
	})
}

// Observation returns the latest observation for a source, if any.
func (p *Poller) Observation(src types.NamespacedName) (Observation, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	rec, ok := p.observations[src]
	return rec.obs, ok
}

// Start blocks until ctx is done, sweeping every configured interval. It
// implements manager.Runnable.
func (p *Poller) Start(ctx context.Context) error {
	interval := p.sweepInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.sweep(ctx)
			// The reconciler may have re-Configured us mid-sweep.
			if current := p.sweepInterval(); current != interval {
				interval = current
				ticker.Reset(interval)
			}
		}
	}
}

// sweepInterval reports the configured sweep cadence.
func (p *Poller) sweepInterval() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.interval
}

// sweep lists every target once, batched per git host with bounded per-host
// concurrency, then notifies exactly once (the reconciler coalesces).
func (p *Poller) sweep(ctx context.Context) {
	p.mu.RLock()
	targets := slices.Clone(p.targets)
	perHost := p.perHostConcurrency
	p.mu.RUnlock()

	byHost := make(map[string][]Target)
	for _, t := range targets {
		host := hostOf(t.URL)
		byHost[host] = append(byHost[host], t)
	}

	var hosts sync.WaitGroup
	for host, hostTargets := range byHost {
		hosts.Go(func() {
			p.sweepHost(ctx, host, hostTargets, perHost)
		})
	}
	hosts.Wait()

	if p.notify != nil {
		p.notify()
	}
}

// sweepHost polls one host's targets, at most perHost at a time.
func (p *Poller) sweepHost(ctx context.Context, host string, targets []Target, perHost int) {
	semaphore := make(chan struct{}, perHost)

	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Go(func() {
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				return
			}

			p.poll(ctx, host, t)
		})
	}
	wg.Wait()
}

// poll performs one target's listing and folds the result into the store.
func (p *Poller) poll(ctx context.Context, host string, t Target) {
	sha, err := p.observe(ctx, t)
	now := time.Now()

	if err != nil {
		p.recordFailure(t, now, err)
		p.countFailure(host)
		return
	}
	p.recordSuccess(t, sha, now)
}

// current reports whether t is still exactly what its source is polled with.
// Callers must hold p.mu. A listing that started before a SetTargets which
// repointed (or removed) the target is answering a question nobody is asking
// any more, and must not be stored.
func (p *Poller) current(t Target) bool {
	live, ok := p.live[t.Source]
	return ok && live == targetRefOf(t)
}

// observe resolves credentials afresh, lists the advertisement and selects the
// candidate SHA for the target's tracking ref.
func (p *Poller) observe(ctx context.Context, t Target) (string, error) {
	data, err := p.secretData(ctx, t)
	if err != nil {
		return "", err
	}

	auth, err := AuthFromSecret(t.URL, data)
	if err != nil {
		return "", err
	}

	advertised, err := p.lister.List(ctx, t.URL, auth)
	if err != nil {
		return "", err
	}

	sha, ok := p.strategy.Candidate(advertised, t.TrackingRef)
	if !ok {
		return "", fmt.Errorf("tracking ref %q is not advertised", t.TrackingRef)
	}
	return sha, nil
}

// secretData reads the target's credentials, freshly on every sweep because
// credentials rotate.
func (p *Poller) secretData(ctx context.Context, t Target) (map[string][]byte, error) {
	if t.SecretRef == nil {
		return nil, nil
	}
	if p.secrets == nil {
		return nil, fmt.Errorf("secret %s is referenced but no secret reader is configured", t.SecretRef)
	}

	var secret corev1.Secret
	if err := p.secrets.Get(ctx, *t.SecretRef, &secret); err != nil {
		return nil, fmt.Errorf("getting secret %s: %w", t.SecretRef, err)
	}
	return secret.Data, nil
}

func (p *Poller) recordSuccess(t Target, sha string, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.current(t) {
		return
	}

	rec := p.observations[t.Source]
	if rec.obs.SHA != sha {
		rec.obs.FirstObserved = now
	}
	rec.ref = targetRefOf(t)
	rec.obs.SHA, rec.obs.ObservedAt, rec.obs.Err = sha, now, nil
	p.observations[t.Source] = rec
}

// recordFailure surfaces the error without ever clearing the last good SHA:
// a frozen-at-known-good observation is the fail-closed behaviour (DESIGN D4).
func (p *Poller) recordFailure(t Target, now time.Time, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.current(t) {
		return
	}

	rec := p.observations[t.Source]
	rec.ref = targetRefOf(t)
	rec.obs.ObservedAt, rec.obs.Err = now, err
	p.observations[t.Source] = rec
}

func (p *Poller) countFailure(host string) {
	if p.failures == nil {
		return
	}
	if host == "" {
		host = unknownHost
	}
	p.failures.WithLabelValues(host).Inc()
}

// hostOf groups targets by git host. An unparseable URL groups under the empty
// host and fails at auth conversion, which reports the parse error properly.
func hostOf(repoURL string) string {
	u, err := url.Parse(repoURL)
	if err != nil {
		return ""
	}
	return u.Host
}
