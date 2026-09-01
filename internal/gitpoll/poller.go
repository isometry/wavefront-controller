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

	// URL and TrackingRef record the plumbing the SHA was observed against,
	// so a consumer can reject an observation that predates a repo edit
	// (DESIGN §3.1: no stale candidate survives a plumbing change).
	URL         string
	TrackingRef string
}

// credentialError marks a failure to read a target's credential Secret —
// an apiserver problem, not a git-host one: poll must not attribute it to
// wavefront_ref_list_failures_total{host}.
type credentialError struct{ err error }

func (e *credentialError) Error() string { return e.err.Error() }
func (e *credentialError) Unwrap() error { return e.err }

// sweepSecrets memoizes credential Secret reads for one sweep: a fleet
// routinely shares one deploy-key Secret across hundreds of targets. The
// memo dies with the sweep, so credentials are still resolved afresh every
// sweep — once per distinct SecretRef instead of once per target.
type sweepSecrets struct {
	reader   client.Reader
	failures prometheus.Counter

	mu      sync.Mutex
	entries map[types.NamespacedName]*secretEntry
}

type secretEntry struct {
	once sync.Once
	data map[string][]byte
	err  error
}

func (s *sweepSecrets) get(ctx context.Context, ref types.NamespacedName) (map[string][]byte, error) {
	s.mu.Lock()
	e, ok := s.entries[ref]
	if !ok {
		e = &secretEntry{}
		s.entries[ref] = e
	}
	s.mu.Unlock()

	e.once.Do(func() {
		var secret corev1.Secret
		if err := s.reader.Get(ctx, ref, &secret); err != nil {
			e.err = &credentialError{err: fmt.Errorf("getting secret %s: %w", ref, err)}
			// Counted here, not in poll: once per distinct ref per sweep,
			// and never for a read that only failed because the sweep is
			// shutting down.
			if ctx.Err() == nil && s.failures != nil {
				s.failures.Inc()
			}
			return
		}
		e.data = secret.Data
	})
	return e.data, e.err
}

// Poller periodically sweeps all targets, batched per git host with bounded
// per-host concurrency (DESIGN §3.1, §4.1 poll.*). Implements manager.Runnable.
type Poller struct {
	secrets  client.Reader
	lister   Lister
	notify   func()
	strategy selection.Strategy

	// failures counts ref-listing failures per host
	// (wavefront_ref_list_failures_total, DESIGN §6); nil when the caller
	// supplied no counter, in which case nothing is recorded.
	failures *prometheus.CounterVec
	// credentialFailures counts credential Secret-read failures
	// (wavefront_credential_read_failures_total); nil when the caller
	// supplied no counter, in which case nothing is recorded. Never the same
	// signal as failures: a Secret-read failure is an apiserver problem, not
	// a git-host one (see credentialError).
	credentialFailures prometheus.Counter

	// reconfigured wakes a waiting Start when the sweep cadence changes, so a
	// shortened interval applies now rather than after the old one elapses.
	// Buffered and signalled without blocking: a pending wake is as good as
	// two, and Configure must never wait on the sweep loop.
	reconfigured chan struct{}

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

// withRef stamps rec's plumbing onto its Observation for external callers:
// the internal record and public Observation disagree on where the ref
// lives, and a consumer needs it on the Observation itself to judge
// staleness against its own, possibly newer, view of the target
// (DESIGN §3.1).
func withRef(rec record) Observation {
	obs := rec.obs
	obs.URL, obs.TrackingRef = rec.ref.url, rec.ref.trackingRef
	return obs
}

// result is one completed listing, held until the whole sweep publishes.
type result struct {
	target Target
	sha    string
	at     time.Time
	err    error
}

var _ manager.Runnable = (*Poller)(nil)

// NewPoller returns a Poller with the CRD's default cadence, ready for
// Configure and SetTargets.
//
// strategy is required: the poller consults only its Candidate method, while
// WavefrontReconciler.Strategy consults only TrackingRef — the two halves of
// one selection policy. The caller must construct a single strategy instance
// and share it between both, or a non-TrackRef strategy injected at one site
// would be silently half-applied (finding 10).
//
// failures is wavefront_ref_list_failures_total and credentialFailures is
// wavefront_credential_read_failures_total (DESIGN §6), both already created
// and registered by the caller — internal/metrics owns every collector's
// registration, so the poller only ever records against handles it is
// given. Both may be nil, in which case nothing is recorded.
func NewPoller(secrets client.Reader, lister Lister, notify func(), strategy selection.Strategy, failures *prometheus.CounterVec, credentialFailures prometheus.Counter) *Poller {
	return &Poller{
		secrets:            secrets,
		lister:             lister,
		notify:             notify,
		strategy:           strategy,
		failures:           failures,
		credentialFailures: credentialFailures,
		interval:           DefaultInterval,
		perHostConcurrency: DefaultPerHostConcurrency,
		reconfigured:       make(chan struct{}, 1),
		live:               map[types.NamespacedName]targetRef{},
		observations:       map[types.NamespacedName]record{},
	}
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
	changed := p.interval != interval
	p.interval, p.perHostConcurrency = interval, perHostConcurrency
	p.mu.Unlock()

	// Only an actual change wakes the sweep loop. Configure is called on every
	// reconcile, so signalling unconditionally would re-arm the timer faster
	// than it could ever expire and no sweep would run at all.
	if !changed {
		return
	}
	select {
	case p.reconfigured <- struct{}{}:
	default:
	}
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
	return withRef(rec), ok
}

// Observations returns a coherent point-in-time snapshot of every current
// observation: an independent copy of the whole store, taken under a single
// read lock.
//
// Coherence across sources is the contract, not an implementation detail. A
// sweep publishes all of its results at once (see publish), so a snapshot
// always reflects exactly one sweep — never a mixture of two. Callers that
// compare sources against one another depend on this: under the rolling
// admission rule a descendant is admitted when its ancestors are *settled*,
// and an ancestor whose observation lagged a sweep behind would look settled
// when it is not, mis-sequencing co-arriving changes (DESIGN §3.3). Reading
// source by source with Observation cannot provide that guarantee.
func (p *Poller) Observations() map[types.NamespacedName]Observation {
	p.mu.RLock()
	defer p.mu.RUnlock()

	observations := make(map[types.NamespacedName]Observation, len(p.observations))
	for src, rec := range p.observations {
		observations[src] = withRef(rec)
	}
	return observations
}

// Start blocks until ctx is done, sweeping every configured interval. It
// implements manager.Runnable.
//
// Each wait is computed as a deadline from the last sweep rather than from a
// fixed ticker, so a Configure that shortens the interval takes effect
// immediately instead of after the old — possibly far longer — interval
// elapses. Anchoring on the last sweep is also what keeps repeated
// reconfiguration from starving sweeps entirely: the deadline moves only with
// the interval, never with the number of times it is set.
func (p *Poller) Start(ctx context.Context) error {
	last := time.Now()

	for {
		timer := time.NewTimer(max(0, time.Until(last.Add(p.sweepInterval()))))

		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-p.reconfigured:
			// Re-arm against the new cadence.
			timer.Stop()
		case <-timer.C:
			p.sweep(ctx)
			last = time.Now()
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
// concurrency, publishes all results at once, then notifies exactly once (the
// reconciler coalesces).
//
// Results are collected rather than stored as they land: listings finish at
// wildly different times, and storing them one by one would leave the store in
// a mixture of this sweep and the last for the whole duration of the sweep —
// exactly the incoherence Observations promises callers is impossible.
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

	results := make(chan result, len(targets))

	// One memo for the whole sweep: a fleet routinely shares one deploy-key
	// Secret across hundreds of targets, and it dies with this sweep so
	// credentials are still resolved afresh every sweep (see sweepSecrets).
	creds := &sweepSecrets{
		reader:   p.secrets,
		failures: p.credentialFailures,
		entries:  map[types.NamespacedName]*secretEntry{},
	}

	var hosts sync.WaitGroup
	for host, hostTargets := range byHost {
		hosts.Go(func() {
			p.sweepHost(ctx, host, hostTargets, perHost, creds, results)
		})
	}
	hosts.Wait()
	close(results)

	p.publish(results)

	if p.notify != nil {
		p.notify()
	}
}

// sweepHost polls one host's targets, at most perHost at a time.
func (p *Poller) sweepHost(ctx context.Context, host string, targets []Target, perHost int, creds *sweepSecrets, results chan<- result) {
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

			p.poll(ctx, host, t, creds, results)
		})
	}
	wg.Wait()
}

// poll performs one target's listing and queues the outcome for publication.
//
// Shutdown is judged by ctx.Err(), never by the error chain: go-git
// transports surface cancellation as EOF/closed-connection errors that never
// wrap context.Canceled, and a healthy sweep's error chain may still contain
// one that is not ours (e.g. an HTTP/2 stream reset). ctx.Err() is the only
// authority. A cancelled context is the manager shutting the poller down, not
// a detection failure: it must neither blip wavefront_ref_list_failures_total
// (a §6 safety alarm operators rate-alert on) nor stamp a shutdown artefact
// onto the observation. Such a listing is discarded outright, exactly as a
// superseded target's is in publish — it answers no question anyone is still
// asking, and the last good observation stands untouched. This does not
// spuriously swallow a genuine listing timeout: the lister's own per-listing
// WithTimeout (lister.go:55-59) is a child context, so its expiry leaves the
// sweep ctx healthy and the failure is correctly still counted.
func (p *Poller) poll(ctx context.Context, host string, t Target, creds *sweepSecrets, results chan<- result) {
	sha, err := p.observe(ctx, t, creds)
	if ctx.Err() != nil {
		// The manager is shutting the poller down: whatever observe returned
		// answers no question anyone is still asking.
		return
	}
	if err != nil {
		var credErr *credentialError
		if !errors.As(err, &credErr) {
			p.countFailure(host)
		}
	}
	results <- result{target: t, sha: sha, at: time.Now(), err: err}
}

// publish folds a whole sweep's results into the store in one locked
// mutation, so the store only ever steps from one sweep to the next.
//
// Staleness is judged here rather than at listing time: a SetTargets that
// repointed a source while its listing was in flight must still discard the
// result, and by publication time the live plumbing is as current as it gets.
func (p *Poller) publish(results <-chan result) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for res := range results {
		if !p.current(res.target) {
			continue
		}

		rec := p.observations[res.target.Source]
		rec.ref = targetRefOf(res.target)

		if res.err != nil {
			// Never clear the last good SHA: a frozen-at-known-good
			// observation is the fail-closed behaviour (DESIGN D4).
			rec.obs.ObservedAt, rec.obs.Err = res.at, res.err
		} else {
			if rec.obs.SHA != res.sha {
				rec.obs.FirstObserved = res.at
			}
			rec.obs.SHA, rec.obs.ObservedAt, rec.obs.Err = res.sha, res.at, nil
		}

		p.observations[res.target.Source] = rec
	}
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
func (p *Poller) observe(ctx context.Context, t Target, creds *sweepSecrets) (string, error) {
	data, err := p.secretData(ctx, t, creds)
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

// secretData reads the target's credentials via creds, which memoizes the
// read for the whole sweep — freshly on every sweep because credentials
// rotate, but at most once per distinct SecretRef within one.
func (p *Poller) secretData(ctx context.Context, t Target, creds *sweepSecrets) (map[string][]byte, error) {
	if t.SecretRef == nil {
		return nil, nil
	}
	if p.secrets == nil {
		return nil, fmt.Errorf("secret %s is referenced but no secret reader is configured", t.SecretRef)
	}

	return creds.get(ctx, *t.SecretRef)
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
