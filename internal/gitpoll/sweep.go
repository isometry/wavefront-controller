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
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/isometry/wavefront-controller/internal/selection"
)

// unknownHost groups targets whose URL yields no host, so they are still
// concurrency-bounded rather than fanned out without limit.
const unknownHost = "unknown"

// Result is one completed listing of a sweep.
type Result struct {
	Target Target
	SHA    string    // the tracking ref's candidate; "" when Err is set
	At     time.Time // when the listing completed
	Err    error
}

// credentialError marks a failure to read a target's credential Secret —
// an apiserver problem, not a git-host one: it must not be attributed to
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
		e.data, e.err = readSecret(ctx, s.reader, ref)
		// Counted here: once per distinct ref per sweep, and never for a
		// read that only failed because the sweep is shutting down.
		if e.err != nil && ctx.Err() == nil && s.failures != nil {
			s.failures.Inc()
		}
	})
	return e.data, e.err
}

func readSecret(ctx context.Context, reader client.Reader, ref types.NamespacedName) (map[string][]byte, error) {
	var secret corev1.Secret
	if err := reader.Get(ctx, ref, &secret); err != nil {
		return nil, &credentialError{err: fmt.Errorf("getting secret %s: %w", ref, err)}
	}
	return secret.Data, nil
}

// Sweep lists every target once, batched per git host with at most perHost
// listings in flight per host (perHost <= 0 means DefaultPerHostConcurrency),
// and selects each tracking ref's candidate under strategy. Credential Secrets
// are read at most once per distinct ref; credentialFailures (may be nil)
// counts the failed reads.
//
// Shutdown is judged by ctx.Err(), never by the error chain: go-git
// transports surface cancellation as EOF/closed-connection errors that never
// wrap context.Canceled, and a healthy sweep's error chain may still contain
// one that is not ours (e.g. an HTTP/2 stream reset). A listing that ended
// after ctx was cancelled answers no question anyone is still asking, so it
// yields no Result at all — callers never see a shutdown artefact. This does
// not swallow a genuine listing timeout: the lister's own per-listing timeout
// is a child context, so its expiry leaves ctx healthy.
func Sweep(
	ctx context.Context,
	secrets client.Reader,
	lister Lister,
	strategy selection.Strategy,
	targets []Target,
	perHost int,
	credentialFailures prometheus.Counter,
) []Result {
	if perHost <= 0 {
		perHost = DefaultPerHostConcurrency
	}

	byHost := make(map[string][]Target)
	for _, t := range targets {
		host := hostOf(t.URL)
		byHost[host] = append(byHost[host], t)
	}

	creds := &sweepSecrets{
		reader:   secrets,
		failures: credentialFailures,
		entries:  map[types.NamespacedName]*secretEntry{},
	}
	results := make(chan Result, len(targets))

	var hosts sync.WaitGroup
	for _, hostTargets := range byHost {
		hosts.Go(func() {
			semaphore := make(chan struct{}, perHost)
			var wg sync.WaitGroup
			for _, t := range hostTargets {
				wg.Go(func() {
					select {
					case semaphore <- struct{}{}:
						defer func() { <-semaphore }()
					case <-ctx.Done():
						return
					}

					sha, err := candidate(ctx, secrets, lister, strategy, t, creds)
					if ctx.Err() != nil {
						return
					}
					results <- Result{Target: t, SHA: sha, At: time.Now(), Err: err}
				})
			}
			wg.Wait()
		})
	}
	hosts.Wait()
	close(results)

	collected := make([]Result, 0, len(results))
	for r := range results {
		collected = append(collected, r)
	}
	return collected
}

// List advertises one target's refs, reading its credential Secret afresh.
func List(ctx context.Context, secrets client.Reader, lister Lister, t Target) (map[string]string, error) {
	return list(ctx, secrets, lister, t, nil)
}

func candidate(
	ctx context.Context,
	secrets client.Reader,
	lister Lister,
	strategy selection.Strategy,
	t Target,
	creds *sweepSecrets,
) (string, error) {
	advertised, err := list(ctx, secrets, lister, t, creds)
	if err != nil {
		return "", err
	}
	sha, ok := strategy.Candidate(advertised, t.TrackingRef)
	if !ok {
		return "", fmt.Errorf("tracking ref %q is not advertised", t.TrackingRef)
	}
	return sha, nil
}

// list resolves t's credentials (through creds when non-nil), builds the
// transport auth and lists the advertisement. The URL is deliberately absent
// from every error: it may carry embedded credentials.
func list(
	ctx context.Context,
	secrets client.Reader,
	lister Lister,
	t Target,
	creds *sweepSecrets,
) (map[string]string, error) {
	var data map[string][]byte
	if t.SecretRef != nil {
		if secrets == nil {
			return nil, fmt.Errorf("secret %s is referenced but no secret reader is configured", t.SecretRef)
		}
		var err error
		if creds != nil {
			data, err = creds.get(ctx, *t.SecretRef)
		} else {
			data, err = readSecret(ctx, secrets, *t.SecretRef)
		}
		if err != nil {
			return nil, err
		}
	}

	auth, err := AuthFromSecret(t.URL, data)
	if err != nil {
		return nil, fmt.Errorf("building credentials: %w", err)
	}
	return lister.List(ctx, t.URL, auth)
}

// hostOf groups targets by git host. An unparseable URL groups under
// unknownHost and fails at auth conversion, which reports the parse error.
func hostOf(repoURL string) string {
	u, err := url.Parse(repoURL)
	if err != nil || u.Host == "" {
		return unknownHost
	}
	return u.Host
}
