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

package snapshot

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/selection"
)

// DefaultPollTimeout bounds one ref listing (plan B3, --poll-timeout).
const DefaultPollTimeout = 30 * time.Second

// unknownHost groups targets whose URL yields no host, so they are still
// concurrency-bounded rather than fanned out without limit.
const unknownHost = "unknown"

// PollOptions turns on live ref-advertisement listing for DeriveSource.
//
// Polling from a CLI is opt-in because it costs credentials: it reads each
// source's Secret and speaks to every git host in the fleet from wherever the
// operator is sitting (plan B3, the derive+poll RBAC tier). Without it the
// derived picture is honest but blind — nothing can be pending.
type PollOptions struct {
	// Timeout bounds one listing; <= 0 means DefaultPollTimeout.
	Timeout time.Duration
	// PerHostConcurrency bounds concurrent listings per git host, the same
	// courtesy the controller extends; <= 0 means the CRD default. The CLI
	// passes the Wavefront's own value.
	PerHostConcurrency int
	// Lister lists advertised refs; nil means the production go-git lister,
	// which fetches no objects and touches no disk (DESIGN §3.1.1).
	Lister gitpoll.Lister
	// Strategy selects the candidate SHA from an advertisement; nil means the
	// v1 default, TrackRef. It must be the same strategy the evaluation uses,
	// or the candidate and the tracking ref would come from different
	// policies.
	Strategy selection.Strategy
}

// Observe lists every target's advertised refs once and returns the
// observations the evaluation should run against.
//
// It never returns an error. A CLI that refuses to report anything because
// one of forty sources has an expired deploy key is useless in the incident
// it exists for: each failure becomes a Diagnostic and leaves that target
// unobserved, which every renderer already knows how to mark (plan B3).
//
// now stamps both ObservedAt and FirstObserved. There is no history to draw a
// real first-observation from — this is a single sweep, not a running poller —
// so a pending node's wait measures from this run, which is why derived LAG is
// rendered as a lower bound (plan B3).
func Observe(
	ctx context.Context,
	r client.Reader,
	targets []gitpoll.Target,
	opts *PollOptions,
	now time.Time,
) (map[types.NamespacedName]gitpoll.Observation, []string) {
	if len(targets) == 0 {
		return nil, nil
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultPollTimeout
	}
	perHost := opts.PerHostConcurrency
	if perHost <= 0 {
		perHost = gitpoll.DefaultPerHostConcurrency
	}
	lister := opts.Lister
	if lister == nil {
		lister = gitpoll.NewGoGitLister(timeout)
	}
	strategy := opts.Strategy
	if strategy == nil {
		strategy = selection.TrackRef()
	}

	// One Secret memo for the whole sweep: a fleet routinely shares one
	// deploy-key Secret across every source, and reading it once per target
	// would turn a single expired key into forty identical diagnostics.
	creds := &secretCache{reader: r, entries: map[types.NamespacedName]*secretEntry{}}

	byHost := map[string][]gitpoll.Target{}
	for _, target := range targets {
		byHost[hostOf(target.URL)] = append(byHost[hostOf(target.URL)], target)
	}

	var (
		mu           sync.Mutex
		observations = map[types.NamespacedName]gitpoll.Observation{}
		diags        []string
	)

	var hosts sync.WaitGroup
	for _, hostTargets := range byHost {
		hosts.Go(func() {
			semaphore := make(chan struct{}, perHost)
			var wg sync.WaitGroup
			for _, target := range hostTargets {
				wg.Go(func() {
					select {
					case semaphore <- struct{}{}:
						defer func() { <-semaphore }()
					case <-ctx.Done():
						return
					}

					sha, err := observe(ctx, target, creds, lister, strategy)
					mu.Lock()
					defer mu.Unlock()
					if err != nil {
						diags = append(diags, fmt.Sprintf(
							"polling %s failed: %v; it is reported unobserved", target.Source, err))
						return
					}
					observations[target.Source] = gitpoll.Observation{
						SHA:           sha,
						ObservedAt:    now,
						FirstObserved: now,
						// Stamped with the plumbing the SHA was observed
						// against: inputs.Build rejects an observation whose
						// URL or tracking ref no longer matches the source.
						URL:         target.URL,
						TrackingRef: target.TrackingRef,
					}
				})
			}
			wg.Wait()
		})
	}
	hosts.Wait()

	// Sorted so a snapshot of the same cluster reads the same twice over,
	// whatever order the listings happened to finish in.
	slices.Sort(diags)
	if len(observations) == 0 {
		observations = nil
	}
	return observations, diags
}

// observe performs one target's listing and selects its candidate SHA. A
// tracking ref the remote does not advertise is not an error to shout about
// twice: the source is simply unobserved, reported as such.
func observe(
	ctx context.Context,
	target gitpoll.Target,
	creds *secretCache,
	lister gitpoll.Lister,
	strategy selection.Strategy,
) (string, error) {
	var data map[string][]byte
	if target.SecretRef != nil {
		var err error
		if data, err = creds.get(ctx, *target.SecretRef); err != nil {
			return "", err
		}
	}

	auth, err := gitpoll.AuthFromSecret(target.URL, data)
	if err != nil {
		// The URL is deliberately absent from the message: it may carry
		// embedded credentials.
		return "", fmt.Errorf("building credentials: %w", err)
	}

	advertised, err := lister.List(ctx, target.URL, auth)
	if err != nil {
		return "", fmt.Errorf("listing refs: %w", err)
	}

	sha, ok := strategy.Candidate(advertised, target.TrackingRef)
	if !ok {
		return "", fmt.Errorf("tracking ref %s is not advertised", target.TrackingRef)
	}
	return sha, nil
}

// secretCache reads each credential Secret at most once per sweep. Its
// contents never leave this package: only the transport auth built from them
// does, and neither ever reaches a Snapshot.
type secretCache struct {
	reader client.Reader

	mu      sync.Mutex
	entries map[types.NamespacedName]*secretEntry
}

type secretEntry struct {
	once sync.Once
	data map[string][]byte
	err  error
}

func (c *secretCache) get(ctx context.Context, ref types.NamespacedName) (map[string][]byte, error) {
	c.mu.Lock()
	entry, ok := c.entries[ref]
	if !ok {
		entry = &secretEntry{}
		c.entries[ref] = entry
	}
	c.mu.Unlock()

	entry.once.Do(func() {
		var secret corev1.Secret
		if err := c.reader.Get(ctx, ref, &secret); err != nil {
			// Named, not dumped: the operator needs to know which Secret to
			// get access to, and nothing more.
			entry.err = fmt.Errorf("reading credential Secret %s: %w", ref, err)
			return
		}
		entry.data = secret.Data
	})
	return entry.data, entry.err
}

// hostOf extracts a URL's host for concurrency batching. It is a grouping key
// only, so an unparseable URL falls into one shared bucket rather than
// failing the sweep.
func hostOf(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return unknownHost
	}
	return parsed.Host
}
