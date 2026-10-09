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
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/isometry/wavefront-controller/internal/gitpoll"
)

// DefaultPollTimeout bounds one ref listing (the --poll-timeout default).
const DefaultPollTimeout = 30 * time.Second

// PollOptions turns on live ref-advertisement listing for DeriveSource.
//
// Polling from a CLI is opt-in because it costs credentials: it reads each
// source's Secret and speaks to every git host in the fleet from wherever the
// operator is sitting, which is why it needs the wider derive+poll RBAC tier
// rather than the read-only viewer one. Without it the derived picture is
// honest but blind — nothing can be pending.
type PollOptions struct {
	// Timeout bounds one listing; <= 0 means DefaultPollTimeout.
	Timeout time.Duration
	// PerHostConcurrency bounds concurrent listings per git host, the same
	// courtesy the controller extends; <= 0 means the CRD default. The CLI
	// passes the Wavefront's own value.
	PerHostConcurrency int
	// Lister lists advertised refs; nil means the production go-git lister,
	// which fetches no objects and touches no disk.
	Lister gitpoll.Lister
}

// Observe lists every target's advertised refs once and returns the
// observations the evaluation should run against.
//
// It never returns an error. A CLI that refuses to report anything because
// one of forty sources has an expired deploy key is useless in the incident
// it exists for: each failure becomes a Diagnostic and leaves that target
// unobserved, which every renderer already knows how to mark.
//
// now stamps both ObservedAt and FirstObserved. There is no history to draw a
// real first-observation from — this is a single sweep, not a running poller —
// so a pending node's wait measures from this run, which is why derived LAG is
// rendered as a lower bound.
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

	lister := opts.Lister
	if lister == nil {
		timeout := opts.Timeout
		if timeout <= 0 {
			timeout = DefaultPollTimeout
		}
		lister = gitpoll.NewGoGitLister(timeout)
	}

	// No credential-failure counter: a CLI run records no metrics, and each
	// failure surfaces as a diagnostic instead.
	results := gitpoll.Sweep(ctx, r, lister, strategy, targets, opts.PerHostConcurrency, nil)

	observations := map[types.NamespacedName]gitpoll.Observation{}
	var diags []string
	for _, res := range results {
		if res.Err != nil {
			diags = append(diags, fmt.Sprintf(
				"polling %s failed: %v; it is reported unobserved", res.Target.Source, res.Err))
			continue
		}
		observations[res.Target.Source] = gitpoll.Observation{
			SHA:           res.SHA,
			ObservedAt:    now,
			FirstObserved: now,
			// Stamped with the plumbing the SHA was observed against:
			// inputs.Build rejects an observation whose URL or tracking ref
			// no longer matches the source.
			URL:         res.Target.URL,
			TrackingRef: res.Target.TrackingRef,
		}
	}

	// Sorted so a snapshot of the same cluster reads the same twice over,
	// whatever order the listings happened to finish in.
	slices.Sort(diags)
	if len(observations) == 0 {
		observations = nil
	}
	return observations, diags
}
