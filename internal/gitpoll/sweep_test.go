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
	"maps"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/types"

	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/selection"
)

func withSecret(t gitpoll.Target, ref types.NamespacedName) gitpoll.Target {
	t.SecretRef = &ref
	return t
}

func TestSweepBoundsPerHostConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const perHost = 2

		lister := newFakeLister()
		lister.delay = listDelay

		targets := make([]gitpoll.Target, 0, 6)
		for i := range 5 {
			u := "https://" + exampleHost + "/org/repo" + string(rune('a'+i)) + ".git"
			lister.setAdvertised(u, map[string]string{trackedRef: shaA})
			targets = append(targets, target("repo"+string(rune('a'+i)), u))
		}
		lister.setAdvertised(otherURL, map[string]string{trackedRef: shaA})
		targets = append(targets, target("gamma", otherURL))

		results := gitpoll.Sweep(t.Context(), nil, lister, selection.TrackRef(), targets, perHost, nil)

		if len(results) != len(targets) {
			t.Fatalf("Sweep returned %d results, want %d", len(results), len(targets))
		}
		for _, r := range results {
			if r.Err != nil || r.SHA != shaA || r.At.IsZero() {
				t.Errorf("result for %s = %+v, want SHA %s, no error, completion time set", r.Target.Source, r, shaA)
			}
		}
		if got := lister.peak(exampleHost); got != perHost {
			t.Errorf("peak concurrent listings for %s = %d, want %d", exampleHost, got, perHost)
		}
		if got := lister.peak(otherHost); got != 1 {
			t.Errorf("peak concurrent listings for %s = %d, want 1", otherHost, got)
		}
	})
}

func TestSweepMemoizesCredentialsPerSweep(t *testing.T) {
	shared := types.NamespacedName{Namespace: testNamespace, Name: "deploy-key"}
	broken := types.NamespacedName{Namespace: testNamespace, Name: "broken"}

	secrets := newFakeSecrets()
	secrets.set(shared, map[string][]byte{"username": []byte("bot"), "password": []byte("pw")})
	secrets.setErr(broken, errSecretReadFailed)

	lister := newFakeLister()
	lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
	lister.setAdvertised(betaURL, map[string]string{trackedRef: shaB})

	targets := []gitpoll.Target{
		withSecret(target("alpha", alphaURL), shared),
		withSecret(target("beta", betaURL), shared),
		withSecret(target("gamma", otherURL), broken),
		withSecret(target("delta", otherURL), broken),
	}
	failures := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_credential_failures_total"})

	results := gitpoll.Sweep(t.Context(), secrets, lister, selection.TrackRef(), targets, 0, failures)

	if got := secrets.getCount(shared); got != 1 {
		t.Errorf("shared Secret read %d times, want 1", got)
	}
	if got := secrets.getCount(broken); got != 1 {
		t.Errorf("broken Secret read %d times, want 1", got)
	}
	if got := testutil.ToFloat64(failures); got != 1 {
		t.Errorf("credential failures = %v, want 1 (once per distinct ref)", got)
	}

	var errored int
	for _, r := range results {
		if r.Err != nil {
			errored++
		}
	}
	if len(results) != 4 || errored != 2 {
		t.Errorf("Sweep returned %d results with %d errors, want 4 with 2", len(results), errored)
	}
}

func TestSweepWithoutSecretReaderFailsCleanly(t *testing.T) {
	lister := newFakeLister()
	lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
	tgt := withSecret(target("alpha", alphaURL), source("deploy-key"))

	results := gitpoll.Sweep(t.Context(), nil, lister, selection.TrackRef(), []gitpoll.Target{tgt}, 0, nil)
	if len(results) != 1 || results[0].Err == nil || !strings.Contains(results[0].Err.Error(), "no secret reader") {
		t.Fatalf("Sweep with nil secret reader = %+v, want one no-secret-reader error", results)
	}

	if _, err := gitpoll.List(t.Context(), nil, lister, tgt); err == nil || !strings.Contains(err.Error(), "no secret reader") {
		t.Fatalf("List with nil secret reader err = %v, want a no-secret-reader error", err)
	}
	if got := lister.callCount(alphaURL); got != 0 {
		t.Errorf("lister called %d times, want 0", got)
	}
}

func TestSweepDropsListingsEndedByCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lister := newFakeLister()
		lister.setAdvertised(alphaURL, map[string]string{trackedRef: shaA})
		lister.gate(alphaURL)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan []gitpoll.Result)
		go func() {
			done <- gitpoll.Sweep(ctx, nil, lister, selection.TrackRef(), []gitpoll.Target{target("alpha", alphaURL)}, 0, nil)
		}()

		synctest.Wait() // the listing is now in flight, held by the gate
		cancel()

		if results := <-done; len(results) != 0 {
			t.Fatalf("Sweep after cancellation = %+v, want no results", results)
		}
	})
}

func TestSweepUnparseableURLStillYieldsAResult(t *testing.T) {
	bad := target("bad", "://not a url")
	results := gitpoll.Sweep(t.Context(), nil, newFakeLister(), selection.TrackRef(), []gitpoll.Target{bad}, 0, nil)
	if len(results) != 1 || results[0].Err == nil || results[0].Target != bad {
		t.Fatalf("Sweep of an unparseable URL = %+v, want one errored result", results)
	}
}

func TestListReturnsWholeAdvertisement(t *testing.T) {
	ref := source("deploy-key")
	secrets := newFakeSecrets()
	secrets.set(ref, map[string][]byte{"username": []byte("bot"), "password": []byte("pw")})

	advertised := map[string]string{trackedRef: shaA, tagRefV2: shaB}
	lister := newFakeLister()
	lister.setAdvertised(alphaURL, advertised)

	got, err := gitpoll.List(t.Context(), secrets, lister, withSecret(target("alpha", alphaURL), ref))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !maps.Equal(got, advertised) {
		t.Errorf("List = %v, want %v", got, advertised)
	}
	if lister.authSeen()[alphaURL] == nil {
		t.Error("List did not pass the Secret's credentials to the lister")
	}
}
