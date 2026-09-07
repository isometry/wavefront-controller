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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing/transport"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/isometry/wavefront-controller/internal/gitpoll"
)

// fakeLister answers ref advertisements from a script, so a poll can be
// exercised without a git host.
type fakeLister struct {
	refs map[string]map[string]string
	fail map[string]error
}

func (f *fakeLister) List(_ context.Context, repoURL string, _ transport.AuthMethod) (map[string]string, error) {
	if err, failing := f.fail[repoURL]; failing {
		return nil, err
	}
	return f.refs[repoURL], nil
}

const shaObserved = "aaa"

func target(name, host, ref string, secret bool) gitpoll.Target {
	t := gitpoll.Target{
		Source:      types.NamespacedName{Namespace: nsFlux, Name: name},
		URL:         "https://" + host + "/org/" + name + ".git",
		TrackingRef: ref,
	}
	if secret {
		t.SecretRef = &types.NamespacedName{Namespace: nsFlux, Name: secretName}
	}
	return t
}

func TestObserve(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFlux, Name: secretName},
		Data:       map[string][]byte{"username": []byte("bot"), "password": []byte("hunter2")},
	}
	reader := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(secret).Build()

	good := target("apps", "git.example.com", refMain, true)
	unadvertised := target("infra", "git.example.com", "refs/heads/release", true)
	failing := target("legacy", "other.example.com", refMain, true)
	noSecret := target("public", "git.example.com", refMain, false)
	missingSecret := gitpoll.Target{
		Source:      types.NamespacedName{Namespace: nsFlux, Name: "orphan"},
		URL:         "https://git.example.com/org/orphan.git",
		TrackingRef: refMain,
		SecretRef:   &types.NamespacedName{Namespace: nsFlux, Name: "absent"},
	}

	lister := &fakeLister{
		refs: map[string]map[string]string{
			good.URL:         {refMain: shaObserved},
			unadvertised.URL: {refMain: "bbb"},
			noSecret.URL:     {refMain + "^{}": "ccc", refMain: "ddd"},
			missingSecret.URL: {
				refMain: "eee",
			},
		},
		fail: map[string]error{failing.URL: errors.New("dial tcp: connection refused")},
	}

	targets := []gitpoll.Target{good, unadvertised, failing, noSecret, missingSecret}
	observations, diags := Observe(context.Background(), reader, targets,
		&PollOptions{Lister: lister, PerHostConcurrency: 2}, now)

	// Only the two reachable, advertised targets are observed; the rest are
	// left unobserved rather than guessed at.
	if len(observations) != 2 {
		t.Fatalf("Observe() returned %d observations, want 2: %v", len(observations), observations)
	}

	obs, ok := observations[good.Source]
	if !ok {
		t.Fatal("Observe() did not observe the healthy target")
	}
	if obs.SHA != shaObserved {
		t.Errorf("observed SHA = %q, want aaa", obs.SHA)
	}
	// Stamped with the plumbing observed against, or inputs.Build would
	// reject it as predating a repo edit.
	if obs.URL != good.URL || obs.TrackingRef != good.TrackingRef {
		t.Errorf("observation plumbing = %q/%q, want %q/%q", obs.URL, obs.TrackingRef, good.URL, good.TrackingRef)
	}
	if !obs.ObservedAt.Equal(now) || !obs.FirstObserved.Equal(now) {
		t.Errorf("observation times = %v/%v, want both %v", obs.ObservedAt, obs.FirstObserved, now)
	}

	// A peeled entry wins: an annotated tag's commit is what can be pinned.
	if got := observations[noSecret.Source].SHA; got != "ccc" {
		t.Errorf("peeled candidate = %q, want ccc", got)
	}

	// Three failures, three diagnostics, and never an error.
	if len(diags) != 3 {
		t.Fatalf("Observe() returned %d diagnostics, want 3: %v", len(diags), diags)
	}
	joined := strings.Join(diags, "\n")
	for _, want := range []string{nsFlux + "/infra", nsFlux + "/legacy", nsFlux + "/orphan"} {
		if !strings.Contains(joined, want) {
			t.Errorf("diagnostics do not name %s:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "hunter2") {
		t.Errorf("diagnostics leaked credential material:\n%s", joined)
	}
}

func TestObserveWithoutTargets(t *testing.T) {
	observations, diags := Observe(context.Background(), nil, nil, &PollOptions{}, time.Now())
	if observations != nil || diags != nil {
		t.Fatalf("Observe() with no targets = %v, %v; want nil, nil", observations, diags)
	}
}

func TestSecretCacheReadsOncePerSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFlux, Name: secretName},
		Data:       map[string][]byte{"username": []byte("bot"), "password": []byte("hunter2")},
	}
	reader := &countingReader{Reader: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(secret).Build()}

	// A fleet routinely shares one deploy-key Secret across every source; the
	// memo is what keeps one expired key from becoming forty diagnostics.
	targets := []gitpoll.Target{
		target("a", "git.example.com", refMain, true),
		target("b", "git.example.com", refMain, true),
		target("c", "git.example.com", refMain, true),
	}
	lister := &fakeLister{refs: map[string]map[string]string{}}
	for _, tgt := range targets {
		lister.refs[tgt.URL] = map[string]string{refMain: shaObserved}
	}

	observations, diags := Observe(context.Background(), reader, targets,
		&PollOptions{Lister: lister}, time.Now())

	if len(observations) != 3 || len(diags) != 0 {
		t.Fatalf("Observe() = %d observations, %v diagnostics; want 3, none", len(observations), diags)
	}
	if got := reader.count(); got != 1 {
		t.Fatalf("credential Secret read %d times, want 1", got)
	}
}

// countingReader counts Get calls, so the Secret memo can be proven rather
// than assumed.
type countingReader struct {
	client.Reader

	mu   sync.Mutex
	gets int
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.mu.Lock()
	r.gets++
	r.mu.Unlock()
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r *countingReader) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gets
}
