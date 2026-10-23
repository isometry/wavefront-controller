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
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/engine"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/pin"
)

const (
	pinA = "aaaa111000000000000000000000000000000000"
	pinB = "bbbb111000000000000000000000000000000000"

	nodeStale   = "stale"
	nodeUnready = "unready"

	// The engine states these tests assert, as the strings a NodeView carries.
	stateSettled    = string(engine.StateSettled)
	stateConverging = string(engine.StateConverging)
)

// unknownFleet holds three pinned nodes, each on its own source and none
// held: "ok" is Ready and applied at its pin, "stale" is Ready but applied
// elsewhere, and "unready" has no Ready condition at all (not Failing either).
func unknownFleet(t *testing.T) *DeriveSource {
	t.Helper()

	sch := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		wavefrontv1alpha1.AddToScheme, sourcev1.AddToScheme, kustomizev1.AddToScheme,
	} {
		if err := add(sch); err != nil {
			t.Fatalf("building scheme: %v", err)
		}
	}

	const selectorKey = "fleet"
	objs := make([]runtime.Object, 0, 7) // the Wavefront plus a source and a Kustomization per node
	objs = append(objs, &wavefrontv1alpha1.Wavefront{
		Name: wfName,
		Spec: wavefrontv1alpha1.WavefrontSpec{
			Nodes: wavefrontv1alpha1.NodesSpec{
				Kinds:    []string{kustomizev1.KustomizationKind},
				Selector: metav1.LabelSelector{MatchLabels: map[string]string{selectorKey: "yes"}},
			},
			Mode: wavefrontv1alpha1.ModeEnforce,
		},
	})
	for _, n := range []struct {
		name, applied string
		ready         bool
	}{
		{"ok", pinA, true},
		{nodeStale, pinB, true},
		{nodeUnready, pinA, false},
	} {
		ks := &kustomizev1.Kustomization{
			Namespace: nsFlux, Name: n.name, Labels: map[string]string{selectorKey: "yes"},
			Spec: kustomizev1.KustomizationSpec{
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: sourcev1.GitRepositoryKind, Name: n.name, Namespace: nsFlux,
				},
			},
			Status: kustomizev1.KustomizationStatus{LastAppliedRevision: "main@sha1:" + n.applied},
		}
		if n.ready {
			ks.Status.Conditions = []metav1.Condition{{Type: fluxmeta.ReadyCondition, Status: metav1.ConditionTrue}}
		}
		objs = append(objs,
			&sourcev1.GitRepository{
				Namespace: nsFlux, Name: n.name, Labels: map[string]string{pin.ManagedLabel: "true"},
				Spec: sourcev1.GitRepositorySpec{
					URL:       testRepoURL,
					Reference: &sourcev1.GitRepositoryRef{Branch: "main", Commit: pinA},
				},
			}, ks)
	}

	return &DeriveSource{Reader: fake.NewClientBuilder().WithScheme(sch).WithRuntimeObjects(objs...).Build()}
}

func states(t *testing.T, d *DeriveSource) (map[string]string, bool) {
	t.Helper()
	snap, err := d.Capture(t.Context())
	if err != nil {
		t.Fatalf("Capture() returned %v", err)
	}
	got := map[string]string{}
	for _, n := range snap.Nodes {
		got[n.Ref.Name] = n.State
	}
	return got, snap.Observed
}

// With no observation set the engine's "Converging" for a Ready node applied at
// its pin only means nothing was observed; any other cause stays Converging.
func TestBlindDeriveReportsUnknown(t *testing.T) {
	got, observed := states(t, unknownFleet(t))
	if observed {
		t.Fatal("a blind derive must report Observed=false")
	}
	want := map[string]string{"ok": StateUnknown, nodeStale: stateConverging, nodeUnready: stateConverging}
	for name, state := range want {
		if got[name] != state {
			t.Errorf("%s: want %s, got %s (all: %v)", name, state, got[name], got)
		}
	}
}

// Once observations are supplied the engine's verdict stands, untouched.
func TestObservedDeriveKeepsEngineVerdict(t *testing.T) {
	d := unknownFleet(t)
	now := time.Now()
	d.Observations = map[types.NamespacedName]gitpoll.Observation{}
	for _, name := range []string{"ok", nodeStale, nodeUnready} {
		d.Observations[types.NamespacedName{Namespace: nsFlux, Name: name}] = gitpoll.Observation{
			SHA: pinA, ObservedAt: now, FirstObserved: now, URL: testRepoURL, TrackingRef: refMain,
		}
	}

	got, observed := states(t, d)
	if !observed {
		t.Fatal("an injected observation set must report Observed=true")
	}
	want := map[string]string{"ok": stateSettled, nodeStale: stateConverging, nodeUnready: stateConverging}
	for name, state := range want {
		if got[name] != state {
			t.Errorf("%s: want %s, got %s (all: %v)", name, state, got[name], got)
		}
	}
}
