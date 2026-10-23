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

package cli

import (
	"context"
	"fmt"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// requestCounts tallies the requests a reader receives, keyed by Go type.
type requestCounts struct{ gets, lists map[string]int }

// deriveFleet builds a fake cluster with n managed GitRepositories, n selected
// Kustomizations (one per repo) in flux-system, and n/10 unselected
// Kustomizations in "infra" that the selected ones dependsOn, and returns a
// reader counting every Get and List it serves.
func deriveFleet(t *testing.T, n int) (client.Reader, *requestCounts) {
	t.Helper()
	const selectorKey = "fleet"
	objs := make([]client.Object, 0, 1+n/10+2*n)
	objs = append(objs, &wavefrontv1alpha1.Wavefront{
		Name: "w",
		Spec: wavefrontv1alpha1.WavefrontSpec{
			Nodes: wavefrontv1alpha1.NodesSpec{
				Kinds:    []string{kustomizev1.KustomizationKind},
				Selector: metav1.LabelSelector{MatchLabels: map[string]string{selectorKey: valYes}},
			},
			Mode: wavefrontv1alpha1.ModeShadow,
		},
	})
	for i := range n / 10 {
		objs = append(objs, &kustomizev1.Kustomization{
			Namespace: nsInfra, Name: fmt.Sprintf("infra-%d", i),
		})
	}
	for i := range n {
		name := fmt.Sprintf("app-%d", i)
		objs = append(objs,
			&sourcev1.GitRepository{
				Namespace: nsFlux, Name: name,
				Labels: map[string]string{pin.ManagedLabel: valTrue},
				Spec: sourcev1.GitRepositorySpec{
					URL:       "https://git.example.com/" + name + ".git",
					Reference: &sourcev1.GitRepositoryRef{Branch: "main"},
				},
			},
			&kustomizev1.Kustomization{
				Namespace: nsFlux, Name: name,
				Labels: map[string]string{selectorKey: valYes},
				Spec: kustomizev1.KustomizationSpec{
					SourceRef: kustomizev1.CrossNamespaceSourceReference{
						Kind: sourcev1.GitRepositoryKind, Name: name, Namespace: nsFlux,
					},
					DependsOn: []kustomizev1.DependencyReference{{Namespace: nsInfra, Name: fmt.Sprintf("infra-%d", i%(n/10))}},
				},
			})
	}

	counts := &requestCounts{gets: map[string]int{}, lists: map[string]int{}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				counts.gets[fmt.Sprintf("%T", obj)]++
				return c.Get(ctx, key, obj, opts...)
			},
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				counts.lists[fmt.Sprintf("%T", list)]++
				return c.List(ctx, list, opts...)
			},
		}).Build()
	return c, counts
}

func deriveWith(t *testing.T, r client.Reader, n int) *snapshot.Snapshot {
	t.Helper()
	snap, err := (&snapshot.DeriveSource{Reader: r}).Capture(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// A derivation that silently resolved nothing would also make no requests.
	if len(snap.Sources) != n {
		t.Fatalf("want %d sources, got %d", n, len(snap.Sources))
	}
	return snap
}

// --derive must cost O(1) API requests, not one live Get per source and per
// out-of-selector dependency: that is what made `wfctl nodes` take ~37s at
// client QPS 5.
func TestDeriveRequestsAreConstantThroughListReader(t *testing.T) {
	const n = 40
	const (
		repoGets = "*v1.GitRepository"
		ksGets   = "*v1.Kustomization"
	)

	// (a) The raw reader is the baseline: one Get per GitRepository, and one
	// per distinct out-of-selector dependency.
	raw, counts := deriveFleet(t, n)
	deriveWith(t, raw, n)
	if got := counts.gets[repoGets]; got < n {
		t.Fatalf("baseline: want >= %d GitRepository Gets, got %d", n, got)
	}
	if got := counts.gets[ksGets]; got != n/10 {
		t.Fatalf("baseline: want %d Kustomization Gets (dependsOn closure), got %d", n/10, got)
	}
	t.Logf("raw reader: Gets=%v Lists=%v", counts.gets, counts.lists)

	// (b) Through the listReader nothing is fetched per object.
	inner, counts := deriveFleet(t, n)
	deriveWith(t, newListReader(inner), n)
	t.Logf("listReader: Gets=%v Lists=%v", counts.gets, counts.lists)
	if counts.gets[repoGets] != 0 || counts.gets[ksGets] != 0 {
		t.Fatalf("want 0 GitRepository/Kustomization Gets, got %v", counts.gets)
	}
	// The exact Lists, each independent of n:
	//   Wavefront:     1 (SelectWavefront) + 1 (inputs.detectOverlap)
	//   Kustomization: 1 (the selector List, passed through) + 1 ("infra",
	//                  memoised, serving every closure Get)
	//   GitRepository: 1 ("flux-system", serving every source Get)
	want := map[string]int{"*v1alpha1.WavefrontList": 2, "*v1.KustomizationList": 2, "*v1.GitRepositoryList": 1}
	if len(counts.lists) != len(want) {
		t.Fatalf("want Lists %v, got %v", want, counts.lists)
	}
	for k, v := range want {
		if counts.lists[k] != v {
			t.Fatalf("want Lists %v, got %v", want, counts.lists)
		}
	}
	// ponytail: Poll is not covered; its observations need a git host. Under
	// Poll the build runs twice, but the memo makes the second pass free.
}
