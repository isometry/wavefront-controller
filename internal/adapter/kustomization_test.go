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

package adapter_test

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/isometry/wavefront-controller/internal/adapter"
)

const (
	kustomizationKind = "Kustomization"
	nsA               = "ns-a"
	appName           = "app"
	repoName          = "repo"
	managedLabelKey   = "wavefront.as-code.io/managed"
	managedLabelValue = "true"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := kustomizev1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding kustomizev1 to scheme: %v", err)
	}
	return scheme
}

func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).Build()
}

func readyCondition(status metav1.ConditionStatus) metav1.Condition {
	return metav1.Condition{
		Type:               fluxmeta.ReadyCondition,
		Status:             status,
		Reason:             "TestReason",
		Message:            "test message",
		LastTransitionTime: metav1.Now(),
	}
}

// sortedRefs returns a copy of refs sorted for order-independent comparison.
func sortedRefs(refs []adapter.NodeRef) []adapter.NodeRef {
	out := slices.Clone(refs)
	slices.SortFunc(out, func(a, b adapter.NodeRef) int {
		return strings.Compare(a.String(), b.String())
	})
	return out
}

func TestKustomizationAdapter_Kind(t *testing.T) {
	a := adapter.NewKustomizationAdapter()
	if got, want := a.Kind(), kustomizationKind; got != want {
		t.Errorf("Kind() = %q, want %q", got, want)
	}
}

func TestKustomizationAdapter_List_LabelSelection(t *testing.T) {
	matchA := &kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "team-a",
			Name:      appName,
			Labels:    map[string]string{managedLabelKey: managedLabelValue},
		},
	}
	matchB := &kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "team-b",
			Name:      appName,
			Labels:    map[string]string{managedLabelKey: managedLabelValue},
		},
	}
	noMatch := &kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "team-c",
			Name:      appName,
			Labels:    map[string]string{"other": "label"},
		},
	}

	c := newFakeClient(t, matchA, matchB, noMatch)
	a := adapter.NewKustomizationAdapter()

	sel := labels.SelectorFromSet(labels.Set{managedLabelKey: managedLabelValue})
	nodes, err := a.List(context.Background(), c, sel)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	gotRefs := make([]adapter.NodeRef, 0, len(nodes))
	for _, n := range nodes {
		gotRefs = append(gotRefs, n.Ref)
	}
	wantRefs := []adapter.NodeRef{
		{Kind: kustomizationKind, Namespace: "team-a", Name: appName},
		{Kind: kustomizationKind, Namespace: "team-b", Name: appName},
	}
	if !reflect.DeepEqual(sortedRefs(gotRefs), sortedRefs(wantRefs)) {
		t.Errorf("List() refs = %v, want %v (spanning namespaces, excluding non-matching label)", gotRefs, wantRefs)
	}
}

func TestKustomizationAdapter_DependsOn(t *testing.T) {
	tests := []struct {
		name string
		ks   *kustomizev1.Kustomization
		want []adapter.NodeRef
	}{
		{
			name: "namespace defaulting",
			ks: &kustomizev1.Kustomization{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: "child"},
				Spec: kustomizev1.KustomizationSpec{
					DependsOn: []kustomizev1.DependencyReference{
						{Name: "parent", ReadyExpr: "self.status.foo == 'bar'"},
					},
				},
			},
			want: []adapter.NodeRef{
				{Kind: kustomizationKind, Namespace: nsA, Name: "parent"},
			},
		},
		{
			name: "cross-namespace dependsOn preserved",
			ks: &kustomizev1.Kustomization{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: "child"},
				Spec: kustomizev1.KustomizationSpec{
					DependsOn: []kustomizev1.DependencyReference{
						{Name: "same-ns-parent"},
						{Name: "other-ns-parent", Namespace: "ns-b"},
					},
				},
			},
			want: []adapter.NodeRef{
				{Kind: kustomizationKind, Namespace: nsA, Name: "same-ns-parent"},
				{Kind: kustomizationKind, Namespace: "ns-b", Name: "other-ns-parent"},
			},
		},
		{
			name: "no dependsOn",
			ks: &kustomizev1.Kustomization{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: "root"},
			},
			want: []adapter.NodeRef{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newFakeClient(t, tt.ks)
			a := adapter.NewKustomizationAdapter()

			node, ok, err := a.Get(context.Background(), c, adapter.NodeRef{
				Kind: kustomizationKind, Namespace: tt.ks.Namespace, Name: tt.ks.Name,
			})
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if !ok {
				t.Fatalf("Get() ok = false, want true")
			}
			if !reflect.DeepEqual(node.DependsOn, tt.want) {
				t.Errorf("DependsOn = %v, want %v", node.DependsOn, tt.want)
			}
		})
	}
}

func TestKustomizationAdapter_SourceRef(t *testing.T) {
	tests := []struct {
		name string
		ks   *kustomizev1.Kustomization
		want *types.NamespacedName
	}{
		{
			name: "GitRepository sourceRef, namespace defaulted",
			ks: &kustomizev1.Kustomization{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: appName},
				Spec: kustomizev1.KustomizationSpec{
					SourceRef: kustomizev1.CrossNamespaceSourceReference{
						Kind: "GitRepository",
						Name: repoName,
					},
				},
			},
			want: &types.NamespacedName{Namespace: nsA, Name: repoName},
		},
		{
			name: "GitRepository sourceRef, explicit namespace",
			ks: &kustomizev1.Kustomization{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: appName},
				Spec: kustomizev1.KustomizationSpec{
					SourceRef: kustomizev1.CrossNamespaceSourceReference{
						Kind:      "GitRepository",
						Name:      repoName,
						Namespace: "flux-system",
					},
				},
			},
			want: &types.NamespacedName{Namespace: "flux-system", Name: repoName},
		},
		{
			name: "non-GitRepository sourceRef yields nil",
			ks: &kustomizev1.Kustomization{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: appName},
				Spec: kustomizev1.KustomizationSpec{
					SourceRef: kustomizev1.CrossNamespaceSourceReference{
						Kind: "OCIRepository",
						Name: repoName,
					},
				},
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newFakeClient(t, tt.ks)
			a := adapter.NewKustomizationAdapter()

			node, ok, err := a.Get(context.Background(), c, adapter.NodeRef{
				Kind: kustomizationKind, Namespace: tt.ks.Namespace, Name: tt.ks.Name,
			})
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if !ok {
				t.Fatalf("Get() ok = false, want true")
			}

			switch {
			case tt.want == nil && node.SourceRef == nil:
				// ok
			case tt.want == nil || node.SourceRef == nil:
				t.Errorf("SourceRef = %v, want %v", node.SourceRef, tt.want)
			case *node.SourceRef != *tt.want:
				t.Errorf("SourceRef = %v, want %v", *node.SourceRef, *tt.want)
			}
		})
	}
}

func TestKustomizationAdapter_Readiness(t *testing.T) {
	tests := []struct {
		name        string
		generation  int64
		observedGen int64
		conditions  []metav1.Condition
		wantReady   bool
		wantFailing bool
	}{
		{
			name:        "ready condition true, current generation",
			generation:  2,
			observedGen: 2,
			conditions:  []metav1.Condition{readyCondition(metav1.ConditionTrue)},
			wantReady:   true,
			wantFailing: false,
		},
		{
			name:        "ready condition true, stale generation is not ready",
			generation:  3,
			observedGen: 2,
			conditions:  []metav1.Condition{readyCondition(metav1.ConditionTrue)},
			wantReady:   false,
			wantFailing: false,
		},
		{
			name:        "ready condition false is failing",
			generation:  1,
			observedGen: 1,
			conditions:  []metav1.Condition{readyCondition(metav1.ConditionFalse)},
			wantReady:   false,
			wantFailing: true,
		},
		{
			name:        "missing ready condition is neither ready nor failing",
			generation:  1,
			observedGen: 1,
			conditions:  nil,
			wantReady:   false,
			wantFailing: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &kustomizev1.Kustomization{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:  nsA,
					Name:       appName,
					Generation: tt.generation,
				},
				Status: kustomizev1.KustomizationStatus{
					ObservedGeneration: tt.observedGen,
					Conditions:         tt.conditions,
				},
			}

			c := newFakeClient(t, ks)
			a := adapter.NewKustomizationAdapter()

			node, ok, err := a.Get(context.Background(), c, adapter.NodeRef{
				Kind: kustomizationKind, Namespace: nsA, Name: appName,
			})
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if !ok {
				t.Fatalf("Get() ok = false, want true")
			}

			if node.Readiness.Ready != tt.wantReady {
				t.Errorf("Readiness.Ready = %v, want %v", node.Readiness.Ready, tt.wantReady)
			}
			if node.Readiness.Failing != tt.wantFailing {
				t.Errorf("Readiness.Failing = %v, want %v", node.Readiness.Failing, tt.wantFailing)
			}
			if len(tt.conditions) > 0 && node.Readiness.Message != tt.conditions[0].Message {
				t.Errorf("Readiness.Message = %q, want %q", node.Readiness.Message, tt.conditions[0].Message)
			}
			if len(tt.conditions) == 0 && node.Readiness.Message != "" {
				t.Errorf("Readiness.Message = %q, want empty", node.Readiness.Message)
			}
		})
	}
}

func TestKustomizationAdapter_AppliedSHA(t *testing.T) {
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 40 hex chars

	tests := []struct {
		name     string
		revision string
		want     string
	}{
		{
			name:     "ref-qualified revision yields the hex SHA",
			revision: "main@sha1:" + sha,
			want:     sha,
		},
		{
			name:     "empty revision yields empty SHA",
			revision: "",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &kustomizev1.Kustomization{
				ObjectMeta: metav1.ObjectMeta{Namespace: nsA, Name: appName},
				Status: kustomizev1.KustomizationStatus{
					LastAppliedRevision: tt.revision,
				},
			}

			c := newFakeClient(t, ks)
			a := adapter.NewKustomizationAdapter()

			node, ok, err := a.Get(context.Background(), c, adapter.NodeRef{
				Kind: kustomizationKind, Namespace: nsA, Name: appName,
			})
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if !ok {
				t.Fatalf("Get() ok = false, want true")
			}
			if node.Readiness.AppliedSHA != tt.want {
				t.Errorf("Readiness.AppliedSHA = %q, want %q", node.Readiness.AppliedSHA, tt.want)
			}
		})
	}
}

func TestKustomizationAdapter_Get_NotFound(t *testing.T) {
	c := newFakeClient(t)
	a := adapter.NewKustomizationAdapter()

	node, ok, err := a.Get(context.Background(), c, adapter.NodeRef{
		Kind: kustomizationKind, Namespace: nsA, Name: "missing",
	})
	if err != nil {
		t.Fatalf("Get() error = %v, want nil", err)
	}
	if ok {
		t.Fatalf("Get() ok = true, want false")
	}
	if !reflect.DeepEqual(node, adapter.Node{}) {
		t.Errorf("Get() node = %+v, want zero value", node)
	}
}
