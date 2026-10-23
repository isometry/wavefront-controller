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
	"errors"
	"fmt"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
)

// countingReader is a fake client whose List and Get calls are counted.
func countingReader(t *testing.T, listErr error, objs ...client.Object) (r *listReader, lists, gets *int) {
	t.Helper()
	lists, gets = new(int), new(int)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				*lists++
				if listErr != nil {
					return listErr
				}
				return c.List(ctx, list, opts...)
			},
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				*gets++
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
	return newListReader(c), lists, gets
}

func gitRepo(name string) *sourcev1.GitRepository {
	return &sourcev1.GitRepository{Namespace: nsFlux, Name: name}
}

func TestListReaderGetsShareOneList(t *testing.T) {
	objs := make([]client.Object, 0, 50)
	for i := range 50 {
		objs = append(objs, gitRepo(fmt.Sprintf("repo-%d", i)))
	}
	r, lists, gets := countingReader(t, nil, objs...)

	for i := range 50 {
		var got sourcev1.GitRepository
		key := client.ObjectKey{Namespace: nsFlux, Name: fmt.Sprintf("repo-%d", i)}
		if err := r.Get(t.Context(), key, &got); err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if got.Name != key.Name {
			t.Fatalf("Get %s returned %q", key, got.Name)
		}
	}
	if *lists != 1 || *gets != 0 {
		t.Fatalf("want 1 List and 0 Gets, got %d and %d", *lists, *gets)
	}
}

func TestListReaderMissingIsNotFound(t *testing.T) {
	r, _, _ := countingReader(t, nil, gitRepo("present"))

	err := r.Get(t.Context(), client.ObjectKey{Namespace: nsFlux, Name: "absent"}, &sourcev1.GitRepository{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("want NotFound, got %v", err)
	}
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		t.Fatalf("not an API status: %v", err)
	}
	if d := status.Status().Details; d.Group != sourcev1.GroupVersion.Group || d.Kind != "gitrepositories" {
		t.Fatalf("NotFound names %s/%s", d.Group, d.Kind)
	}
}

func TestListReaderMemoisesListError(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "gitrepositories"}, "", fmt.Errorf("no"))
	r, lists, _ := countingReader(t, forbidden)

	for i := range 5 {
		key := client.ObjectKey{Namespace: nsFlux, Name: fmt.Sprintf("repo-%d", i)}
		if err := r.Get(t.Context(), key, &sourcev1.GitRepository{}); !apierrors.IsForbidden(err) {
			t.Fatalf("Get %s: want Forbidden, got %v", key, err)
		}
	}
	if *lists != 1 {
		t.Fatalf("want 1 List, got %d", *lists)
	}
}

func TestListReaderPassesOtherTypesThrough(t *testing.T) {
	r, lists, gets := countingReader(t, nil,
		&corev1.Secret{Namespace: nsFlux, Name: "s"},
		&wavefrontv1alpha1.Wavefront{Name: "w"},
	)

	if err := r.Get(t.Context(), client.ObjectKey{Namespace: nsFlux, Name: "s"}, &corev1.Secret{}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(t.Context(), client.ObjectKey{Name: "w"}, &wavefrontv1alpha1.Wavefront{}); err != nil {
		t.Fatal(err)
	}
	if *lists != 0 || *gets != 2 {
		t.Fatalf("want 0 Lists and 2 Gets, got %d and %d", *lists, *gets)
	}
}

func TestListReaderReturnsCopies(t *testing.T) {
	r, _, _ := countingReader(t, nil, gitRepo("repo"))
	key := client.ObjectKey{Namespace: nsFlux, Name: "repo"}

	var first, second sourcev1.GitRepository
	if err := r.Get(t.Context(), key, &first); err != nil {
		t.Fatal(err)
	}
	first.Labels = map[string]string{"mutated": valYes}
	first.Spec.URL = "mutated"
	if err := r.Get(t.Context(), key, &second); err != nil {
		t.Fatal(err)
	}
	if len(second.Labels) != 0 || second.Spec.URL != "" {
		t.Fatalf("mutation leaked into the memo: %+v", second.ObjectMeta)
	}
}
