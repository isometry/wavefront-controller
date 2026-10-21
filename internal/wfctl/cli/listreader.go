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
	"reflect"
	"sync"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// listReader answers Gets of GitRepository and Kustomization from one List per
// (kind, namespace) instead of one request per object. The controller reads
// through an informer cache, so its per-source Gets are free; wfctl has no
// cache, and ~185 serial live Gets is what made every read command slow.
//
// Every other Get (Wavefront, Secrets, Events) and every List passes through:
// Secrets in particular must never be bulk-listed.
//
// ponytail: a role granting `get` on named sources without `list` now sees
// them as partial (the List is Forbidden); fall back to a live Get if anyone
// runs wfctl with such a role.
type listReader struct {
	client.Reader

	mu   sync.Mutex
	memo map[listKey]*listResult
}

type listKey struct {
	kind      reflect.Type
	namespace string
}

// listResult is the memoised outcome of a List: the objects by name, or the
// error every Get in that namespace gets back.
type listResult struct {
	byName map[string]runtime.Object
	err    error
}

func newListReader(r client.Reader) *listReader {
	return &listReader{Reader: r, memo: map[listKey]*listResult{}}
}

// Get implements client.Reader.
func (r *listReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	var list client.ObjectList
	var gr schema.GroupResource
	switch obj.(type) {
	case *sourcev1.GitRepository:
		list = &sourcev1.GitRepositoryList{}
		gr = schema.GroupResource{Group: sourcev1.GroupVersion.Group, Resource: "gitrepositories"}
	case *kustomizev1.Kustomization:
		list = &kustomizev1.KustomizationList{}
		gr = schema.GroupResource{Group: kustomizev1.GroupVersion.Group, Resource: "kustomizations"}
	default:
		return r.Reader.Get(ctx, key, obj, opts...)
	}

	res := r.listed(ctx, key.Namespace, reflect.TypeOf(obj), list)
	if res.err != nil {
		return res.err
	}
	found, ok := res.byName[key.Name]
	if !ok {
		return apierrors.NewNotFound(gr, key.Name)
	}
	reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(found.DeepCopyObject()).Elem())
	return nil
}

// listed returns the memoised List of one kind in one namespace, listing it on
// first use. The lock is held across the List so concurrent first Gets share it.
func (r *listReader) listed(ctx context.Context, namespace string, kind reflect.Type, list client.ObjectList) *listResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	k := listKey{kind: kind, namespace: namespace}
	if res, ok := r.memo[k]; ok {
		return res
	}
	res := &listResult{}
	r.memo[k] = res
	if res.err = r.Reader.List(ctx, list, client.InNamespace(namespace)); res.err != nil {
		return res
	}
	items, err := apimeta.ExtractList(list)
	if err != nil {
		res.err = err
		return res
	}
	res.byName = make(map[string]runtime.Object, len(items))
	for _, item := range items {
		res.byName[item.(client.Object).GetName()] = item
	}
	return res
}
