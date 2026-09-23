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

package adapter

import (
	"context"
	"fmt"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/git"
	"github.com/fluxcd/pkg/runtime/conditions"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// kustomizationAdapter is the Adapter implementation for Kustomization nodes.
type kustomizationAdapter struct{}

// NewKustomizationAdapter returns the Adapter for Kustomization nodes.
func NewKustomizationAdapter() Adapter {
	return kustomizationAdapter{}
}

func (kustomizationAdapter) Kind() string {
	return kustomizev1.KustomizationKind
}

func (a kustomizationAdapter) List(ctx context.Context, r client.Reader, sel labels.Selector) ([]Node, error) {
	var list kustomizev1.KustomizationList
	if err := r.List(ctx, &list, &client.ListOptions{LabelSelector: sel}); err != nil {
		return nil, fmt.Errorf("listing Kustomizations: %w", err)
	}

	nodes := make([]Node, 0, len(list.Items))
	for i := range list.Items {
		nodes = append(nodes, kustomizationToNode(&list.Items[i]))
	}
	return nodes, nil
}

func (a kustomizationAdapter) Get(ctx context.Context, r client.Reader, ref NodeRef) (Node, bool, error) {
	var ks kustomizev1.Kustomization
	key := client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}
	if err := r.Get(ctx, key, &ks); err != nil {
		if apierrors.IsNotFound(err) {
			return Node{}, false, nil
		}
		return Node{}, false, fmt.Errorf("getting Kustomization %s: %w", key, err)
	}
	return kustomizationToNode(&ks), true, nil
}

// kustomizationToNode maps a Kustomization to the adapter's uniform Node
// representation.
func kustomizationToNode(ks *kustomizev1.Kustomization) Node {
	ref := NodeRef{Kind: kustomizev1.KustomizationKind, Namespace: ks.Namespace, Name: ks.Name}

	dependsOn := make([]NodeRef, 0, len(ks.Spec.DependsOn))
	for _, dep := range ks.Spec.DependsOn {
		ns := dep.Namespace
		if ns == "" {
			ns = ks.Namespace
		}
		// ReadyExpr deliberately ignored: it tunes Flux's apply-side gating;
		// wavefront settledness always uses the full Ready condition.
		dependsOn = append(dependsOn, NodeRef{Kind: kustomizev1.KustomizationKind, Namespace: ns, Name: dep.Name})
	}

	var sourceRef *types.NamespacedName
	if ks.Spec.SourceRef.Kind == sourcev1.GitRepositoryKind {
		ns := ks.Spec.SourceRef.Namespace
		if ns == "" {
			ns = ks.Namespace
		}
		sourceRef = &types.NamespacedName{Namespace: ns, Name: ks.Spec.SourceRef.Name}
	}

	return Node{
		Ref:       ref,
		DependsOn: dependsOn,
		SourceRef: sourceRef,
		Readiness: Readiness{
			Ready:      conditions.IsTrue(ks, fluxmeta.ReadyCondition) && ks.Status.ObservedGeneration == ks.Generation,
			Failing:    conditions.IsFalse(ks, fluxmeta.ReadyCondition),
			Message:    conditions.GetMessage(ks, fluxmeta.ReadyCondition),
			AppliedSHA: git.ExtractHashFromRevision(ks.Status.LastAppliedRevision).String(),
		},
		Labels: ks.Labels,
	}
}
