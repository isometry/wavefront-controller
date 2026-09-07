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

// Package adapter provides the node adapter seam (DESIGN §4.3, D12): a
// per-kind interpretation of graph members that keeps the graph, engine, and
// reconciler kind-agnostic. v1alpha1 ships only the Kustomization adapter;
// a future HelmRelease adapter is a non-breaking addition behind the same
// interface.
package adapter

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeRef identifies a node; Kind distinguishes future HelmRelease planes.
type NodeRef struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// String renders the reference as "Kind/ns/name".
func (r NodeRef) String() string {
	return fmt.Sprintf("%s/%s/%s", r.Kind, r.Namespace, r.Name)
}

// Readiness is the uniform health signal (DESIGN D7).
type Readiness struct {
	Ready      bool   // Ready condition True AND status.observedGeneration == metadata.generation
	Failing    bool   // Ready condition explicitly False (unhealthy, not merely converging)
	Message    string // Ready condition message (for status attribution)
	AppliedSHA string // 40-hex SHA parsed from status.lastAppliedRevision ("" if none)
}

// Node is the adapter's read of one graph member.
type Node struct {
	Ref       NodeRef
	DependsOn []NodeRef             // same-kind, namespace-defaulted to the node's
	SourceRef *types.NamespacedName // referenced GitRepository; nil when sourceRef is not a GitRepository
	Readiness Readiness
	Labels    map[string]string
}

// Adapter interprets one node kind (DESIGN §4.3, D12).
type Adapter interface {
	Kind() string
	// List returns all nodes matching sel across all namespaces.
	List(ctx context.Context, r client.Reader, sel labels.Selector) ([]Node, error)
	// Get fetches a single node (used for dependsOn targets outside the selector).
	// Returns (Node{}, false, nil) when the resource does not exist.
	Get(ctx context.Context, r client.Reader, ref NodeRef) (Node, bool, error)
}
