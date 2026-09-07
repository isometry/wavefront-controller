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
	"fmt"
	"strings"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/isometry/wavefront-controller/internal/adapter"
)

// ParseNodeRef parses a node reference as typed on the command line:
// "ns/name", where the kind defaults to Kustomization, or the explicit
// "Kind/ns/name" (decision "Conventions"). The kind is spelled out from day
// one so a future HelmRelease plane needs no new syntax (DESIGN D12).
//
// The kind is matched case-insensitively and returned in its canonical
// spelling, so "kustomization/apps/web" and "Kustomization/apps/web" name the
// same node. v1alpha1 graphs only Kustomizations, so any other kind is
// rejected rather than accepted into a lookup that could never match.
func ParseNodeRef(s string) (adapter.NodeRef, error) {
	parts := strings.Split(s, "/")

	var kind, namespace, name string
	switch len(parts) {
	case 2:
		kind, namespace, name = kustomizev1.KustomizationKind, parts[0], parts[1]
	case 3:
		kind, namespace, name = parts[0], parts[1], parts[2]
	default:
		return adapter.NodeRef{}, fmt.Errorf(
			"invalid node reference %q: want \"namespace/name\" or \"Kind/namespace/name\"", s)
	}

	if !strings.EqualFold(kind, kustomizev1.KustomizationKind) {
		return adapter.NodeRef{}, fmt.Errorf(
			"invalid node reference %q: unsupported kind %q (only %s)", s, kind, kustomizev1.KustomizationKind)
	}
	if namespace == "" || name == "" {
		return adapter.NodeRef{}, fmt.Errorf("invalid node reference %q: empty namespace or name", s)
	}

	return adapter.NodeRef{Kind: kustomizev1.KustomizationKind, Namespace: namespace, Name: name}, nil
}

// ParseSource parses a GitRepository reference as typed on the command line:
// "ns/name", the same form SourceView.Name and status.held[].source use.
func ParseSource(s string) (types.NamespacedName, error) {
	namespace, name, found := strings.Cut(s, "/")
	if !found || namespace == "" || name == "" || strings.Contains(name, "/") {
		return types.NamespacedName{}, fmt.Errorf(
			"invalid source reference %q: want \"namespace/name\"", s)
	}
	return types.NamespacedName{Namespace: namespace, Name: name}, nil
}
