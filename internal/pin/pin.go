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

// Package pin implements the pin mechanism: advancing a
// GitRepository's spec.ref.commit to an admitted SHA under the controller's
// own field manager, and detecting hand-pins made by anyone else.
//
// The mechanism rests entirely on server-side apply co-ownership. The catalog
// renders the tracking ref (spec.ref.name) and omits spec.ref.commit,
// so the controller can own the commit field alone without ever contending
// with kustomize-controller. A commit owned by a third field manager is a
// human override: it is reported and never forced past — which is
// why nothing here ever passes client.ForceOwnership.
package pin

import (
	"context"
	"errors"
	"fmt"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

const (
	// FieldManager is the field manager under which the controller owns
	// spec.ref.commit and its provenance annotations.
	FieldManager = "wavefront-controller"

	// ManagedLabel is the participation label. The controller
	// only ever reads it: labels are the catalog's to write.
	ManagedLabel = "wavefront.as-code.io/managed"

	// AnnotAdmittedAt records when the pin was advanced, in RFC3339 UTC.
	AnnotAdmittedAt = "wavefront.as-code.io/admitted-at"
	// AnnotPreviousPin records the outgoing pin, enabling rollback.
	// It is set to the empty string on an initial pin so that the applied
	// field set stays stable across advances.
	AnnotPreviousPin = "wavefront.as-code.io/previous-pin"
	// AnnotObservedRef records the tracking ref the admitted SHA came from.
	AnnotObservedRef = "wavefront.as-code.io/observed-ref"

	// WfctlFieldManager is the SSA field manager wfctl uses for hand-pins, so
	// the controller reports them as external holds.
	WfctlFieldManager = "wfctl"

	// AnnotDisplacedPin records, on a wfctl hand-pin, the spec.ref.commit value
	// the hand-pin displaced, so `wfctl release` can restore provenance.
	AnnotDisplacedPin = "wavefront.as-code.io/displaced-pin"
)

// ErrHeld is returned when the SSA patch conflicts with a foreign field
// manager — i.e. a human hand-pin. Never force past it.
var ErrHeld = errors.New("spec.ref.commit is held by another field manager")

// commitPath is the field-set path of the pin itself. In an entry's
// FieldsV1 encoding this is the leaf f:spec.f:ref.f:commit.
var commitPath = fieldpath.MakePathOrDie("spec", "ref", "commit")

// Owner is one managedFields entry that owns spec.ref.commit.
type Owner struct {
	Manager   string                            `json:"manager"`
	Operation metav1.ManagedFieldsOperationType `json:"operation"` // Apply | Update
}

// Owners lists every managedFields entry owning spec.ref.commit, in
// managedFields order, skipping subresource entries and unparseable
// FieldsV1 (same rules as Hold). Includes the controller's own entry.
func Owners(repo *sourcev1.GitRepository) []Owner {
	if repo == nil {
		return nil
	}

	var owners []Owner
	for _, entry := range repo.GetManagedFields() {
		if entry.Subresource != "" || entry.FieldsV1 == nil {
			continue
		}

		set := &fieldpath.Set{}
		if err := set.FromJSON(entry.FieldsV1.GetRawReader()); err != nil {
			// An unparseable entry cannot be shown to own the pin; treating
			// it as an owner would wedge the node on apiserver malformation.
			continue
		}

		if set.Has(commitPath) {
			owners = append(owners, Owner{Manager: entry.Manager, Operation: entry.Operation})
		}
	}

	return owners
}

// Hold inspects managedFields and reports a foreign owner of spec.ref.commit.
//
// Ownership by anyone other than FieldManager means the pin was set by hand:
// the node is held, and the controller must report it rather
// than advance it. The first foreign owner encountered wins; entries for
// subresources (status) cannot own spec and are skipped.
func Hold(repo *sourcev1.GitRepository) (manager string, held bool) {
	for _, owner := range Owners(repo) {
		if owner.Manager != FieldManager {
			return owner.Manager, true
		}
	}

	return "", false
}

// Writer advances pins via server-side apply.
type Writer struct{ Client client.Client }

// Advance pins repo to sha with provenance annotations, via SSA under
// FieldManager WITHOUT ForceOwnership. A 409 conflict is normalised to ErrHeld.
// prev is the outgoing pin ("" for an initial pin); observedRef the tracking ref.
//
// The applied object is deliberately minimal — identity, the three provenance
// annotations, and spec.ref.commit — so the controller's field set never grows
// to cover anything the catalog owns.
func (w *Writer) Advance(ctx context.Context, repo types.NamespacedName, prev, sha, observedRef string, at time.Time) error {
	desired := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": sourcev1.GroupVersion.String(),
		"kind":       sourcev1.GitRepositoryKind,
		"metadata": map[string]any{
			"name":      repo.Name,
			"namespace": repo.Namespace,
			"annotations": map[string]any{
				AnnotAdmittedAt:  at.UTC().Format(time.RFC3339),
				AnnotPreviousPin: prev,
				AnnotObservedRef: observedRef,
			},
		},
		"spec": map[string]any{
			"ref": map[string]any{
				"commit": sha,
			},
		},
	}}

	err := w.Client.Apply(ctx, client.ApplyConfigurationFromUnstructured(desired), client.FieldOwner(FieldManager))
	switch {
	case err == nil:
		return nil
	case apierrors.IsConflict(err):
		return fmt.Errorf("%w: %s", ErrHeld, err)
	default:
		return fmt.Errorf("advancing pin of %s to %s: %w", repo, sha, err)
	}
}
