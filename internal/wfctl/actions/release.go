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

package actions

import (
	"context"
	"fmt"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
)

// commitPath is the field-set path of the pin itself, and commitSet the set
// containing it alone — what a release subtracts from a holder's entry. In an
// entry's FieldsV1 encoding this is the leaf f:spec.f:ref.f:commit.
var (
	commitPath = fieldpath.MakePathOrDie("spec", "ref", "commit")
	commitSet  = fieldpath.NewSet(commitPath)
)

// Release ends a hold on one source.
//
// The default is a *transfer*, not a removal: the hand-pinned value is kept and
// handed to the controller, which then reports HoldReleased and admits from it
// normally. That is the honest end of an incident — the SHA an operator pinned
// by hand is the SHA the fleet is running, and unpinning would instead hand the
// source back to its tracking ref and let the next commit through the moment
// the gate opens.
//
// It takes two writes, because SSA has no "disown" verb:
//
//  1. pin.Writer.Advance under wavefront-controller with the *same* value.
//     Applying a value identical to the live one is not a change, so the
//     apiserver reports no conflict and simply adds the controller as a
//     co-owner.
//  2. relinquish the holder's share, so the controller is left sole owner and
//     pin.Hold stops reporting a hold.
//
// --float takes the other road: delete the pin entirely and let the source
// float on its tracking ref until the controller next discovers it and pins
// it from the artifact.
type Release struct {
	Client client.Client
	Source types.NamespacedName
	// Float removes the pin instead of transferring it.
	Float bool
	// Strategy resolves the tracking ref recorded as provenance; nil means
	// the v1 default.
	Strategy selection.Strategy
	// Now stamps the admitted-at annotation; nil means the wall clock.
	Now func() time.Time
}

var _ Action = (*Release)(nil)

// Plan implements Action.
func (a *Release) Plan(ctx context.Context) (*Plan, error) {
	repo, err := getSource(ctx, a.Client, a.Source)
	if err != nil {
		return nil, err
	}

	current := pinOf(repo)
	if current == "" {
		return nil, fmt.Errorf("%s has no spec.ref.commit: there is no pin to release", a.Source)
	}

	if a.Float {
		return a.floatPlan(repo, current)
	}
	return a.transferPlan(repo, current)
}

// floatPlan removes the pin. The JSON patch is the runbook's own one-liner:
// deleting a field owned by someone else is the one thing server-side apply
// cannot express.
func (a *Release) floatPlan(repo *sourcev1.GitRepository, current string) (*Plan, error) {
	trackingRef, err := trackingRefOf(a.Strategy, repo)
	if err != nil {
		return nil, err
	}

	warnings := []string{
		fmt.Sprintf("%s floats on %s until the controller initial-pins it from its artifact SHA "+
			"— which it will do on the next sweep unless the fleet is suspended",
			a.Source, trackingRef),
	}
	if displaced := annotationOf(repo, pin.AnnotDisplacedPin); displaced != "" {
		warnings = append(warnings, fmt.Sprintf(
			"the displaced-pin annotation (%s) is left behind; --float discards the displaced value "+
				"rather than restoring it", displaced))
	}

	return &Plan{
		Summary: fmt.Sprintf("Unpin %s: remove spec.ref.commit (JSON patch), so the source floats on its tracking ref",
			a.Source),
		Before:   map[string]string{commitField: current},
		After:    map[string]string{commitField: unset},
		Warnings: warnings,
		Apply:    a.applyFloat,
	}, nil
}

// applyFloat performs the removal.
func (a *Release) applyFloat(ctx context.Context) (bool, error) {
	repo := &sourcev1.GitRepository{}
	repo.Namespace, repo.Name = a.Source.Namespace, a.Source.Name

	if err := a.Client.Patch(ctx, repo, client.RawPatch(types.JSONPatchType, removeCommitPatch),
		client.FieldOwner(pin.WfctlFieldManager)); err != nil {
		return false, fmt.Errorf("removing spec.ref.commit of %s: %w", a.Source, err)
	}
	return true, nil
}

// transferPlan hands the held value to the controller.
func (a *Release) transferPlan(repo *sourcev1.GitRepository, current string) (*Plan, error) {
	holders := foreignOwners(repo)
	if len(holders) == 0 {
		return nil, fmt.Errorf(
			"%s is not held: spec.ref.commit is already owned by %q alone, so there is nothing to "+
				"release; `wfctl release %s --float` removes the pin entirely",
			a.Source, pin.FieldManager, a.Source)
	}

	trackingRef, err := trackingRefOf(a.Strategy, repo)
	if err != nil {
		return nil, err
	}

	displaced := annotationOf(repo, pin.AnnotDisplacedPin)
	at := a.now()

	warnings := []string{
		fmt.Sprintf("the controller will fire HoldReleased and resume admission from %s; "+
			"nodes behind this source stop reporting AncestorHeld", current),
	}
	if displaced == "" {
		warnings = append(warnings, fmt.Sprintf(
			"no %s annotation: the hold was not set by wfctl, so the previous pin is unknown and "+
				"provenance records it as empty", pin.AnnotDisplacedPin))
	}
	for _, holder := range holders {
		if applyOp(holder) && holder.Manager == pin.WfctlFieldManager {
			continue
		}
		warnings = append(warnings, fmt.Sprintf(
			"%s holds the field through an %s operation, which has no apply-based disown: wfctl "+
				"rewrites metadata.managedFields to drop its claim, and reports an error naming "+
				"--float if the apiserver will not have it", holder.Manager, holder.Operation))
	}

	before, after := transferFields(repo, holders, current, displaced, trackingRef, at)

	return &Plan{
		Summary: fmt.Sprintf(
			"Release %s: transfer spec.ref.commit (%s) to field manager %q, then relinquish %s",
			a.Source, current, pin.FieldManager, describeOwners(holders)),
		Before:   before,
		After:    after,
		Warnings: warnings,
		Apply: func(ctx context.Context) (bool, error) {
			return a.applyTransfer(ctx, holders, displaced, current, trackingRef, at)
		},
	}, nil
}

// transferFields renders the transfer's before/after table: the value that
// does not move, the ownership that does, and the provenance the controller
// gains.
func transferFields(
	repo *sourcev1.GitRepository,
	holders []pin.Owner,
	current, displaced, trackingRef string,
	at time.Time,
) (before, after map[string]string) {
	displacedField := annotationField(pin.AnnotDisplacedPin)

	before = map[string]string{
		commitField:                           current,
		commitOwnersField:                     describeOwners(pin.Owners(repo)),
		annotationField(pin.AnnotAdmittedAt):  shown(annotationOf(repo, pin.AnnotAdmittedAt)),
		annotationField(pin.AnnotPreviousPin): shown(annotationOf(repo, pin.AnnotPreviousPin)),
		annotationField(pin.AnnotObservedRef): shown(annotationOf(repo, pin.AnnotObservedRef)),
		displacedField:                        shown(displaced),
	}
	after = map[string]string{
		commitField:                           current,
		commitOwnersField:                     fmt.Sprintf("%s (Apply)", pin.FieldManager),
		annotationField(pin.AnnotAdmittedAt):  at.UTC().Format(time.RFC3339),
		annotationField(pin.AnnotPreviousPin): shown(displaced),
		annotationField(pin.AnnotObservedRef): trackingRef,
		displacedField:                        shown(displaced),
	}

	// Only the wfctl apply-op share can be dropped wholesale, and dropping it
	// takes the annotation with it.
	for _, holder := range holders {
		if holder.Manager == pin.WfctlFieldManager && applyOp(holder) {
			after[displacedField] = unset
		}
	}
	return before, after
}

// applyTransfer performs the two writes.
func (a *Release) applyTransfer(
	ctx context.Context,
	holders []pin.Owner,
	displaced, current, trackingRef string,
	at time.Time,
) (bool, error) {
	writer := &pin.Writer{Client: a.Client}
	if err := writer.Advance(ctx, a.Source, displaced, current, trackingRef, at); err != nil {
		return false, fmt.Errorf("transferring the pin of %s to %q: %w", a.Source, pin.FieldManager, err)
	}

	// Past here the cluster has changed — the controller co-owns the pin —
	// whatever happens next.
	for _, holder := range holders {
		if err := a.relinquish(ctx, holder); err != nil {
			return true, err
		}
	}

	// The transfer is only done when the controller is sole owner: an
	// apiserver that accepted the write but ignored it would otherwise leave
	// the source held with nobody the wiser.
	repo, err := getSource(ctx, a.Client, a.Source)
	if err != nil {
		return true, err
	}
	if remaining := foreignOwners(repo); len(remaining) > 0 {
		return true, fmt.Errorf(
			"the pin of %s was transferred to %q but %s still owns spec.ref.commit, so the "+
				"controller still sees a hold: remove the field with `wfctl release %s --float`, "+
				"or clear that manager's claim by hand",
			a.Source, pin.FieldManager, describeOwners(remaining), a.Source)
	}
	return true, nil
}

// relinquish drops one holder's claim on spec.ref.commit.
//
// An Apply-op wfctl hold is given up the clean way: apply an object that no
// longer mentions the commit, and the apiserver removes it from wfctl's field
// set. The value survives because the controller now co-owns it; the
// displaced-pin annotation does not, because wfctl owned that alone — which is
// the intended tidy-up.
//
// Anything else — kubectl-patch, kubectl-edit, any Update-op manager — has no
// such verb. There the only surgery available is on metadata.managedFields
// itself, which the apiserver accepts on an update: it decodes the incoming
// managedFields in preference to the live object's. That is why every transfer
// verifies its own outcome afterwards.
func (a *Release) relinquish(ctx context.Context, holder pin.Owner) error {
	if holder.Manager == pin.WfctlFieldManager && applyOp(holder) {
		return a.relinquishApply(ctx)
	}
	return a.relinquishManagedFields(ctx, holder)
}

// relinquishApply gives up the wfctl apply share.
func (a *Release) relinquishApply(ctx context.Context) error {
	empty := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": sourcev1.GroupVersion.String(),
		"kind":       sourcev1.GitRepositoryKind,
		"metadata": map[string]any{
			"name":      a.Source.Name,
			"namespace": a.Source.Namespace,
		},
	}}

	if err := a.Client.Apply(ctx, client.ApplyConfigurationFromUnstructured(empty),
		client.FieldOwner(pin.WfctlFieldManager)); err != nil {
		return fmt.Errorf("relinquishing the %q hold on %s: %w", pin.WfctlFieldManager, a.Source, err)
	}
	return nil
}

// relinquishManagedFields removes spec.ref.commit from one holder's
// managedFields entry, dropping the entry entirely when nothing else is left
// in it.
//
// The round trip is deliberately untyped. This is the only update of a whole
// GitRepository anywhere in the controller or wfctl — everything else is a
// server-side apply or a JSON patch — and an update is a PUT: whatever the
// object is read as becomes the object that is stored. Read through the
// vendored Go type and any field a newer source-controller CRD carries but
// this build does not know about would be silently deleted by a command whose
// entire business is with metadata.managedFields. Unstructured preserves it,
// and the resourceVersion it carries makes the update its own optimistic lock.
func (a *Release) relinquishManagedFields(ctx context.Context, holder pin.Owner) error {
	repo := &unstructured.Unstructured{}
	repo.SetGroupVersionKind(sourcev1.GroupVersion.WithKind(sourcev1.GitRepositoryKind))
	if err := a.Client.Get(ctx, a.Source, repo); err != nil {
		return fmt.Errorf("getting GitRepository %s: %w", a.Source, err)
	}

	entries, changed := withoutCommit(repo.GetManagedFields(), holder)
	if !changed {
		return nil
	}
	repo.SetManagedFields(entries)

	if err := a.Client.Update(ctx, repo, client.FieldOwner(pin.WfctlFieldManager)); err != nil {
		return fmt.Errorf(
			"rewriting metadata.managedFields of %s to drop the %s hold by %q: %w; "+
				"`wfctl release %s --float` removes the pin instead",
			a.Source, holder.Operation, holder.Manager, err, a.Source)
	}
	return nil
}

// withoutCommit rewrites holder's managedFields entry without the pin.
func withoutCommit(
	entries []metav1.ManagedFieldsEntry,
	holder pin.Owner,
) ([]metav1.ManagedFieldsEntry, bool) {
	kept := make([]metav1.ManagedFieldsEntry, 0, len(entries))
	changed := false

	for _, entry := range entries {
		set, ok := ownedSet(entry, holder)
		if !ok {
			kept = append(kept, entry)
			continue
		}

		reduced := set.Difference(commitSet)
		changed = true
		if reduced.Empty() {
			// An entry that owns nothing is not an entry: leaving one behind
			// would keep the manager visible in every `-o wide` listing.
			continue
		}
		raw, err := reduced.ToJSON()
		if err != nil {
			// Unserialisable is unfixable; leave the entry as it was and let
			// the caller's verification report the surviving hold.
			kept = append(kept, entry)
			continue
		}
		entry.FieldsV1 = metav1.NewFieldsV1(string(raw))
		kept = append(kept, entry)
	}

	return kept, changed
}

// ownedSet returns the field set of an entry that is holder's and owns the pin.
func ownedSet(entry metav1.ManagedFieldsEntry, holder pin.Owner) (*fieldpath.Set, bool) {
	if entry.Manager != holder.Manager || entry.Operation != holder.Operation {
		return nil, false
	}
	if entry.Subresource != "" || entry.FieldsV1 == nil {
		return nil, false
	}
	set := &fieldpath.Set{}
	if err := set.FromJSON(entry.FieldsV1.GetRawReader()); err != nil {
		return nil, false
	}
	if !set.Has(commitPath) {
		return nil, false
	}
	return set, true
}

// now resolves the clock.
func (a *Release) now() time.Time {
	if a.Now == nil {
		return time.Now()
	}
	return a.Now()
}
