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
	"errors"
	"fmt"
	"slices"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// gitOpsAppliers are the field managers whose ownership of a Wavefront spec
// field means the cluster is not where that field is decided. Changing such a
// field by hand works for exactly as long as it takes Flux to reconcile.
var gitOpsAppliers = []string{"kustomize-controller", "helm-controller"}

// WavefrontChange flips one of the two fleet-level brakes: spec.suspend (the
// gentle one) or spec.mode (Shadow ⇄ Enforce). See docs/runbook.md, "Modes and
// brakes".
//
// The write is a merge patch, deliberately, and never a server-side apply: SSA
// would make wfctl an *applier* of the Wavefront spec, and the next GitOps
// reconcile would then have to fight it. A merge patch changes the value and
// leaves the shape of ownership alone, which is what lets a GitOps-managed
// Wavefront be suspended in an incident and then correctly reverted by git
// (plan B4).
type WavefrontChange struct {
	Client client.Client
	// Wavefront is the object as it was read, and the optimistic-lock base.
	Wavefront *wavefrontv1alpha1.Wavefront
	// Suspend sets spec.suspend; nil leaves it alone.
	Suspend *bool
	// Mode sets spec.mode; nil leaves it alone.
	Mode *wavefrontv1alpha1.Mode
}

var _ Action = (*WavefrontChange)(nil)

// errNoWavefrontChange means the action was built with neither field set.
var errNoWavefrontChange = errors.New("no Wavefront spec change requested")

// Plan implements Action.
func (a *WavefrontChange) Plan(_ context.Context) (*Plan, error) {
	field, before, after, err := a.change()
	if err != nil {
		return nil, err
	}

	wf := a.Wavefront
	plan := &Plan{
		Summary: fmt.Sprintf("Set %s of Wavefront %s to %s (merge patch as field manager %q)",
			field, wf.Name, shown(after), pin.WfctlFieldManager),
		Before: map[string]string{field: shown(before)},
		After:  map[string]string{field: shown(after)},
		Apply:  a.apply,
	}

	if before == after {
		plan.Warnings = append(plan.Warnings,
			fmt.Sprintf("no change: %s is already %s", field, shown(after)))
	}
	if owner := snapshot.SpecOwners(wf)[field]; slices.Contains(gitOpsAppliers, owner) {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf(
			"%s is GitOps-owned (field manager %q); Flux will revert this — change it in git", field, owner))
	}

	return plan, nil
}

// change resolves which field is being set, and to what.
func (a *WavefrontChange) change() (field, before, after string, err error) {
	spec := a.Wavefront.Spec
	switch {
	case a.Suspend != nil:
		return snapshot.FieldSuspend, strconv.FormatBool(spec.Suspend), strconv.FormatBool(*a.Suspend), nil
	case a.Mode != nil:
		return snapshot.FieldMode, string(spec.Mode), string(*a.Mode), nil
	default:
		return "", "", "", errNoWavefrontChange
	}
}

// apply issues the merge patch, with an optimistic lock so that a concurrent
// change is reported rather than overwritten.
func (a *WavefrontChange) apply(ctx context.Context) (bool, error) {
	wf := a.Wavefront.DeepCopy()
	patch := client.MergeFromWithOptions(wf.DeepCopy(), client.MergeFromWithOptimisticLock{})

	if a.Suspend != nil {
		wf.Spec.Suspend = *a.Suspend
	}
	if a.Mode != nil {
		wf.Spec.Mode = *a.Mode
	}

	err := a.Client.Patch(ctx, wf, patch, client.FieldOwner(pin.WfctlFieldManager))
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsConflict(err):
		return false, fmt.Errorf("the Wavefront %s changed while wfctl was asking; re-run: %w", wf.Name, err)
	default:
		return false, fmt.Errorf("patching Wavefront %s: %w", wf.Name, err)
	}
}
