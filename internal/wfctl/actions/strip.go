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
	"strconv"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
)

// Strip is the break-glass procedure documented in docs/runbook.md's
// "Break-glass: pin-strip" section: remove every managed source's
// spec.ref.commit, restoring plain floating-ref Flux.
//
// It is the runbook's own loop, made safe. The shell one-liner strips whatever
// it finds; this refuses to touch a source somebody is holding by hand unless
// told to, keeps going when one namespace denies it, and says out loud that the
// controller will re-pin everything on its next sweep unless the fleet is
// suspended — which --suspend does first, in the same command.
type Strip struct {
	Client client.Client
	// Wavefront is the fleet whose suspend state governs whether the strip
	// sticks, and the object --suspend flips.
	Wavefront *wavefrontv1alpha1.Wavefront
	// IncludeHeld strips hand-pinned sources too.
	IncludeHeld bool
	// Suspend suspends the fleet before stripping.
	Suspend bool
}

var _ Action = (*Strip)(nil)

// fleetField names the Wavefront's suspend flag in the plan table, qualified
// because every other row of a strip plan is a source.
const fleetField = "Wavefront spec.suspend"

// Plan implements Action.
func (a *Strip) Plan(ctx context.Context) (*Plan, error) {
	list := &sourcev1.GitRepositoryList{}
	if err := a.Client.List(ctx, list, client.MatchingLabels{pin.ManagedLabel: "true"}); err != nil {
		return nil, fmt.Errorf("listing managed GitRepositories: %w", err)
	}

	var (
		targets  []types.NamespacedName
		before   = map[string]string{}
		after    = map[string]string{}
		warnings []string
	)

	for i := range list.Items {
		repo := &list.Items[i]
		current := pinOf(repo)
		if current == "" {
			continue
		}
		key := types.NamespacedName{Namespace: repo.Namespace, Name: repo.Name}

		// The same hold the controller sees, which is a hand-pin *or* the
		// source's own suspension: stripping a suspended source's pin
		// silently would undo somebody's brake.
		if reason, held := holdOf(repo); held && !a.IncludeHeld {
			warnings = append(warnings, fmt.Sprintf(
				"skipping %s: %s (--include-held strips it too)", key, reason))
			continue
		}

		targets = append(targets, key)
		before[key.String()] = current
		after[key.String()] = unset
	}

	// The suspend step is a WavefrontChange like any other, planned rather
	// than performed inline: its warnings are the ones that matter most here.
	// Being told that spec.suspend is GitOps-owned — that Flux will put the
	// brake back on the next reconcile — is the difference between a
	// break-glass that holds and one that quietly re-pins the fleet.
	var suspendPlan *Plan
	if a.Suspend {
		var err error
		if suspendPlan, err = a.suspendChange().Plan(ctx); err != nil {
			return nil, err
		}
		before[fleetField] = strconv.FormatBool(a.Wavefront.Spec.Suspend)
		after[fleetField] = "true"
		warnings = append(warnings, suspendPlan.Warnings...)
	}

	return &Plan{
		Summary:  a.summary(len(targets)),
		Before:   before,
		After:    after,
		Warnings: append(warnings, a.consequences(len(targets))...),
		Apply: func(ctx context.Context) (bool, error) {
			return a.apply(ctx, suspendPlan, targets)
		},
	}, nil
}

// suspendChange is the --suspend step: the ordinary fleet-suspend action.
func (a *Strip) suspendChange() *WavefrontChange {
	suspend := true
	return &WavefrontChange{Client: a.Client, Wavefront: a.Wavefront, Suspend: &suspend}
}

// summary says how much is about to be unpinned.
func (a *Strip) summary(targets int) string {
	suspend := ""
	if a.Suspend {
		suspend = fmt.Sprintf("suspend Wavefront %s, then ", a.Wavefront.Name)
	}
	return fmt.Sprintf("Break-glass: %sremove spec.ref.commit from %d managed %s (JSON patch)",
		suspend, targets, plural(targets, "source", "sources"))
}

// consequences is the warning block every strip must show.
func (a *Strip) consequences(targets int) []string {
	warnings := []string{fmt.Sprintf(
		"the controller re-pins every stripped source on its next sweep unless the fleet is "+
			"suspended — Wavefront %s is currently suspend=%t",
		a.Wavefront.Name, a.Wavefront.Spec.Suspend)}

	if a.Suspend {
		warnings = append(warnings, fmt.Sprintf(
			"--suspend sets spec.suspend=true on Wavefront %s first; nothing resumes until it is "+
				"set back to false", a.Wavefront.Name))
	}
	if targets == 0 {
		warnings = append(warnings, "no managed source carries a pin: this strips nothing")
	}
	return warnings
}

// apply suspends if asked, then strips, reporting every failure and stopping
// for none of them: a break-glass procedure that abandons forty sources
// because the thirty-first is in a namespace this token cannot patch has made
// the incident worse.
//
// It reports whether anything landed, which for a partial strip is the whole
// point: thirty stripped sources are a fact about the fleet whether or not the
// command exits 0.
func (a *Strip) apply(ctx context.Context, suspendPlan *Plan, targets []types.NamespacedName) (bool, error) {
	if suspendPlan != nil {
		if written, err := suspendPlan.Apply(ctx); err != nil {
			return written, fmt.Errorf("suspending before the strip (nothing was stripped): %w", err)
		}
	}
	written := suspendPlan != nil

	var failures []error
	for _, key := range targets {
		repo := &sourcev1.GitRepository{}
		repo.Namespace, repo.Name = key.Namespace, key.Name
		if err := a.Client.Patch(ctx, repo, client.RawPatch(types.JSONPatchType, removeCommitPatch),
			client.FieldOwner(pin.WfctlFieldManager)); err != nil {
			failures = append(failures, fmt.Errorf("stripping %s: %w", key, err))
			continue
		}
		written = true
	}

	if len(failures) > 0 {
		return written, fmt.Errorf("%d of %d sources could not be stripped: %w",
			len(failures), len(targets), errors.Join(failures...))
	}
	return written, nil
}

// plural picks the noun. Counts appear in every strip summary, and "1 sources"
// in an incident channel reads as a bug in the tool.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
