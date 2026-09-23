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

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/isometry/wavefront-controller/internal/pin"
)

// Pin hand-pins one source: it sets spec.ref.commit under the wfctl field
// manager, which is precisely how the controller comes to report the source as
// held (docs/runbook.md "Hand-pin etiquette").
//
// The write carries no provenance annotations. Those three annotations are the
// controller's record of an *admission* it made, and forging them for a human
// decision would corrupt the one durable ledger the fleet has. What it
// does record is the displaced pin, so that `wfctl release` can hand the value
// back to the controller with its history intact.
type Pin struct {
	Client client.Client
	Source types.NamespacedName
	// SHA is the commit to pin.
	SHA string
	// Force displaces a third-party owner of spec.ref.commit.
	Force bool
	// Unverified pins a SHA nobody checked against the remote.
	Unverified bool
	// Verify lists the source's refs and checks SHA against them (--poll).
	Verify bool

	Advertisement
}

var _ Action = (*Pin)(nil)

// Plan implements Action.
func (a *Pin) Plan(ctx context.Context) (*Plan, error) {
	repo, err := getSource(ctx, a.Client, a.Source)
	if err != nil {
		return nil, err
	}

	// wfctl is tolerated: re-pinning a source this tool already holds is a
	// correction, not a displacement.
	foreign := foreignOwners(repo, pin.WfctlFieldManager)
	if len(foreign) > 0 && !a.Force {
		return nil, fmt.Errorf(
			"spec.ref.commit of %s is owned by %s, which is neither the controller nor wfctl; "+
				"re-run with --force to take it, having established that nothing else is mid-flight",
			a.Source, describeOwners(foreign))
	}

	warnings, err := a.verify(ctx, repo)
	if err != nil {
		return nil, err
	}

	current := pinOf(repo)
	displacedField := annotationField(pin.AnnotDisplacedPin)

	if len(foreign) > 0 {
		warnings = append(warnings, fmt.Sprintf(
			"--force takes spec.ref.commit from %s", describeOwners(foreign)))
	}
	if current == a.SHA {
		warnings = append(warnings, fmt.Sprintf(
			"%s is already pinned to %s; this only moves ownership of the field to %q",
			a.Source, a.SHA, pin.WfctlFieldManager))
	}
	warnings = append(warnings, fmt.Sprintf(
		"the controller will report %s held (HoldDetected, reason HandPin) and every node behind it "+
			"blocked (AncestorHeld) until `wfctl release %s` runs", a.Source, a.Source),
		"no provenance annotations are written: a hand-pin is not an admission")

	return &Plan{
		Summary: fmt.Sprintf("Hand-pin %s to %s (server-side apply as field manager %q, with force ownership)",
			a.Source, a.SHA, pin.WfctlFieldManager),
		Before: map[string]string{
			commitField:    shown(current),
			displacedField: shown(annotationOf(repo, pin.AnnotDisplacedPin)),
		},
		After: map[string]string{
			commitField:    a.SHA,
			displacedField: shown(current),
		},
		Warnings: warnings,
		Apply:    func(ctx context.Context) (bool, error) { return a.apply(ctx, current) },
	}, nil
}

// verify checks the SHA against the remote's advertisement, or explains why
// nobody did. A pin nobody verified is a perfectly reasonable thing to want
// during an incident — a SHA read off a build log, from a remote wfctl has no
// credentials for — but it has to be asked for by name.
func (a *Pin) verify(ctx context.Context, repo *sourcev1.GitRepository) ([]string, error) {
	if !a.Verify {
		if !a.Unverified {
			return nil, fmt.Errorf(
				"--sha %s has not been checked against %s: add --poll to verify it against the "+
					"remote's advertised refs, or --unverified to pin it unchecked", a.SHA, a.Source)
		}
		return []string{fmt.Sprintf(
			"--unverified: %s was not checked against the remote; a SHA the remote does not have "+
				"will leave the source failing to fetch", a.SHA)}, nil
	}

	advertised, err := a.list(ctx, a.Client, repo)
	if err != nil {
		return nil, err
	}
	if !advertises(advertised, a.SHA) {
		return nil, fmt.Errorf(
			"%s advertises no ref at %s (%d refs listed); check the SHA, or pass --unverified",
			a.Source, a.SHA, len(advertised))
	}
	return nil, nil
}

// apply performs the server-side apply.
//
// ForceOwnership is deliberate and is the one place in this repository that
// uses it: the controller never forces past a human, but a human asking for a
// hand-pin is asking for exactly that, and the alternative — a conflict error
// naming the controller — would be noise in an incident.
func (a *Pin) apply(ctx context.Context, displaced string) (bool, error) {
	desired := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": sourcev1.GroupVersion.String(),
		"kind":       sourcev1.GitRepositoryKind,
		"metadata": map[string]any{
			"name":      a.Source.Name,
			"namespace": a.Source.Namespace,
			"annotations": map[string]any{
				// Written even when empty, so that the applied field set is
				// the same shape whether or not the source was pinned — and so
				// that `release` can tell "displaced nothing" from "never
				// hand-pinned by wfctl".
				pin.AnnotDisplacedPin: displaced,
			},
		},
		"spec": map[string]any{
			"ref": map[string]any{
				"commit": a.SHA,
			},
		},
	}}

	if err := a.Client.Apply(ctx, client.ApplyConfigurationFromUnstructured(desired),
		client.FieldOwner(pin.WfctlFieldManager), client.ForceOwnership); err != nil {
		return false, fmt.Errorf("hand-pinning %s to %s: %w", a.Source, a.SHA, err)
	}
	return true, nil
}
