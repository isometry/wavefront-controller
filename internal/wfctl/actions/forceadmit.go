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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/pin"
)

// ForceAdmit admits one source past its gate, once.
//
// It writes exactly the pin the controller would have written — same field
// manager, same three provenance annotations — for the SHA the source's
// tracking ref currently advertises. The controller then finds pin == observed
// and walks the node through Converging to Settled without an admission of its
// own, so no PinAdvanced event is emitted: the ForceAdmitted audit event is the
// record that a human, not the graph, let this through.
//
// Writing the pin is the whole point, and is why "just unpin it" is not the
// same thing: an unpinned source is initial-pinned to its *artifact* SHA
// (DESIGN §3.5.4), which during an incident is usually the stale revision the
// operator is trying to get past.
type ForceAdmit struct {
	Client client.Client
	Source types.NamespacedName
	// Wavefront is the fleet, read for the suspend warning.
	Wavefront *wavefrontv1alpha1.Wavefront
	// SHA overrides the advertised tracking-ref SHA.
	SHA string
	// Unverified accepts SHA without listing the remote's refs.
	Unverified bool
	// Now stamps the admitted-at annotation; nil means the wall clock.
	Now func() time.Time

	Advertisement
}

var _ Action = (*ForceAdmit)(nil)

// Plan implements Action.
func (a *ForceAdmit) Plan(ctx context.Context) (*Plan, error) {
	repo, err := getSource(ctx, a.Client, a.Source)
	if err != nil {
		return nil, err
	}

	// A hold is a decision somebody made, and force-admitting past it would
	// overwrite that decision with no trace beyond an event. Releasing it is a
	// separate, deliberate command.
	if reason, held := holdOf(repo); held {
		return nil, fmt.Errorf(
			"refusing to force-admit %s: %s; release the hold first (`wfctl release %s`, or clear "+
				"spec.suspend on the source)", a.Source, reason, a.Source)
	}

	trackingRef, err := a.trackingRef(repo)
	if err != nil {
		return nil, err
	}

	sha, warnings, err := a.resolve(ctx, repo, trackingRef)
	if err != nil {
		return nil, err
	}

	current := pinOf(repo)
	at := a.now()

	if current == sha {
		warnings = append(warnings, fmt.Sprintf(
			"%s is already pinned to %s; this only refreshes its provenance", a.Source, sha))
	}
	if a.Wavefront.Spec.Suspend {
		warnings = append(warnings, fmt.Sprintf(
			"Wavefront %s is suspended: this pin is written anyway, but the controller will not "+
				"advance anything behind it until suspend is cleared", a.Wavefront.Name))
	}
	if a.Wavefront.Spec.Mode == wavefrontv1alpha1.ModeShadow {
		warnings = append(warnings, fmt.Sprintf(
			"Wavefront %s is in Shadow mode, which suppresses the controller's own writes; this "+
				"one is not suppressed", a.Wavefront.Name))
	}
	warnings = append(warnings,
		"this bypasses ancestor gating once: the controller sees pin == observed and settles the "+
			"node without an admission, so no PinAdvanced event is emitted — the ForceAdmitted "+
			"audit event is the record")

	return &Plan{
		Summary: fmt.Sprintf("Force-admit %s to %s observed on %s (server-side apply as field manager %q)",
			a.Source, sha, trackingRef, pin.FieldManager),
		Before: map[string]string{
			commitField:                           shown(current),
			annotationField(pin.AnnotAdmittedAt):  shown(annotationOf(repo, pin.AnnotAdmittedAt)),
			annotationField(pin.AnnotPreviousPin): shown(annotationOf(repo, pin.AnnotPreviousPin)),
			annotationField(pin.AnnotObservedRef): shown(annotationOf(repo, pin.AnnotObservedRef)),
		},
		After: map[string]string{
			commitField:                           sha,
			annotationField(pin.AnnotAdmittedAt):  at.UTC().Format(time.RFC3339),
			annotationField(pin.AnnotPreviousPin): shown(current),
			annotationField(pin.AnnotObservedRef): trackingRef,
		},
		Warnings: warnings,
		Apply: func(ctx context.Context) (bool, error) {
			writer := &pin.Writer{Client: a.Client}
			err := writer.Advance(ctx, a.Source, current, sha, trackingRef, at)
			return err == nil, err
		},
	}, nil
}

// resolve settles which SHA is admitted: the one the tracking ref advertises,
// or an override checked against the same advertisement.
func (a *ForceAdmit) resolve(
	ctx context.Context,
	repo *sourcev1.GitRepository,
	trackingRef string,
) (string, []string, error) {
	if a.SHA != "" && a.Unverified {
		// The one path that speaks to no remote, and so needs no credentials:
		// asked for by name, and warned about.
		return a.SHA, []string{fmt.Sprintf(
			"--unverified: %s was not checked against the remote; a SHA the remote does not have "+
				"will leave the source failing to fetch", a.SHA)}, nil
	}

	advertised, err := a.list(ctx, a.Client, repo)
	if err != nil {
		return "", nil, err
	}

	if a.SHA != "" {
		if !advertises(advertised, a.SHA) {
			return "", nil, fmt.Errorf(
				"%s advertises no ref at %s (%d refs listed); check the SHA, or pass --unverified",
				a.Source, a.SHA, len(advertised))
		}
		return a.SHA, nil, nil
	}

	sha, ok := a.candidate(advertised, trackingRef)
	if !ok {
		return "", nil, fmt.Errorf("%s does not advertise %s, so there is nothing to admit",
			a.Source, trackingRef)
	}
	return sha, nil, nil
}

// now resolves the clock.
func (a *ForceAdmit) now() time.Time {
	if a.Now == nil {
		return time.Now()
	}
	return a.Now()
}
