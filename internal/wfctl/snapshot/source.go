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
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
)

// Source captures one Snapshot. It is the only seam every renderer and write
// command sees, so where a picture came from — published status, live
// re-derivation, a replayed file, or a future controller API — is a choice
// the CLI makes once, at the top.
type Source interface {
	Capture(ctx context.Context) (*Snapshot, error)
}

// SelectWavefront resolves the Wavefront to operate on. A named one is
// fetched directly; with no name, a single Wavefront is auto-selected and
// anything else is an error that names the candidates — guessing which
// Wavefront an operator meant is exactly the mistake an incident cannot
// afford (decision "Conventions").
func SelectWavefront(ctx context.Context, r client.Reader, name string) (*wavefrontv1alpha1.Wavefront, error) {
	if name != "" {
		wf := &wavefrontv1alpha1.Wavefront{}
		// Wavefronts are cluster-scoped, so the key carries no namespace.
		if err := r.Get(ctx, types.NamespacedName{Name: name}, wf); err != nil {
			return nil, fmt.Errorf("getting Wavefront %q: %w", name, err)
		}
		return wf, nil
	}

	list := &wavefrontv1alpha1.WavefrontList{}
	if err := r.List(ctx, list); err != nil {
		return nil, fmt.Errorf("listing Wavefronts: %w", err)
	}

	switch len(list.Items) {
	case 0:
		return nil, fmt.Errorf("no Wavefronts exist in this cluster")
	case 1:
		return &list.Items[0], nil
	default:
		names := make([]string, 0, len(list.Items))
		for i := range list.Items {
			names = append(names, list.Items[i].Name)
		}
		slices.Sort(names)
		return nil, fmt.Errorf(
			"%d Wavefronts exist; select one with --wavefront: %s",
			len(names), strings.Join(names, ", "))
	}
}

// specModePath and specSuspendPath are the field-set paths of the two spec
// fields wfctl writes. In a managedFields entry's FieldsV1 encoding these are
// the leaves f:spec.f:mode and f:spec.f:suspend.
var (
	specModePath    = fieldpath.MakePathOrDie("spec", "mode")
	specSuspendPath = fieldpath.MakePathOrDie("spec", "suspend")
)

// The SpecOwners keys — the two Wavefront spec fields wfctl writes.
const (
	FieldMode    = "spec.mode"
	FieldSuspend = "spec.suspend"
)

// specOwnerFields names the SpecOwners keys, alongside the path each is read
// from.
var specOwnerFields = []struct {
	name string
	path fieldpath.Path
}{
	{FieldMode, specModePath},
	{FieldSuspend, specSuspendPath},
}

// wavefrontView renders the Wavefront itself, including who owns the two
// spec fields the write commands patch. The raw managedFields never leave
// SpecOwners: only the manager names do.
func wavefrontView(wf *wavefrontv1alpha1.Wavefront) WavefrontView {
	return WavefrontView{
		Name:       wf.Name,
		Generation: wf.Generation,
		Spec:       wf.Spec,
		Status:     wf.Status,
		SpecOwners: SpecOwners(wf),
	}
}

// SpecOwners maps "spec.mode" and "spec.suspend" to their owning field
// manager, so a write command can warn that a GitOps applier owns the field
// and will revert the change. It is exported because that warning
// is built by internal/wfctl/actions, from a Wavefront it read itself.
//
// The first entry to claim a field wins: managedFields is returned in a
// stable order by the apiserver, and co-ownership of a scalar is rare enough
// that reporting one manager beats inventing a list the schema has no room
// for. Subresource entries cannot own spec and are skipped, as are entries
// whose FieldsV1 will not parse — an unparseable entry proves no ownership.
func SpecOwners(wf *wavefrontv1alpha1.Wavefront) map[string]string {
	owners := map[string]string{}
	for _, entry := range wf.GetManagedFields() {
		if entry.Subresource != "" || entry.FieldsV1 == nil {
			continue
		}
		set := &fieldpath.Set{}
		if err := set.FromJSON(entry.FieldsV1.GetRawReader()); err != nil {
			continue
		}
		for _, field := range specOwnerFields {
			if _, claimed := owners[field.name]; !claimed && set.Has(field.path) {
				owners[field.name] = entry.Manager
			}
		}
	}
	if len(owners) == 0 {
		return nil
	}
	return owners
}

// stripUserinfo removes any embedded credentials from a git URL.
//
// A URL that will not parse yields "": a snapshot is written to files and
// pasted into incident channels, so an unparseable URL is dropped rather than
// forwarded on the chance that it embeds a password.
func stripUserinfo(raw string) string {
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if parsed.User == nil {
		return raw
	}
	parsed.User = nil
	return parsed.String()
}

// timePtr converts a time to the optional form the schema uses, mapping the
// zero time to nil: "never" and "the epoch" must not render alike.
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	out := t
	return &out
}

// stampTime renders a derived moment the way both origins can agree on:
// whole seconds, in UTC.
//
// status.members round-trips every timestamp through metav1.Time, which
// serialises as RFC3339 and so keeps only whole seconds, and comes back in the
// local zone. A derived timestamp carries full nanosecond precision in
// whatever zone it was made in, so without one normal form the same instant
// would compare unequal between the two providers — and a renderer diffing
// REPORTED against DERIVED would report a disagreement that is nothing but
// serialisation.
func stampTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	out := t.UTC().Truncate(time.Second)
	return &out
}

// nowFunc defaults an injected clock to the wall clock.
func nowFunc(now func() time.Time) func() time.Time {
	if now == nil {
		return time.Now
	}
	return now
}
