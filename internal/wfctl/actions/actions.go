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

// Package actions implements wfctl's write commands: the seven operations
// that change a fleet, and nothing else.
//
// Every action is two-phase. Plan reads the cluster and returns what the write
// would do — the object, the fields, the field manager, the before/after
// values, and every warning the operator should weigh — with the write itself
// sealed in a closure. Nothing is written until the caller has printed the
// plan and obtained consent (confirm.go).
//
// That split is what makes --dry-run honest: a dry run is the real code path
// with its last step withheld, not a second implementation that might describe
// a write nobody would perform.
//
// The writes themselves are chosen for what they leave behind in managedFields,
// because managedFields is what the controller reads to detect a hold: a
// hand-pin is an SSA apply under the wfctl field manager precisely so
// pin.Hold sees it, and a Wavefront spec change is a merge patch precisely so
// a GitOps applier keeps owning the spec.
package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// unset renders an absent field in a plan's before/after table. A blank cell
// would read as "unchanged", which is the opposite of what it means.
const unset = "(unset)"

// The rows every pin-related plan is built around: the field itself, and who
// owns it — which for these commands is half of what is changing.
const (
	commitField       = "spec.ref.commit"
	commitOwnersField = "spec.ref.commit owners"
)

// shaPattern is a full git object ID, SHA-1 or SHA-256, lower-case as git
// prints it. Anything else is a typo, a branch name, or a flag value that went
// missing, and would be written to spec.ref.commit as it stands.
var shaPattern = regexp.MustCompile("^(?:[0-9a-f]{40}|[0-9a-f]{64})$")

// removeCommitPatch is the break-glass one-liner of docs/runbook.md: a JSON
// patch is the only way to *delete* a field without owning it under SSA. The
// leading test op makes the apiserver reject it (422) if spec.ref.commit is no
// longer the value that was planned, so a hand-pin made after the plan is not
// stripped by a command that never showed it to the operator.
func removeCommitPatch(current string) []byte {
	value, _ := json.Marshal(current) // a string always marshals
	return fmt.Appendf(nil,
		`[{"op":"test","path":"/spec/ref/commit","value":%s},{"op":"remove","path":"/spec/ref/commit"}]`, value)
}

// scopeOf is the set of sources a Wavefront owns, as its status records them:
// the pinned members, plus the pins in its release ledger (which outlive a
// source that was de-scoped but not yet released).
func scopeOf(wf *wavefrontv1alpha1.Wavefront) map[types.NamespacedName]bool {
	scope := map[types.NamespacedName]bool{}
	for _, member := range wf.Status.Members {
		if member.Source == "" {
			continue
		}
		if src, err := snapshot.ParseSource(member.Source); err == nil {
			scope[src] = true
		}
	}
	if wf.Status.Pinned != nil {
		for _, ref := range wf.Status.Pinned.Entries {
			if src, ok := pin.LedgerSource(ref); ok {
				scope[src] = true
			}
		}
	}
	return scope
}

// refuseOutOfScope stops a single-source write on a source another Wavefront
// owns: the fleet named by --wavefront is the one the operator thinks they are
// acting on. The limit is that status.members is empty before a Wavefront's
// first reconcile and capped at MembersCap.
func refuseOutOfScope(verb string, wf *wavefrontv1alpha1.Wavefront, src types.NamespacedName) error {
	if scopeOf(wf)[src] {
		return nil
	}
	return fmt.Errorf("refusing to %s %s: not a source of Wavefront %s (status.members / status.pinned)",
		verb, src, wf.Name)
}

// refuseShadow stops a pin write the controller would undo: outside Enforce it
// releases every pin it owns, and a pin it wrote on someone's behalf is its own.
func refuseShadow(verb string, wf *wavefrontv1alpha1.Wavefront, src types.NamespacedName) error {
	if wf.Spec.Mode == wavefrontv1alpha1.ModeEnforce {
		return nil
	}
	return fmt.Errorf("refusing to %s %s: Wavefront %s is in Shadow mode, and the controller "+
		"releases every pin it owns, so this write would be undone on the next pass",
		verb, src, wf.Name)
}

// Plan is what a write would do, and the closure that does it.
type Plan struct {
	// Summary is one line: the operation, the object, the field manager.
	Summary string
	// Before and After are the fields the write touches, keyed identically so
	// that the two render as a single table. A field the write creates appears
	// in After alone, and one it deletes in Before alone.
	Before map[string]string
	After  map[string]string
	// Warnings are what the operator has to weigh before consenting: what the
	// controller will do next, what will revert this, what is being bypassed.
	Warnings []string
	// Apply performs the write and reports whether anything reached the
	// cluster. Never nil: an action with nothing to do says so in Summary and
	// applies a no-op, so that every caller has one path.
	//
	// The two results are independent on purpose. A strip that patched thirty
	// sources and was denied the thirty-first, or a release whose transfer
	// landed and whose relinquish did not, has both changed the cluster and
	// failed — and the audit trail has to say so, because the operator's next
	// question is what state the fleet is in, not whether the command exited 0.
	Apply func(ctx context.Context) (written bool, err error)
}

// Action builds one write's Plan. The CLI knows nothing else about a write.
type Action interface {
	Plan(ctx context.Context) (*Plan, error)
}

// Fields lists every field named by either side of the plan, sorted, so the
// before/after table renders the same way twice over.
func (p *Plan) Fields() []string {
	named := make(map[string]struct{}, len(p.Before)+len(p.After))
	for field := range p.Before {
		named[field] = struct{}{}
	}
	for field := range p.After {
		named[field] = struct{}{}
	}
	return slices.Sorted(maps.Keys(named))
}

// annotationField names an annotation the way the plan table shows it.
func annotationField(key string) string {
	return "metadata.annotations[" + key + "]"
}

// shown renders a possibly-absent value for the plan table.
func shown(value string) string {
	if value == "" {
		return unset
	}
	return value
}

// getSource reads one GitRepository.
func getSource(ctx context.Context, c client.Client, key types.NamespacedName) (*sourcev1.GitRepository, error) {
	repo := &sourcev1.GitRepository{}
	if err := c.Get(ctx, key, repo); err != nil {
		return nil, fmt.Errorf("getting GitRepository %s: %w", key, err)
	}
	return repo, nil
}

// annotationOf reads one annotation, "" when absent.
func annotationOf(repo *sourcev1.GitRepository, key string) string {
	return repo.GetAnnotations()[key]
}

// foreignOwners lists the managers owning spec.ref.commit that are neither the
// controller nor one the caller is entitled to displace.
//
// The controller is never foreign: its ownership is the normal state. wfctl is
// foreign to `pin` (a second hand-pin displaces the first) but not to the
// operator who set it, which is why the tolerated set is a parameter.
func foreignOwners(repo *sourcev1.GitRepository, tolerated ...string) []pin.Owner {
	var foreign []pin.Owner
	for _, owner := range pin.Owners(repo) {
		if owner.Manager == pin.FieldManager || slices.Contains(tolerated, owner.Manager) {
			continue
		}
		foreign = append(foreign, owner)
	}
	return foreign
}

// describeOwners renders owners as "manager (Operation)" for a message.
func describeOwners(owners []pin.Owner) string {
	described := make([]string, 0, len(owners))
	for _, owner := range owners {
		described = append(described, fmt.Sprintf("%s (%s)", owner.Manager, owner.Operation))
	}
	return strings.Join(described, ", ")
}

// holdOf reports the source-scoped hold the controller would see: a foreign
// owner of spec.ref.commit, or the source's own suspension.
func holdOf(repo *sourcev1.GitRepository) (string, bool) {
	switch kind, manager, held := pin.HoldOf(repo); {
	case !held:
		return "", false
	case kind == wavefrontv1alpha1.HoldReasonHandPin:
		return fmt.Sprintf("spec.ref.commit is held by field manager %q", manager), true
	default:
		return "the source is suspended (spec.suspend: true)", true
	}
}

// Advertisement is the ref-listing seam shared by `pin --poll` (which verifies
// a SHA against it) and `force-admit` (which cannot work without one).
//
// Listing from a CLI costs credentials — it reads the source's Secret and
// speaks to the git host from wherever the operator is sitting — so it happens
// only where a command's contract says it must.
type Advertisement struct {
	// Lister lists advertised refs; nil means the production go-git lister,
	// which fetches no objects and touches no disk.
	Lister gitpoll.Lister
	// Timeout bounds the listing; <= 0 means snapshot.DefaultPollTimeout.
	Timeout time.Duration
}

// trackingRef maps a source's ref spec to the advertised ref name to observe.
func (a Advertisement) trackingRef(repo *sourcev1.GitRepository) (string, error) {
	return trackingRefOf(repo)
}

// candidate picks the SHA the tracking ref advertises, under the same strategy
// the controller would apply.
func (a Advertisement) candidate(advertised map[string]string, trackingRef string) (string, bool) {
	return selection.TrackRef().Candidate(advertised, trackingRef)
}

// trackingRefOf resolves the ref a source tracks under the v1 strategy,
// TrackRef. Every action that records provenance needs it, whether or not it
// lists anything.
func trackingRefOf(repo *sourcev1.GitRepository) (string, error) {
	ref, err := selection.TrackRef().TrackingRef(repo.Spec.Reference)
	if err != nil {
		return "", fmt.Errorf("resolving the tracking ref of %s/%s: %w", repo.Namespace, repo.Name, err)
	}
	return ref, nil
}

// list advertises one source's refs, reading its credential Secret if it has
// one. The URL never appears in an error: it may embed credentials.
func (a Advertisement) list(
	ctx context.Context,
	c client.Client,
	repo *sourcev1.GitRepository,
) (map[string]string, error) {
	lister := a.Lister
	if lister == nil {
		timeout := a.Timeout
		if timeout <= 0 {
			timeout = snapshot.DefaultPollTimeout
		}
		lister = gitpoll.NewGoGitLister(timeout)
	}

	target := gitpoll.Target{
		Source: types.NamespacedName{Namespace: repo.Namespace, Name: repo.Name},
		URL:    repo.Spec.URL,
	}
	if repo.Spec.SecretRef != nil {
		target.SecretRef = &types.NamespacedName{Namespace: repo.Namespace, Name: repo.Spec.SecretRef.Name}
	}

	advertised, err := gitpoll.List(ctx, c, lister, target)
	if err != nil {
		return nil, fmt.Errorf("listing the refs of %s/%s: %w", repo.Namespace, repo.Name, err)
	}
	return advertised, nil
}

// advertises reports whether sha is one of the advertised SHAs. Verification
// is against the whole advertisement, not just the tracking ref: an operator
// pinning a SHA from another branch during an incident is pinning something
// the remote demonstrably has, which is the property worth checking.
func advertises(advertised map[string]string, sha string) bool {
	for _, candidate := range advertised {
		if strings.EqualFold(candidate, sha) {
			return true
		}
	}
	return false
}

// applyOp reports whether an owner holds its fields through server-side apply.
// Apply-op and Update-op holders are relinquished by different means, and the
// apiserver keys them separately even under the same manager name.
func applyOp(owner pin.Owner) bool {
	return owner.Operation == metav1.ManagedFieldsOperationApply
}
