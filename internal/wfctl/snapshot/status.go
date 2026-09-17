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
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/fluxcd/pkg/git"
	"github.com/fluxcd/pkg/runtime/conditions"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/gitpoll"
	"github.com/isometry/wavefront-controller/internal/graph"
	"github.com/isometry/wavefront-controller/internal/pin"
	"github.com/isometry/wavefront-controller/internal/selection"
)

// staleGrace is the slack added to the poll interval before status counts as
// stale: the controller advances status.lastEvaluated at most once per
// interval, so a snapshot taken just before an update is legitimately one
// interval old, and a reconcile itself takes time.
const staleGrace = 30 * time.Second

// staleFactor doubles the interval-plus-grace budget, so a single missed
// sweep does not cry wolf. Together: stale when the last evaluation is older
// than 2 x (poll.interval + 30s) (plan B2).
const staleFactor = 2

// StatusSource is the default provider: it reports what the controller
// published, and needs nothing beyond get/list on wavefronts (plan B5's
// viewer tier).
//
// The Wavefront's status.members is a complete, self-contained picture of the
// last evaluation — every node, its state, its edges, its pin and its
// observed SHA (DESIGN §4.1) — so this is the exact inverse of
// inputs.Summarise's members mapping. What status cannot carry, it does not
// invent: the Ready condition's message, the applied SHA and the
// explicit-Failing distinction are left zero (see NodeView), and a snapshot
// older than the controller's own cadence earns a Diagnostic rather than a
// silent lie.
type StatusSource struct {
	Reader    client.Reader
	Wavefront string
	// Now is the clock used for CapturedAt and the staleness verdict; nil
	// means time.Now.
	Now func() time.Time
}

var _ Source = (*StatusSource)(nil)

// Capture implements Source.
func (s *StatusSource) Capture(ctx context.Context) (*Snapshot, error) {
	wf, err := SelectWavefront(ctx, s.Reader, s.Wavefront)
	if err != nil {
		return nil, err
	}

	now := nowFunc(s.Now)()
	snap := &Snapshot{
		APIVersion: Version,
		Kind:       KindSnapshot,
		Origin:     OriginStatus,
		CapturedAt: now,
		Evaluated:  evaluatedAt(wf),
		// The controller only ever writes an observed SHA it actually
		// observed, so every member's ObservedSHA is meaningful; an empty one
		// means that node is unobserved, not that observation is unavailable.
		Observed:  true,
		Wavefront: wavefrontView(wf),
	}

	snap.Nodes = statusNodes(wf)
	applyWaves(snap.Nodes)
	snap.Graph = statusGraph(snap.Nodes)

	sources, sourceDiags, err := s.sources(ctx, wf, snap.Nodes)
	if err != nil {
		return nil, err
	}
	snap.Sources = sources

	snap.Diagnostics = append(staleDiagnostics(wf, now), sourceDiags...)
	if wf.Status.MembersOmitted > 0 {
		snap.Diagnostics = append(snap.Diagnostics, fmt.Sprintf(
			"status.members is capped: %d of %d evaluated nodes omitted; the counts remain authoritative (use --derive for the full set)",
			wf.Status.MembersOmitted, len(wf.Status.Members)+wf.Status.MembersOmitted))
	}

	return snap, nil
}

// evaluatedAt lifts status.lastEvaluated out of its metav1 wrapper.
func evaluatedAt(wf *wavefrontv1alpha1.Wavefront) *time.Time {
	if wf.Status.LastEvaluated == nil {
		return nil
	}
	return timePtr(wf.Status.LastEvaluated.Time)
}

// statusNodes inverts inputs.Summarise's members mapping. Members are already
// sorted by kind/namespace/name — the same key NodeView is sorted by — so the
// order carries through unchanged.
func statusNodes(wf *wavefrontv1alpha1.Wavefront) []NodeView {
	nodes := make([]NodeView, 0, len(wf.Status.Members))
	for i := range wf.Status.Members {
		member := &wf.Status.Members[i]
		node := NodeView{
			Ref:          nodeRef(member.Node),
			Role:         member.Role,
			State:        member.State,
			Held:         member.Held,
			Blocked:      member.Blocked.DeepCopy(),
			PendingSince: memberTime(member.PendingSince),
			Ready:        member.Ready,
			Pin:          member.Pin,
			ObservedSHA:  member.ObservedSHA,
		}
		if len(member.DependsOn) > 0 {
			node.DependsOn = make([]adapter.NodeRef, 0, len(member.DependsOn))
			for _, dep := range member.DependsOn {
				node.DependsOn = append(node.DependsOn, nodeRef(dep))
			}
		}
		if member.Source != "" {
			source := member.Source
			node.Source = &source
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// statusGraph rebuilds the structural verdict from the members' own edges.
// Missing is left empty: status records a dangling dependency as an ordinary
// unready gate member, indistinguishable from a real one, so claiming a
// missing set here would be a guess (see GraphView).
func statusGraph(nodes []NodeView) GraphView {
	edges := make(map[adapter.NodeRef][]adapter.NodeRef, len(nodes))
	for _, node := range nodes {
		edges[node.Ref] = node.DependsOn
	}
	built := graph.Build(edges)
	return GraphView{Cycles: built.Cycles(), Unknown: built.Unknown()}
}

// sources builds one SourceView per distinct source named by the members,
// reading each GitRepository for the detail status does not carry: URL,
// tracking ref, owners, provenance, conditions.
//
// A GitRepository that is forbidden or gone is not an error. The viewer RBAC
// tier deliberately grants no access to them (plan B5), so a partial source
// list is the normal case for a flotilla team owner: the entry keeps what the
// members already proved — its name, its pin, its observed SHA, the nodes
// referencing it and its hold — is marked Partial, and earns one Diagnostic.
// Any other read failure is a real fault and is returned.
func (s *StatusSource) sources(
	ctx context.Context,
	wf *wavefrontv1alpha1.Wavefront,
	nodes []NodeView,
) ([]SourceView, []string, error) {
	byName := map[string][]adapter.NodeRef{}
	pins := map[string]string{}
	observed := map[string]string{}
	for _, node := range nodes {
		if node.Source == nil {
			continue
		}
		byName[*node.Source] = append(byName[*node.Source], node.Ref)
		// Every node referencing a source reports the same pin and observed
		// SHA — they are properties of the source, not of the node — so the
		// first is as good as any.
		if _, seen := pins[*node.Source]; !seen {
			pins[*node.Source], observed[*node.Source] = node.Pin, node.ObservedSHA
		}
	}

	holds := heldLedger(wf)
	strategy := selection.TrackRef()

	var diags []string
	views := make([]SourceView, 0, len(byName))
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		view := SourceView{
			Name:        name,
			Pin:         pins[name],
			ObservedSHA: observed[name],
			Nodes:       byName[name],
			// Status still knows whether the source is held, and by whom, so
			// the hold survives even when the GitRepository does not.
			Hold: holds[name],
		}

		repo, err := s.readRepo(ctx, name)
		switch {
		case apierrors.IsForbidden(err) || apierrors.IsNotFound(err):
			view.Partial = true
			diags = append(diags, fmt.Sprintf(
				"GitRepository %s could not be read (%s): its detail is partial",
				name, apierrors.ReasonForError(err)))
		case err != nil:
			return nil, nil, fmt.Errorf("reading source %s: %w", name, err)
		default:
			describeRepo(&view, repo, strategy)
			view.Hold = repoHold(repo)
			// SourceView is the live view of the source, so it keeps the live
			// pin; a node's Pin comes from the published members. When the two
			// disagree the status is simply behind the cluster, and saying so
			// is the difference between a confusing table and a diagnosed one.
			if view.Pin != pins[name] {
				diags = append(diags, fmt.Sprintf(
					"source %s: pin %s differs from last evaluated %s (status is behind; controller has advanced or a hand-pin landed)",
					name, shortSHA(view.Pin), shortSHA(pins[name])))
			}
		}

		views = append(views, view)
	}

	return views, diags, nil
}

// readRepo fetches one source by its "ns/name". A name that will not parse is
// reported as NotFound: the members list is the controller's own output, so a
// malformed name there is a degradation to report, not a fault to abort on.
func (s *StatusSource) readRepo(ctx context.Context, name string) (*sourcev1.GitRepository, error) {
	src, err := ParseSource(name)
	if err != nil {
		return nil, apierrors.NewNotFound(sourcev1.GroupVersion.WithResource("gitrepositories").GroupResource(), name)
	}
	repo := &sourcev1.GitRepository{}
	if err := s.Reader.Get(ctx, src, repo); err != nil {
		return nil, err
	}
	return repo, nil
}

// heldLedger indexes status.held by source, so a source whose GitRepository
// is unreadable still reports the hold the controller recorded.
func heldLedger(wf *wavefrontv1alpha1.Wavefront) map[string]*HoldView {
	ledger := make(map[string]*HoldView, len(wf.Status.Held))
	for _, held := range wf.Status.Held {
		ledger[held.Source] = &HoldView{Kind: held.Reason, Manager: held.Manager}
	}
	return ledger
}

// describeRepo fills in everything only the GitRepository itself carries.
// It is shared with DeriveSource so that a source renders identically under
// either origin.
func describeRepo(view *SourceView, repo *sourcev1.GitRepository, strategy selection.Strategy) {
	view.URL = stripUserinfo(repo.Spec.URL)
	view.Suspended = repo.Spec.Suspend
	view.CommitOwners = pin.Owners(repo)
	view.Provenance = provenance(repo)
	view.ArtifactSHA = artifactSHA(repo)
	view.FetchFailing = conditions.IsTrue(repo, sourcev1.FetchFailedCondition)
	view.Conditions = scrubConditions(repo.Status.Conditions, repo.Spec.URL)
	view.Pin = currentPin(repo)

	if repo.Spec.SecretRef != nil {
		// The name only: a Snapshot never carries a Secret's contents.
		view.SecretRefName = repo.Spec.SecretRef.Name
	}
	// An unsupported ref style (semver, DESIGN D10) has no tracking ref to
	// report; the source is a gate and is announced as such elsewhere.
	if ref, err := strategy.TrackingRef(repo.Spec.Reference); err == nil {
		view.TrackingRef = ref
	}
}

// credentialedURL matches any "scheme://userinfo@" prefix, so a message
// quoting a URL this package never saw — a redirect target, a submodule, a
// previous spec.url — is scrubbed too.
var credentialedURL = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.\-]*://)[^\s/@]*@`)

// scrubConditions copies a source's conditions with every embedded credential
// removed from their messages.
//
// source-controller quotes spec.url verbatim in clone and authentication
// failures, so a GitRepository whose URL embeds a token would smuggle it into
// a Snapshot through the one field that is otherwise a passthrough — and a
// Snapshot is written to files and pasted into incident channels. The exact
// URL is replaced first (so the stripped form still identifies the repository)
// and the general pattern catches anything else.
func scrubConditions(status []metav1.Condition, repoURL string) []metav1.Condition {
	if len(status) == 0 {
		return nil
	}
	out := slices.Clone(status)
	for i := range out {
		out[i].Message = scrubURLs(out[i].Message, repoURL)
	}
	return out
}

// scrubURLs removes userinfo from every URL in text.
func scrubURLs(text, repoURL string) string {
	if text == "" {
		return text
	}
	if repoURL != "" {
		if stripped := stripUserinfo(repoURL); stripped != repoURL {
			text = strings.ReplaceAll(text, repoURL, stripped)
		}
	}
	return credentialedURL.ReplaceAllString(text, "$1")
}

// repoHold reports how a source is held, with the same precedence
// inputs.resolve applies: a hand-pin outranks a suspend because it names an
// actor and a suspend does not (decision D-B).
func repoHold(repo *sourcev1.GitRepository) *HoldView {
	if manager, held := pin.Hold(repo); held {
		return &HoldView{Kind: wavefrontv1alpha1.HoldReasonHandPin, Manager: manager}
	}
	if repo.Spec.Suspend {
		return &HoldView{Kind: wavefrontv1alpha1.HoldReasonSuspend}
	}
	return nil
}

// provenance extracts the three pin annotations — the durable ledger, since
// events expire with the apiserver's --event-ttl (plan B3, `history`).
func provenance(repo *sourcev1.GitRepository) map[string]string {
	annotations := repo.GetAnnotations()
	out := map[string]string{}
	for _, key := range []string{pin.AnnotAdmittedAt, pin.AnnotPreviousPin, pin.AnnotObservedRef, pin.AnnotDisplacedPin} {
		if value, ok := annotations[key]; ok {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// shortSHA renders a commit the way every column does — seven characters —
// naming the unpinned case rather than printing nothing.
func shortSHA(sha string) string {
	const short = 7
	switch {
	case sha == "":
		return "(unpinned)"
	case len(sha) <= short:
		return sha
	default:
		return sha[:short]
	}
}

// currentPin reads spec.ref.commit, "" when unpinned.
func currentPin(repo *sourcev1.GitRepository) string {
	if repo.Spec.Reference == nil {
		return ""
	}
	return repo.Spec.Reference.Commit
}

// artifactSHA extracts the commit of the last successful reconciliation.
func artifactSHA(repo *sourcev1.GitRepository) string {
	if repo.Status.Artifact == nil {
		return ""
	}
	return git.ExtractHashFromRevision(repo.Status.Artifact.Revision).String()
}

// staleDiagnostics warns when the published picture may no longer describe the
// cluster: either it is older than twice the cadence the user asked for, or
// the controller is itself reporting failure. Both point at the same fix
// (plan B2).
func staleDiagnostics(wf *wavefrontv1alpha1.Wavefront, now time.Time) []string {
	var diags []string

	budget := staleFactor * (pollInterval(wf) + staleGrace)
	switch last := wf.Status.LastEvaluated; {
	case last == nil:
		diags = append(diags, "status has never been evaluated: the controller may not be running; use --derive")
	case now.Sub(last.Time) > budget:
		diags = append(diags, fmt.Sprintf(
			"status may be stale: last evaluated %s ago (budget %s); the controller may be down; use --derive",
			now.Sub(last.Time).Round(time.Second), budget))
	}

	if ready := apimeta.FindStatusCondition(wf.Status.Conditions, wavefrontv1alpha1.ConditionReady); ready != nil &&
		ready.Status == metav1.ConditionFalse {
		diags = append(diags, fmt.Sprintf(
			"Wavefront Ready is False (%s: %s); status may be stale, use --derive",
			ready.Reason, ready.Message))
	}

	return diags
}

// pollInterval defaults the cadence exactly as the controller does, so the
// staleness budget is measured against the interval actually in force.
func pollInterval(wf *wavefrontv1alpha1.Wavefront) time.Duration {
	if wf.Spec.Poll.Interval.Duration <= 0 {
		return gitpoll.DefaultInterval
	}
	return wf.Spec.Poll.Interval.Duration
}

// nodeRef converts a status NodeReference to the adapter's ref.
func nodeRef(ref wavefrontv1alpha1.NodeReference) adapter.NodeRef {
	return adapter.NodeRef{Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
}

// memberTime lifts an optional metav1.Time out of its wrapper.
func memberTime(t *metav1.Time) *time.Time {
	if t == nil {
		return nil
	}
	return stampTime(t.Time)
}
