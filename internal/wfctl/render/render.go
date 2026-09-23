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

// Package render turns a snapshot.Snapshot into text. Every renderer is a
// pure function of (Snapshot, Options): no clock, no environment, no
// Kubernetes, no globals — which is exactly what makes the golden tests in
// this package a complete specification of wfctl's output.
//
// Two conventions run through every renderer, because a fleet view that
// cannot tell "nothing" from "unknown" is worse than no view at all:
//
//   - "-" is an absent value: a gate has no source, a settled node has no
//     pending age.
//   - "?" is an unproven one: this snapshot cannot say. A table that prints
//     any "?" also prints a footnote naming the fix.
//
// Colour is decided by the caller, never here: NO_COLOR, --no-color and TTY
// detection all belong to the CLI, and a renderer that sniffed the
// environment could not be golden-tested.
package render

import (
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/duration"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// Options is every knob the renderers share.
type Options struct {
	// Wide selects the extra columns of `-o wide`.
	Wide bool
	// Palette colours state cells; the zero value is colourless.
	Palette Palette
	// Now anchors every rendered age. Zero means time.Now — the golden tests
	// set it so ages are stable.
	Now time.Time
}

// now resolves the anchor for age arithmetic.
func (o Options) now() time.Time {
	if o.Now.IsZero() {
		return time.Now()
	}
	return o.Now
}

const (
	// absent marks a cell with legitimately no value.
	absent = "-"
	// unknown marks a cell this snapshot cannot prove.
	unknown = "?"
	// unproven marks a cell behind a GitRepository that could not be read.
	unproven = "!"
	// defaultKind is elided from rendered node names: v1alpha1 graphs are all
	// Kustomizations, and "Kustomization/" on every row is noise.
	defaultKind = "Kustomization"
	// shaLen is the conventional git short-SHA length.
	shaLen = 7
	// colObserved names the observed-SHA column, shared by the node table,
	// the source table and the status counts.
	colObserved = "OBSERVED"
	// atLeast prefixes an age measured from this run rather than from the
	// controller's own first observation (under the derive origin).
	atLeast = "≥"
)

// Footnotes explaining a "?" cell. Which one applies is a property of the
// snapshot, not of the row: either observation was unavailable wholesale, or
// this particular ref has not been observed yet.
const (
	footnoteUnobservable = "? = unknown: this snapshot carries no ref observations; re-run with --derive --poll."
	footnoteUnobserved   = "? = unknown: this ref has not been observed yet; " +
		"wait for the controller's next sweep, or re-run with --derive --poll."
	footnoteUnproven = "! = unproven: the GitRepository could not be read (RBAC or deleted); see the diagnostics."
)

// nodeName renders a node reference the way a user types it: "ns/name", with
// the kind restored only when it is not the default (the inverse of ParseNodeRef).
func nodeName(ref adapter.NodeRef) string {
	if ref.Kind == "" || ref.Kind == defaultKind {
		return ref.Namespace + "/" + ref.Name
	}
	return ref.String()
}

// referenceName renders an API NodeReference — the shape status uses — the
// same way. A nil reference is absent, not empty: BlockedRef.Ancestor is nil
// for SelfHeld and GraphCycle, which name no ancestor at all.
func referenceName(ref *wavefrontv1alpha1.NodeReference) string {
	if ref == nil {
		return absent
	}
	return nodeName(nodeRef(*ref))
}

// nodeRef converts an API NodeReference to the adapter reference the rest of
// the snapshot is keyed by.
func nodeRef(ref wavefrontv1alpha1.NodeReference) adapter.NodeRef {
	return adapter.NodeRef{Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
}

// shortSHA abbreviates a commit to its conventional short form, leaving
// anything already shorter (or empty) alone.
func shortSHA(sha string) string {
	if sha == "" {
		return absent
	}
	if len(sha) <= shaLen {
		return sha
	}
	return sha[:shaLen]
}

// age renders a kubectl-style duration since at ("3m", "2h", "4d"), or
// absent when there is no such instant.
func age(now time.Time, at *time.Time) string {
	if at == nil || at.IsZero() {
		return absent
	}
	return duration.HumanDuration(now.Sub(*at))
}

// agePrefix marks ages a derive snapshot can only bound from below: it
// measured first-observation itself, this run, so the true age is at least
// what it prints. The controller's own status carries the real one.
func agePrefix(snap *snapshot.Snapshot) string {
	if snap.Origin == snapshot.OriginDerive {
		return atLeast
	}
	return ""
}

// pendingAge renders a pending age with the origin's prefix, or absent.
func pendingAge(snap *snapshot.Snapshot, now time.Time, at *time.Time) string {
	rendered := age(now, at)
	if rendered == absent {
		return absent
	}
	return agePrefix(snap) + rendered
}

// observationUnknown reports whether a node's observed SHA is unproven
// rather than absent. A gate has no source and therefore nothing to observe:
// its blank is a fact, not a gap.
func observationUnknown(snap *snapshot.Snapshot, node *snapshot.NodeView) bool {
	if node.Source == nil {
		return false
	}
	return !snap.Observed || node.ObservedSHA == ""
}

// unknownFootnote names the fix for whichever kind of "?" this snapshot
// produces.
func unknownFootnote(snap *snapshot.Snapshot) string {
	if !snap.Observed {
		return footnoteUnobservable
	}
	return footnoteUnobserved
}

// joinRefs renders a list of node references as one cell.
func joinRefs(refs []adapter.NodeRef) string {
	if len(refs) == 0 {
		return absent
	}
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, nodeName(ref))
	}
	return strings.Join(names, ",")
}

// yesNo renders a boolean column. Booleans print as booleans: "-" would read
// as "unknown", which is a different claim.
func yesNo(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

// orAbsent falls back to the absent marker for an empty string.
func orAbsent(value string) string {
	if value == "" {
		return absent
	}
	return sanitize(value)
}

// cellBreaks are the characters a table cell must not contain: they are
// tabwriter's own column and row separators, and cluster data (annotations,
// URLs, condition messages) is not obliged to avoid them.
var cellBreaks = strings.NewReplacer("\t", " ", "\n", " ", "\r", " ")

// sanitize makes arbitrary cluster text safe to place in one table cell.
func sanitize(value string) string {
	return cellBreaks.Replace(value)
}

// holdCell renders a source's hold: kind plus the actor, when there is one.
// A Suspend hold names no actor.
func holdCell(hold *snapshot.HoldView) string {
	if hold == nil {
		return absent
	}
	if hold.Manager == "" {
		return sanitize(hold.Kind)
	}
	return sanitize(hold.Kind) + "(" + sanitize(hold.Manager) + ")"
}

// sourceView finds a source by its "ns/name", or nil.
func sourceView(snap *snapshot.Snapshot, name string) *snapshot.SourceView {
	for i := range snap.Sources {
		if snap.Sources[i].Name == name {
			return &snap.Sources[i]
		}
	}
	return nil
}

// nodeView finds a node by reference, or nil.
func nodeView(snap *snapshot.Snapshot, ref adapter.NodeRef) *snapshot.NodeView {
	for i := range snap.Nodes {
		if snap.Nodes[i].Ref == ref {
			return &snap.Nodes[i]
		}
	}
	return nil
}
