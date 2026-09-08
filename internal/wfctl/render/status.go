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

package render

import (
	"fmt"
	"io"
	"strconv"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/wfctl/snapshot"
)

// differs marks a derived value that disagrees with the reported one. The
// disagreement is the whole point of `--derive`: it is how an operator finds
// out the controller's published picture is not the cluster's.
const differs = "≠ "

// Status renders the fleet summary (plan B3): the Wavefront itself, the
// phase and counts, the structural verdict, the exceptional-state lists, and
// the diagnostics.
//
// Under the derive origin every count is printed twice — what the controller
// published, and what re-deriving now says — with the disagreements marked.
// Under any other origin there is only one number to print, and pretending
// otherwise would imply a comparison that was never made.
func Status(w io.Writer, s *snapshot.Snapshot, o Options) error {
	out := newSink(w)
	now := o.now()
	derived := s.Origin == snapshot.OriginDerive

	if err := statusHeader(w, s, now); err != nil {
		return err
	}
	out.blank()
	if out.err != nil {
		return out.err
	}

	if err := statusCounts(w, s, o, derived); err != nil {
		return err
	}

	if err := blockedSection(w, "BLOCKED", s.Wavefront.Status.Blocked, o, now); err != nil {
		return err
	}
	if err := heldSection(w, "HELD", s.Wavefront.Status.Held, o); err != nil {
		return err
	}
	if err := shadowSection(w, s.Wavefront.Status.Shadow); err != nil {
		return err
	}
	if derived {
		if err := blockedSection(w, "BLOCKED (derived)", s.Derived.Blocked, o, now); err != nil {
			return err
		}
		if err := heldSection(w, "HELD (derived)", s.Derived.Held, o); err != nil {
			return err
		}
	}

	return diagnostics(w, s, o)
}

// statusHeader prints the Wavefront's own identity and this snapshot's
// provenance: which object, in what mode, and how old the picture is.
func statusHeader(w io.Writer, s *snapshot.Snapshot, now time.Time) error {
	wf := &s.Wavefront
	table := newPlainTable(w)
	table.Row("NAME:", wf.Name)
	table.Row("MODE:", orAbsent(string(wf.Spec.Mode)))
	table.Row("SUSPEND:", yesNo(wf.Spec.Suspend))
	table.Row("GENERATION:", strconv.FormatInt(wf.Generation, 10))
	table.Row("ORIGIN:", originCell(s))
	table.Row("CAPTURED:", stampCell(now, &s.CapturedAt))
	table.Row("LAST EVALUATED:", stampCell(now, s.Evaluated))
	if s.Cluster.Context != "" || s.Cluster.Server != "" {
		table.Row("CLUSTER:", clusterCell(s))
	}
	return table.Flush()
}

// originCell says how the picture was obtained, and whether it was replayed
// from a file — orthogonal facts (snapshot.Snapshot.Replayed).
func originCell(s *snapshot.Snapshot) string {
	if s.Replayed {
		return s.Origin + " (replayed from file)"
	}
	return s.Origin
}

// clusterCell identifies where the snapshot came from.
func clusterCell(s *snapshot.Snapshot) string {
	switch {
	case s.Cluster.Context == "":
		return sanitize(s.Cluster.Server)
	case s.Cluster.Server == "":
		return sanitize(s.Cluster.Context)
	default:
		return sanitize(s.Cluster.Context) + " (" + sanitize(s.Cluster.Server) + ")"
	}
}

// stampCell renders an instant with its age, which is the part an operator
// reads first.
func stampCell(now time.Time, at *time.Time) string {
	if at == nil || at.IsZero() {
		return absent
	}
	return fmt.Sprintf("%s (%s ago)", at.UTC().Format(time.RFC3339), age(now, at))
}

// statusCounts prints the phase, the node counts and the structural verdict,
// reported beside derived when there is a derivation to compare against.
func statusCounts(w io.Writer, s *snapshot.Snapshot, o Options, derived bool) error {
	reported := s.Wavefront.Status
	counts := reported.Nodes
	dcounts := s.Derived.Counts

	rows := [][3]string{
		{"PHASE", string(reported.Phase), string(s.Derived.Phase)},
		{"GRAPHVALID", reportedGraphValid(&reported), derivedGraphValid(&s.Derived)},
		{colObserved, strconv.Itoa(counts.Observed), strconv.Itoa(dcounts.Observed)},
		{"PINNED", strconv.Itoa(counts.Pinned), strconv.Itoa(dcounts.Pinned)},
		{"GATES", strconv.Itoa(counts.Gates), strconv.Itoa(dcounts.Gates)},
		{"PENDING", strconv.Itoa(counts.Pending), strconv.Itoa(dcounts.Pending)},
		{"CONVERGING", strconv.Itoa(counts.Converging), strconv.Itoa(dcounts.Converging)},
		{"BLOCKED", strconv.Itoa(counts.Blocked), strconv.Itoa(dcounts.Blocked)},
		{"HELD", strconv.Itoa(counts.Held), strconv.Itoa(dcounts.Held)},
	}

	var table *Table
	if derived {
		table = NewTable(w, "FIELD", "REPORTED", "DERIVED")
	} else {
		table = NewTable(w, "FIELD", "VALUE")
	}
	for _, row := range rows {
		if !derived {
			table.Row(row[0], row[1])
			continue
		}
		value := row[2]
		if value != row[1] {
			value = o.Palette.Warn(differs + value)
		}
		table.Row(row[0], row[1], value)
	}
	return table.Flush()
}

// reportedGraphValid reads the published GraphValid condition. A condition
// the controller has never written is unknown, not valid.
func reportedGraphValid(status *wavefrontv1alpha1.WavefrontStatus) string {
	cond := apimeta.FindStatusCondition(status.Conditions, wavefrontv1alpha1.ConditionGraphValid)
	if cond == nil {
		return unknown
	}
	valid := yesNo(cond.Status == "True")
	if cond.Reason == "" || cond.Reason == wavefrontv1alpha1.GraphValidReasonValid {
		return valid
	}
	return valid + " (" + sanitize(cond.Reason) + ")"
}

// derivedGraphValid renders the verdict this run computed.
func derivedGraphValid(derived *snapshot.DerivedStatus) string {
	valid := yesNo(derived.GraphValid)
	if derived.GraphReason == "" || derived.GraphReason == wavefrontv1alpha1.GraphValidReasonValid {
		return valid
	}
	return valid + " (" + sanitize(derived.GraphReason) + ")"
}

// blockedSection prints one blocked-node list, skipped when empty: an empty
// exceptional-state list is the normal case and deserves no ceremony.
func blockedSection(w io.Writer, title string, nodes []wavefrontv1alpha1.BlockedNode, o Options, now time.Time) error {
	if len(nodes) == 0 {
		return nil
	}
	out := newSink(w)
	out.blank()
	out.printf("%s (%d)\n", title, len(nodes))
	if out.err != nil {
		return out.err
	}

	table := NewTable(w, "NODE", "REASON", "ANCESTOR", "SINCE")
	for i := range nodes {
		node := &nodes[i]
		since := node.Since.Time
		table.Row(
			nodeName(nodeRef(node.Node)),
			o.Palette.Warn(sanitize(node.Reason)),
			referenceName(node.Ancestor),
			age(now, &since),
		)
	}
	return table.Flush()
}

// heldSection prints one held-source list.
func heldSection(w io.Writer, title string, nodes []wavefrontv1alpha1.HeldNode, o Options) error {
	if len(nodes) == 0 {
		return nil
	}
	out := newSink(w)
	out.blank()
	out.printf("%s (%d)\n", title, len(nodes))
	if out.err != nil {
		return out.err
	}

	table := NewTable(w, "NODE", "SOURCE", "REASON", "MANAGER")
	for i := range nodes {
		node := &nodes[i]
		table.Row(
			nodeName(nodeRef(node.Node)),
			orAbsent(node.Source),
			o.Palette.Held(orAbsent(node.Reason)),
			orAbsent(node.Manager),
		)
	}
	return table.Flush()
}

// shadowSection lists the pin advances Shadow mode announced but did not
// perform.
func shadowSection(w io.Writer, admissions []wavefrontv1alpha1.ShadowAdmission) error {
	if len(admissions) == 0 {
		return nil
	}
	out := newSink(w)
	out.blank()
	out.printf("SHADOW (%d)\n", len(admissions))
	if out.err != nil {
		return out.err
	}

	table := NewTable(w, "SOURCE", "WOULD-PIN")
	for _, admission := range admissions {
		table.Row(orAbsent(admission.Source), shortSHA(admission.To))
	}
	return table.Flush()
}

// diagnostics always prints, even when there is nothing to say: "no
// diagnostics" is itself the answer to "is this picture trustworthy?", and
// an operator must not have to infer it from an absent heading.
func diagnostics(w io.Writer, s *snapshot.Snapshot, o Options) error {
	out := newSink(w)
	out.blank()
	out.printf("DIAGNOSTICS (%d)\n", len(s.Diagnostics))
	if len(s.Diagnostics) == 0 {
		out.line(o.Palette.Dim("  (none)"))
		return out.err
	}
	for _, diag := range s.Diagnostics {
		out.printf("  - %s\n", o.Palette.Warn(diag))
	}
	return out.err
}
