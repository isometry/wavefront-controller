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

// Package snapshot is wfctl's truth model: one versioned, self-describing
// picture of a Wavefront that every command renders from, however it was
// obtained.
//
// Three providers yield the same Snapshot (decision "Truth model"):
// StatusSource reads what the controller published (status.members — the
// default, and the only tier a read-only viewer needs), DeriveSource
// re-derives it live through the shared internal/inputs pipeline (which works
// while the controller is down, and checks it when it is not), and FileSource
// replays a captured snapshot, preserving whichever of the two origins
// produced it. The Source interface is deliberately the only
// seam the renderers see, so a future --live provider backed by a controller
// API is a non-breaking addition.
//
// A Snapshot never carries Secret data, auth material, kubeconfig, raw
// managedFields, or URL userinfo: it is written to files and pasted into
// incident channels.
package snapshot

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wavefrontv1alpha1 "github.com/isometry/wavefront-controller/api/v1alpha1"
	"github.com/isometry/wavefront-controller/internal/adapter"
	"github.com/isometry/wavefront-controller/internal/pin"
)

const (
	// Version is the Snapshot's apiVersion. FileSource rejects anything else:
	// a renderer must never guess at a schema it does not know.
	Version = "wfctl.wavefront.as-code.io/v1alpha1"
	// KindSnapshot is the Snapshot's kind.
	KindSnapshot = "Snapshot"
)

// Snapshot origins. A snapshot always names how it was obtained,
// because what it can prove differs: a status snapshot reports what the
// controller last published, a derive snapshot what is true right now.
//
// There is deliberately no "file" origin. Replaying a snapshot does not change
// what it proved when it was captured, so a replayed derive snapshot must
// still render its DERIVED columns; Replayed says how the snapshot reached the
// renderer, Origin what it is.
const (
	// OriginStatus: read back from status.members.
	OriginStatus = "status"
	// OriginDerive: re-derived live from Kustomizations and GitRepositories.
	OriginDerive = "derive"
)

// Snapshot is one complete, renderable picture of a Wavefront.
type Snapshot struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	// Origin is how the picture was obtained when it was captured: status or
	// derive. It survives a round trip through a file unchanged.
	Origin string `json:"origin"`
	// Replayed marks a Snapshot that came from a file rather than a live
	// cluster (`--from`). It is orthogonal to Origin: a replayed derive
	// snapshot is still a derive snapshot, and still carries Derived.
	Replayed bool `json:"replayed,omitempty"`
	// CapturedAt is when this snapshot was taken.
	CapturedAt time.Time `json:"capturedAt"`
	// Evaluated is when the picture was last derived: status.lastEvaluated
	// under the status origin, CapturedAt under derive. nil means the
	// controller has never published an evaluation.
	Evaluated *time.Time `json:"evaluated"`
	// Cluster identifies where the snapshot came from. Filled by the CLI,
	// which owns the kubeconfig; the Sources here never read one.
	Cluster ClusterIdent `json:"cluster"`
	// Observed reports whether observed SHAs are meaningful. False means
	// UNKNOWN, not "nothing pending": a renderer must say so rather than
	// present an empty ObservedSHA as a fact (renderers show `?` instead).
	Observed bool `json:"observed"`
	// Wavefront is the object itself, as the cluster holds it.
	Wavefront WavefrontView `json:"wavefront"`
	// Nodes is every evaluated node, sorted by Ref.String().
	Nodes []NodeView `json:"nodes"`
	// Sources is every managed GitRepository backing a pinned node, sorted by
	// Name ("ns/name"). Entries may be partial under the status origin when
	// RBAC denies the GitRepository read; a Diagnostic says so.
	Sources []SourceView `json:"sources"`
	// Graph is the structural verdict on the dependsOn DAG.
	Graph GraphView `json:"graph"`
	// Derived carries what only a live re-derivation can prove; zero under
	// every other origin.
	Derived DerivedStatus `json:"derived"`
	// Diagnostics are human-readable degradations of this snapshot: selector
	// overlap, unsupported ref styles, poll and credential errors, stale
	// status, RBAC degradation. Never fatal — a degraded picture beats none.
	Diagnostics []string `json:"diagnostics"`
}

// ClusterIdent identifies the cluster a snapshot came from. Server is the
// host only: an apiserver URL's path and userinfo are neither useful here nor
// safe to share.
type ClusterIdent struct {
	Context      string `json:"context,omitempty"`
	Server       string `json:"server,omitempty"`
	WfctlVersion string `json:"wfctlVersion,omitempty"`
}

// WavefrontView is the Wavefront object as the cluster holds it, plus who
// owns the two fields wfctl writes.
type WavefrontView struct {
	Name       string                            `json:"name"`
	Generation int64                             `json:"generation"`
	Spec       wavefrontv1alpha1.WavefrontSpec   `json:"spec"`
	Status     wavefrontv1alpha1.WavefrontStatus `json:"status"`
	// SpecOwners maps "spec.mode" and "spec.suspend" to the field manager
	// owning them, so a write command can warn that a GitOps applier will
	// revert the change. Derived from managedFields; the raw
	// managedFields never appear in a Snapshot.
	SpecOwners map[string]string `json:"specOwners,omitempty"`
}

// NodeView is one evaluated node's derived state.
//
// ReadyMessage, AppliedSHA and Failing are derive-only: status.members
// carries neither the Ready condition's message nor its explicit-False
// distinction, so under the status origin they are zero and a renderer must
// not read anything into that.
//
// A renderer decides what to show from Origin, never from Replayed: replaying
// a derive snapshot from a file loses none of these fields.
type NodeView struct {
	Ref   adapter.NodeRef `json:"ref"`
	Role  string          `json:"role"`
	State string          `json:"state"`
	Held  bool            `json:"held,omitempty"`
	// Blocked attributes a pending node's non-admission to its nearest
	// unsettled ancestor (or blocking sibling).
	Blocked      *wavefrontv1alpha1.BlockedRef `json:"blocked,omitempty"`
	PendingSince *time.Time                    `json:"pendingSince,omitempty"`
	Ready        bool                          `json:"ready"`
	// Failing is populated only under the derive origin (see above).
	Failing      bool              `json:"failing,omitempty"`
	ReadyMessage string            `json:"readyMessage,omitempty"`
	AppliedSHA   string            `json:"appliedSHA,omitempty"`
	DependsOn    []adapter.NodeRef `json:"dependsOn,omitempty"`
	// Source is the "ns/name" of the backing GitRepository; nil for a gate,
	// which by definition has none.
	Source      *string `json:"source,omitempty"`
	Pin         string  `json:"pin,omitempty"`
	ObservedSHA string  `json:"observedSHA,omitempty"`
	// Wave is the node's dependsOn depth; -1 means it is in, or behind, a
	// cycle and therefore never layered.
	Wave int `json:"wave"`
}

// SourceView is one managed GitRepository as wfctl reports it.
type SourceView struct {
	// Name is "ns/name".
	Name string `json:"name"`
	// URL has any userinfo stripped, and is empty when the URL could not be
	// parsed — never a URL that might still embed credentials.
	URL           string `json:"url,omitempty"`
	SecretRefName string `json:"secretRefName,omitempty"`
	TrackingRef   string `json:"trackingRef,omitempty"`
	Pin           string `json:"pin,omitempty"`
	Suspended     bool   `json:"suspended,omitempty"`
	// CommitOwners lists every field manager owning spec.ref.commit — the
	// evidence behind a hold, and what `release` has to unpick.
	CommitOwners []pin.Owner `json:"commitOwners,omitempty"`
	Hold         *HoldView   `json:"hold,omitempty"`
	// Provenance carries the three wavefront.as-code.io pin annotations: the
	// durable ledger, unlike events, which the apiserver eventually expires.
	Provenance    map[string]string  `json:"provenance,omitempty"`
	ArtifactSHA   string             `json:"artifactSHA,omitempty"`
	FetchFailing  bool               `json:"fetchFailing,omitempty"`
	Conditions    []metav1.Condition `json:"conditions,omitempty"`
	ObservedSHA   string             `json:"observedSHA,omitempty"`
	FirstObserved *time.Time         `json:"firstObserved,omitempty"`
	// Nodes lists every node referencing this source, sorted.
	Nodes []adapter.NodeRef `json:"nodes,omitempty"`
	// Partial marks a source whose GitRepository could not be read (RBAC or
	// deletion): every field beyond Name, Pin and Nodes is unproven.
	Partial bool `json:"partial,omitempty"`
}

// HoldView is one source's hold, in the unified form the engine reports
// (inputs.Hold): a foreign field manager owning spec.ref.commit, or
// spec.suspend, which names no actor.
type HoldView struct {
	// Kind is HandPin or Suspend.
	Kind string `json:"kind"`
	// Manager is empty for a Suspend hold.
	Manager string `json:"manager,omitempty"`
}

// GraphView is the structural verdict on the dependsOn DAG. Under the status
// origin it is rebuilt from the members' own dependsOn edges; Missing is
// derive-only, because status cannot distinguish a dangling dependency from
// an unready gate.
type GraphView struct {
	Cycles  [][]adapter.NodeRef `json:"cycles,omitempty"`
	Unknown []adapter.NodeRef   `json:"unknown,omitempty"`
	Missing []adapter.NodeRef   `json:"missing,omitempty"`
}

// DerivedStatus is what only a live re-derivation proves: the same numbers
// the controller would publish, computed here and now. `wfctl status
// --derive` prints these beside the reported ones and flags disagreement.
type DerivedStatus struct {
	Phase           wavefrontv1alpha1.Phase         `json:"phase,omitempty"`
	Counts          wavefrontv1alpha1.NodeCounts    `json:"counts"`
	Blocked         []wavefrontv1alpha1.BlockedNode `json:"blocked,omitempty"`
	Held            []wavefrontv1alpha1.HeldNode    `json:"held,omitempty"`
	BlockedByReason map[string]int                  `json:"blockedByReason,omitempty"`
	FetchFailures   int                             `json:"fetchFailures,omitempty"`
	// GraphValid, GraphReason and GraphMessage are inputs.GraphVerdict — the
	// GraphValid condition in all but name.
	GraphValid   bool   `json:"graphValid"`
	GraphReason  string `json:"graphReason,omitempty"`
	GraphMessage string `json:"graphMessage,omitempty"`
	// Admissions are the ancestor-gated pin advances this evaluation would
	// perform; Initial the ungated pins for sources seen for the first time.
	Admissions []AdmissionView `json:"admissions,omitempty"`
	Initial    []AdmissionView `json:"initial,omitempty"`
}

// AdmissionView is one pin advance the evaluation would perform.
type AdmissionView struct {
	Node adapter.NodeRef `json:"node"`
	// Source is the "ns/name" of the GitRepository.
	Source       string     `json:"source"`
	From         string     `json:"from,omitempty"`
	To           string     `json:"to"`
	ObservedRef  string     `json:"observedRef,omitempty"`
	Initial      bool       `json:"initial,omitempty"`
	PendingSince *time.Time `json:"pendingSince,omitempty"`
}

// Waves layers nodes by dependsOn depth, as `wfctl graph` renders them: 0 for a
// node with no dependencies, otherwise 1 + the deepest dependency.
//
// It is Kahn's algorithm run over the snapshot's own edges rather than
// internal/graph, so it works identically for a status snapshot (whose edges
// come from status.members) and for a replayed file — neither of which has a
// cluster to rebuild a graph.Graph from.
//
// A node that never dequeues is in, or behind, a cycle and gets -1: no depth
// is meaningful for it, and rendering one would invent an order the engine
// explicitly refuses to assume. A dependency that is not itself a node in the
// list is a missing gate: it layers at 0 and is included in the result, so a
// renderer can show it as the wave-0 blocker it behaves as.
func Waves(nodes []NodeView) map[adapter.NodeRef]int {
	deps := make(map[adapter.NodeRef][]adapter.NodeRef, len(nodes))
	for _, node := range nodes {
		deps[node.Ref] = node.DependsOn
	}
	// Dangling targets join the graph as dependency-free nodes; without them
	// their dependants would never dequeue and would be mistaken for a cycle.
	for _, node := range nodes {
		for _, dep := range node.DependsOn {
			if _, known := deps[dep]; !known {
				deps[dep] = nil
			}
		}
	}

	outstanding := make(map[adapter.NodeRef]int, len(deps))
	dependants := make(map[adapter.NodeRef][]adapter.NodeRef, len(deps))
	queue := make([]adapter.NodeRef, 0, len(deps))
	for ref, edges := range deps {
		seen := make(map[adapter.NodeRef]bool, len(edges))
		for _, dep := range edges {
			if seen[dep] {
				continue
			}
			seen[dep] = true
			// A self-edge is deliberately counted: it can never be satisfied,
			// which is exactly how a self-looping node earns its -1.
			outstanding[ref]++
			dependants[dep] = append(dependants[dep], ref)
		}
		if outstanding[ref] == 0 {
			queue = append(queue, ref)
		}
	}

	// depth accumulates the deepest dependency seen so far; it is only a
	// final answer once outstanding has reached zero, which is why the wave
	// map is built from outstanding afterwards rather than from depth alone.
	depth := make(map[adapter.NodeRef]int, len(deps))
	for len(queue) > 0 {
		ref := queue[0]
		queue = queue[1:]
		for _, dependant := range dependants[ref] {
			depth[dependant] = max(depth[dependant], depth[ref]+1)
			outstanding[dependant]--
			if outstanding[dependant] == 0 {
				queue = append(queue, dependant)
			}
		}
	}

	wave := make(map[adapter.NodeRef]int, len(deps))
	for ref := range deps {
		// Whatever never dequeued is in, or behind, a cycle.
		if outstanding[ref] > 0 {
			wave[ref] = -1
			continue
		}
		wave[ref] = depth[ref]
	}
	return wave
}

// applyWaves stamps each node's wave in place, so Nodes is self-contained and
// a replayed snapshot needs no recomputation to render.
func applyWaves(nodes []NodeView) {
	wave := Waves(nodes)
	for i := range nodes {
		nodes[i].Wave = wave[nodes[i].Ref]
	}
}
