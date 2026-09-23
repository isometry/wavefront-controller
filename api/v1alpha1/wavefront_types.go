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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Mode controls whether admissions are executed or only reported.
// +kubebuilder:validation:Enum=Shadow;Enforce
type Mode string

const (
	ModeShadow  Mode = "Shadow"
	ModeEnforce Mode = "Enforce"
)

// Phase summarises fleet admission state.
// +kubebuilder:validation:Enum=Quiescent;Advancing;Blocked
type Phase string

const (
	PhaseQuiescent Phase = "Quiescent"
	PhaseAdvancing Phase = "Advancing"
	PhaseBlocked   Phase = "Blocked"
)

const (
	ConditionReady      = "Ready"
	ConditionGraphValid = "GraphValid"
)

// Ready condition reasons.
const (
	ReadyReasonSucceeded            = "Succeeded"
	ReadyReasonReconciliationFailed = "ReconciliationFailed"
)

// GraphValid condition reasons.
const (
	GraphValidReasonValid           = "Valid"
	GraphValidReasonSelectorOverlap = "SelectorOverlap"
	GraphValidReasonCyclesDetected  = "CyclesDetected"
)

// Hold reasons; match the enum on HeldNode.Reason.
const (
	HoldReasonHandPin = "HandPin"
	HoldReasonSuspend = "Suspend"
)

// NodeReference identifies a graph node. Typed {kind, namespace, name} from
// day one so HelmRelease nodes are a non-breaking addition later.
type NodeReference struct {
	// +kubebuilder:validation:Enum=Kustomization
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type NodesSpec struct {
	// Kinds of node resources to graph. v1alpha1 supports only Kustomization.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1
	// +kubebuilder:validation:XValidation:rule="self.all(k, k == 'Kustomization')",message="only Kustomization nodes are supported"
	Kinds []string `json:"kinds"`
	// Selector matches graph-member node resources across all namespaces.
	Selector metav1.LabelSelector `json:"selector"`
}

type PollSpec struct {
	// Interval between ref-advertisement polling sweeps.
	// +kubebuilder:default="90s"
	// +kubebuilder:validation:XValidation:rule="self.matches('^([0-9]+([.][0-9]+)?(ms|s|m|h))+$')",message="interval must be a valid Go duration (e.g. \"90s\", \"1h30m\")"
	Interval metav1.Duration `json:"interval,omitempty"`
	// PerHostConcurrency bounds concurrent ref listings per git host.
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	PerHostConcurrency int `json:"perHostConcurrency,omitempty"`
}

// WavefrontSpec defines the desired state of Wavefront
type WavefrontSpec struct {
	Nodes NodesSpec `json:"nodes"`
	// +kubebuilder:default=Shadow
	Mode Mode `json:"mode,omitempty"`
	// Suspend freezes all pin writes; detection and status continue.
	Suspend bool `json:"suspend,omitempty"`
	// +kubebuilder:default={}
	Poll PollSpec `json:"poll,omitempty"`
}

type NodeCounts struct {
	Observed   int `json:"observed"`
	Pinned     int `json:"pinned"`
	Gates      int `json:"gates"`
	Pending    int `json:"pending"`
	Converging int `json:"converging"`
	Blocked    int `json:"blocked"`
	Held       int `json:"held"`
}

type BlockedNode struct {
	Node     NodeReference  `json:"node"`
	Since    metav1.Time    `json:"since"`
	Reason   string         `json:"reason"`
	Ancestor *NodeReference `json:"ancestor,omitempty"`
}

type HeldNode struct {
	Node   NodeReference `json:"node"`
	Source string        `json:"source"` // "<namespace>/<name>" of the GitRepository
	// Manager names the foreign field manager owning spec.ref.commit; empty
	// for a Suspend hold, which has no owning actor.
	// +optional
	Manager string `json:"manager,omitempty"`
	// Reason distinguishes how the source is held: a foreign field manager
	// owning spec.ref.commit (HandPin) or spec.suspend (Suspend).
	// +kubebuilder:validation:Enum=HandPin;Suspend
	// +optional
	Reason string `json:"reason,omitempty"`
}

// ShadowAdmission records a would-be admission announced in Shadow mode:
// the pin write the controller would have performed in Enforce.
type ShadowAdmission struct {
	// Source is the "<namespace>/<name>" of the GitRepository.
	Source string `json:"source"`
	// To is the SHA that would be pinned.
	To string `json:"to"`
}

// BlockedRef attributes a blocked node to its nearest unsettled ancestor.
type BlockedRef struct {
	// Reason is the engine's BlockedReason (AncestorUnhealthy, AncestorPending, AncestorHeld,
	// AncestorUnobserved, SelfHeld, GraphCycle, SharedSourceBlocked).
	Reason   string         `json:"reason"`
	Ancestor *NodeReference `json:"ancestor,omitempty"`
}

// Member is one evaluated node's derived state for the last evaluation
// (selected nodes and the gate nodes reached through dependsOn).
// Write-only output: the reconciler never reads it back.
type Member struct {
	Node NodeReference `json:"node"`
	// +kubebuilder:validation:Enum=Pinned;Gate
	Role string `json:"role"`
	// +kubebuilder:validation:Enum=Settled;Pending;Admissible;Converging;Unhealthy
	State string `json:"state"`
	// +listType=atomic
	DependsOn    []NodeReference `json:"dependsOn,omitempty"` // graph edges, so status is self-contained
	Source       string          `json:"source,omitempty"`    // "<ns>/<name>" of the GitRepository (pinned nodes)
	Pin          string          `json:"pin,omitempty"`
	ObservedSHA  string          `json:"observedSHA,omitempty"` // "" = unobserved this pass
	PendingSince *metav1.Time    `json:"pendingSince,omitempty"`
	Ready        bool            `json:"ready"`
	Held         bool            `json:"held,omitempty"`
	Blocked      *BlockedRef     `json:"blocked,omitempty"`
}

// WavefrontStatus defines the observed state of Wavefront.
type WavefrontStatus struct {
	Phase Phase      `json:"phase,omitempty"`
	Nodes NodeCounts `json:"nodes,omitempty"`
	// Blocked and Held are capped exceptional-state lists (see StatusListCap);
	// the counts in Nodes are authoritative.
	// +listType=atomic
	Blocked []BlockedNode `json:"blocked,omitempty"`
	// +listType=atomic
	Held []HeldNode `json:"held,omitempty"`
	// Shadow lists would-be admissions already announced in Shadow mode
	// (capped at StatusListCap); the edge-trigger ledger for ShadowAdmission
	// events and the shadow admissions counter.
	// +listType=atomic
	// +optional
	Shadow []ShadowAdmission `json:"shadow,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	// Members lists every evaluated node's derived state (selected nodes and the
	// gate nodes reached through dependsOn), sorted by kind/namespace/name,
	// capped at MembersCap; MembersOmitted counts the rest.
	// +listType=atomic
	// +optional
	Members []Member `json:"members,omitempty"`
	// +optional
	MembersOmitted int `json:"membersOmitted,omitempty"`
	// LastEvaluated is advanced at most once per spec.poll.interval so that
	// watch-triggered reconciles do not rewrite status every pass.
	// +optional
	LastEvaluated *metav1.Time `json:"lastEvaluated,omitempty"`
}

// StatusListCap bounds the Blocked and Held status lists.
const StatusListCap = 20

// MembersCap bounds status.members; beyond it MembersOmitted counts the
// rest.
const MembersCap = 2000

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Pending",type=integer,JSONPath=`.status.nodes.pending`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`

// Wavefront is the Schema for the wavefronts API
type Wavefront struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Wavefront
	// +required
	Spec WavefrontSpec `json:"spec"`

	// status defines the observed state of Wavefront
	// +optional
	Status WavefrontStatus `json:"status,omitzero"`
}

// GetConditions returns the status conditions, satisfying fluxcd/pkg/runtime/conditions.Getter.
func (in *Wavefront) GetConditions() []metav1.Condition {
	return in.Status.Conditions
}

// SetConditions sets the status conditions, satisfying fluxcd/pkg/runtime/conditions.Setter.
func (in *Wavefront) SetConditions(conditions []metav1.Condition) {
	in.Status.Conditions = conditions
}

// +kubebuilder:object:root=true

// WavefrontList contains a list of Wavefront
type WavefrontList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Wavefront `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Wavefront{}, &WavefrontList{})
		return nil
	})
}
