# Wavefront Controller Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Wavefront Controller specified in `DESIGN.md` (v4.0): a Kubernetes controller that sequences flotilla source updates by owning `spec.ref.commit` on Flux `GitRepository`s, deriving the ordering DAG from `Kustomization` `spec.dependsOn` edges, and running per-node rolling admission.

**Architecture:** A single cluster-scoped `Wavefront` CRD carries scope/policy and fleet status. A background poller lists git ref advertisements (go-git `Remote.List` — no clone, no checkout) for all managed sources and feeds a channel source. The reconciler re-derives everything on every event: label-select `Kustomization`s → build the DAG → resolve sources → combine pins, observations, and readiness in a **pure, stateless admission engine** → execute pin advances via server-side apply under field manager `wavefront-controller` (or emit `ShadowAdmission` events in Shadow mode). Graph, engine, selection, and adapters are pure library packages with table-driven unit tests; envtest covers SSA ownership, hold detection, and the control loop; a kind+Flux e2e suite proves sequenced admission end-to-end.

**Tech Stack:** Go 1.27; kubebuilder v4.15.0 (go/v4 layout); controller-runtime v0.24.1 (k8s.io/* v0.36.0); Flux APIs `source.toolkit.fluxcd.io/v1` + `kustomize.toolkit.fluxcd.io/v1` (api modules v1.9.4); `fluxcd/pkg/git` v0.52.0, `fluxcd/pkg/git/gogit` v0.43.0, `fluxcd/pkg/runtime` v0.111.0, `fluxcd/pkg/apis/meta` v1.31.0, `fluxcd/pkg/gittestserver` v0.29.0; `github.com/go-git/go-git/v5` v5.19.2; envtest (k8s 1.36) + Ginkgo v2/Gomega; kind + Flux v2.9.4 for e2e; golangci-lint v2 (scaffolded config).

**Spec:** `DESIGN.md` (repo root, v4.1). This plan argues from that spec — read both before starting. DESIGN.md v4.1 already uses the real group **`wavefront.as-code.io`** throughout.

## Global Constraints

Every task's requirements implicitly include this section.

- Go `1.27.0`; module `github.com/isometry/wavefront-controller` (existing `go.mod`). Modern idioms expected: `slices`/`maps`, `iter.Seq`, `errors.Join`, `log/slog`-compatible logr usage, `testing/synctest` for clock-driven tests, generics where they clarify.
- Scaffold with **kubebuilder v4.15.0** (NOT operator-sdk): `--domain as-code.io`. The scaffold writes `go 1.26.0` in go.mod — restore to `1.27.0`.
- GVK: `wavefront.as-code.io/v1alpha1`, `Kind: Wavefront`, **cluster-scoped**.
- SSA field manager: **`wavefront-controller`**. The controller writes ONLY (a) `spec.ref.commit` + its own `wavefront.as-code.io/*` annotations on flotilla `GitRepository`s, (b) `Wavefront` status, (c) Kubernetes Events. Never `spec.suspend`, never labels, never `Kustomization`s/`HelmRelease`s/workloads (DESIGN §3, D2). It never uses `client.ForceOwnership` on `GitRepository`s — an SSA conflict is hold detection, not an obstacle.
- **Observed-SHAs-only invariant** (DESIGN §3.1): never pin a SHA that this process has not itself seen in a ref advertisement — the sole exception is initial-pin from `status.artifact.revision` (§3.5.4).
- Participation label **`wavefront.as-code.io/managed: "true"`** is rendered by the catalog on graph-member `Kustomization`s (the Wavefront selector's target) AND on flotilla `GitRepository`s (the managed-source marker distinguishing them from the catalog repo). The controller only reads it.
- Provenance annotations: `wavefront.as-code.io/admitted-at`, `wavefront.as-code.io/previous-pin`, `wavefront.as-code.io/observed-ref`.
- v1 scope: `Kustomization` nodes only (D12) and `TrackRef` selection only (D10) — but both seams (`adapter.Adapter`, `selection.Strategy`) must exist as interfaces. No rollback, no `SemverWindow`.
- Flux API gotchas (changed since 2024 — do not "fix" them back): `Kustomization.spec.dependsOn` is `[]kustomizev1.DependencyReference` (alias of `meta.DependencyReference{Name, Namespace, ReadyExpr}`), NOT `[]meta.NamespacedObjectReference`. The artifact type is `meta.Artifact` (`fluxcd/pkg/apis/meta`), NOT `sourcev1.Artifact` (moved in Flux 2.7). Artifact revision format is `<ref>@sha1:<40-hex>`; parse with `git.ExtractHashFromRevision` / `git.SplitRevision` from `github.com/fluxcd/pkg/git`.
- SHAs are handled as lowercase 40-hex strings throughout; a `Hash` from `fluxcd/pkg/git` is converted with `.String()`.
- TDD: in every task the failing test is written and run before the implementation. All commits follow Conventional Commits (`feat(engine): …`, `fix:`, `test(graph): …`, `chore:`). Stage separately; run each commit command alone.
- `make lint test` must pass at the end of every task.

## Repository layout (target)

```
api/v1alpha1/                  wavefront_types.go, groupversion_info.go, zz_generated.deepcopy.go
cmd/main.go                    manager wiring (scaffolded; extended in Task 9)
internal/adapter/              NodeRef, Node, Adapter interface, KustomizationAdapter
internal/graph/                DAG build, cycle detection, transitive ancestors
internal/inputs/               discover/resolve/derive/summarise, shared by the controller and wfctl
internal/engine/               settledness, admissibility, state derivation (pure)
internal/selection/            Strategy interface, TrackRef
internal/gitpoll/              auth glue, ref lister, poller + observation store
internal/pin/                  SSA pin writer, provenance, hold detection
internal/controller/           wavefront_controller.go, suite_test.go (envtest)
internal/metrics/              prometheus instruments
cmd/wfctl/main.go              wfctl entry (cobra root only; renames itself as kubectl-wavefront)
internal/wfctl/                snapshot providers, renderers, write actions, CLI wiring
config/                        CRD, RBAC, manager, samples (kubebuilder-generated)
test/crds/flux/                vendored Flux CRDs for envtest (make update-flux-crds)
test/e2e/                      kind+Flux e2e suite; test/e2e/gitserver/ (git server image)
docs/runbook.md                break-glass + operations
```

---

### Task 1: Scaffold and dependencies

**Files:**
- Create (generated): kubebuilder go/v4 layout — `cmd/main.go`, `api/v1alpha1/*`, `internal/controller/*`, `config/*`, `test/e2e/*`, `test/utils/utils.go`, `Makefile`, `Dockerfile`, `.golangci.yml`, `PROJECT`
- Create: `test/crds/flux/` (vendored CRDs), Makefile target `update-flux-crds`
- Modify: `go.mod` (Go version, Flux deps)

**Interfaces:**
- Consumes: nothing (first task).
- Produces: compiling scaffold; `make manifests generate fmt vet lint test` all green; Flux API modules importable; `test/crds/flux/` containing `source.toolkit.fluxcd.io_gitrepositories.yaml` and `kustomize.toolkit.fluxcd.io_kustomizations.yaml` for every later envtest suite.

- [ ] **Step 1: Scaffold.** Working dir is the repo root (existing `go.mod` with only the module line is fine — kubebuilder augments it; if `kubebuilder init` refuses a non-empty dir, temporarily move `DESIGN.md`/`PLAN.md` aside and restore after):

```sh
kubebuilder init --domain as-code.io --repo github.com/isometry/wavefront-controller
kubebuilder create api --group wavefront --version v1alpha1 --kind Wavefront --resource --controller --namespaced=false
```

- [ ] **Step 2: Restore `go 1.27.0`** in `go.mod`; run `make manifests generate fmt vet`; verify `make test` passes on the scaffold (envtest downloads k8s 1.36 binaries on first run).

- [ ] **Step 3: Add Flux dependencies** (exact versions — do not float):

```sh
go get github.com/fluxcd/source-controller/api@v1.9.4 \
       github.com/fluxcd/kustomize-controller/api@v1.9.4 \
       github.com/fluxcd/pkg/git@v0.52.0 \
       github.com/fluxcd/pkg/git/gogit@v0.43.0 \
       github.com/fluxcd/pkg/runtime@v0.111.0 \
       github.com/fluxcd/pkg/apis/meta@v1.31.0 \
       github.com/go-git/go-git/v5@v5.19.2
go get -t github.com/fluxcd/pkg/gittestserver@v0.29.0
go mod tidy
```

- [ ] **Step 4: Vendor Flux CRDs for envtest.** Add a Makefile target and run it:

```makefile
FLUX_SC_VERSION ?= v1.9.4
FLUX_KC_VERSION ?= v1.9.4
.PHONY: update-flux-crds
update-flux-crds: ## Refresh vendored Flux CRDs used by envtest.
	mkdir -p test/crds/flux
	curl -fsSL -o test/crds/flux/source.toolkit.fluxcd.io_gitrepositories.yaml \
	  https://raw.githubusercontent.com/fluxcd/source-controller/api/$(FLUX_SC_VERSION)/config/crd/bases/source.toolkit.fluxcd.io_gitrepositories.yaml
	curl -fsSL -o test/crds/flux/kustomize.toolkit.fluxcd.io_kustomizations.yaml \
	  https://raw.githubusercontent.com/fluxcd/kustomize-controller/api/$(FLUX_KC_VERSION)/config/crd/bases/kustomize.toolkit.fluxcd.io_kustomizations.yaml
```

(If the `api/vX` tag form 404s, use tag `v1.9.4` without the `api/` prefix — check `git ls-remote --tags https://github.com/fluxcd/source-controller 'api/*'` to see which exists.) Commit the downloaded YAMLs.

- [ ] **Step 5: Register Flux schemes** in `cmd/main.go` alongside the scaffolded scheme registration:

```go
import (
    sourcev1 "github.com/fluxcd/source-controller/api/v1"
    kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
)
// in init():
utilruntime.Must(sourcev1.AddToScheme(scheme))
utilruntime.Must(kustomizev1.AddToScheme(scheme))
```

- [ ] **Step 6: Verify** `make manifests generate fmt vet lint test` all pass.

- [ ] **Step 7: Commit** — `chore: scaffold kubebuilder project with Flux dependencies` (separate commits for scaffold and deps are fine: `chore: kubebuilder init (go/v4, domain as-code.io)`, `chore(deps): add Flux API and pkg modules`, `chore(test): vendor Flux CRDs for envtest`).

---

### Task 2: `Wavefront` API types

**Files:**
- Modify: `api/v1alpha1/wavefront_types.go` (replace scaffold stubs)
- Generated: `config/crd/bases/wavefront.as-code.io_wavefronts.yaml`, `zz_generated.deepcopy.go`
- Test: `internal/controller/wavefront_crd_test.go` (CRD-level validation via the scaffolded envtest suite)

**Interfaces:**
- Consumes: scaffold from Task 1.
- Produces: the types below, used by every later task. Constants `wavefrontv1alpha1.ModeShadow/ModeEnforce`, `PhaseQuiescent/PhaseAdvancing/PhaseBlocked`, condition types `ConditionReady = "Ready"`, `ConditionGraphValid = "GraphValid"`.

- [ ] **Step 1: Write the types** (full content — kubebuilder markers matter):

```go
package v1alpha1

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

// NodeReference identifies a graph node. Typed {kind, namespace, name} from
// day one so HelmRelease nodes are a non-breaking addition (DESIGN D12).
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
	Interval metav1.Duration `json:"interval,omitempty"`
	// PerHostConcurrency bounds concurrent ref listings per git host.
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	PerHostConcurrency int `json:"perHostConcurrency,omitempty"`
}

type WavefrontSpec struct {
	Nodes NodesSpec `json:"nodes"`
	// +kubebuilder:default=Shadow
	Mode Mode `json:"mode,omitempty"`
	// Suspend freezes all pin writes; detection and status continue.
	Suspend bool `json:"suspend,omitempty"`
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
	Node    NodeReference `json:"node"`
	Source  string        `json:"source"` // "<namespace>/<name>" of the GitRepository
	Manager string        `json:"manager"`
}

type WavefrontStatus struct {
	Phase Phase `json:"phase,omitempty"`
	Nodes NodeCounts `json:"nodes,omitempty"`
	// Blocked and Held are capped exceptional-state lists (see StatusListCap);
	// the counts in Nodes are authoritative.
	// +listType=atomic
	Blocked []BlockedNode `json:"blocked,omitempty"`
	// +listType=atomic
	Held []HeldNode `json:"held,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// StatusListCap bounds the Blocked and Held status lists (DESIGN §4.1).
const StatusListCap = 20

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Pending",type=integer,JSONPath=`.status.nodes.pending`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
type Wavefront struct { /* scaffolded TypeMeta/ObjectMeta/Spec/Status */ }
```

Also add `GetConditions()/SetConditions()` on `Wavefront` (returning/setting `Status.Conditions`) so `fluxcd/pkg/runtime/conditions` helpers work on it.

- [ ] **Step 2:** `make manifests generate` — CRD renders with CEL rule, defaults, printer columns.

- [ ] **Step 3: Write failing CRD validation tests** in the envtest suite (extend the scaffolded `internal/controller/suite_test.go` to include `CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases"), filepath.Join("..", "..", "test", "crds", "flux")}, ErrorIfCRDPathMissing: true`). Cases:

```go
It("rejects non-Kustomization node kinds", func() {
    wf := validWavefront("wf-badkind")
    wf.Spec.Nodes.Kinds = []string{"HelmRelease"}
    Expect(k8sClient.Create(ctx, wf)).To(MatchError(ContainSubstring("only Kustomization nodes are supported")))
})
It("defaults mode to Shadow and poll to 90s/4", func() { /* create minimal, read back, assert defaults */ })
It("accepts a fully specified Wavefront", func() { /* DESIGN §4.1 example, group substituted */ })
```

- [ ] **Step 4:** Run `make test` — validation tests fail (types incomplete) → finish types → tests pass.

- [ ] **Step 5: Commit** — `feat(api): Wavefront v1alpha1 types with CEL validation and defaults`.

---

### Task 3: Node adapter (`internal/adapter`)

**Files:**
- Create: `internal/adapter/adapter.go`, `internal/adapter/kustomization.go`
- Test: `internal/adapter/kustomization_test.go`

**Interfaces:**
- Consumes: Flux API types (Task 1).
- Produces (used by graph, engine, controller):

```go
package adapter

// NodeRef identifies a node; Kind distinguishes future HelmRelease planes.
type NodeRef struct{ Kind, Namespace, Name string }

func (r NodeRef) String() string // "Kustomization/ns/name"

// Readiness is the uniform health signal (DESIGN D7).
type Readiness struct {
	Ready       bool   // Ready condition True AND status.observedGeneration == metadata.generation
	Failing     bool   // Ready condition explicitly False (unhealthy, not merely converging)
	Message     string // Ready condition message (for status attribution)
	AppliedSHA  string // 40-hex SHA parsed from status.lastAppliedRevision ("" if none)
}

// Node is the adapter's read of one graph member.
type Node struct {
	Ref       NodeRef
	DependsOn []NodeRef              // same-kind, namespace-defaulted to the node's
	SourceRef *types.NamespacedName  // referenced GitRepository; nil when sourceRef is not a GitRepository
	Readiness Readiness
	Labels    map[string]string
}

// Adapter interprets one node kind (DESIGN §4.3, D12).
type Adapter interface {
	Kind() string
	// List returns all nodes matching sel across all namespaces.
	List(ctx context.Context, r client.Reader, sel labels.Selector) ([]Node, error)
	// Get fetches a single node (used for dependsOn targets outside the selector).
	// Returns (Node{}, false, nil) when the resource does not exist.
	Get(ctx context.Context, r client.Reader, ref NodeRef) (Node, bool, error)
}

func NewKustomizationAdapter() Adapter
```

Implementation notes for `KustomizationAdapter`:
- `DependsOn`: map `kustomizev1.DependencyReference{Name, Namespace, ReadyExpr}` → `NodeRef{Kind: "Kustomization", Namespace: coalesce(dep.Namespace, obj.Namespace), Name: dep.Name}`. `ReadyExpr` is deliberately ignored: it tunes Flux's *apply-side* gating; wavefront settledness always uses the full Ready condition.
- `SourceRef`: non-nil only when `spec.sourceRef.Kind == sourcev1.GitRepositoryKind`; namespace defaults to the Kustomization's.
- `Readiness`: `Ready` = `conditions.IsTrue(obj, meta.ReadyCondition)` && `obj.Status.ObservedGeneration == obj.Generation`; `Failing` = `conditions.IsFalse(obj, meta.ReadyCondition)`; `AppliedSHA` = `git.ExtractHashFromRevision(obj.Status.LastAppliedRevision).String()` (empty-safe).

- [ ] **Step 1: Write failing table-driven tests** using `sigs.k8s.io/controller-runtime/pkg/client/fake` with the kustomizev1 scheme. Cases: label selection across namespaces; dependsOn namespace defaulting; cross-namespace dependsOn; non-GitRepository sourceRef (`Kind: OCIRepository`) → `SourceRef == nil`; readiness mapping (True+currentGen → Ready; True+staleGen → not Ready; False → Failing; missing condition → neither); `lastAppliedRevision` `"main@sha1:aaaa…"` → `AppliedSHA "aaaa…"`; empty revision → `""`; `Get` on absent object → `(_, false, nil)`.
- [ ] **Step 2:** Run — fail. Implement. Run — pass.
- [ ] **Step 3: Commit** — `feat(adapter): node adapter seam with Kustomization implementation`.

---

### Task 4: Graph (`internal/graph`)

**Files:**
- Create: `internal/graph/graph.go`
- Test: `internal/graph/graph_test.go`

**Interfaces:**
- Consumes: `adapter.NodeRef`.
- Produces:

```go
package graph

// Build constructs the DAG from nodes and their dependsOn edges.
// Edges to refs absent from nodes are auto-registered as external nodes
// (retrievable via Unknown()); the caller decides their semantics.
// Build never fails: cycles are reported, not errors (DESIGN §3.2).
func Build(nodes map[adapter.NodeRef][]adapter.NodeRef) *Graph

type Graph struct { /* opaque */ }

func (g *Graph) Nodes() iter.Seq[adapter.NodeRef]
func (g *Graph) DependsOn(ref adapter.NodeRef) []adapter.NodeRef
// TransitiveAncestors returns every ancestor reachable via dependsOn (memoized).
func (g *Graph) TransitiveAncestors(ref adapter.NodeRef) []adapter.NodeRef
// Cycles returns the strongly connected components with len > 1 (plus self-loops).
func (g *Graph) Cycles() [][]adapter.NodeRef
// InCycle reports whether ref belongs to, or transitively depends on, a cycle.
func (g *Graph) InCycle(ref adapter.NodeRef) bool
// Unknown returns refs that appear only as edge targets (not supplied as nodes).
func (g *Graph) Unknown() []adapter.NodeRef
```

Cycle detection via Tarjan SCC or iterative DFS coloring; `InCycle` covers descendants of cycles so the whole affected component is excluded from admission (DESIGN §3.2).

- [ ] **Step 1: Write failing table-driven tests.** Cases: linear chain A←B←C (`TransitiveAncestors(C) == {A,B}`); diamond (A←B, A←C, B/C←D — D's ancestors {A,B,C}, no duplicates); disconnected branches (no cross-ancestors); self-loop (cycle of one, `InCycle` true); 2-cycle and 3-cycle (members + descendants `InCycle`, unrelated branch untouched); dangling edge target → in `Unknown()` and still an ancestor; deterministic ordering of returned slices (sort by String()) so status output is stable.
- [ ] **Step 2:** Run — fail. Implement. Run — pass.
- [ ] **Step 3: Commit** — `feat(graph): dependsOn DAG with cycle detection and transitive ancestors`.

---

### Task 5: Admission engine (`internal/engine`)

This is the correctness core (DESIGN §3.3, D6, D13). Pure functions; no clock, no I/O, no Kubernetes.

**Files:**
- Create: `internal/engine/engine.go`
- Test: `internal/engine/engine_test.go`

**Interfaces:**
- Consumes: `graph.Graph`, `adapter.NodeRef`.
- Produces:

```go
package engine

type Role string
const (
	RolePinned Role = "Pinned" // selected node whose source is a managed GitRepository
	RoleGate   Role = "Gate"   // health-only participant (DESIGN §3.2)
)

type State string
const (
	StateSettled    State = "Settled"
	StatePending    State = "Pending"
	StateAdmissible State = "Admissible"
	StateConverging State = "Converging"
	StateUnhealthy  State = "Unhealthy"
)

type BlockedReason string
const (
	ReasonAncestorUnhealthy  BlockedReason = "AncestorUnhealthy"
	ReasonAncestorPending    BlockedReason = "AncestorPending"    // pending or converging
	ReasonAncestorHeld       BlockedReason = "AncestorHeld"       // held or suspended
	ReasonAncestorUnobserved BlockedReason = "AncestorUnobserved" // no ref observation yet
	ReasonSelfHeld           BlockedReason = "SelfHeld"
	ReasonGraphCycle         BlockedReason = "GraphCycle"
)

// SourceState is the controller's read of one managed GitRepository,
// combined with the poller's observation of its tracking ref.
type SourceState struct {
	Source        types.NamespacedName
	TrackingRef   string    // full ref name being tracked, e.g. "refs/heads/main"
	Pin           string    // spec.ref.commit ("" = unpinned)
	Held          bool      // commit owned by a foreign field manager
	HeldBy        string
	Suspended     bool      // spec.suspend (human incident action)
	ArtifactSHA   string    // parsed from status.artifact.revision ("" if no artifact)
	FetchFailing  bool      // sourcev1 FetchFailed condition True (metrics only)
	ObservedSHA   string    // latest advertised SHA of TrackingRef ("" = not observed)
	FirstObserved time.Time // when ObservedSHA was first seen (admission_wait, pin lag)
}

type NodeInput struct {
	Ref    adapter.NodeRef
	Role   Role
	Ready  bool   // adapter.Readiness.Ready
	Failing bool  // adapter.Readiness.Failing
	AppliedSHA string
	Source *SourceState // nil iff Role == RoleGate
}

type Admission struct {
	Node        adapter.NodeRef
	Source      types.NamespacedName
	From        string // previous pin ("" for initial pin)
	To          string // the observed SHA being admitted
	ObservedRef string
	Initial     bool // true = initial-pin-on-discovery (§3.5.4), not ancestor-gated
	PendingSince time.Time
}

type NodeResult struct {
	State        State
	Held         bool
	Blocked      *Blocked   // set when Pending but not Admissible
	PendingSince time.Time  // zero unless pending
}

type Blocked struct {
	Ancestor adapter.NodeRef // nearest unsettled transitive ancestor (zero for SelfHeld/GraphCycle)
	Reason   BlockedReason
}

type Evaluation struct {
	Nodes      map[adapter.NodeRef]NodeResult
	Admissions []Admission // ancestor-gated pin advances, deterministic order
	Initial    []Admission // initial pins (not ancestor-gated)
}

// Evaluate derives all node states and the admissible set. Pure: same inputs,
// same outputs; restart-safe by construction (DESIGN D9).
func Evaluate(g *graph.Graph, inputs map[adapter.NodeRef]NodeInput) Evaluation
```

Semantics to implement (each is a test below):
1. **settled(n)** — gate: `Ready`. Pinned, held or suspended: `Ready && (ObservedSHA == "" || ObservedSHA == Pin) && (Pin == "" || AppliedSHA == Pin)` (a held node with pending changes is unsettled, §3.3; the `Pin == ""` escape is deliberate — with rule 5 also refusing an initial pin onto a held/suspended source, requiring `AppliedSHA == Pin` unconditionally would livelock a Ready, never-pinned, suspended source permanently unsettled). Pinned normal: `Pin != "" && ObservedSHA == Pin && Ready && AppliedSHA == Pin`.
2. **Unobserved ancestors are unsettled** (conservative): a pinned ancestor with `ObservedSHA == ""` cannot prove quiescence, so it blocks (reason `AncestorUnobserved`). Gates need no observation.
3. **pending(n)**: pinned, `Pin != ""`, `ObservedSHA != ""`, `ObservedSHA != Pin`.
4. **Admissible** = pending ∧ ¬Held ∧ ¬Suspended ∧ ¬`g.InCycle` ∧ every transitive ancestor settled (D13). Emits `Admission{From: Pin, To: ObservedSHA}`. Two or more selected nodes sharing one Source are gated together (`gateSharedSources`): a node's `NodeResult` stays Admissible only when every referencing node is admissible too, and the emitted `Admissions`/`Initial` slice carries at most one Admission per Source (first by node order) regardless of how many nodes reference it.
5. **Initial pin**: pinned role, `Pin == ""`, source neither Held nor Suspended → `Admission{Initial: true, To: ArtifactSHA or ObservedSHA}` (artifact preferred, §3.5.4); if neither exists yet, no admission (wait for first observation); a held or suspended unpinned source gets no initial pin either. Not ancestor-gated. `From` records `""`.
6. **State assignment** — gate: Settled iff Ready, else Unhealthy. Pinned: pending → Admissible/Pending (per rule 4); else Failing → Unhealthy; else settled → Settled; else → Converging.
7. **Blocked attribution**: for each Pending-not-Admissible node, walk ancestors in BFS order from the node and report the nearest unsettled one with a reason derived from that ancestor (Unhealthy/Failing → `AncestorUnhealthy`; held/suspended → `AncestorHeld`; unobserved → `AncestorUnobserved`; else → `AncestorPending`). Self-held pending nodes get `SelfHeld`; cycle members/descendants get `GraphCycle`; a node demoted by rule 4's shared-source gate because a sibling referencing the same Source is not admissible gets `SharedSourceBlocked`, attributed to that blocking sibling rather than an ancestor.
8. **Determinism**: `Admissions`/`Initial` sorted by `NodeRef.String()`.

- [ ] **Step 1: Write the failing test table.** Helper builders keep cases terse (`pinnedNode(ref, opts...)`, `gateNode(ref, ready)`, `chain(g, "A", "B", "C")`). Required cases (assert full `NodeResult` + admission set for each):
  - lone pinned node, pending, no ancestors → Admissible, admission `{From: pin, To: observed}`.
  - chain A←B, both pending → A Admissible; B Pending blocked `{A, AncestorPending}` (strict ordering for co-arriving changes, §3.3).
  - A admitted-but-converging (observed==pin, Ready false, Failing false), B pending → B blocked `AncestorPending`.
  - A Unhealthy (Failing), chain A←B←C with B Settled, C pending → C blocked `{A, AncestorUnhealthy}` — **transitive through a settled intermediate** (D6/D13 key case).
  - gate G unready, G←X pending → X blocked `{G, AncestorUnhealthy}`; gate ready → X Admissible.
  - held node H (Held=true) with pending changes → H Pending + `SelfHeld` + `Held: true`, no admission; descendant blocked `{H, AncestorHeld}`. Held with nothing pending and Ready → Settled (still no admissions).
  - suspended source: same shape as held.
  - unpinned source with artifact → Initial admission to artifact SHA even while ancestors unsettled; unpinned, no artifact, observed → Initial to observed SHA; unpinned, neither → nothing.
  - ancestor with `ObservedSHA == ""` → descendant blocked `AncestorUnobserved`; the unobserved node itself (Ready at pin) → Converging, no admission.
  - two disconnected branches, one wholly unhealthy → other branch admits (concurrency is free, §3.3).
  - 2-cycle members + their descendant → `GraphCycle`, no admissions in the component; disconnected branch unaffected.
  - fix-on-unhealthy: A Failing AND pending, no ancestors → A Admissible (the fix is the normal path, §3.3).
  - observed == pin, Ready at pin → Settled, empty admission set (quiescent system is silent).
- [ ] **Step 2:** Run — fail. Implement `Evaluate`. Run — pass.
- [ ] **Step 3:** Fuzz-ish property test: generate random DAGs (no cycles) with random states; assert (a) no admission for a node with any unsettled transitive ancestor, (b) admissions ⊆ pending nodes, (c) evaluation is deterministic across two runs.
- [ ] **Step 4: Commit** — `feat(engine): stateless rolling-admission engine (settled-ancestors rule)`.

---

### Task 6: Candidate selection (`internal/selection`)

**Files:**
- Create: `internal/selection/selection.go`
- Test: `internal/selection/selection_test.go`

**Interfaces:**
- Consumes: `sourcev1.GitRepositoryRef`.
- Produces (the D10 seam):

```go
package selection

// ErrUnsupportedRef marks ref styles v1 cannot sequence (semver).
var ErrUnsupportedRef = errors.New("unsupported ref style for candidate selection")

// Strategy answers "which SHA is the candidate?" (DESIGN §3.6).
type Strategy interface {
	// TrackingRef maps a GitRepository ref spec to the advertised ref name to observe.
	TrackingRef(ref *sourcev1.GitRepositoryRef) (string, error)
	// Candidate selects the candidate SHA from an advertisement listing
	// (map of ref name → SHA, including peeled "<ref>^{}" entries).
	Candidate(advertised map[string]string, trackingRef string) (sha string, ok bool)
}

// TrackRef is the v1 default and only strategy (DESIGN D10).
func TrackRef() Strategy
```

`TrackingRef`: `ref.Name` → verbatim; `ref.Branch` → `"refs/heads/" + branch`; `ref.Tag` → `"refs/tags/" + tag`; `ref.SemVer != ""` → `ErrUnsupportedRef`; nil/empty ref → `"refs/heads/master"` (Flux's default branch is `master` — `git.DefaultBranch`). `Candidate`: prefer `advertised[trackingRef + "^{}"]` (annotated-tag peel, §3.6), else `advertised[trackingRef]`.

- [ ] **Step 1: Failing table tests** covering every mapping above plus: peeled entry preferred over tag object SHA; missing ref → `ok == false`; and a ref with only `Commit` set (no name/branch/tag) → falls back to the default branch — a rendered `ref.commit` is a hand-pin concern handled by `pin.Hold`, not a tracking style, so `TrackingRef` deliberately considers only Name/Branch/Tag/SemVer.
- [ ] **Step 2:** Run — fail. Implement. Run — pass.
- [ ] **Step 3: Commit** — `feat(selection): TrackRef candidate strategy behind the D10 seam`.

---

### Task 7: Ref listing, auth, and the poller (`internal/gitpoll`)

**Files:**
- Create: `internal/gitpoll/auth.go`, `internal/gitpoll/lister.go`, `internal/gitpoll/poller.go`
- Test: `internal/gitpoll/auth_test.go`, `internal/gitpoll/lister_test.go` (gittestserver), `internal/gitpoll/poller_test.go` (synctest + fake lister)

**Interfaces:**
- Consumes: `selection.Strategy` output (tracking ref names); Kubernetes secrets via `client.Reader`.
- Produces:

```go
package gitpoll

// AuthFromSecret builds a go-git transport auth method from a GitRepository's
// secret, using the same parser as source-controller (DESIGN §7.1).
// data may be nil (anonymous HTTP).
func AuthFromSecret(repoURL string, data map[string][]byte) (transport.AuthMethod, error)

// Lister lists advertised refs for one repository URL.
// Returned map: full ref name → 40-hex SHA, including peeled "<ref>^{}" entries.
type Lister interface {
	List(ctx context.Context, repoURL string, auth transport.AuthMethod) (map[string]string, error)
}

// NewGoGitLister returns the production Lister (go-git Remote.List, no clone).
func NewGoGitLister(timeout time.Duration) Lister

// Target is one source to poll.
type Target struct {
	Source      types.NamespacedName // the GitRepository
	URL         string
	SecretRef   *types.NamespacedName // secret holding credentials, if any
	TrackingRef string
}

// Observation is the latest advertisement result for a target's tracking ref.
type Observation struct {
	SHA           string    // "" while unobserved or on persistent failure
	ObservedAt    time.Time
	FirstObserved time.Time // when this SHA value was first seen (reset on change)
	Err           error     // last listing error, nil on success
}

// Poller periodically sweeps all targets, batched per git host with bounded
// per-host concurrency (DESIGN §3.1, §4.1 poll.*). Implements manager.Runnable.
type Poller struct { /* opaque */ }

func NewPoller(secrets client.Reader, lister Lister, notify func(), strategy selection.Strategy, reg prometheus.Registerer) *Poller
func (p *Poller) Configure(interval time.Duration, perHostConcurrency int)
func (p *Poller) SetTargets(targets []Target)  // replaces the poll set (reconciler calls this)
func (p *Poller) Observation(src types.NamespacedName) (Observation, bool)
func (p *Poller) Start(ctx context.Context) error // blocks until ctx done
```

Implementation notes:
- `AuthFromSecret`: `u, err := url.Parse(repoURL)` → `opts, err := git.NewAuthOptions(*u, data)` (`github.com/fluxcd/pkg/git`) → convert to go-git: `BearerToken` → `&githttp.TokenAuth{Token}`; `Username/Password` → `&githttp.BasicAuth{...}`; SSH (`Identity` set) → `gossh.NewPublicKeys("git", opts.Identity, opts.Password)` with host key callback built from `opts.KnownHosts` (`golang.org/x/crypto/ssh/knownhosts` via a temp file, or `fluxcd/pkg/git/gogit`'s exported `CustomPublicKeys` wrapper if its signature fits — check at compile time; the SDK's own `transportAuth` is unexported, DESIGN §7.2 anticipated this glue).
- `lister.go`: `import _ "github.com/fluxcd/pkg/git/gogit"` for its `init()` side effect (registers capabilities for protocol-v2-only hosts, DESIGN §7.3). Listing: `rem := gogit.NewRemote(memory.NewStorage(), &config.RemoteConfig{Name: "origin", URLs: []string{repoURL}})` then `rem.ListContext(ctx, &gogit.ListOptions{Auth: auth, PeelingOption: gogit.AppendPeeled})`; convert to the map (ref `Name().String()` → `Hash().String()`; peeled refs already carry the `^{}` suffix).
- Poller sweep: group targets by `u.Host`; hosts in parallel; per-host semaphore of `perHostConcurrency`; per-target: resolve secret (fresh `Get` each sweep — credentials rotate), list, update observation store under mutex. `FirstObserved` is preserved while SHA is unchanged, reset when it changes; failures set `Err` and increment `wavefront_ref_list_failures_total{host}` but never clear the last good SHA. After each sweep, call `notify()` exactly once (the reconciler coalesces).
- No git objects are ever fetched; no disk is touched (statelessness invariant, §3.1.1).

- [ ] **Step 1 (lister, failing test):** with `gittestserver`: `srv, _ := gittestserver.NewTempGitServer(); srv.AutoCreate(); srv.StartHTTP(); defer srv.StopHTTP()`. Author a repo with go-git (init in temp dir, commit, push to `srv.HTTPAddress() + "/test.git"`, including one annotated tag), then assert `List` returns `refs/heads/main` → head SHA and both `refs/tags/v1` and `refs/tags/v1^{}` (peeled = commit SHA). Add an auth case: `srv.Auth("user", "pass")` + `AuthFromSecret` with `{username, password}` secret data succeeds, anonymous fails.
- [ ] **Step 2:** Run — fail. Implement `AuthFromSecret` + `NewGoGitLister`. Run — pass.
- [ ] **Step 3 (poller, failing tests):** use Go 1.27 `testing/synctest` with a fake `Lister` (scripted responses) and a fake secrets reader: interval ticking (no sweep before interval, sweep at interval); per-host concurrency bound (fake lister records concurrent in-flight per host; assert ≤ configured); `FirstObserved` stable across repeated identical SHAs and reset on change; listing error retains previous SHA and surfaces `Err`; `SetTargets` removing a target drops its observation; `notify` called once per sweep.
- [ ] **Step 4:** Run — fail. Implement `Poller`. Run — pass.
- [ ] **Step 5: Commit** — `feat(gitpoll): ref-advertisement lister, source-controller-parity auth, batched poller`.

---

### Task 8: Pin writer and hold detection (`internal/pin`)

**Files:**
- Create: `internal/pin/pin.go`
- Test: `internal/pin/pin_test.go` (envtest — SSA semantics need a real apiserver; add a small suite here with the Flux CRD dir, mirroring `internal/controller/suite_test.go`)

**Interfaces:**
- Consumes: `sourcev1.GitRepository`; observed SHAs from the engine's admissions.
- Produces:

```go
package pin

const (
	FieldManager      = "wavefront-controller"
	ManagedLabel      = "wavefront.as-code.io/managed"       // read-only for the controller
	AnnotAdmittedAt   = "wavefront.as-code.io/admitted-at"
	AnnotPreviousPin  = "wavefront.as-code.io/previous-pin"
	AnnotObservedRef  = "wavefront.as-code.io/observed-ref"
)

// ErrHeld is returned when the SSA patch conflicts with a foreign field
// manager — i.e. a human hand-pin. Never force past it (DESIGN §3.5.3).
var ErrHeld = errors.New("spec.ref.commit is held by another field manager")

// Hold inspects managedFields and reports a foreign owner of spec.ref.commit.
func Hold(repo *sourcev1.GitRepository) (manager string, held bool)

type Writer struct{ Client client.Client }

// Advance pins repo to sha with provenance annotations, via SSA under
// FieldManager WITHOUT ForceOwnership. A 409 conflict is normalised to ErrHeld.
// prev is the outgoing pin ("" for an initial pin); observedRef the tracking ref.
func (w *Writer) Advance(ctx context.Context, repo types.NamespacedName, prev, sha, observedRef string, at time.Time) error
```

Implementation notes:
- `Advance` builds a **minimal** unstructured object — `apiVersion: source.toolkit.fluxcd.io/v1`, `kind: GitRepository`, `metadata.name/namespace`, the three annotations, and `spec.ref.commit` only — and applies it with controller-runtime v0.24's first-class SSA: `w.Client.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldManager))`. No `client.ForceOwnership`. `apierrors.IsConflict(err)` → `return fmt.Errorf("%w: %s", ErrHeld, err)`.
- `Hold` iterates `repo.GetManagedFields()`; for entries with `Manager != FieldManager`, parse `entry.FieldsV1.Raw` with `sigs.k8s.io/structured-merge-diff/v6/fieldpath` (`s := &fieldpath.Set{}; s.FromJSON(bytes.NewReader(raw)); s.Has(fieldpath.MakePathOrDie("spec", "ref", "commit"))` — note the leaf lives in the entry's `f:spec.f:ref.f:commit`); first foreign owner wins.
- Annotation values: `admitted-at` RFC3339 (`at.UTC().Format(time.RFC3339)`), `previous-pin` = `prev` (empty string omitted → annotation set to `""`? No: for initial pins set `previous-pin: ""` explicitly so the SSA set stays stable), `observed-ref` = `observedRef`.

- [ ] **Step 1: Failing envtest tests:**
  - *Clean co-ownership:* create a GitRepository via SSA as manager `kustomize-controller` (url, `ref.name`, label, interval — no commit). `Advance` to `shaA`. Assert: `spec.ref.commit == shaA`, annotations present, `spec.ref.name` and labels untouched, managedFields shows `kustomize-controller` owning `f:name` and `wavefront-controller` owning `f:commit`.
  - *Second advance:* `Advance` to `shaB` with `prev: shaA` → `previous-pin == shaA`, commit `shaB`.
  - *Hand-pin detection:* separate client (`client.FieldOwner("kubectl-edit")` via Update) sets `spec.ref.commit` to `shaX`. `Hold` → `("kubectl-edit", true)`; `Advance` → `ErrHeld`; commit remains `shaX`.
  - *Hold release:* remove the commit field via the foreign manager (json patch remove) → `Hold` → `(_, false)`; `Advance` succeeds.
  - *No label writes:* after all operations, the managed label's field owner is still the creator.
- [ ] **Step 2:** Run — fail. Implement. Run — pass.
- [ ] **Step 3: Commit** — `feat(pin): SSA pin writer with provenance and foreign-manager hold detection`.

---

### Task 9: The `Wavefront` reconciler (`internal/controller`)

**Files:**
- Modify: `internal/controller/wavefront_controller.go` (replace scaffold stub), `cmd/main.go` (wire poller + reconciler)
- Test: `internal/controller/wavefront_controller_test.go` (envtest integration; suite from Task 2)

**Interfaces:**
- Consumes: everything above — `adapter.NewKustomizationAdapter`, `graph.Build`, `engine.Evaluate`, `selection.TrackRef`, `gitpoll.Poller`/`AuthFromSecret` types, `pin.Writer`/`pin.Hold`/`pin.ErrHeld`, `wavefrontv1alpha1` types.
- Produces:

```go
type WavefrontReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	Adapter   adapter.Adapter        // NewKustomizationAdapter()
	Strategy  selection.Strategy     // selection.TrackRef()
	Poller    *gitpoll.Poller
	PinWriter *pin.Writer
	Metrics   *metrics.Instruments   // Task 10 (constructor takes a no-op until then)
	Clock     func() time.Time       // time.Now in main; fixed in tests
}
func (r *WavefrontReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error)
func (r *WavefrontReconciler) SetupWithManager(mgr ctrl.Manager, events <-chan event.GenericEvent) error
```

**Reconcile flow** (implement in this order; each numbered part is a private method):
1. Fetch the `Wavefront` (cluster-scoped). Not found → done (no finalizer: deleting a Wavefront releases management, pins stay in place — DESIGN D8's "released from management" semantics; document in runbook).
2. **Selector overlap** (DESIGN §4.1): list all `Wavefront`s; after discovery (step 3) test every selected node's labels against each *other* Wavefront's selector. Any match → `GraphValid: False` reason `SelectorOverlap` (message naming the other Wavefront), skip all admissions, still publish status.
3. **Discover nodes**: `sel, _ := metav1.LabelSelectorAsSelector(&wf.Spec.Nodes.Selector)`; `Adapter.List(ctx, r.Client, sel)` → selected set. Transitive closure over `DependsOn` targets not in the set via `Adapter.Get` (breadth-first until no new refs; missing targets recorded — they become permanently-unready gates so descendants block, matching Flux's own behaviour on a missing dependency).
4. **Resolve sources & roles**: for each node with `SourceRef != nil`, `Get` the `GitRepository`. Role `Pinned` iff the node is in the *selected* set AND the GitRepository carries `pin.ManagedLabel == "true"`; else `Gate`. Build `engine.SourceState`: `Pin` from `spec.ref.commit`; `Held/HeldBy` from `pin.Hold`; `Suspended` from `spec.suspend`; `ArtifactSHA` via `git.ExtractHashFromRevision(repo.Status.Artifact.Revision).String()` when artifact non-nil; `FetchFailing` = `conditions.IsTrue(repo, sourcev1.FetchFailedCondition)`; observation from `Poller.Observation`. `TrackingRef` via `Strategy.TrackingRef(repo.Spec.Reference)` — `ErrUnsupportedRef` (semver) demotes the node to `Gate` with a `UnsupportedRefStyle` warning event (catalog CI should have rejected it, D10).
5. **Update poll set**: `Poller.Configure(wf.Spec.Poll.Interval.Duration, wf.Spec.Poll.PerHostConcurrency)`; `Poller.SetTargets` with every managed source (URL, `spec.secretRef` → namespaced secret ref, tracking ref).
6. **Graph + engine**: `g := graph.Build(edges)`; cycles → `GraphValid: False` reason `CyclesDetected` (message lists one cycle); `ev := engine.Evaluate(g, inputs)`.
7. **Execute** (order: initial pins, then admissions):
   - `wf.Spec.Suspend` → skip all writes (status still updates; phase computed normally).
   - `Mode == Shadow` → for each would-be admission (initial included) emit `ShadowAdmission` event on the Wavefront (message: `would pin <ns>/<name> to <sha> (from <prev>, ref <ref>)`); no writes (§3.5.4: initial pins suppressed in Shadow).
   - `Mode == Enforce` → `PinWriter.Advance` per admission; success → event `PinAdvanced` / `InitialPin` on the *GitRepository* (and mirrored on the Wavefront); `pin.ErrHeld` → re-mark node held this pass, event `HoldDetected`.
8. **Hold edge events**: diff currently-held sources against `status.held` from the *previous* status → emit `HoldDetected` / `HoldReleased` on transitions (status is the ledger for edge-triggering; the evaluation itself remains stateless).
9. **Status**: counts (`observed` = all nodes, `pinned`/`gates` by role, `pending`/`converging`/`blocked`/`held` from results); `blocked`/`held` lists sorted, capped at `StatusListCap`; phase — any block with reason `AncestorUnhealthy`/`AncestorHeld`/`SelfHeld`/`GraphCycle` → `Blocked`; else any pending/converging/admissions → `Advancing`; else `Quiescent`. Conditions: `Ready` (True unless the pass errored; False with reason on list/get failures), `GraphValid` (per steps 2/6, True otherwise), both with `ObservedGeneration`. Patch status with `client.MergeFrom` on the pre-mutation copy.
10. **Metrics** (no-op until Task 10): admissions counter, pin-lag/blocked gauges, admission-wait observations (`Clock() − admission.PendingSince`), pinned-fetch-failure gauge from `FetchFailing`.
11. Return `ctrl.Result{RequeueAfter: wf.Spec.Poll.Interval.Duration}` as a safety net (events drive the loop normally; `Requeue` bool is deprecated in controller-runtime v0.24).

**SetupWithManager:**

```go
ctrl.NewControllerManagedBy(mgr).
	For(&wavefrontv1alpha1.Wavefront{}).
	Watches(&kustomizev1.Kustomization{}, handler.EnqueueRequestsFromMapFunc(r.mapToWavefronts)).
	Watches(&sourcev1.GitRepository{}, handler.EnqueueRequestsFromMapFunc(r.mapToWavefronts)).
	WatchesRawSource(source.Channel(events, &handler.EnqueueRequestForObject{})).
	Complete(r)
```

`mapToWavefronts` lists Wavefronts and returns a request per item (fleet counts are ~1; do not filter by label — gates are reachable via unlabeled objects and status changes matter, so no `GenerationChangedPredicate` on the Flux watches). In `cmd/main.go`: build the poller (`gitpoll.NewPoller(mgr.GetClient(), gitpoll.NewGoGitLister(30*time.Second), notify, ctrlmetrics.Registry)`) where `notify` sends a `event.GenericEvent{Object: &wavefrontv1alpha1.Wavefront{ObjectMeta: metav1.ObjectMeta{Name: wavefrontName}}}`… — simpler and correct for N Wavefronts: `notify` enqueues one GenericEvent per existing Wavefront (list via the manager's client). Add the poller with `mgr.Add(p)`.

**RBAC markers** (on the reconciler; `make manifests` regenerates roles):

```go
// +kubebuilder:rbac:groups=wavefront.as-code.io,resources=wavefronts,verbs=get;list;watch
// +kubebuilder:rbac:groups=wavefront.as-code.io,resources=wavefronts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=gitrepositories,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=kustomize.toolkit.fluxcd.io,resources=kustomizations,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
```

- [ ] **Step 1: Failing integration tests** (envtest; remember: no Flux controllers run — tests hand-set Flux statuses via `Status().Update`, and no GC). Shared helpers: `makeGitRepo(ns, name, url, refName string, managed bool)`, `makeKustomization(ns, name string, sourceRef, dependsOn, labels)`, `setKustomizationReady(obj, revision string, ready bool)`, `setArtifact(repo, revision string)`. Run the manager (with a **fake lister** behind a real `Poller`, scripted per-URL SHAs; interval 100ms) from `BeforeSuite`. Scenarios:
  - *Initial pin:* managed GitRepository with artifact `main@sha1:<shaA>` + selected Kustomization → `spec.ref.commit` becomes `shaA`, `previous-pin: ""`, `InitialPin` event, status counts `pinned: 1`.
  - *Rolling admission with gate:* chain `gate ← flotilla`; gate Kustomization (no managed source) not Ready; fake lister advertises `shaB` → flotilla stays pinned `shaA`, `status.blocked[0].ancestor == gate`, reason `AncestorUnhealthy`, phase `Blocked`; set gate Ready → pin advances to `shaB` with full provenance; `PinAdvanced` event; phase eventually `Quiescent` once test marks flotilla Ready at `shaB`.
  - *Strict co-arrival:* chain of two pinned nodes, advertise new SHAs for both → only upstream advances; downstream advances only after test sets upstream Ready at its new pin (settled), never before.
  - *Shadow:* `mode: Shadow` Wavefront → no writes ever (commit stays unset), `ShadowAdmission` events carry the would-be SHA, status/counts still live.
  - *Suspend:* flip `spec.suspend: true` mid-pending → no further writes; status still updates.
  - *Hand-pin:* foreign-manager update of commit → `status.held` names the manager, `HoldDetected` once (not repeated), no advancement; remove → `HoldReleased`, pin advances.
  - *Selector overlap:* second Wavefront matching the same label → both get `GraphValid: False` reason `SelectorOverlap`, no admissions.
  - *Cycle:* two Kustomizations depending on each other → `GraphValid: False` reason `CyclesDetected`; unrelated flotilla still admits.
  - *CRD validation cases from Task 2 remain green.*
- [ ] **Step 2:** Run — fail. Implement reconciler + main wiring, part by part (the numbered methods above), re-running the suite as parts land.
- [ ] **Step 3:** Run full `make test` — pass.
- [ ] **Step 4: Commits** (split naturally): `feat(controller): node discovery, source resolution and graph derivation`; `feat(controller): admission execution, shadow mode and holds`; `feat(controller): fleet status, conditions and events`; `feat: wire poller and reconciler in main`.

---

### Task 10: Metrics (`internal/metrics`)

**Files:**
- Create: `internal/metrics/metrics.go`
- Test: `internal/metrics/metrics_test.go`
- Modify: `internal/controller/wavefront_controller.go`, `internal/gitpoll/poller.go` (inject)

**Interfaces:**
- Consumes: `prometheus` + `sigs.k8s.io/controller-runtime/pkg/metrics` (`metrics.Registry`).
- Produces (DESIGN §6, names verbatim):

```go
package metrics

type Instruments struct {
	AdmissionsTotal        *prometheus.CounterVec   // wavefront_admissions_total{wavefront,result="admitted|initial|shadow|conflict"}
	PinLagSeconds          *prometheus.GaugeVec     // wavefront_node_pin_lag_seconds{wavefront,kind,namespace,name}
	AdmissionWaitSeconds   *prometheus.HistogramVec // wavefront_admission_wait_seconds{wavefront} (buckets: 30s..2h exponential)
	BlockedNodes           *prometheus.GaugeVec     // wavefront_blocked_nodes{wavefront,reason}
	RefListFailures        *prometheus.CounterVec   // wavefront_ref_list_failures_total{host}
	PinnedFetchFailures    *prometheus.GaugeVec     // wavefront_pinned_fetch_failures{wavefront}
	CredentialReadFailures prometheus.Counter       // wavefront_credential_read_failures_total
}

func New(reg prometheus.Registerer) (*Instruments, error)  // main passes ctrlmetrics.Registry; error is fatal in main
func Nop() *Instruments                            // isolated registry, for tests/earlier tasks
```

Per-Wavefront gauges (`PinLagSeconds`, `BlockedNodes`, `PinnedFetchFailures`) are
recomputed wholesale each pass, but a pass retires only its own Wavefront's
series — `DeletePartialMatch` on the `wavefront` label, never `Reset()`,
which would erase a co-resident Wavefront's series until its own next pass —
and only a valid resolved pass publishes series at all, so an aborted pass or
a graph-invalidated Wavefront leaves its series retired rather than stale.
`AdmissionsTotal` and `AdmissionWaitSeconds` are cumulative: a pass never
retires them; only Wavefront deletion (`Forget`) deletes them.

- [ ] **Step 1: Failing tests** with `prometheus/client_golang/prometheus/testutil`: after a scripted sequence of calls, `CollectAndCompare` against expected exposition text; `testutil.CollectAndLint` passes; double-`New` on one registry does not panic (guard or document single-call).
- [ ] **Step 2:** Run — fail. Implement; inject into reconciler step 10 and the poller's failure path; extend one envtest scenario to assert `wavefront_admissions_total` increments.
- [ ] **Step 3: Commit** — `feat(metrics): fleet admission metrics (§6 launch set)`.

---

### Task 11: Deploy manifests, samples, runbook

**Files:**
- Modify: `config/rbac/*` (generated), `config/manager/manager.yaml` (resources, probes — scaffold defaults are fine to keep)
- Create: `config/samples/wavefront_v1alpha1_wavefront.yaml` (replace stub), `docs/runbook.md`

**Interfaces:**
- Consumes: everything shipped.
- Produces: installable `make build-installer` bundle; operational documentation for DESIGN §8.5.

- [ ] **Step 1: Sample CR** — DESIGN §4.1's example with the real group, `mode: Shadow`, participation label `wavefront.as-code.io/managed: "true"`.
- [ ] **Step 2: `docs/runbook.md`** covering: **break-glass pin-strip** (restores plain floating-ref Flux):

```sh
kubectl get gitrepositories -A -l wavefront.as-code.io/managed=true -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' |
while read -r ns name; do
  kubectl -n "$ns" patch gitrepository "$name" --type=json -p='[{"op":"remove","path":"/spec/ref/commit"}]'
done
```

  plus: the gentler `spec.suspend: true` on the Wavefront; the **release-set query** (§4.2) — pins + provenance via jsonpath over managed GitRepositories; hand-pin etiquette (any foreign manager on `spec.ref.commit` is honoured as a hold; remove it to resume); what deleting the Wavefront does (management released, pins left in place); Shadow→Enforce flip procedure; alarm suggestions (§6 safety alarms: liveness, pin-staleness `wavefront_node_pin_lag_seconds`, `wavefront_ref_list_failures_total` rate, `wavefront_pinned_fetch_failures`).
- [ ] **Step 3:** `make manifests build-installer` succeeds; `kubectl apply --dry-run=client -f dist/install.yaml` clean.
- [ ] **Step 4: Commit** — `docs: sample Wavefront, operations runbook, installer manifest`.

---

### Task 12: End-to-end suite (kind + real Flux)

This is the acceptance test for DESIGN §9 Phase 1's exercises, run against real source/kustomize controllers.

**Files:**
- Create: `test/e2e/gitserver/main.go`, `test/e2e/gitserver/Dockerfile`, `test/e2e/gitserver/manifests.yaml` (Deployment + Service `gitserver.wavefront-e2e.svc`)
- Modify: `test/e2e/e2e_suite_test.go`, `test/e2e/e2e_test.go` (extend the scaffold; keep its manager-deployment checks), `test/utils/utils.go` (helpers), `Makefile` (`test-e2e` gains Flux install + gitserver build/load)

**Interfaces:**
- Consumes: the released image (`make docker-build`), `config/default` kustomization.
- Produces: `make test-e2e` — full lifecycle proof on a kind cluster.

**Infrastructure notes:**
- *Git server:* a ~40-line `main.go` reusing the same library the tests already use: `gittestserver.NewGitServer("/srv/git").AutoCreate().StartHTTP()` on `:8080` (gitkit shells out to git — base the image on `alpine:3.22` + `apk add --no-cache git ca-certificates`; build the Go binary with `CGO_ENABLED=0`). `kind load docker-image` both this and the controller image.
- *Flux:* `kubectl apply -f test/e2e/flux-install.yaml` — vendored copy of `https://github.com/fluxcd/flux2/releases/download/v2.9.4/install.yaml` (pinned; add Makefile target `update-flux-install`). Only source-controller and kustomize-controller are exercised; the rest are harmless.
- *Repo seeding/pushing:* helpers in `test/utils` push with go-git through a `kubectl port-forward svc/gitserver 18080:8080` (in-cluster URL `http://gitserver.wavefront-e2e.svc:8080/<repo>.git` goes into GitRepository specs; the test pushes to `http://127.0.0.1:18080/<repo>.git` — same repos). `pushCommit(repo, path, content) string` returns the new SHA. Flotilla repos contain `./kustomize/configmap.yaml` (a ConfigMap) so Kustomizations (`path: ./kustomize`, `prune: true`, `wait: true`, `interval: 1m`) have something real to apply; a "breaking" push writes invalid YAML → kustomize-controller goes Ready=False for real.
- *Fixture fleet:* namespace `wavefront-e2e`; repos `infra`, `team-a` (Kustomization dependsOn infra's), plus gate Kustomization `wave-gate` (labeled, sourced from an unmanaged repo, between infra and team-a: `infra ← wave-gate ← team-a`); all Kustomizations labeled `wavefront.as-code.io/managed: "true"`, both flotilla GitRepositories labeled likewise, `spec.ref.name: refs/heads/main`, sane `interval` (30s). One `Wavefront` `fleet`, `mode: Enforce`, `poll.interval: 15s`.

- [ ] **Step 1: Failing e2e scenarios** (Ginkgo `-tags=e2e`, generous `Eventually` timeouts — 3m default, 5m for convergence chains; each `It` leaves the fleet quiescent):
  1. *Bootstrap:* apply fleet → both flotilla repos get initial pins equal to their artifact SHAs; everything converges Ready; Wavefront `phase: Quiescent`, counts `{observed: 3, pinned: 2, gates: 1}`.
  2. *Sequenced co-arrival:* push to infra (`shaI`) and team-a (`shaT`) back-to-back → infra pin advances to `shaI` and team-a's pin **must not** advance while infra/wave-gate are unsettled (poll `Consistently` during the window), then advances to `shaT` after the chain settles; provenance annotations on team-a show `previous-pin` = old SHA, `observed-ref: refs/heads/main`; `PinAdvanced` events exist for both.
  3. *Blocked subtree + fix as normal path:* push breaking manifest to infra → infra Kustomization Ready=False (real kustomize-controller); push to team-a → team-a stays pinned, Wavefront `phase: Blocked` with attribution to infra's node; push fix to infra → infra re-admits (Unhealthy→Admissible), converges, team-a then admits; phase returns to `Quiescent`.
  4. *Hand-pin coexistence:* `kubectl patch` team-a's repo `spec.ref.commit` to an older SHA (foreign manager) → `status.held` names it, pushes to team-a do not advance it; remove the hand-pin → `HoldReleased`, next poll admits.
  5. *Controller restart mid-rollout:* push to both flotillas, `kubectl delete pod` on the controller while infra converges → rollout completes correctly after restart (stateless resume, D9); no duplicate or out-of-order pins (assert team-a's `previous-pin` chain).
  6. *Shadow mode:* flip `mode: Shadow`, push to infra → no pin write (`Consistently`), `ShadowAdmission` event on the Wavefront; flip back to `Enforce` → admitted.
  7. *Force-push over a pinned commit* (DESIGN §10): force-push infra's `main` to a rewritten history → source-controller may report `FetchFailed` transiently; next poll observes the new SHA and re-pins; fleet self-heals (assert eventual Quiescent at the rewritten SHA).
- [ ] **Step 2:** Wire Makefile: `test-e2e` = kind cluster up → `kind load` images → apply Flux install → apply gitserver → `go test -tags=e2e ./test/e2e/ -v`. Run — fail (scenarios unimplemented) → implement helpers/fixtures → green.
- [ ] **Step 3: Commit** — `test(e2e): kind+Flux acceptance suite covering DESIGN §9 phase-1 exercises`.

---

### Task 13: README and final polish

**Files:**
- Create/replace: `README.md`
- Modify: anything flagged by the final sweep

- [ ] **Step 1: README** — what/why (three paragraphs distilled from DESIGN §1–2, linking DESIGN.md); install (`make deploy` / installer manifest); prerequisites checklist (DESIGN §8, with `wavefront.as-code.io` labels: catalog renders `ref.name` only + omits `ref.commit`, participation label on Kustomizations **and** flotilla GitRepositories, `wait: true` on Milestone-owning Kustomizations, chart-source colocation policy, RBAC/secrets note); `Wavefront` spec reference (every field + defaults); status/conditions/events/metrics tables (§6); Shadow→Enforce rollout summary (§9); link to `docs/runbook.md`.
- [ ] **Step 2: Final sweep** — `make manifests generate fmt vet lint test`; `go test ./... -race`; coverage sanity: `go test ./internal/... -cover` (expect engine/graph/selection near-total; adapter/gitpoll/pin high; controller covered by envtest scenarios). Fix anything found.
- [ ] **Step 3: Commit** — `docs: README with spec reference and rollout guide`.

---

## `wfctl` (shipped after Task 13)

The CLI companion: reports what a `Wavefront` is doing and why, and operates it when it is stuck. Built as `bin/wfctl` (`make build-wfctl`), installed with `make install-wfctl` into `GOBIN` alongside a `kubectl-wavefront` symlink, and deliberately **absent from the manager image** — the manager's ServiceAccount is exactly the RBAC an exec into that pod should not reach. Delivered on the same branch as the controller.

**Packages**

| Package | Role |
|---|---|
| `cmd/wfctl` | cobra root only; `Use` becomes `kubectl wavefront` when invoked through the symlink |
| `internal/inputs` | discover → resolve → derive → summarise, extracted from `internal/controller` so the controller and `--derive` run *the same* pipeline rather than two that agree by inspection |
| `internal/wfctl/snapshot` | the truth model: `Source` providers, the `Snapshot` schema, node/source ref parsing, ref polling, event listing |
| `internal/wfctl/render` | pure `Snapshot` → text; tables, colour, waves, tree, DOT, Mermaid, JSON/YAML encoding |
| `internal/wfctl/actions` | write commands as `Plan{Summary, Before, After, Warnings}` + `Apply`, with the confirmation gate and best-effort audit events |
| `internal/wfctl/cli` | flag wiring only (cli-runtime `genericclioptions` + the wfctl flags); no logic |

**Providers (the truth model).** Status-first, with a seam for a future `--live` controller API:

- *default* — `StatusSource`: reads `status.members` (added to the API for this; §4.1). Needs only `get`/`list` on `wavefronts`, which is what makes a viewer tier meaningful.
- `--derive` — `DeriveSource`: re-derives live via `inputs.Build` + `engine.Evaluate`. Answers while the controller is down; cross-checks it while it is up. Needs cluster-wide `get` **and** `list` on `Kustomization`s and `GitRepository`s — `inputs.Build` lists the selected nodes but also `Get`s individual objects (closure gates outside the selector, and every resolved source), and `list` does not imply `get`.
- `--derive --poll` — adds a ref-advertisement sweep (`gitpoll` lister + `AuthFromSecret`, per-host concurrency from the Wavefront's own spec), so observed SHAs are real rather than `?`. Reads the sources' credential `Secret`s; per-source failures become diagnostics, never fatal.
- `--from FILE` — `FileSource`: replays a `wfctl snapshot` document with no cluster at all. Snapshots never carry secret data, credentials, kubeconfig or URL userinfo.

Staleness is explicit: under the status provider wfctl warns when `status.lastEvaluated` is older than `2 × (poll.interval + 30s)` or `Ready` is `False`, and points at `--derive`.

**Commands.** Read — `status` (exits 2 on `Blocked`; `--derive` prints reported vs derived side by side), `nodes`, `sources`, `source ns/name`, `explain ns/name` (walks the blocked chain to the root cause and names the fix), `graph` (waves, `--tree`, `-o dot|mermaid`), `snapshot`, `history` (events with the retention caveat). Write — `suspend`, `resume`, `mode`, `pin`, `release` (`--float`), `pin-strip` (`--include-held`, `--suspend`), `force-admit`; each plans, prints its effect, confirms unless `--yes` (never prompting off a TTY), supports `--dry-run`, and records an audit event on the `Wavefront`. Hand-pins go under the `wfctl` SSA field manager, which the controller reads as an external hold like any other.

**Tests.** Golden renderer tests over eight snapshot fixtures (`internal/wfctl/render/testdata/`, `-update` regenerates); envtest suites for status/derive parity and for every write command's `managedFields` effect; the existing controller suites guard the `internal/inputs` extraction unchanged.

**RBAC.** Three tiers documented in the README; `config/rbac/wfctl_viewer_role.yaml` is a hand-written `ClusterRole` for the viewer tier (read on `wavefronts` plus `get`/`list` on `events.events.k8s.io`, which `history` lists in namespace `default`).

---

## Deferred (explicitly out of scope — do not build)

- `SemverWindow` selection (D10), `HelmRelease` adapter (D12), rollback tooling (§3.7), release-report tool (§9 Phase 3), pin-mirroring to git (D2), webhook-based detection (D5), starvation staleness bound (D13). The seams for the first two exist (`selection.Strategy`, `adapter.Adapter`); the rest need operational evidence first.
- Provider-auth (`spec.provider: github|azure|aws` / `serviceAccountName` workload identity): v1 supports `secretRef` (and anonymous) auth only — matching DESIGN §7.1's secret-parity story. A managed source using provider auth gets a warning event and is treated like `UnsupportedRefStyle`. Record in README limitations.

## Verification (definition of done)

1. `make lint test` green; `go test ./... -race` green.
2. Engine/graph unit suites encode every DESIGN §3.3 state transition and D13 rule (the test-case list in Task 5 is the checklist).
3. envtest suite proves: SSA field co-ownership with a foreign `ref.name` owner; hold detect/release; initial pin; strict co-arrival ordering; Shadow writes nothing; suspend freezes writes; overlap and cycles surface as `GraphValid: False`.
4. `make test-e2e` on kind with Flux v2.9.4 passes all seven scenarios — these are DESIGN §9 Phase 1's exercises, mechanised.
5. Manual smoke (optional but recommended): `make deploy` to any cluster with Flux, apply the sample Wavefront in Shadow, watch `ShadowAdmission` events against a real repo.
