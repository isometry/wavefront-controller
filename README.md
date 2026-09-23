# wavefront-controller

## What and why

Flux's `dependsOn` gates the *apply* of manifests, but not *source artifact
propagation*: each flotilla's `HelmRelease`s resolve chart and values from
the flotilla's own `GitRepository` at a floating tracking ref, so a
repository update reaches `helm-controller` the moment `source-controller`
produces a new artifact — bypassing all sequencing built on `Kustomization`
dependencies and `Milestone`s. Across a fleet of team-owned flotillas this
means already-deployed workloads can pick up new artifacts out of dependency
order, both for one-off updates and for a large accumulated batch that
should roll out deterministically (the demo / air-gapped case).

The Wavefront Controller closes that gap by owning `spec.ref.commit` on
matched flotilla `GitRepository`s. The catalog renders each source with its
floating tracking ref (`spec.ref.name`) only; the controller adds and
advances a commit pin under its own SSA field manager. Flux's ref precedence
makes `commit` authoritative over `name`/`semver`/`tag`/`branch`, so sources
keep reconciling continuously and healthily, but only ever at their pinned
revision — **admission is pin advancement**. The sequencing order is never
declared to the controller: it is discovered by label-selecting the fleet's
`Kustomization`s and deriving a dependency DAG from their `spec.dependsOn`
edges, then running rolling, per-node admission over that graph — a pinned
node's pin advances to the latest observed SHA the instant every transitive
ancestor is *settled* (nothing pending and `Ready` at its own pin).

The result is stateless, restart-safe, and auditable: the controller holds
no git state (it only reads ref advertisements, `git ls-remote`-style, never
clones or checks out), admissibility is a pure function of live pins,
observed refs and readiness, and at quiescence the fleet's pin set — plus
its per-pin provenance annotations — *is* the release manifest. See
[`DESIGN.md`](DESIGN.md) for the full design: [topology and
constraints](DESIGN.md#2-context), the [admission state machine and
pin-ownership rules](DESIGN.md#3-design-overview), the
[API](DESIGN.md#4-api), the [decision log](DESIGN.md#5-decision-log),
[observability](DESIGN.md#6-observability-launch-requirements),
[prerequisites](DESIGN.md#8-prerequisites) and the [rollout
plan](DESIGN.md#9-rollout-plan).

## Getting started

### Prerequisites

- go version v1.24.6+ (developed against Go 1.27)
- docker version 17.03+
- kubectl version v1.11.3+
- access to a Kubernetes v1.11.3+ cluster running Flux v2 (`source-controller`,
  `kustomize-controller`) — the e2e suite targets Flux v2.9.4

### Fleet prerequisites

Before enabling the controller against real flotillas, the surrounding
catalog and RBAC must satisfy the following (mirrored from
[`DESIGN.md`'s prerequisites](DESIGN.md#8-prerequisites)) — the controller
assumes these hold and does not itself enforce them:

- [ ] **Catalog renders `spec.ref.name` only and omits `spec.ref.commit`** on
      every flotilla `GitRepository`, enforced by catalog CI. Required for
      clean SSA field ownership of the pin field. CI additionally rejects
      `spec.ref.semver` until a `SemverWindow` selection strategy exists
      (see [Limitations](#limitations)).
- [ ] **The participation label (`wavefront.as-code.io/managed: "true"`) is
      rendered onto every graph-member `Kustomization`** (flotilla and gate
      alike — the `Wavefront` selector's target) **and onto every flotilla
      `GitRepository`** (the managed-source marker that distinguishes
      flotilla repos from `GitRepository/catalog`). The controller only ever
      reads this label; it never writes it.
- [ ] **`wait: true` on every Milestone-owning `Kustomization`**, so Milestone
      health folds into that Kustomization's `Ready` condition — the one
      health signal the controller consumes per node.
- [ ] **Chart-source colocation policy:** flotilla `HelmRelease`s resolve
      charts from the flotilla `GitRepository` itself (chart + values
      colocated). A flotilla pulling charts from a shared `HelmRepository`
      with a semver range bypasses the gate entirely; forbid or pin such
      cases via catalog/flotilla CI or admission policy.
- [ ] **RBAC / secrets:** the controller needs read access to flotilla
      source secrets (for ref listing), read on
      `kustomizations.kustomize.toolkit.fluxcd.io`, patch on
      `gitrepositories.source.toolkit.fluxcd.io` (scoped, where policy
      tooling allows, to `spec.ref.commit` + `metadata.annotations`), and
      full ownership of `wavefronts.wavefront.as-code.io` status. `make
      deploy`/the installer manifest ship the ClusterRole for this.

### Deploy

**With Helm** (recommended):

```sh
helm install wavefront-controller \
  -n wavefront-controller-system --create-namespace \
  oci://ghcr.io/isometry/charts/wavefront-controller
```

See the [chart README](deploy/charts/wavefront-controller/README.md) for
configuration, upgrade and uninstall.

**Or build your own image and deploy via Kustomize:**

```sh
make ko-build IMG=<some-registry>/wavefront-controller:tag
make install                                  # CRDs
make deploy IMG=<some-registry>/wavefront-controller:tag
```

**Or apply the pre-built installer bundle:**

```sh
kubectl apply -f https://github.com/isometry/wavefront-controller/releases/download/<tag>/install.yaml
```

Each release publishes `install.yaml` as an asset, with that release's
versioned image baked in; `<tag>` is the git tag, e.g. `v0.3.0`.

`make build-installer IMG=<...>` regenerates `dist/install.yaml` locally. The
copy committed at `dist/install.yaml` references `controller:latest` and is for
development, not for applying to a cluster. Always pass `IMG=` to `make deploy`
and `make ko-build` — they default to `$(IMAGE_TAG_BASE):$(VERSION)`, a tag
derived from `git describe` that generally does not exist in the registry.

**Apply a `Wavefront`:**

```sh
kubectl apply -k config/samples/
```

The sample starts in `mode: Shadow` (detect and report, zero writes) — see
[Shadow → Enforce rollout](#shadow--enforce-rollout) below before flipping
it. To remove: `kubectl delete -k config/samples/`, `make uninstall`, `make
undeploy`.

## The `Wavefront` spec

A single, cluster-scoped CRD (`wavefronts.wavefront.as-code.io/v1alpha1`)
carries the controller's configuration and fleet-level status. It declares
scope and policy only — never topology, which is discovered from
`dependsOn` (see [how nodes, edges and roles are derived from the
graph](DESIGN.md#32-the-graph-nodes-edges-roles)). Multiple `Wavefront`s are permitted (e.g. per
context) but their node selectors must not overlap; overlap is reported as
an error condition on both, and their per-Wavefront gauges (see
[Metrics](#metrics)) are suppressed for as long as the overlap stands, so a
fleet-wide `sum()` never double-counts either Wavefront.

```yaml
apiVersion: wavefront.as-code.io/v1alpha1
kind: Wavefront
metadata:
  name: fleet
spec:
  nodes:
    kinds: [Kustomization]                     # only value accepted in v1 (see below)
    selector:
      matchLabels:
        wavefront.as-code.io/managed: "true"   # the participation label
  mode: Shadow                                 # Shadow | Enforce
  suspend: false
  poll:
    interval: 90s
    perHostConcurrency: 4
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `spec.nodes.kinds` | `[]string`, 1–1 items | *(required)* | Must be exactly `[Kustomization]` — CEL-validated (`self.all(k, k == 'Kustomization')`); `HelmRelease` is reserved for a future, non-breaking addition (v1 nodes: Kustomization only). |
| `spec.nodes.selector` | `metav1.LabelSelector` | *(required)* | Selects graph-member `Kustomization`s across all namespaces; also the boundary for cross-`Wavefront` overlap detection. |
| `spec.mode` | `Shadow` \| `Enforce` | `Shadow` | `Shadow`: full detection, graph derivation, admissibility evaluation, status/metrics, `ShadowAdmission` events — **zero writes**. `Enforce`: pins are actually advanced. |
| `spec.suspend` | `bool` | `false` | Freezes all pin *writes* fleet-wide; detection and status keep running. The gentle brake, orthogonal to `mode` and to Flux's own `spec.suspend`. |
| `spec.poll.interval` | `metav1.Duration` | `90s` | Interval between ref-advertisement polling sweeps. Each sweep is anchored to the *end* of the previous one, not a fixed clock tick, so the **effective poll period observed by the fleet is `interval + sweep duration`**, not `interval` alone — budget for that when setting a pin-staleness alarm threshold. |
| `spec.poll.perHostConcurrency` | `int`, min 1 | `4` | Bounds concurrent ref listings per git host. |

### Status

```yaml
status:
  phase: Advancing                        # Quiescent | Advancing | Blocked
  nodes: { observed: 287, pinned: 271, gates: 16, pending: 12, converging: 3, blocked: 0, held: 0 }
  blocked:                                # capped exceptional-state list (StatusListCap = 20); counts are authoritative
    - node: { kind: Kustomization, namespace: flotillas, name: team-x }
      since: "2026-08-27T09:14:03Z"
      reason: AncestorUnhealthy
      ancestor: { kind: Kustomization, namespace: waves, name: wave-2-gate }
  held:
    - node: { kind: Kustomization, namespace: flotillas, name: team-y }
      source: flotillas/team-y-repo
      manager: kubectl-edit
  members:                                # every evaluated node (MembersCap = 2000)
    - node: { kind: Kustomization, namespace: flotillas, name: team-x }
      role: Pinned                        # Pinned | Gate
      state: Pending                      # Settled | Pending | Admissible | Converging | Unhealthy
      dependsOn: [{ kind: Kustomization, namespace: waves, name: wave-2-gate }]
      source: flotillas/team-x-repo
      pin: "ab12…"
      observedSHA: "cd34…"                # "" = unobserved this pass
      pendingSince: "2026-08-27T09:14:03Z"
      ready: true
      blocked: { reason: AncestorUnhealthy, ancestor: { kind: Kustomization, namespace: waves, name: wave-2-gate } }
  lastEvaluated: "2026-08-27T09:15:11Z"   # advanced at most once per spec.poll.interval
  conditions:
    - type: Ready
    - type: GraphValid                    # False on dependsOn cycles or selector overlap
  observedGeneration: 4
```

`status.nodes` gives fleet-wide counts; `status.blocked` and `status.held`
are capped, attributed lists of exceptional states. `status.members` is the
full per-node picture — every evaluated node, flotilla and gate alike, with
its state, pin, observed SHA and blocking attribution — bounded only by
`MembersCap` = 2000, past which `status.membersOmitted` counts the rest. It
is what [`wfctl`](#wfctl) reads by default, and it is **write-only** output:
the reconciler never reads it back, so admission never depends on it — see
[why rolling admission is a pure function of live
inputs](DESIGN.md#d9--rolling-admission-not-cycle-coherent-admission-sets-reversed-from-v31-of-this-document).
`status.lastEvaluated` stamps when the picture was derived, and
advances at most once per `spec.poll.interval` so watch-triggered reconciles
do not rewrite status on every pass — budget for that when judging staleness.

### Events

The controller emits `events.k8s.io/v1` events (not the legacy `core/v1`
API), attached to the `Wavefront` object:

| Reason | Type | Action | When |
|---|---|---|---|
| `InitialPin` | Normal | `Pin` | First pin of a newly discovered/matched source |
| `PinAdvanced` | Normal | `Pin` | A subsequent pin advance |
| `ShadowAdmission` | Normal | `ShadowPin` | Would-be admission while `mode: Shadow` (no write performed) |
| `HoldDetected` | Warning | `Hold` | `spec.ref.commit` is owned by a field manager other than the controller |
| `HoldReleased` | Normal | `Release` | A previously-held source's foreign ownership was removed; controller resumes |
| `PinFailed` | Warning | `Pin` | A pin write attempt failed (e.g. apply/conflict error) |
| `UnsupportedRefStyle` | Warning | `Demote` | A source's `spec.ref` uses a selection style v1 can't sequence (e.g. `spec.ref.semver`); the source is demoted to a gate node |

Every per-source event also names that source's `GitRepository` as its
`related` object (and, on the `GitRepository`-regarding copy of a pin event,
the `Wavefront` as `related`) — required for the events.k8s.io/v1 recorder's
own dedup key (`{type, action, reason, reportingController,
reportingInstance, regarding, related}`, note excluded) to keep one source's
event distinct from another's when several admit in the same pass.

### Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wavefront_admissions_total` | Counter | `wavefront`, `result` | Admission attempts by outcome |
| `wavefront_node_pin_lag_seconds` | Gauge | `wavefront`, `kind`, `namespace`, `name` | Age of an unadmitted observed revision per node |
| `wavefront_admission_wait_seconds` | Histogram | `wavefront` | Observed→admitted latency; the starvation signal for the settled-ancestors rule |
| `wavefront_blocked_nodes` | Gauge | `wavefront`, `reason` | Currently blocked nodes |
| `wavefront_ref_list_failures_total` | Counter | `host` | Ref-listing failures per git host |
| `wavefront_credential_read_failures_total` | Counter | — | Git credential Secret-read failures during ref-advertisement sweeps (apiserver-side, distinct from ref-listing failures above) |
| `wavefront_pinned_fetch_failures` | Gauge | `wavefront` | `source-controller` reporting `FetchFailed` on a pinned commit |

Every per-Wavefront gauge (`wavefront_node_pin_lag_seconds`,
`wavefront_blocked_nodes`, `wavefront_pinned_fetch_failures`) is published
only by a valid, resolved pass for that Wavefront, and is retired — deleted,
not zeroed — on an aborted pass, when the Wavefront's graph is invalidated
(selector overlap or a `dependsOn` cycle), and when the Wavefront itself is
deleted. That means a cross-fleet `sum()` (e.g.
`sum(wavefront_pinned_fetch_failures)`) never double-counts a Wavefront's
series during an overlap window, and an absent series means "not currently
measured", not zero — don't alert on `absent()` alone; pair it with the
Wavefront's `Ready` condition (see the
[runbook's safety alarms](docs/runbook.md#safety-alarms)).
`wavefront_admissions_total` and `wavefront_admission_wait_seconds` are
cumulative and attributed per Wavefront the same way, but are deleted only
when their Wavefront is deleted — a pass never retires them.

## Shadow → Enforce rollout

Per [`DESIGN.md`'s rollout plan](DESIGN.md#9-rollout-plan), roll out in
phases rather than flipping the whole fleet at once:

1. **Phase 0 — Shadow.** Deploy with `mode: Shadow` against the intended
   selector; detection, graph derivation, and `ShadowAdmission` events run
   with zero writes. Validates credential reuse, poll load, participation
   labelling, and graph shape, and produces real admission-wait data against
   the starvation caveat below.
2. **Phase 1 — Pilot.** Label a small subset spanning at least one
   dependency chain (include one infrastructure flotilla) and flip
   `mode: Enforce`. Enabling management of an existing source starts with
   an initial pin to its current revision — a no-op write, making
   per-flotilla enablement incremental and risk-free. Exercise blocked-subtree
   recovery, controller-kill mid-rollout (stateless resume), hand-pin/suspend
   coexistence, and break-glass.
3. **Phase 2 — Fleet enablement.** Ramp by dependency depth (infrastructure
   first); wire the safety alarms below before ramping past the pilot.
4. **Phase 3 — Air-gapped.** Same controller, same loop; the static mirror
   during a rollout is what makes rolling admission reproduce a deterministic
   release set, with the fleet's pin set + provenance as the derived
   audit artefact.

Full step-by-step flip procedure, safety-alarm wiring, hand-pin etiquette,
and the break-glass pin-strip: see **[`docs/runbook.md`](docs/runbook.md)**.

## `wfctl`

`wfctl` reports what a `Wavefront` is doing and why, and operates it when it
is stuck — the CLI half of [`DESIGN.md`'s observability launch
requirements](DESIGN.md#6-observability-launch-requirements). It is a
separate binary; it is deliberately **not** shipped in the manager image,
whose ServiceAccount is precisely the RBAC an exec into that pod should not
reach.

### Install

**With Homebrew:**

```sh
brew trust isometry/tap && brew install isometry/tap/wfctl
```

The poured bottle installs `wfctl` and its shell completions, but **not** the
`kubectl-wavefront` plugin link — only `brew install --build-from-source`
creates that. Add it yourself if you want the kubectl plugin form:

```sh
ln -s "$(brew --prefix)/bin/wfctl" "$(brew --prefix)/bin/kubectl-wavefront"
```

**From a GitHub Release archive:**

Download `wfctl_<version>_<os>_<arch>.tar.gz` (or `.zip` on Windows) from the
[releases page](https://github.com/isometry/wavefront-controller/releases),
extract it, and symlink `kubectl-wavefront` yourself if you want the kubectl
plugin form. See [`docs/verification.md`](docs/verification.md) to verify the
archive before running it.

**With `go install`:**

```sh
go install github.com/isometry/wavefront-controller/cmd/wfctl@<tag>
```

**From a checkout of this repo:**

```sh
make build-wfctl                          # bin/wfctl only
make install-wfctl                        # into GOBIN, + kubectl-wavefront symlink
```

`install-wfctl` installs into `GOBIN` (`go env GOBIN`, falling back to
`$(go env GOPATH)/bin`) and symlinks `kubectl-wavefront` beside it, so every
command is equally reachable as a kubectl plugin — the binary renames its own
usage line when invoked that way:

```sh
wfctl status
kubectl wavefront status
```

`Wavefront`s are cluster-scoped and every node and source is named
`namespace/name`, so `-n`/`--namespace` is accepted for kubectl's sake but has
no effect on what wfctl reports. `--wavefront` is only required when the
cluster holds more than one. Exit codes: `0` success, `1` error, `2`
`wfctl status` found the fleet `Blocked`.

### Where the picture comes from

| Mode | Reads | Use it when |
|---|---|---|
| *(default)* | `status.members` — the per-node picture the controller published | Always, unless the controller is down or suspect |
| `--derive` | Re-derives live through the controller's own pipeline (`internal/inputs` → `internal/engine`) | The controller is down, or you want the reported and derived pictures compared (`wfctl status --derive` prints them side by side and marks disagreement) |
| `--derive --poll` | …plus a ref-advertisement sweep, so observed SHAs are real | You need observed SHAs and pin lag, not `?` |
| `--from FILE` | Replays a snapshot captured by `wfctl snapshot` — no cluster at all | Triage from a ticket attachment |

`--poll` requires `--derive` on the read commands (on `pin` it instead
verifies the given `--sha` against the remote's advertisement; `force-admit`
always lists its own source's refs). Without an observation sweep, observed
SHAs render as an explicit `?` rather than as blank, with a footnote naming
the fix. Poll failures per source become diagnostics, never a fatal error.

**Staleness.** `status.lastEvaluated` is advanced at most once per
`spec.poll.interval`, so a fresh-looking fleet is normal; wfctl warns when it
is older than `2 × (poll.interval + 30s)`, or when the Wavefront's `Ready`
condition is `False` — status may be stale, the controller may be down, use
`--derive`.

**What the default provider sees.** `status.members` carries every evaluated
node — flotilla and gate alike, including the closure gates reached through
`dependsOn` — rather than being a capped exceptional-state list like
`status.blocked`/`status.held`; its only bound is `MembersCap` = 2000, past
which `status.membersOmitted` counts the rest and wfctl says so. It is
*write-only* output: the controller never reads it back, so a hand-edited or
truncated list cannot change what the controller does — admissibility is
always re-derived from live inputs — it can only mislead a reader, which is
what `--derive` is for.

### Access tiers

Each tier is a superset of the last. Only the operator tier writes anything.

| Tier | Grants | Unlocks |
|---|---|---|
| **viewer** | `get`/`list` on `wavefronts.wavefront.as-code.io`; `get`/`list` on `events.events.k8s.io` (namespace `default`, where events regarding a cluster-scoped `Wavefront` land) | Every read command on the default status-backed provider, plus `history` |
| **derive** | + `get`, `list` on `kustomizations.kustomize.toolkit.fluxcd.io` and `gitrepositories.source.toolkit.fluxcd.io` **cluster-wide** — deriving lists the selected nodes but reads individual objects too (closure gates outside the selector, and every source it resolves), and `list` does not imply `get`; for `--poll`, `get` on the sources' credential `secrets` | `--derive`, `--derive --poll` |
| **operator** | + `patch` on `gitrepositories.source.toolkit.fluxcd.io` and `wavefronts.wavefront.as-code.io`; `create` on `events.events.k8s.io` | `suspend`, `resume`, `mode`, `pin`, `release`, `pin-strip`, `force-admit` and their audit events |

[`config/rbac/wfctl_viewer_role.yaml`](config/rbac/wfctl_viewer_role.yaml) is
a ready-made `ClusterRole` (`wfctl-viewer-role`) for the viewer tier — read on
`wavefronts` plus the events `wfctl history` lists — and ships in the
installer bundle. If you only need the CR reads and not the event history, the
kubebuilder-scaffolded
[`config/rbac/wavefront_viewer_role.yaml`](config/rbac/wavefront_viewer_role.yaml)
(`wavefront-viewer-role`) covers that half on its own. Bind either with a
`ClusterRoleBinding` — note that `dist/install.yaml` applies the project's name
prefix, so the installed roles are `wavefront-controller-wfctl-viewer-role` and
`wavefront-controller-wavefront-viewer-role`. The derive and operator tiers are
not scaffolded: their extra verbs are on Flux's own resources and belong to
whatever policy regime already governs those.

Installing via the [Helm chart](deploy/charts/wavefront-controller) instead
gains the chart's `<fullname>` as a prefix: `rbac.userRoles.enabled` (default
`true`) renders `<fullname>-wavefront-{admin,editor,viewer}-role` and
`<fullname>-wfctl-viewer-role`. `<fullname>` is the release name when the
release is named `wavefront-controller` (the same names as above) — for any
other release name it's `<release>-wavefront-controller-...` (release name
prefixed onto the chart name). See the [chart
README](deploy/charts/wavefront-controller/README.md#user-facing-roles) for
details.

### Commands

Read commands honour `--derive`, `--poll` and `--from` and take
`-o table|wide|json|yaml`; `graph` additionally accepts `-o dot|mermaid`.

| Command | Shows |
|---|---|
| `status` | Wavefront name, mode, suspend, generation, `lastEvaluated`; phase, node counts, `GraphValid`; the blocked/held/shadow lists and diagnostics. Exits `2` when the fleet is `Blocked`. |
| `nodes` | Every evaluated node with role, state, held flag, blocking attribution, source, pin, observed SHA and lag; `-o wide` adds wave, readiness and `dependsOn`. |
| `sources` | Every managed `GitRepository` with pin, observed SHA, pending flag, hold, field-manager owners, artifact, admitted-at and referencing nodes; `-o wide` adds previous pin, observed ref, fetch health and URL. |
| `source ns/name` | One source in full: pin, provenance annotations, owners, conditions, referencing nodes. |
| `explain ns/name` | Walks a node's blocked chain to its root cause and names the fix, per [the rolling-admission state machine](DESIGN.md#33-rolling-admission). Accepts `Kind/ns/name`; `Kustomization` is the default kind. |
| `graph` | Dependency graph as waves (default), or `--tree` for a rooted tree; anything in or behind a cycle is listed unlayered. |
| `snapshot` | Writes the whole snapshot as JSON (or `-o yaml`) to stdout or `-f FILE`, for replay with `--from`. Never contains secret data, credentials, kubeconfig or URL userinfo — it is meant to be attached to a ticket. |
| `history` | The controller's and wfctl's own events for this Wavefront, filterable with `--source`, `--node`, `--reason`, `--since`, `--warnings`. Events are at-least-once and retained only for the API server's `--event-ttl` (1h by default); the durable ledger is the provenance annotations (`wfctl sources -o wide`) and log aggregation. |

Write commands read, build a plan, print its effect (objects, fields, field
managers, before/after, warnings), then confirm — unless `--yes`, and they
refuse to prompt when stdin is not a TTY. `--dry-run` prints the plan and
stops. Each records a best-effort audit event on the `Wavefront`; a failure to
record one is a warning, never a failed command.

| Command | Effect |
|---|---|
| `suspend` / `resume` | `spec.suspend` on the `Wavefront`, by merge patch so a GitOps applier keeps owning the spec — wfctl warns when it sees one, because Flux will revert the change. |
| `mode Shadow\|Enforce` | `spec.mode`, same merge-patch rule. |
| `pin ns/name --sha SHA` | Hand-pins one source under the `wfctl` field manager, which the controller reads as an external hold (`HoldDetected`, descendants `AncestorHeld`). The SHA must be checked (`--poll`) or explicitly not (`--unverified`); `--force` displaces a third-party owner of `spec.ref.commit`. |
| `release ns/name` | Ends a hold on one source, **keeping the pinned value**: the controller re-applies the same SHA under its own field manager with provenance restored, then the holder's claim is relinquished. `--float` removes the pin instead, so the source floats until the controller initial-pins it — see [initial pin on discovery](DESIGN.md#35-pin-ownership-provenance-and-coexistence). |
| `pin-strip` | Break-glass: removes `spec.ref.commit` from every managed source. Sources the controller already treats as held — a hand-pin, or the source's own `spec.suspend` — are skipped unless `--include-held`; `--suspend` suspends the fleet first, in the same command, so the controller does not simply re-pin. |
| `force-admit ns/name` | Admits one source past its gate, once: lists that one source's refs, takes the tracking ref's SHA (or `--sha`, verified unless `--unverified`), and writes it under the controller's own field manager with provenance. Refused when the source is held or suspended. |

Every command's `--help` is the authoritative reference; `docs/runbook.md`
gives the operational procedures each one belongs to.

## Supply chain

Tagged releases publish a keyless-signed (Sigstore) container image, OCI
Helm chart, `wfctl` GitHub Release archives and Homebrew bottles, each with
SLSA build provenance; the image also carries an SBOM attestation.
[`docs/verification.md`](docs/verification.md) documents how to verify all
of them with `gh attestation verify` and `cosign`, plus the artifact layout
and mirroring guidance. (Everything is signed with cosign, not Helm's own
PGP-based `--verify`/`.prov` mechanism, which this pipeline does not use.)

## Limitations

- **Ref-listing auth is limited to `secretRef` basic-auth, bearer token, SSH,
  and anonymous access.** TLS client material (`caFile`/custom CA bundles,
  mTLS client certificates) and provider-specific auth (GitHub App, Azure
  DevOps, AWS CodeCommit IAM, etc.) are both **unsupported** and fail ref
  listing exactly like any other auth failure: `wavefront_ref_list_failures_total`
  rises for that host, no event fires, and the source stays a real node with
  no successful observation. Per the fail-closed rule, an unobserved node is
  treated as unsettled, so it **conservatively blocks its own descendants** —
  it is *not* demoted to a gate. This can look like "no changes" if you only
  watch events; watch the metric.
- **`spec.ref.semver` sources are not sequenced.** v1 ships only the
  `TrackRef` candidate-selection strategy (advertised SHA of `spec.ref.name`);
  a source rendered with `spec.ref.semver` fires a `warning` `UnsupportedRefStyle`
  event and is demoted to a gate node (health-only, never pinned). Catalog CI
  should reject `spec.ref.semver` until a `SemverWindow` strategy exists.
- **v1 graph nodes are `Kustomization` only.** `HelmRelease` nodes are a
  reserved, non-breaking future addition (`spec.nodes.kinds` is CEL-validated
  to accept exactly `[Kustomization]` today).
- **`SemverWindow` selection, `HelmRelease` adapter, rollback tooling,
  pin-mirroring to git, webhook-based detection, and a starvation staleness
  bound** are all explicitly deferred — the extension seams
  (`selection.Strategy`, `adapter.Adapter`) exist, but these need operational
  evidence before they're worth building. See the design doc's notes on
  [pluggable candidate-selection strategies](DESIGN.md#36-candidate-selection-strategies-extension-point),
  [rollback as a recorded but out-of-scope capability](DESIGN.md#37-rollback-recorded-capability-out-of-scope-for-v1),
  and the [rollout plan's open questions](DESIGN.md#9-rollout-plan).

See also [`docs/runbook.md`](docs/runbook.md#known-limitations-unsupported-git-auth)
for the operational read on the auth limitation above.

## Contributing

Run `make help` for all available `make` targets. More on the underlying
scaffolding: the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html).

## License

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
