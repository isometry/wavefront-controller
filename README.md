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
[`DESIGN.md`](DESIGN.md) for the full design: topology and constraints
(§1–2), the admission state machine and pin-ownership rules (§3), the API
(§4), the decision log (§5), observability (§6), prerequisites (§8) and the
rollout plan (§9).

## Getting started

### Prerequisites

- go version v1.24.6+ (developed against Go 1.27)
- docker version 17.03+
- kubectl version v1.11.3+
- access to a Kubernetes v1.11.3+ cluster running Flux v2 (`source-controller`,
  `kustomize-controller`) — the e2e suite targets Flux v2.9.4

### Fleet prerequisites (DESIGN §8)

Before enabling the controller against real flotillas, the surrounding
catalog and RBAC must satisfy the following — the controller assumes these
hold and does not itself enforce them:

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

**Build and push the image, then deploy:**

```sh
make docker-build docker-push IMG=<some-registry>/wavefront-controller:tag
make install                                  # CRDs
make deploy IMG=<some-registry>/wavefront-controller:tag
```

**Or apply the pre-built installer bundle:**

```sh
kubectl apply -f https://raw.githubusercontent.com/isometry/wavefront-controller/<tag-or-branch>/dist/install.yaml
```

(`make build-installer IMG=<...>` regenerates `dist/install.yaml` locally.)

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
`dependsOn` (DESIGN §3.2). Multiple `Wavefront`s are permitted (e.g. per
context) but their node selectors must not overlap; overlap is reported as
an error condition on both.

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
  conditions:
    - type: Ready
    - type: GraphValid                    # False on dependsOn cycles or selector overlap
  observedGeneration: 4
```

`status.nodes` gives fleet-wide counts; `status.blocked` and `status.held`
are capped, attributed lists — per-node detail beyond the cap lives in
metrics and events, not status.

### Events

The controller emits `events.k8s.io/v1` events (not the legacy `core/v1`
API), attached to the `Wavefront` object:

| Reason | Type | When |
|---|---|---|
| `InitialPin` | Normal | First pin of a newly discovered/matched source |
| `PinAdvanced` | Normal | A subsequent pin advance |
| `ShadowAdmission` | Normal | Would-be admission while `mode: Shadow` (no write performed) |
| `HoldDetected` | Warning | `spec.ref.commit` is owned by a field manager other than the controller |
| `HoldReleased` | Normal | A previously-held source's foreign ownership was removed; controller resumes |
| `PinFailed` | Warning | A pin write attempt failed (e.g. apply/conflict error) |
| `UnsupportedRefStyle` | Warning | A source's `spec.ref` uses a selection style v1 can't sequence (e.g. `spec.ref.semver`); the source is demoted to a gate node |

### Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wavefront_admissions_total` | Counter | `result` | Admission attempts by outcome |
| `wavefront_node_pin_lag_seconds` | Gauge | `wavefront`, `kind`, `namespace`, `name` | Age of an unadmitted observed revision per node |
| `wavefront_admission_wait_seconds` | Histogram | — | Observed→admitted latency; the starvation signal for the settled-ancestors rule |
| `wavefront_blocked_nodes` | Gauge | `wavefront`, `reason` | Currently blocked nodes |
| `wavefront_ref_list_failures_total` | Counter | `host` | Ref-listing failures per git host |
| `wavefront_pinned_fetch_failures` | Gauge | `wavefront` | `source-controller` reporting `FetchFailed` on a pinned commit |

Every fleet gauge carries the owning Wavefront's name so that co-resident
Wavefronts cannot retire each other's series: a cross-fleet total is a PromQL
`sum()` (e.g. `sum(wavefront_pinned_fetch_failures)`).

## Shadow → Enforce rollout

Per DESIGN §9, roll out in phases rather than flipping the whole fleet at
once:

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
  evidence before they're worth building. See `DESIGN.md` §3.6, §3.7, §9,
  §10, D10, D13.

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
