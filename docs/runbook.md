# Wavefront Controller — Operations Runbook

This runbook covers the day-to-day and incident-response procedures for
operating the Wavefront Controller. It assumes familiarity with `DESIGN.md`;
section references below (`§x.y`) point back into that document.

- [Modes and brakes](#modes-and-brakes)
- [Checking fleet status](#checking-fleet-status)
- [The release set (§4.2)](#the-release-set-42)
- [Hand-pin etiquette](#hand-pin-etiquette)
- [Break-glass: pin-strip (§8.5)](#break-glass-pin-strip-85)
- [Shadow → Enforce flip procedure](#shadow--enforce-flip-procedure)
- [Deleting a `Wavefront`](#deleting-a-wavefront)
- [Poll tuning](#poll-tuning)
- [Events](#events)
- [Safety alarms (§6)](#safety-alarms-6)
- [Known limitations: unsupported git auth](#known-limitations-unsupported-git-auth)

## Modes and brakes

The controller exposes three levers, from gentlest to most drastic:

| Lever | Effect | Touches Flux resources? |
|---|---|---|
| `spec.mode: Shadow` | Full detection, graph derivation, admissibility evaluation, status and metrics, `ShadowAdmission` events — **zero writes**. | No |
| `spec.suspend: true` | Freezes all pin *writes* fleet-wide; detection and status keep running. The gentle brake — flip back to `false` to resume. | No |
| Break-glass pin-strip (below) | Removes every controller-managed `spec.ref.commit`, restoring plain floating-ref Flux behaviour. | **Yes** — mutates `GitRepository` objects |

Prefer `suspend: true` for "pause and think" situations. Reach for the
break-glass procedure only when you need the fleet to stop tracking pins
entirely (e.g. a suspected controller bug pinning bad SHAs, or evacuating the
controller from the cluster).

## Checking fleet status

```sh
kubectl get wavefronts
kubectl get wavefront fleet -o yaml
```

`status.phase` is `Quiescent` (nothing pending), `Advancing` (admissions in
flight), or `Blocked` (something is stuck; see `status.blocked`).
`status.nodes` gives fleet-wide counts; `status.blocked` and `status.held` are
capped, attributed lists (see `DESIGN.md` §4.1) — per-node detail beyond the
cap lives in metrics and events, not status.

## The release set (§4.2)

There is no per-release CR — the release set is *derived* from the pin state
of managed `GitRepository` sources at quiescence. One query renders it, with
per-pin provenance attached:

```sh
kubectl get gitrepositories -A -l wavefront.as-code.io/managed=true -o jsonpath='{range .items[*]}{.metadata.namespace}{"/"}{.metadata.name}{"\t"}{.spec.ref.commit}{"\t"}{.metadata.annotations.wavefront\.as-code\.io/admitted-at}{"\t"}{.metadata.annotations.wavefront\.as-code\.io/observed-ref}{"\n"}{end}'
```

Run it only when `status.phase: Quiescent` — mid-rollout the pin set is a
snapshot of an in-progress admission wave, not a coherent release. Columns:
namespace/name, the pinned commit, when the controller admitted it, and the
tracking ref it was observed on (`wavefront.as-code.io/previous-pin` is also
available via the same annotation prefix if you need the prior SHA). This is
the audit artefact for air-gapped/Phase 3 environments; archive it externally
if you need retention beyond the events window (a report tool that does this
is a Phase 3 item, not yet shipped).

## Hand-pin etiquette

The controller writes `spec.ref.commit` under its own SSA field manager
(`wavefront-controller`) and never force-owns a field held by anyone else.
If you (or another tool) set `spec.ref.commit` by hand during an incident —
`kubectl edit`, `kubectl patch`, a different field manager — the controller
detects that the field is foreign-owned and treats the source as an
**external hold**: it stops advancing that pin, fires a `HoldDetected`
event, and lists the node under `status.held` with the manager's name.

To resume controller management, simply remove your hand-set value (or
re-apply with the `wavefront-controller` field manager, which you should not
normally do by hand). On the next poll the controller sees the field is
unowned again, fires `HoldReleased`, and resumes normal admission — no
special "release" command needed. There is no need to touch `spec.suspend`
just to hand-pin one source; the hold is scoped to that `GitRepository`
alone, and its descendants will correctly show as blocked-behind-an-unsettled-
ancestor for as long as the hold stands (§3.3/§3.5).

## Break-glass: pin-strip (§8.5)

Restores plain floating-ref Flux behaviour fleet-wide by removing every
controller-managed `spec.ref.commit`:

```sh
kubectl get gitrepositories -A -l wavefront.as-code.io/managed=true -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' |
while read -r ns name; do
  kubectl -n "$ns" patch gitrepository "$name" --type=json -p='[{"op":"remove","path":"/spec/ref/commit"}]'
done
```

Effects:

- Every managed `GitRepository` immediately reverts to tracking `spec.ref.name`
  (or whatever other ref subfield is set) — the pre-controller behaviour.
- The `remove` op is a no-op (and errors harmlessly, safe to ignore/retry) on
  any source that has no `spec.ref.commit` set, e.g. one still on its initial
  unpinned state or already stripped.
- This does **not** stop the controller from re-pinning on its next
  reconcile unless you also suspend it (`spec.suspend: true`) or scale the
  deployment to zero. Strip-and-leave-running will simply re-pin everything
  back on the next poll (each such source looks exactly like "initial pin on
  discovery", §3.5.4 — a no-op-effect re-pin to the currently observed SHA).
  For a durable break-glass, pair the strip with `spec.suspend: true` (or
  stop the controller) *before* stripping, so pins stay stripped until you
  choose to resume.
- Use this when you need the fleet to genuinely stop tracking any pins (e.g.
  suspected controller bug pinning wrong SHAs, per `DESIGN.md` §10) — for
  merely pausing new admissions while keeping current pins in place, use
  `spec.suspend: true` instead, which touches no Flux resource at all.

## Shadow → Enforce flip procedure

1. Deploy with `mode: Shadow` (the sample default) against the intended
   `spec.nodes.selector`. Let it run through several poll cycles.
2. Validate before flipping:
   - `status.conditions[type=GraphValid]` is `True` — no `dependsOn` cycles
     or selector overlaps with another `Wavefront`.
   - `ShadowAdmission` events look sane for a representative sample of
     flotillas (correct candidate SHAs, expected sequencing given
     `dependsOn`).
   - `wavefront_admission_wait_seconds` distribution looks reasonable (no
     surprise starvation behind a hot, never-quiescing upstream — the D13
     caveat).
   - `wavefront_ref_list_failures_total` is flat/zero per git host you care
     about (a private-CA host, or one needing provider-specific auth, will
     show failures here even though source-controller clones it fine — see
     [Known limitations](#known-limitations-unsupported-git-auth)).
3. Flip with a single field edit:

   ```sh
   kubectl patch wavefront fleet --type=merge -p '{"spec":{"mode":"Enforce"}}'
   ```

   This is a visible, auditable, one-field change (§4.1) — no other spec
   field needs to move. On the first reconcile after the flip, freshly
   discovered/unpinned sources get an **initial pin to their currently
   observed SHA** — a no-op-effect write, not a deployment change (§3.5.4,
   §9 migration note) — so flipping is risk-free even against
   already-running flotillas.
4. Watch `PinAdvanced`/`InitialPin` events and `status.phase` return to
   `Quiescent`. Roll back by flipping `mode` back to `Shadow` (writes stop;
   already-written pins are untouched) or by the break-glass procedure above
   if you need pins actively reverted.

Per DESIGN §9, prefer ramping by dependency depth (infrastructure flotillas
first) rather than flipping the whole fleet's selector at once — label a
small pilot subset initially and widen the selector/participation labelling
over time.

## Deleting a `Wavefront`

Deleting a `Wavefront` resource has **no finalizer**: it simply stops that
object's management. Concretely:

- The controller stops reconciling against that selector — no more polling,
  no more admissions, no more status/metrics for those nodes.
- **Pins already written are left exactly as they are.** `spec.ref.commit`
  values and the controller's provenance annotations on managed
  `GitRepository`s are *not* reverted or cleaned up. The fleet keeps running
  on its last-admitted pins, forever, until something else touches them.
- This is functionally equivalent to the break-glass pin-strip's "controller
  goes away" half, minus the un-pinning — if you actually want floating refs
  back, run the pin-strip one-liner separately (before or after deleting the
  `Wavefront`; it operates on `GitRepository` labels, not on the `Wavefront`
  object).
- If you intend to fully decommission Wavefront management of a set of
  sources, do the pin-strip *first*, then delete the `Wavefront` (or narrow
  its selector) — otherwise you're left with permanently stale pins with no
  controller to advance them.

## Poll tuning

`spec.poll.interval` is not the true cadence: each sweep is anchored to the
**end** of the previous sweep, not to a fixed clock tick (deliberately — it
avoids overlapping/piling-up sweeps if listing ever gets slow). So the
effective poll period observed by the fleet is `poll.interval + sweep
duration`, not `poll.interval` alone. Budget for that when reasoning about
detection latency or setting a pin-staleness alarm threshold.

## Events

The controller emits `events.k8s.io/v1` events (not the legacy `core/v1`
events API). `kubectl get events` still shows them, but `kubectl events` (or
`kubectl get events.events.k8s.io`) is the preferred, more informative way to
read them. Reasons emitted, all attached to the `Wavefront` object:

| Reason | Type | When |
|---|---|---|
| `InitialPin` | Normal | First pin of a newly discovered/matched source |
| `PinAdvanced` | Normal | A subsequent pin advance |
| `ShadowAdmission` | Normal | Would-be admission while `mode: Shadow` (no write performed) |
| `HoldDetected` | Warning | `spec.ref.commit` is owned by a field manager other than the controller |
| `HoldReleased` | Normal | A previously-held source's foreign ownership was removed; controller resumes |
| `PinFailed` | Warning | A pin write attempt failed (e.g. apply/conflict error) |
| `UnsupportedRefStyle` | Warning | A source's `spec.ref` uses a selection style v1's strategy can't sequence (e.g. `spec.ref.semver`) — the source is demoted to a gate node (no pinning attempted). This is a distinct code path from an auth failure; see [Known limitations](#known-limitations-unsupported-git-auth) for the auth case, which fires no event and is *not* gate-treated |

## Safety alarms (§6)

Wire these before ramping past a pilot (DESIGN §9, Phase 2):

- **Controller liveness.** Standard `/healthz` probe / pod-restart alerting
  on the `controller-manager` Deployment. If the controller is down, pins
  freeze silently — everything else below assumes the controller is at
  least trying to run.
- **Pin staleness — `wavefront_node_pin_lag_seconds` (gauge).** Age of an
  unadmitted observed revision per node. Alert on this growing past a
  threshold, and treat it as more urgent if it's growing *while liveness is
  also failing* (frozen pins + dead controller is the worst case in §10).
  Remember the [poll tuning](#poll-tuning) note above when picking a
  threshold — some lag is structural, not a symptom.
- **Ref-listing failure rate — `rate(wavefront_ref_list_failures_total[...])` per host.**
  A sustained non-zero rate for one git host means detection has stopped for
  every source on that host (no admissions, but deployed state is
  unaffected) — distinct from "no changes observed". Check first whether
  the host is one of the [known unsupported-auth
  cases](#known-limitations-unsupported-git-auth) (TLS-only, or a
  provider-auth scheme such as github/azure/aws apps) before assuming an
  outage.
- **Pinned-commit fetch failures — `wavefront_pinned_fetch_failures` (gauge).**
  source-controller reporting `FetchFailed` on a pinned commit (typically a
  force-push rewriting the pinned SHA out of history, §10). Deployed state
  stays intact (source-controller keeps the last-good artifact); this is
  self-healing on the next poll once ancestors permit re-pinning, but should
  still page so you know which flotilla is affected and can watch for the
  one *unrecoverable* side effect: a rollback target rewritten away is
  genuinely lost.
- Also useful, not launch-blocking safety alarms per se but worth a
  dashboard: `wavefront_admissions_total{result}`,
  `wavefront_admission_wait_seconds` (the starvation signal, D13), and
  `wavefront_blocked_nodes{reason}`.

## Known limitations: unsupported git auth

Ref-listing authentication (`internal/gitpoll`) supports `secretRef`
basic-auth, bearer tokens, SSH, and anonymous access only. Two host-auth
shapes fall outside that and fail ref listing like any other auth failure —
`wavefront_ref_list_failures_total` rises for that host, and the source is
treated **conservatively, not permissively**: it is *not* pinned, but it is
also *not* demoted to a gate — it stays a real node with no successful
observation, and (per the fail-closed rule, DESIGN D4) an unobserved node is
treated as unsettled, so **it can block its own descendants** exactly like
any other stuck node would. This is a different — and more consequential —
failure mode than `UnsupportedRefStyle` (which really does gate the node,
see the [events table](#events)): no event fires, and an operator checking
only for warning events would miss it. Watch the metric, not just events, for
either case below.

- **TLS client material.** No support for `caFile`/custom CA bundles or
  mTLS client certificates. A git host behind a private CA will show
  sustained `wavefront_ref_list_failures_total` for that host **even though
  source-controller can clone it fine** (source-controller's TLS handling
  is separate and unaffected). If failures are isolated to one host, check
  whether its `GitRepository`/`Secret` relies on `caFile` or client-cert
  fields before escalating as a wider outage.
- **Provider-specific auth** (GitHub App, Azure DevOps, AWS CodeCommit IAM,
  etc.). v1's `secretRef` handling doesn't speak any of these. A source
  relying on one will likewise show sustained
  `wavefront_ref_list_failures_total` for that host with no corresponding
  `UnsupportedRefStyle` event — it lands in the same generic-listing-failure,
  conservative-blocking bucket as the TLS case above, not the gate-node
  treatment `UnsupportedRefStyle` implies.
