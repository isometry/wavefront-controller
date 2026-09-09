# Wavefront Controller — Operations Runbook

This runbook covers the day-to-day and incident-response procedures for
operating the Wavefront Controller. It assumes familiarity with `DESIGN.md`;
section references below (`§x.y`) point back into that document.

Every procedure is given as a raw `kubectl` one-liner *and*, where one exists,
as its `wfctl` equivalent (`make install-wfctl`; also reachable as `kubectl
wavefront …`). The `kubectl` form is the ground truth and always works; the
`wfctl` form adds attribution, a confirmation gate and an audit event. See the
README's [`wfctl` section](../README.md#wfctl) for install and the three RBAC
tiers.

- [Modes and brakes](#modes-and-brakes)
- [Checking fleet status](#checking-fleet-status)
- [Status staleness](#status-staleness)
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
capped, attributed lists of exceptional states (see `DESIGN.md` §4.1).
`status.members` carries the full per-node picture — every evaluated node with
its state, pin, observed SHA and blocking attribution — bounded only by
`MembersCap` = 2000, with `status.membersOmitted` counting any excess.

`wfctl` reads exactly that published picture, so the whole of this section
needs nothing but read access to `wavefronts`:

```sh
wfctl status                # phase, counts, GraphValid, blocked/held/shadow, diagnostics
wfctl nodes                 # every node: state, held, blocking attribution, pin, observed, lag
wfctl nodes -o wide         # + wave, readiness, dependsOn
wfctl sources               # every managed GitRepository: pin, hold, owners, artifact
wfctl explain flotillas/team-x   # walk the blocked chain to the root cause and the fix
wfctl graph                 # dependency waves; --tree for a rooted tree
```

(`sources` and `source` enrich each row from the `GitRepository` itself when
that is readable — owners, conditions, provenance — and explicitly mark the
rows they could not read rather than inventing them, so they degrade rather
than fail on the viewer tier.)

`wfctl status` exits `2` when the fleet is `Blocked`, which makes it usable
directly as a CI or alerting probe. `wfctl snapshot -f fleet.json` captures
the whole picture for a ticket (no secrets, credentials, kubeconfig or URL
userinfo ever appear in it); anyone can replay it offline with
`wfctl nodes --from fleet.json`.

## Status staleness

`status.lastEvaluated` is when the controller last derived the picture. It is
advanced **at most once per `spec.poll.interval`**, so a timestamp up to one
poll interval old is normal, not a symptom — and remember [poll
tuning](#poll-tuning): the effective period is `interval + sweep duration`.

Beyond that, the status surface freezes exactly when the controller does: a
dead controller leaves the last-published `status.members` sitting there
looking authoritative. Two checks distinguish "quiet" from "dead":

- `status.conditions[type=Ready]` on the `Wavefront`, plus the controller
  liveness alarm ([safety alarms](#safety-alarms-6)).
- `wfctl status`, which warns when `lastEvaluated` is older than
  `2 × (poll.interval + 30s)` or `Ready` is `False`, and says to re-run with
  `--derive`.

`wfctl <cmd> --derive` bypasses the published status entirely and re-derives
the same picture live through the controller's own pipeline, so it answers
while the controller is down — and, while it is up, cross-checks it:
`wfctl status --derive` prints the reported and derived pictures side by side
and marks every disagreement. `--derive` needs cluster-wide `get` **and**
`list` on `Kustomization`s and `GitRepository`s — it lists the selected nodes
but reads individual objects too (closure gates pulled in through `dependsOn`
from outside the selector, and every source it resolves), and `list` does not
imply `get`. Add `--poll` for a real ref-advertisement sweep (which also reads
the sources' credential `Secret`s), without which observed SHAs render as an
explicit `?`.

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

Keep the `kubectl` query for the archived artefact — it is dependency-free and
reproducible. `wfctl sources -o wide` renders the same per-source facts (pin,
previous pin, observed ref, admitted-at, owners) for reading on the spot, and
`wfctl source ns/name` shows one source's provenance in full.

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

`wfctl` does both halves under a field manager of its own (`wfctl`), which the
controller honours as a hold like any other:

```sh
wfctl pin flotillas/team-y-repo --sha <sha> --poll   # verify the SHA against the remote first
wfctl pin flotillas/team-y-repo --sha <sha> --unverified
wfctl release flotillas/team-y-repo                  # hand the pin back, keeping its value
wfctl release flotillas/team-y-repo --float          # drop the pin instead
```

`--poll` checks the SHA against the remote's ref advertisement before writing
it; `--unverified` is the explicit way to pin something the remote does not
advertise. wfctl refuses to displace a *third* manager's `spec.ref.commit`
(neither the controller's nor its own) unless you pass `--force`, and every
write prints its plan and asks before applying — `--yes` skips the prompt,
`--dry-run` stops at the plan.

`release` is the part that has no hand equivalent worth typing: by default it
**keeps the pinned value** and transfers ownership, re-applying the same SHA
under `wavefront-controller` with provenance restored from the displaced-pin
annotation before relinquishing wfctl's claim — so the fleet carries on
running exactly the commit the hold pinned, and the controller resumes from
there. `--float` is the other choice: remove `spec.ref.commit` and let the
source float on its tracking ref until the controller initial-pins it from its
artifact (§3.5.4). Removing a hand-set value with `kubectl` is always the
`--float` behaviour.

## Break-glass: pin-strip (§8.5)

Restores plain floating-ref Flux behaviour fleet-wide by removing every
controller-managed `spec.ref.commit`:

```sh
kubectl get gitrepositories -A -l wavefront.as-code.io/managed=true -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' |
while read -r ns name; do
  kubectl -n "$ns" patch gitrepository "$name" --type=json -p='[{"op":"remove","path":"/spec/ref/commit"}]'
done
```

Or, with the brake applied in the same command:

```sh
wfctl pin-strip --suspend            # suspend the fleet first, then strip every managed pin
wfctl pin-strip --suspend --dry-run  # see exactly which sources would be stripped
```

`wfctl pin-strip` skips every source the controller already treats as held —
a hand-pin under a foreign field manager, or the source's own
`spec.suspend: true` (§3.5.3) — because somebody is holding those
deliberately; `--include-held` strips them too. The `kubectl` loop above
strips them regardless, since it cannot tell the difference. Per-source
errors are reported and the run continues rather than abandoning the fleet
half-stripped. The plan always warns that the controller re-pins everything on
its next sweep unless the fleet is suspended, and names the Wavefront's
current `suspend` value.

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
     or selector overlaps with another `Wavefront`. While `GraphValid` is
     `False`, this Wavefront's per-Wavefront gauges are suppressed (retired,
     not zeroed) so a fleet-wide `sum()` never double-counts against the
     Wavefront it overlaps with — see [Safety alarms](#safety-alarms-6).
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
   wfctl mode Enforce                       # same merge patch, with a plan and an audit event
   ```

   `wfctl mode` (like `wfctl suspend`/`resume`) uses a merge patch rather than
   server-side apply precisely so a GitOps applier keeps owning the
   `Wavefront` spec; if it sees `kustomize-controller` or `helm-controller`
   owning the field it warns that Flux will revert the change and that the
   edit belongs in git. A conflicting concurrent write is reported as such —
   re-run.

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
read them. Because the `Wavefront` is cluster-scoped, events regarding it land
in the `default` namespace:

```sh
kubectl -n default get events.events.k8s.io \
  --field-selector regarding.kind=Wavefront,regarding.name=fleet
wfctl history                                  # same events, oldest first, with the note rendered
wfctl history --warnings --since 1h
wfctl history --reason PinAdvanced --source flotillas/team-a-repo
wfctl history --node flotillas/team-a          # resolves the node to its source, then filters
```

`wfctl history` lists the controller's *and* wfctl's own audit events in one
stream, so a hand-pin and the admissions around it read in order. It needs
`get`/`list` on `events.events.k8s.io` — the viewer tier — and nothing more.

**Retention caveat, printed after every listing:** events are at-least-once
(§4.2) and are retained only for the API server's `--event-ttl` (1 hour by
default), so `history` is a convenience trail, not the record. The durable
ledger is the provenance annotations on each source (`wfctl sources -o wide`,
or the [release-set query](#the-release-set-42)) plus whatever log aggregation
retains.

Reasons emitted, all attached to the `Wavefront` object:

| Reason | Type | When |
|---|---|---|
| `InitialPin` | Normal | First pin of a newly discovered/matched source |
| `PinAdvanced` | Normal | A subsequent pin advance |
| `ShadowAdmission` | Normal | Would-be admission while `mode: Shadow` (no write performed) |
| `HoldDetected` | Warning | `spec.ref.commit` is owned by a field manager other than the controller |
| `HoldReleased` | Normal | A previously-held source's foreign ownership was removed; controller resumes |
| `PinFailed` | Warning | A pin write attempt failed (e.g. apply/conflict error) |
| `UnsupportedRefStyle` | Warning | A source's `spec.ref` uses a selection style v1's strategy can't sequence (e.g. `spec.ref.semver`) — the source is demoted to a gate node (no pinning attempted). This is a distinct code path from an auth failure; see [Known limitations](#known-limitations-unsupported-git-auth) for the auth case, which fires no event and is *not* gate-treated |

`wfctl`'s write commands record their own best-effort audit events on the same
object, distinguished by `reportingController: wfctl`, with a note naming the
kubeconfig user/context and the argv that asked for it — so `wfctl history`
reads as one stream of what the controller did and what a human did:

| Reason | Command |
|---|---|
| `Suspended` / `Resumed` | `wfctl suspend` / `wfctl resume` |
| `ModeChanged` | `wfctl mode Shadow\|Enforce` |
| `HandPinned` | `wfctl pin` |
| `PinReleased` | `wfctl release` (with or without `--float`) |
| `PinStripped` | `wfctl pin-strip` |
| `ForceAdmitted` | `wfctl force-admit` — the *only* record of that admission, since the controller finds pin == observed and emits no `PinAdvanced` of its own |

Recording an audit event needs `create` on `events.events.k8s.io` (the
operator tier); failing to record one is reported as a warning and never
fails the write, which has already happened by then.

## Safety alarms (§6)

Wire these before ramping past a pilot (DESIGN §9, Phase 2):

- **Controller liveness.** Standard `/healthz` probe / pod-restart alerting
  on the `controller-manager` Deployment. If the controller is down, pins
  freeze silently — everything else below assumes the controller is at
  least trying to run.
- **Pin staleness — `wavefront_node_pin_lag_seconds{wavefront,kind,namespace,name}` (gauge).**
  Age of an unadmitted observed revision per node, labelled with its owning
  Wavefront. Alert on this growing past a
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
- **Credential-read failure rate — `rate(wavefront_credential_read_failures_total[...])`
  (counter, no labels).** Counts failures reading a git-credential `Secret`
  during a ref-advertisement sweep — an apiserver-side problem (RBAC, a
  missing or malformed `Secret`), distinct from
  `wavefront_ref_list_failures_total` above (a git-host problem). Secret
  reads are memoized once per distinct `secretRef` per sweep (the memo dies
  with the sweep, so a rotated credential is still picked up on the very
  next sweep), and happen ahead of any one Wavefront's evaluation, so this
  metric carries no `wavefront` label — correlate the alert time with recent
  `Secret` changes rather than trying to attribute it to a Wavefront. Both
  failure kinds still stamp the affected source's `Observation.Err` and
  surface via `status`.
- **Pinned-commit fetch failures — `wavefront_pinned_fetch_failures{wavefront}` (gauge;
  `sum()` it for a cross-fleet total).** source-controller reporting `FetchFailed` on a pinned commit (typically a
  force-push rewriting the pinned SHA out of history, §10). Deployed state
  stays intact (source-controller keeps the last-good artifact); this is
  self-healing on the next poll once ancestors permit re-pinning, but should
  still page so you know which flotilla is affected and can watch for the
  one *unrecoverable* side effect: a rollback target rewritten away is
  genuinely lost.
- Also useful, not launch-blocking safety alarms per se but worth a
  dashboard: `wavefront_admissions_total{wavefront,result}`,
  `wavefront_admission_wait_seconds{wavefront}` (the starvation signal,
  D13), and `wavefront_blocked_nodes{wavefront,reason}`.
- **Deadman / gauge-absence.** Per-Wavefront gauges
  (`wavefront_node_pin_lag_seconds`, `wavefront_blocked_nodes`,
  `wavefront_pinned_fetch_failures`) are published only by a valid, resolved
  pass, and are deleted — not zeroed — on an aborted pass, on graph
  invalidation (selector overlap or a `dependsOn` cycle — this is also why a
  fleet-wide `sum()` never double-counts a Wavefront twice during an overlap
  window), and on the Wavefront's deletion. **Absent means "not currently
  measured", not zero** — never build a deadman alert on a standing zero
  series or on bare `absent()`; pair it with that Wavefront's `status.conditions[type=Ready]`
  instead (e.g. via `kube-state-metrics`'s CustomResourceState feature, or
  by alerting on `absent()` **and** a separate check that the Wavefront
  object itself still exists and reports `Ready`), so the alert distinguishes
  "genuinely nothing pending" from "this Wavefront isn't being measured
  right now". `wavefront_admissions_total` and `wavefront_admission_wait_seconds`
  are cumulative and are never affected by this — they persist across passes
  and are deleted only on Wavefront deletion.

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
