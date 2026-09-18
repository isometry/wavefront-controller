# wavefront-controller Helm chart

Deploys [wavefront-controller](https://github.com/isometry/wavefront-controller)
— a controller that gates *source artifact propagation* across a fleet of Flux
flotillas by owning `spec.ref.commit` on the `GitRepository`s it matches. The
sequencing order is never declared: it is derived from the fleet's
`Kustomization` `spec.dependsOn` edges, and a pinned node's commit pin advances
to the latest observed SHA only once every transitive ancestor is settled.
Admission is pin advancement.

## Installing the Chart

To install with the release name `wavefront-controller`:

```sh
helm install wavefront-controller \
  -n wavefront-controller-system --create-namespace \
  oci://ghcr.io/isometry/charts/wavefront-controller
```

## Verifying the chart

Releases are keyless-signed (Sigstore) and carry SLSA build provenance.
Verify before installing:

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/isometry/wavefront-controller/\.github/workflows/publish\.yaml@refs/tags/v.+$' \
  ghcr.io/isometry/charts/wavefront-controller:<version>

gh attestation verify \
  oci://ghcr.io/isometry/charts/wavefront-controller:<version> \
  --repo isometry/wavefront-controller
```

Helm's own `--verify` (PGP `.prov` provenance) is not used by this chart —
it is signed with cosign instead, so `helm pull --verify` does not work
here; use the commands above.

See [`docs/verification.md`](../../../docs/verification.md) for the full
verification commands and trust anchor details.

## Uninstalling the Chart

```sh
helm uninstall wavefront-controller -n wavefront-controller-system
```

By default the CRD is annotated `helm.sh/resource-policy: keep`, so a
`helm uninstall` leaves `wavefronts.wavefront.as-code.io` — and any `Wavefront`
resources — in place. Set `crds.keep=false` to let Helm remove it.

A kept CRD stays *owned* by the release that installed it. Reinstalling the
chart under a different release name or namespace therefore fails with Helm's
ownership-metadata error ("invalid ownership metadata"), because the surviving
CRD still carries the old `meta.helm.sh/release-name` and
`meta.helm.sh/release-namespace` annotations. Either update those two
annotations on the CRD to match the new release, or manage the CRD out of band
and install with `crds.install=false`.

## Configuration

| Parameter | Description | Default |
| --- | --- | --- |
| `commonLabels` | Labels added to every rendered resource | `{}` |
| `commonAnnotations` | Annotations added to every rendered resource (values are `tpl`-rendered) | `{}` |
| `nameOverride` | Override the chart name used in names and labels | `~` |
| `fullnameOverride` | Override the fully qualified resource name | `~` |
| `namespace` | Namespace for namespaced resources | release namespace |
| `crds.install` | Install the `Wavefront` CRD | `true` |
| `crds.keep` | Annotate the CRD with `helm.sh/resource-policy: keep` | `true` |
| `leaderElection.enabled` | Run a single active manager via a Lease (`--leader-elect`) | `true` |
| `rbac.create` | Create the ServiceAccount, Roles and Bindings | `true` |
| `rbac.serviceAccount.name` | Override the ServiceAccount name | chart fullname |
| `rbac.serviceAccount.annotations` | Annotations on the ServiceAccount (e.g. IRSA) | `{}` |
| `rbac.extraClusterRoleBindings` | Bind pre-existing ClusterRoles to the controller SA | `[]` |
| `rbac.userRoles.enabled` | Render the four user-facing ClusterRoles (see below) | `true` |
| `metrics.enabled` | Serve and expose the `/metrics` endpoint | `true` |
| `metrics.listen.port` | Metrics port (`--metrics-bind-address`) | `8443` |
| `metrics.secure` | Serve metrics over HTTPS with bearer-token auth (`--metrics-secure`); also gates the metrics-auth `ClusterRole`/`ClusterRoleBinding` that filter needs | `true` |
| `metrics.service.type` | Metrics Service type | `ClusterIP` |
| `metrics.serviceMonitor.enabled` | Render a Prometheus `ServiceMonitor` (needs the Prometheus Operator CRDs) | `false` |
| `metrics.networkPolicy.enabled` | Render a `NetworkPolicy` allowing metrics ingress from namespaces labelled `metrics: enabled` | `false` |
| `manager.repository` | Image repository | `ghcr.io/isometry/wavefront-controller` |
| `manager.tag` | Image tag (a `sha256:…` value is treated as a digest) | chart `appVersion`, then `latest` |
| `manager.imagePullPolicy` | Container `imagePullPolicy`; rendered only when set | `~` |
| `manager.imagePullSecrets` | Pod-level `imagePullSecrets` (e.g. `[{name: regcred}]`); rendered only when non-empty | `[]` |
| `manager.replicas` | Replica count | `1` |
| `manager.annotations` | Additional annotations on the Deployment | `{}` |
| `manager.podAnnotations` | Additional annotations on the manager Pod template | `{}` |
| `manager.extraLabels` | Additional labels on the Deployment | `{}` |
| `manager.priorityClassName` | PriorityClass for the manager Pod | `~` |
| `manager.nodeSelector` | Pod node selector | `~` |
| `manager.tolerations` | Pod tolerations | `~` |
| `manager.affinity` | Pod affinity rules | `~` |
| `manager.env` | Additional container environment variables | `[]` |
| `manager.extraArgs` | Additional CLI flags as a `key: value` map (`--key=value`, or `--key` for a null value) | `{}` |
| `manager.resources` | Manager container resource requests/limits | req `10m`/`64Mi`, lim `500m`/`128Mi` |

## User-facing roles

`rbac.userRoles.enabled` (default `true`) renders four ClusterRoles for humans
and tooling. The chart **never binds them** — they exist so a cluster admin can
delegate access with their own `ClusterRoleBinding`s:

| ClusterRole | Grants |
| --- | --- |
| `<fullname>-wavefront-admin-role` | full (`*`) control of `wavefronts` |
| `<fullname>-wavefront-editor-role` | create/update/delete `wavefronts` |
| `<fullname>-wavefront-viewer-role` | read-only on `wavefronts` |
| `<fullname>-wfctl-viewer-role` | the `wfctl` **viewer** tier: read on `wavefronts` plus `get`/`list` on `events.events.k8s.io` (what `wfctl history` lists) |

With the default release name the `<fullname>` prefix is
`wavefront-controller`, so binding the wfctl viewer tier to a group looks like:

```sh
kubectl create clusterrolebinding wfctl-viewers \
  --clusterrole=wavefront-controller-wfctl-viewer-role \
  --group=sre
```

The `wfctl` **derive** and **operator** tiers are deliberately not shipped:
their extra verbs are on Flux's own `Kustomization`, `GitRepository` and
`Secret` resources and belong to whatever policy regime already governs those.
See the project README, "Access tiers". Set `rbac.userRoles.enabled=false` if
your cluster manages these roles out-of-band.

## Observability

The manager exposes the controller-runtime Prometheus `/metrics` endpoint on
port `8443` over HTTPS with a bearer-token AuthN/AuthZ filter
(`metrics.secure=true`); set `metrics.secure=false` for plain HTTP. The
`<fullname>-metrics-service` Service publishes it as the named port `metrics`,
and `<fullname>-metrics-reader-role` grants `get` on the `/metrics`
non-resource URL for scrapers that need it.

With `metrics.serviceMonitor.enabled=true` the chart renders a `ServiceMonitor`
matching that port (the Prometheus Operator CRDs must be installed); its scheme
and TLS config follow `metrics.secure`.

With `metrics.networkPolicy.enabled=true` the chart renders
`<fullname>-allow-metrics-traffic`, admitting ingress to the metrics port only
from namespaces labelled `metrics: enabled`.
