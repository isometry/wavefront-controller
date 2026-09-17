#!/usr/bin/env bash
# Regenerate the Helm chart's generated templates from the controller-gen output
# in config/, preserving the chart's Helm templating (the crds.install /
# rbac.create guards and the metadata annotations/labels):
#
#   templates/crds.yaml                <- config/crd/bases/…_wavefronts.yaml
#   templates/clusterrole-manager.yaml <- config/rbac/role.yaml
#
# Run by `make manifests` so the chart never drifts from the generated source of
# truth. Never hand-edit the outputs.
#
# Each body (the CRD's spec, the ClusterRole's rules) is copied verbatim from
# its source file — both already carry the indentation the chart wants — so the
# chart stays byte-identical to the source and no YAML re-serialization style
# creeps in.
#
# The CRD is a template rather than a file in Helm's crds/ directory on purpose:
# crds/ cannot be templated (no labels, no annotations, no crds.install toggle)
# and Helm never upgrades what it installs from there. As a template the CRD
# carries the chart's labels, honours crds.install, is upgraded with the release,
# and survives `helm uninstall` via helm.sh/resource-policy: keep when
# crds.keep is set.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

chart="deploy/charts/wavefront-controller"

require() {
  if [[ ! -f "$1" ]]; then
    echo "sync-chart: missing generated source $1" >&2
    exit 1
  fi
}

# extract <anchor> <file>: print everything from the top-level "<anchor>:" key to
# EOF. Fails loudly when the anchor is absent or its block is empty, so a
# controller-gen output change can never silently produce a body-less template.
extract() {
  local anchor="$1" src="$2" body
  body="$(awk -v re="^${anchor}:" 'f || $0 ~ re { f = 1; print }' "$src")"
  if [[ -z "$body" ]]; then
    echo "sync-chart: no '${anchor}:' block found in ${src}" >&2
    exit 1
  fi
  printf '%s\n' "$body"
}

# Write stdin to $1 atomically (tmp file in the same directory, then mv).
write_atomic() {
  local out="$1" tmp
  tmp="$(mktemp "${out}.XXXXXX")"
  # shellcheck disable=SC2064 # expand $tmp now, not at trap time
  trap "rm -f '$tmp'" EXIT
  cat >"$tmp"
  chmod 0644 "$tmp" # mktemp creates 0600; these are ordinary chart sources
  mv "$tmp" "$out"
  trap - EXIT
  echo "sync-chart: wrote ${out}"
}

# --- CRD --------------------------------------------------------------------
# controller-gen writes a single CRD per file (metadata then spec), so the spec
# block runs from the top-level "spec:" key to EOF.
crd_src="config/crd/bases/wavefront.as-code.io_wavefronts.yaml"
require "$crd_src"
crd_spec="$(extract spec "$crd_src")"

{
  printf '%s\n' '{{ if .Values.crds.install -}}'
  cat <<EOF
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: wavefronts.wavefront.as-code.io
  {{- with mergeOverwrite (deepCopy (default dict .Values.commonAnnotations)) (ternary (dict "helm.sh/resource-policy" "keep") (dict) .Values.crds.keep) }}
  annotations:
    {{- range \$key, \$value := . }}
    {{ \$key }}: {{ tpl (toString \$value) \$ | quote }}
    {{- end }}
  {{- end }}
  labels:
    component: crd
    {{- include "labels" . | nindent 4 }}
EOF
  printf '%s\n' "$crd_spec"
  printf '%s\n' '{{- end }}'
} | write_atomic "${chart}/templates/crds.yaml"

# --- manager ClusterRole ----------------------------------------------------
# controller-gen's rule list items sit at column 0 under "rules:", which is
# valid YAML at this nesting.
role_src="config/rbac/role.yaml"
require "$role_src"
role_rules="$(extract rules "$role_src")"

{
  cat <<EOF
{{- if .Values.rbac.create }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "chart.fullname" . }}-manager-role
  {{- with (deepCopy (default dict .Values.commonAnnotations)) }}
  annotations:
    {{- range \$key, \$value := . }}
    {{ \$key }}: {{ tpl (toString \$value) \$ | quote }}
    {{- end }}
  {{- end }}
  labels:
    component: rbac
    {{- include "labels" . | nindent 4 }}
EOF
  printf '%s\n' "$role_rules"
  printf '%s\n' '{{- end }}'
} | write_atomic "${chart}/templates/clusterrole-manager.yaml"
