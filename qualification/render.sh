#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
helm version --short
helm lint --strict baseline-chart --values render-values.yaml
helm lint --strict baseline-chart --values baseline-values-k3d.yaml --values render-values.yaml

# The missing-token guard must still reject an unconfigured release.
if failure="$(helm template executor-k8s-openhands baseline-chart --namespace flowai-qualification 2>&1)" ||
   [[ "$failure" != *'broker.token is required'* ]]; then
  echo 'The chart did not reject a missing broker.token as expected.' >&2
  exit 1
fi

helm template executor-k8s-openhands baseline-chart --namespace flowai-qualification --values render-values.yaml
helm template executor-k8s-openhands baseline-chart --namespace flowai-qualification \
  --values baseline-values-k3d.yaml --values render-values.yaml
