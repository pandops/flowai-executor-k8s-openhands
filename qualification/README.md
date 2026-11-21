# Standalone qualification inputs

`publisher-schemas.txt` pins the schema bytes used by the Go tests. The
dispatch stages those bytes under `FLOWAI_CONTRACT_FIXTURE_DIR` and validates
their hashes. `schema-declarations.json` retains the matching entries and
their consumer lists from the baseline Gitea selector.

`baseline-chart/` and `baseline-values-k3d.yaml` are static copies of the
Kubernetes packaging inputs at FlowAI
`9cf0d0c5f0769b286119be8f12ff6e4edd1d8d0b`. The deployment repository owns
live releases and Helmfile values. Run `bash qualification/render.sh` to
lint and render both inputs with `render-values.yaml`. The synthetic broker
value is for rendering only and cannot authenticate an Executor.
