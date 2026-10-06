<!--
SPDX-FileCopyrightText: 2026 Scitrera LLC
SPDX-License-Identifier: AGPL-3.0-only
SPDX-FileComment: The Sparkrun additional permission in LICENSE_EXCEPTION applies.
-->

# Development

Run these commands from the repository root.

## Build and test

Go 1.25 or newer and Python 3.12 or newer are required. CI uses Go 1.25.14.
The generated release builds are static Linux amd64 and arm64 executables.

```sh
CGO_ENABLED=0 go build -trimpath -o bin/oci-relay ./cmd/oci-relay
go test -race ./...
go vet ./...
# Opt-in: builds/removes disposable images on the local Docker daemon.
OCI_RELAY_DOCKER_TESTS=1 go test -race -count=1 ./...
```

The plugin requires Sparkrun 0.4.0 with image-distribution API 1 and pre-pull API 1. Use the
sourceable development setup, following the Sparkroute/Coldsnap plugin workflow:

```bash
source dev.sh
sparkrun --version
pytest -q
# Opt-in local Docker lifecycle/transport tests:
OCI_RELAY_PLUGIN_TESTS=1 pytest -q
# Run recipes normally using the enabled provider:
# sparkrun run @official/deepseek-v4-flash-0731-b12x-dspark-vllm
deactivate
```

`dev.sh` requires Bash, `uv`, and Go (on PATH, supplied through `GO`, or installed
under `~/sdk/go*/bin/go`). It creates `.venv`, installs this plugin and Sparkrun
editably, builds `bin/oci-relay` with the CI-pinned toolchain, and verifies actual
installed-plugin registration. It uses `../oss-sparkrun` by default; select a
different compatible checkout with `export SPARKRUN_CHECKOUT=/path/to/sparkrun`
before sourcing. Use `develop-next` / 0.4.0. The script uses that checkout's
current files without fetching, switching branches or copying plugin source
into it. When that checkout contains a bundled OCI Relay, the private dev configuration
disables its feature gate and enables the editable installed adapter instead.

The active shell uses private `.dev/config/config.yaml`. On first setup,
config.yaml, cluster definitions, registry definitions and local recipes are
copied from `SPARKRUN_CONFIG_DIR` (or `~/.config/sparkrun`); auth-token and
service-state files are not copied. Regular user configuration is unchanged.
The dev config selects and enables `oci-relay`, permits native/auto source
selection, points to the local binary, and disables builtin fallback so failures
stay visible during testing. Edit its plugin settings to try optional tuning.
Repeated sourcing rebuilds Go and preserves local config edits, while refreshing
the enabled integration, provider and binary path. Cluster/registry snapshots
are not recopied over development edits. Normal Sparkrun caches remain shared.
`deactivate` restores the prior shell configuration. `.venv` and `.dev` are ignored
by Git; `.dev` is private because its config may contain copied credentials.

## Versions, CI and vendoring

[versions.yaml](../versions.yaml) owns both versions and the generated GitHub
workflows. The wrapper uses `~/scitrera-repo-tools`, or
`SCITRERA_REPO_TOOLS`, when available; otherwise it resolves the pinned toolkit
revision with `uvx`.

```sh
python scripts/repo-tools.py sync-versions --check
python scripts/repo-tools.py generate-ci-gha --check
python scripts/dependency-licenses.py --check
```

For a future deliberate bundling operation, from this repository:

```sh
python scripts/vendor-plugin.py --sparkrun /path/to/target-sparkrun --development
python scripts/vendor-plugin.py --sparkrun /path/to/target-sparkrun --check
```

Vendoring is never invoked by `dev.sh`. The script copies the adapter and license
material, adds an alpha-enabled `plugins.oci_relay` feature and in-tree
loader binding, and includes license/release files in host package metadata.
It also excludes the immutable vendor directory from host Ruff checks. Review
those target-tree changes before committing. After bundling, the feature defaults on for alpha and off for stable/beta.
Use the `plugins.oci_relay` key under `features` for an explicit override, with
the installed integration disabled. `--check` verifies both snapshot hashes and host bindings;
include that check in the future host release process.

The script records exact content hashes and labels worktree imports as
development snapshots. Production vendoring requires a clean commit
containing the matching engine release tag, unchanged engine source, and
checksum entries for both binary architectures.
See [release procedure](releasing.md). No public repository, tag, release
or push is created by local generation.

For published releases, prefer the host-owned script from Sparkrun:

```sh
python scripts/vendor-oci-relay.py update --latest --initial
python scripts/vendor-oci-relay.py verify
```

Run those commands from the Sparkrun checkout; omit `--initial` on updates.
The script pins the adapter commit from `plugin-release.json` attached to the
engine release, imports license material and offline contract tests, and records
hashes in `vendor/oci-relay.lock`. Explicit reviewed revisions use
`--source PATH --rev FULL_COMMIT`. Verification requires no network access.
