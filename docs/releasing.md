# Release procedure

`versions.yaml` is the version and CI source of truth. Use the shared
`scitrera-repo-tools` checkout to sync versions and generate workflows;
do not edit generated workflow YAML independently.

1. Update both project versions together. Sync, regenerate, and run the local
   equivalents of generated CI: formatting, vet, race tests, Python tests/lint,
   dependency-license check and govulncheck with the pinned Go version.
2. Run the opt-in Docker and Sparkrun plugin tests. Qualify each advertised
   Docker store/architecture and route, recording evidence in validation.md.
   Inspect cancellation, failed receivers, cleanup and large-upload rejection.
3. Regenerate THIRD_PARTY_LICENSES.txt. Confirm LICENSE, LICENSE_EXCEPTION,
   COPYRIGHT and dependency notices are included in both binary archives.
   Verify plugin wheels and vendored copies contain the AGPL license/exception.
4. When the public repository exists, review and commit the source and generated
   CI. A matching `vVERSION` tag triggers Linux amd64/arm64 archive builds plus
   `checksums.txt`. Local generation does not create or push that tag.
5. Publish release-matched corresponding source alongside binaries. The Git tag
   must include all source, module pins, CLI/plugin code, versions.yaml and build
   scripts. For a self-contained source bundle, run `go mod vendor` in a clean
   temporary checkout of the tag, retain dependency notices, and archive the
   checkout including vendor/ (exclude .git). Include the exact Go toolchain
   version and instructions to build with `go build -mod=vendor`.
6. Download and independently review/verify the built artifacts. Record their
   actual SHA-256 checksums in `plugin/src/sparkrun_oci_relay/releases.json`:

   ```json
   {
     "VERSION": {
       "linux/amd64": {"url": "https://github.com/OWNER/REPO/releases/download/vVERSION/oci-relay_VERSION_linux_amd64.tar.gz", "sha256": "ACTUAL_ARCHIVE_SHA256"},
       "linux/arm64": {"url": "https://github.com/OWNER/REPO/releases/download/vVERSION/oci-relay_VERSION_linux_arm64.tar.gz", "sha256": "ACTUAL_ARCHIVE_SHA256"}
     }
   }
   ```

   Commit these pins after the engine release. Do not move the engine tag.
   A post-release adapter commit is necessary because archive checksums do not
   exist until the engine assets have been built. No corresponding engine code
   may change beneath the same engine version.
7. From that clean commit run `scripts/vendor-plugin.py --sparkrun PATH`.
   It requires the matching engine tag in history, unchanged engine source,
   both platform pins, and records the exact adapter commit and file hashes.
   This is an explicit future bundling decision: development uses `source dev.sh`
   and an installed editable plugin. Sparkrun `develop-next` / 0.4.0 currently
   contains only the general image-distribution compatibility API, with no OCI
   Relay snapshot or bindings. Vendoring adds the disabled feature/loader bindings,
   host package data and Ruff exclusion as reviewable target-tree changes.
   Run `--check` in the eventual host validation/release process; it verifies
   the snapshot and those bindings. Disable the installed integration before
   enabling the bundled feature.
8. A development snapshot uses explicit `--development` and has
   `development_snapshot = true`. Do not describe it as a public release pin.
   Development binaries similarly require explicit configuration.

Python package publication is not enabled in the generated workflows yet.
The installable plugin and reproducible vendoring path are available locally.
Public repository creation, pushes, release publication and production
enablement are separate operations.
