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
3. Build helpers on native runners only: Linux AMD64 `ubuntu-24.04`, Linux
   ARM64 `ubuntu-24.04-arm`, and macOS ARM64 `macos-15`. The repo-tools binary
   package hook builds source-pinned musl/zlib/pigz, executes tests, and adds
   `unpigz`, `bundle.json` and `UNPIGZ_LICENSES.txt`. No QEMU or cross-architecture
   helper execution. Dispatch the publish workflow before tagging to validate
   native artifacts. Review source pins in `scripts/native-dependencies.json`.
   Regenerate THIRD_PARTY_LICENSES.txt. Confirm LICENSE, LICENSE_EXCEPTION,
   COPYRIGHT and dependency notices are included in every binary archive.
   Verify plugin wheels and vendored copies contain the AGPL license/exception.
4. Review and commit the source and generated CI, then push to the public repository. A matching `vVERSION` tag triggers Linux amd64/arm64 archive builds plus
   `checksums.txt`, plus the macOS arm64 archive. Local generation does not
   create or push that tag. See [platform qualification](platforms.md).
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
       "linux/arm64": {"url": "https://github.com/OWNER/REPO/releases/download/vVERSION/oci-relay_VERSION_linux_arm64.tar.gz", "sha256": "ACTUAL_ARCHIVE_SHA256"},
       "darwin/arm64": {"url": "https://github.com/OWNER/REPO/releases/download/vVERSION/oci-relay_VERSION_darwin_arm64.tar.gz", "sha256": "ACTUAL_ARCHIVE_SHA256"}
     }
   }
   ```

   Commit these pins after the engine release. Do not move the engine tag.
   A post-release adapter commit is necessary because archive checksums do not
   exist until the engine assets have been built. No corresponding engine code
   may change beneath the same engine version.
7. Attach `plugin-release.json` to the same GitHub release after committing and
   pushing the verified archive pins. It identifies the adapter commit, which
   descends from the engine tag without changing `cmd`, `internal`, `go.mod`,
   or `go.sum`:

   ```json
   {
     "schema": 1,
     "repository": "https://github.com/scitrera/oci-relay.git",
     "version": "0.1.0",
     "commit": "FULL_ADAPTER_COMMIT_SHA"
   }
   ```

   From Sparkrun, run `python scripts/vendor-oci-relay.py update --latest --initial`
   (omit `--initial` on updates), then `python scripts/vendor-oci-relay.py verify`.
   The script resolves this published descriptor rather than a floating branch,
   validates host APIs and all archive pins, and verifies engine ancestry.
   It records exact file hashes in `vendor/oci-relay.lock` and imports offline
   contract tests. The bundled `plugins.oci_relay` feature defaults on only for
   alpha. Keep installed and bundled registration mutually exclusive.
8. A development snapshot uses explicit `--development` and has
   `development_snapshot = true`. Do not describe it as a public release pin.
   Development binaries similarly require explicit configuration.

Python package publication is not enabled in the generated workflows yet.
Plugin source and the pinned vendoring metadata are available from GitHub.
Binary tagging and the subsequent adapter-pin publication are separate steps;
never move the engine tag to add its archive checksums.
