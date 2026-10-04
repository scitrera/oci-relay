<!--
SPDX-FileCopyrightText: 2026 Scitrera LLC
SPDX-License-Identifier: AGPL-3.0-only
SPDX-FileComment: The Sparkrun additional permission in LICENSE_EXCEPTION applies.
-->

# OCI Relay

OCI Relay distributes a prepared OCI image from one source to multiple Docker
hosts. The Go executable provides authenticated HTTP/2, bounded shared blob
buffers, digest verification, a demand-driven Docker source, and a loopback
registry on each receiver. The Sparkrun plugin handles deployment, SSH,
transport selection, adaptive resource limits, and operation cleanup.

This is the initial implementation, qualified on Linux arm64 with Docker's
classic overlay2 store. See [validation and limitations](docs/validation.md).
It is not yet a published release.

## Build and test

Go 1.25 or newer and Python 3.12 or newer are required. CI uses Go 1.25.14.
The generated release builds are static Linux amd64 and arm64 executables.

```sh
go build -trimpath -o bin/oci-relay ./cmd/oci-relay
go test -race ./...
go vet ./...
# Opt-in: builds/removes disposable images on the local Docker daemon.
OCI_RELAY_DOCKER_TESTS=1 go test -race -count=1 ./...
```

The plugin requires Sparkrun 0.4.0 with image-distribution API 1. Use the
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
into it. A checkout containing a vendored OCI Relay is rejected to avoid two
copies registering the same provider.

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

## Sparkrun integration

The plugin replaces **image copy** operations after Sparkrun has chosen and
prepared the source and filtered matching destinations. Core retains build,
pull/offline, staging, Coldsnap materialization, launch ordering and final image
checks. Registry-pull policies that perform no image copy do not invoke it.

Sparkrun `develop-next` retains the general image-distribution API, but has no
OCI Relay source, feature binding or vendor metadata by default. This project
is an independently installed plugin. `source dev.sh` installs and enables it
locally; manual installations use this configuration:

```yaml
integrations:
  oci-relay: true
container_distribution_provider: oci-relay
plugins:
  oci-relay:
    source_mode: auto  # Default; choose a permitted source from detected facts.
    allow_native_store: true  # Default; set false to prohibit native-store access.
    allow_preparation_read: true  # Default; set false to prohibit full preparation.
    transport: auto
    # Development only, until a public release has trusted checksum pins:
    development_binary: /absolute/path/to/oci-relay
```

The installed distribution exposes the `sparkrun.plugins` entry point named
`oci-relay`. Future bundling is an explicit operation described below; choose
one registration path and do not enable installed and bundled copies together.

For a locally supplied production binary, configure `binary_paths` and
`binary_sha256` maps keyed by `amd64`/`arm64`. The controller verifies the ELF
architecture and hash, stages it to each execution host, verifies the remote
hash, and checks the version/protocol before use. Hosts do not need internet
access. Offline mode never downloads a binary. `releases.json` is deliberately
empty until real release archive checksums can be pinned.

Transport options:

- `auto`: preflight direct HTTP/2, then managed SSH forwards, then stdio.
- `http2-direct`: authenticated source listener; receivers connect outbound.
- `http2-ssh`: loopback listeners carried through owned SSH forwards.
- `ssh-stdio`: end-to-end TLS/HTTP/2 carried through managed SSH byte pipes.

Direct mode defaults to a source wildcard listener and advertises the source
address on the route to the selected transfer network. Set `listen` and
`advertise` to override this. Credentials are per operation and per receiver.
Forwarded/stdio mode binds to loopback; the controller bridges their traffic.
SSH user, key, options and host-key policy come from Sparkrun.

Independent host setup runs concurrently, with at most eight setup workers.
Binary verification is retained on every operation; release acquisition is
shared per architecture. Discovery batches host facts into two commands per
host, including cgroup ancestor caps. All routes must pass preflight before
any receiver pull starts. Failed setup settles its workers before cleanup.
The plugin logs binary/discovery/session/preparation/preflight/import/cleanup
times and a total including cleanup, plus each receiver's download/import
timings. See the [startup and import profile](docs/benchmarks/startup-2026-10-04.md).

The plugin logs the limits selected from route speed, CPU count, available
memory and cgroup v2 headroom. These are startup hints, not bandwidth guarantees
or a runtime congestion controller:

| Direct route hint | Cache target per process | Acquisition concurrency ceiling |
|---|---:|---:|
| Unknown / below 25 Gbps | 128 MiB | 4 |
| 25–99 Gbps | 256 MiB | 8 |
| 100–199 Gbps | 512 MiB | 16 |
| 200+ Gbps | 1 GiB | 32 |

Automatic buffers consume at most one sixteenth of reported available memory,
shared between colocated operation roles; CPU and memory further limit
concurrency. Unknown memory headroom stays at 128 MiB or less. SSH transports
use 128 MiB / 4. An automatic source selected before transport negotiation may
retain its larger direct-route limit if receivers fall back to SSH.

Set `network_gbps` when Sparkrun-specific network knowledge is better than the
interface probe. On detected NVIDIA DGX Spark systems the plugin caps the
effective hint for one route at 100 Gbps, retaining the reported link speed in
diagnostics. It does not add the second port's bandwidth to a single route.
Explicit `max_buffer_bytes` and `source_streams` override the
policy. `max_spool_bytes` defaults to 8 GiB, and `max_upload_bytes` defaults to
that total. A push-source blob must fit the upload cap; metadata preparation
is separately measured. Image groups are serialized within the plugin process.
Separate Sparkrun invocations do not share a global resource governor.

Provider failures are surfaced. Archive fallback requires both
`container_distribution_provider: auto` and
`container_distribution_fallback: true`, and is allowed only when a provider
reports that it cannot support a request before destination transfer starts.

## Manifest input and source modes

The plugin defaults to `source_mode: auto`. It logs the chosen mode and reason
before source preparation or destination pulls. Explicit `docker`, `docker-save`
and `docker-classic` settings override selection. The plugin defaults both
`allow_native_store` and `allow_preparation_read` to `true`; explicit `false`
values are honored. Native access still requires a qualified store, and full
archive staging still requires an explicit budget. These plugin defaults do
not change the standalone Go CLI's `--allow-preparation-read` opt-in.

Selection follows these rules:

1. A supplied manifest selects `docker`, preserving its exact representation.
2. A permitted, qualified rootful Linux overlay2 store with `runc` selects
   `docker-classic`, avoiding export/compression and source payload staging.
3. Otherwise, a qualified archive exporter may select `docker-save` when
   preparation is permitted, the archive budget is explicit, the requested
   transport is `http2-direct` with a hint of at least 25 Gbps, and image-size /
   free-space checks leave room for the archive and Docker's export scratch.
4. Otherwise, permitted preparation selects the bounded `docker` push source.
   If no source is permitted, the provider reports unsupported before transfer;
   Sparkrun's explicitly configured builtin fallback policy then applies.

This is a conservative policy, not a prediction of the fastest mode for every
image or link. In particular, a native source remains uncompressed on a slow
link. Auto does not stage a full export on an unnegotiated `transport: auto`
route that might fall back to SSH. Image size is only an archive estimate; the
Go source still enforces the exact staging cap. Hash, metadata, source startup
and transfer failures are surfaced without retrying a different representation.
See [source selection](docs/source-selection.md) for the policy and limitations.

A manifest can be obtained independently, including with:

```sh
docker buildx imagetools inspect --raw IMAGE:TAG > manifest.json
```

Use a **single-platform image manifest**, not a multi-platform index.
For an index, resolve the desired child digest first and inspect that digest.
Pass it using `--manifest manifest.json` or plugin setting `manifest` (a path
on the controller). The manifest alone does not supply its blobs.

OCI Relay preserves exact manifest bytes and requires exact descriptor lengths
and SHA-256 digests. A registry-produced manifest can be used with local Docker
only if its config matches the pinned local image and Docker reproduces the
same compressed layers and manifest representation. Incompatible sources fail;
the relay does not silently substitute another manifest.

With no supplied manifest, `--allow-preparation-read` opts into a classic-store
preparation push that reads/hashes the whole image while discarding blob bodies.
Runtime push rounds then request only currently needed blobs and spool
unidentified uploads under a hard reservation limit. This avoids full-image
staging but still stages each active unidentified blob. Supplying a manifest
avoids that full preparation pass; an exact config acquisition is still needed.

An existing OCI-layout directory is also supported and can stream blobs larger
than memory without Docker upload staging. Native registry fetching, diskless
identity-first Docker uploads, HTTP Range resume, full indexes and peer-to-peer
sourcing are not implemented yet.

For classic Docker on fast local networks, an **explicit experimental**
`docker-save` source avoids the daemon's push gzip work. It pins the local image
ID, exports once through the Engine API, stages a single bounded archive, and
serves its OCI blobs in parallel using file offsets. It does not extract another
copy or access Docker's private storage. Docker must export an OCI layout (the
tested Docker 29 engines do); older archive formats fail explicitly.

```yaml
plugins:
  oci-relay:
    source_mode: docker-save
    allow_preparation_read: true
    max_spool_bytes: 34359738368  # Explicit 32 GiB full-archive cap; size for your image.
    # spool_dir: /path/on/source/with/enough/space
```

Do not supply `manifest` in this mode. The native export's uncompressed OCI
manifest differs from the registry's compressed manifest; the config/Docker
image ID must remain identical. These are distinct, reported representations.
Staging uses ordinary buffered file I/O and is deleted on completion or failure.
The cap bounds relay-owned archive bytes, excluding Docker's own export scratch
space and Linux page cache. Preparation time, time to first export byte, and
archive bytes are reported separately. The source can still spend substantial
time preparing before emitting any bytes. See the
[large-image benchmark](docs/benchmarks/deepseek-2026-10-03.md) before choosing a
mode; this is not an automatic switch or a guaranteed speedup.

An opt-in `docker-classic` source removes the Docker export/push preparation
step on qualified **rootful Docker 29.1.3/29.2.1 overlay2** stores:

```yaml
plugins:
  oci-relay:
    source_mode: docker-classic
    allow_native_store: true
    transport: auto
```

This mode is an explicit exception to the original public-API-only design.
The plugin pins the image ID and creates an owned helper container referencing
that image, keeping its layers alive for the operation. The verified static
relay is its entrypoint; the model/application is never started. Only Docker's
image metadata and overlay2 trees are mounted read-only, alongside the relay
binary and private operation directory. It receives no Docker socket. Rootless
and user-namespace stores, other drivers, and unqualified Engine versions are
rejected before receiver pulls. Existing provider fallback rules apply.

The adapter scans tar-split metadata to determine exact tar lengths, then
reconstructs uncompressed tar streams on demand using Moby's upstream tar-split
library. Headers, whiteouts, padding and file order are preserved. Config bytes
must hash to the pinned image ID; each uncompressed descriptor must equal its
config DiffID. Both source and receiver verify full lengths and SHA-256 digests,
and withhold the final byte until verification succeeds. The receiver also
checks the imported Docker ID before assigning the final tag. This generates
an uncompressed manifest, **not** the registry's compressed manifest.

No full image or layer payload is staged by this source. Reads use ordinary
buffered filesystem I/O; metadata decoding, filesystem page cache and Docker's
receiver-side download/import storage remain additional to relay ring budgets.
Decoded metadata is limited to 512 MiB and one million entries per layer, with
individual segments limited to 8 MiB. This bounds supported input; it is not an
RSS guarantee. The helper and its owned anonymous volumes are removed during
cleanup. See [native-source measurements](docs/benchmarks/native-2026-10-03.md).

Metrics now include active/peak source acquisitions, verified bytes and blobs
(including replays), and the last verified completion time. Receiver results
also separate Docker pull duration, last download completion and last layer
registration. Time origins are per process/phase, not synchronized wall clocks.
The configured stream count is a ceiling; Docker's download limit and actual
missing layers control demand. Each blob still uses one stream; 64 KiB buffer
frames are not independent parallel pieces.

Direct TCP currently uses one selected route. Two-port striping and RoCE/RDMA
are future transports, with authenticated TCP retained as the portable path.

## Optional import and relay tuning

Defaults remain registry pull, no source join delay, and Go's runtime CPU
selection. The following plugin settings are experimental and do not change
Docker daemon configuration:

```yaml
plugins:
  oci-relay:
    source_join_milliseconds: 250  # 0 disables; maximum 2000
    relay_gomaxprocs: 4             # 0 inherits; maximum 64, relay processes only
    receiver_import: load-cached    # pull (default), load-cached, or load
    max_import_bytes: 34359738368   # explicit 32 GiB Docker import archive cap
```

`source_join_milliseconds` briefly retains a new layer's initial ring window
for other receivers before evicting it. Fetch starts immediately and memory
stays within the configured ring budget. It can avoid duplicate reconstruction;
receivers that already have a layer cannot hold it indefinitely. The wait is
bounded from stream creation, and does not guarantee every receiver joins.
`source_streams` still controls acquisition slots. Neither option changes
Docker's download concurrency or creates independent pieces within a layer.
`relay_gomaxprocs` applies only to relay binaries and the owned source helper;
it does not alter dockerd, its process environment or settings.

`load-cached` probes at most 128 listed receiver images through public Docker
APIs for an exact prefix of the requested DiffID chain, including every parent.
It pins the best matching base with a temporary tag, streams only missing raw
layers into a Docker load archive, and preserves the original image config.
This can reuse layers across compressed and uncompressed representations when
registry pull would download them again. Unexamined older images can be missed.
With no usable prefix, an unsupported store or compressed input, selection
returns to normal pull **before transfer**. Errors after import begins are
fatal; they do not trigger another importer or weaken verification.

`load` streams all raw layers to the same importer without a cache probe.
Both load modes are currently qualified only on Linux overlay2 Docker 29.1.3
and 29.2.1, and require exact uncompressed OCI descriptors plus an explicit
`max_import_bytes` (4 MiB–1 PiB). The cap covers the complete outer tar archive,
including headers and padding. The relay streams it without disk staging;
Docker first stages it on disk using its normal buffered I/O, then registers
layers. This cap does **not** bound extracted Docker storage or process RSS.
Cached-layer placeholders are archive entries, not falsely labeled OCI blobs;
a missing cached chain fails Docker's DiffID check. Transferred layers still
pass full relay SHA-256 checks, and the final image ID must match the source.

Standalone equivalents are `serve --source-join-milliseconds`,
`peer --import` and `peer --max-import-bytes`; use the `GOMAXPROCS` environment
variable for standalone relay processes. Results report `import_method`,
`reused_layers`, `import_archive_bytes`, and `load_seconds`. Download/extraction
progress times remain zero for load, which does not expose those phases.
See [ordered optimization measurements](docs/benchmarks/optimization-3241-2026-10-04.md).

## Standalone commands

The plugin is the multi-host coordinator. The executable exposes composable
commands; there is no standalone SSH `send` command yet.

```sh
oci-relay version
oci-relay prepare --source docker --image example:tag \
  --allow-preparation-read --output manifest.json
# Explicit full-archive alternative (also usable with serve/run):
oci-relay prepare --source docker-save --image example:tag \
  --allow-preparation-read --max-spool-bytes 34359738368 --output export-manifest.json
oci-relay session --out /private/session --peers node1,node2
oci-relay serve --image example:tag --manifest manifest.json \
  --session-dir /private/session --listen 0.0.0.0:9443 --advertise SOURCE_IP
# Copy only node1.json to node1 over the trusted management channel, then:
oci-relay peer --endpoint https://SOURCE_IP:9443 \
  --session /private/node1.json --tag example:tag
```

`session.json` contains all session secrets: keep it private. Each receiver
gets only its own JSON credential. An existing destination tag with a different
image is rejected unless `--replace-tag` is specified. Completion follows a
successful Engine progress stream, identity/platform checks and tag inspection.

`run --plan FILE` consumes a version-1 JSON plan equivalent to `serve` flags.
`--managed-stdin` accepts heartbeat/cancel JSON lines and cancels on EOF.
`--lease-seconds` enforces the manager lease after readiness. Stdout emits
JSONL readiness, progress and final results. `peer --check` verifies the
transport and metadata without pulling. `attach --socket PATH` bridges raw
stdin/stdout to the private source socket; `peer --stdio` uses that bridge and
writes diagnostics/results to stderr.

## Versions, CI and vendoring

[versions.yaml](versions.yaml) owns both versions and the generated GitHub
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
material, adds an initially disabled `plugins.oci_relay` feature and in-tree
loader binding, and includes license/release files in host package metadata.
It also excludes the immutable vendor directory from host Ruff checks. Review
those target-tree changes before committing. After bundling, enable
`features.plugins.oci_relay: true` and select the provider, with the installed
integration disabled. `--check` verifies both snapshot hashes and host bindings;
include that check in the future host release process.

The script records exact content hashes and labels worktree imports as
development snapshots. Production vendoring requires a clean commit
containing the matching engine release tag, unchanged engine source, and
checksum entries for both binary architectures.
See [release procedure](docs/releasing.md). No public repository, tag, release
or push is created by local generation.

## License and contributions

Copyright 2026 Scitrera LLC. Original code and documentation are
[AGPL-3.0-only](LICENSE), with the Sparkrun additional permission in
[LICENSE_EXCEPTION](LICENSE_EXCEPTION). The permission covers marked Go relay
and plugin code and allows combination/vendoring with Sparkrun while its
Apache-licensed portions retain Apache-2.0. OCI Relay remains under AGPLv3,
including applicable corresponding-source obligations.

Ship both license documents with binaries and plugin copies. Third-party
dependencies retain their licenses; see [notices](THIRD_PARTY_NOTICES.md) and
[full dependency texts](THIRD_PARTY_LICENSES.txt).

Contributions require the [CLA](CLA.md) before merging. It retains contributor
ownership and grants Scitrera LLC the rights stated in the agreement.
[CONTRIBUTING.md](CONTRIBUTING.md) describes acceptance and source notices.
