# Validation record

Initial implementation, 2026-10-03. This is a compatibility record, not a
throughput benchmark or a claim that all design2 milestones have shipped.
Later sections supersede earlier states. Development now uses the installed
plugin via `source dev.sh`; the previous Sparkrun vendor snapshot was removed.

## Published v0.1.0 qualification (2026-10-06)

The public engine tag is `9dd73f5ef8a1ba3a8717c305fb8461201696aca8`.
The release's `plugin-release.json` pins adapter commit
`bfc0c636d7c06c9eacb7acbbde952b28de102cd5`, which adds verified archive checksums
without changing the engine. The engine tag was not moved.

- Downloaded all three release archives, checked their SHA-256 values and
  executable architectures, and compared redistributed license/notice files.
  Executed the published Linux arm64 binary and checked version, protocol and
  engine commit. Verified real online acquisition and offline cache reuse for
  both Linux architectures.
- All 181 plugin tests passed with that published arm64 binary, including the
  opt-in real Docker and registry-source tests. Release CI passed Go race
  tests, vet, license checks, govulncheck and all three platform builds.
- Imported the adapter using Sparkrun's actual `update --latest` GitHub flow.
  Clean-process tests confirmed alpha auto-enablement, stable/beta disablement,
  explicit overrides, and no import of the disabled bundled module.
- The bundled provider downloaded its binary from GitHub with no development
  override and completed fresh/warm copies between two Linux arm64 Spark hosts
  over direct HTTP/2, SSH forwarding and SSH stdio. Docker image IDs and ordered
  layer hashes matched. Warm copies transferred zero layer bytes. Disposable
  test images were removed; no model was started or daemon reconfigured.
- Sparkrun wheel/source-package checks verified the adapter, pins, license
  exception, provenance, vendoring script/lock and exported contract tests.
- The release includes corresponding source from the engine tag, vendored Go
  dependencies and Go 1.25.14 build instructions. It built successfully with
  `GOPROXY=off` and `-mod=vendor`.

Linux amd64 Docker and native macOS runtime qualification remain open. Windows
amd64/arm64 cross-compile but are not published; see [platforms](platforms.md)
for the runtime work needed. The bundled plugin uses Linux execution hosts;
Mac controllers coordinate delegated transfers without executing a local relay.

## Real multi-host qualification

### Controller latest-refresh regression

After v0.1.0, plugin regression tests cover an already-present controller image
with a mutable `:latest` tag. Unforced refreshes use the registry relay before
any builtin Docker pull: a changed image downloads only the newly needed layer,
and the next unchanged refresh downloads no layers. These tests run through the
actual Sparkrun pre-pull hook on all three transports, using the published
v0.1.0 engine. Metadata/auth failure before receiver startup reuses the pinned
cached local image; explicit/forced pulls and started-transfer failures cannot
take that fallback. Clock-controlled tests verify the 30-second progress cadence.
The complete plugin suite passed 197 tests, including real Docker fixtures.

### Digest-pinned recipe qualification

The adapter and updated Sparkrun `develop-next` support manifest and index pins,
including `tag@sha256:...`, using the unchanged published v0.1.0 engine. All 224
plugin tests passed, including real Docker fixture transfers over direct HTTP/2,
SSH forwarding and SSH stdio. The pin fixtures verify fresh imports, download of
only a newly added layer, warm/offline reuse, local sourcing from a prior relay
import, immutable Docker container creation, moved retention tags, deleted
images and rejection of a corrupt root before receiver startup. No model was
started and no Docker daemon was reconfigured.

Host regressions cover the full launcher handoff to image probes and per-host
Docker executors, differing runtime IDs across stores, preserved recipe/job
identity, content-ID staging, offline preflight, invalid provider results,
context cleanup, dry runs and rejection of conflicting pull options. The new
pin-specific real-Docker fixtures use classic overlay2; different store IDs are
covered by host contract tests and existing engine storage qualification, not a
new mixed-store multi-host pin benchmark.

### Initial cluster qualification

Authorized Sparkrun cluster `benchmark-cluster`: source `host-a`, receivers
`host-b`, `host-c`, `host-d`. Linux arm64, Docker 29.2.1,
classic overlay2. Each run created a fresh incompressible 32 MiB payload,
imported a disposable source image, and verified the final Docker image ID
on every receiver.

| Route | Receivers | Result | Source acquired bytes | Replays | Peak source ring bytes |
|---|---:|---|---:|---:|---:|
| Direct HTTP/2 | 3 | Complete | 33,564,856 | 0 | 2,097,152 |
| HTTP/2 over SSH forwards | 3 | Complete | 33,564,856 | 0 | 2,097,152 |
| HTTP/2 over SSH stdio | 3 | Complete | 33,564,854 | 0 | 2,097,152 |

Each process had an 8 MiB cache budget and the source had a 64 MiB upload-spool
reservation limit. Each blob was larger than its ring window. Metrics above
exclude the explicitly enabled full metadata-preparation read, protocol
traffic and the receiver fan-out bytes. Shared acquisition read each layer
once during transfer. Classic Docker still staged the active unidentified
upload to disk.

The selected route was `management-interface`, reported as 10 Gbps. These results do not
measure CX7 performance. The plugin's 25/100/200 Gbps sizing tiers, memory
headroom and explicit overrides have unit coverage; their high-speed
performance still needs measurement on the intended fast-network route.

Reproduce with explicitly authorized idle hosts:

```sh
PYTHONPATH=plugin/src:../oss-sparkrun/src python scripts/qualify-cluster.py \
  --hosts SOURCE,RECEIVER1,RECEIVER2,RECEIVER3 --binary "$PWD/bin/oci-relay"
```

This script removes its disposable tags and source fixture directory. The
verified version/hash-addressed executable cache is intentionally retained.
It does not prune unrelated Docker images, containers or caches.

## Local checks

- Real Docker 29.1.3 / Linux arm64 / classic overlay2: preparation, exact config,
  supplied-manifest preparation bypass, selective acquisition, replay,
  last-demand cancellation followed by immediate retry, and cleanup.
- Real receiver pulls from an OCI-layout fixture with an absent random layer:
  original manifest/digests, exact Docker ID and bounded cache.
- Go race tests: concurrent consumers, lag isolation, corrupt blobs, consumer
  cancellation/retry, raw manifest identity, malformed descriptors, authenticated
  HTTP/2, unrelated-credential rejection, and TLS/HTTP2 through a byte pipe.
- Docker asynchronous progress errors are tested independently of HTTP status.
- Plugin tests: checksum/ELF verification, offline behavior, archive file type,
  corrupted cached executable replacement, bounded output parsing, short pipe
  writes, adaptive limits and real local process/deployment lifecycle.
- Sparkrun tests cover provider selection/fallback, host mapping, missing results,
  transaction rollback, operation config across workers and process cancellation;
  existing image distribution and transport regressions also pass.
- The main affected Sparkrun suite passed 419 tests; the final forwarding
  ownership regression and its transport suite passed another 25 tests
  (24 overlap). All 12 plugin tests, including opt-in local Docker lifecycle
  checks, passed. Both static Linux builds and plugin wheel license checks passed.
- Generated CI checks versions, formatting, race tests, vet, Python tests/lint,
  dependency license drift, and reachable Go vulnerabilities. Local
  `govulncheck v1.1.4` with Go 1.25.14 reported no vulnerabilities.

## Explicit limits and remaining qualification

- Public-API Docker sources use bounded per-upload staging or the explicit
  full-archive mode. The opt-in read-only classic-store adapter below removes
  source payload staging on qualified engines. The plugin auto-selects a permitted
  source from qualified host facts; the Go CLI receives an explicit mode. An unidentified push upload larger than its cap fails explicitly.
- OCI-layout and registry sources are supported. Registry operations select one
  platform; full index distribution, HTTP Range resume, receiver peer sourcing
  and persistent caches remain future work.
- Linux amd64 is cross-compiled; real amd64 Docker qualification remains open.
  Containerd-backed Docker stores and arbitrary Engine versions are unqualified.
- `max_buffer_bytes` bounds managed blob ring allocations, not total process RSS.
  Metadata, TLS/HTTP2, fixed copy buffers, Go runtime and Docker memory are
  additional. The plugin leaves memory headroom but does not enforce an OS
  memory limit. Spool reservations are tracked separately from cache bytes.
- SSH stdio carries TLS/HTTP2 directly. This intentionally replaces design2's
  proposed custom frame multiplexer. Manager heartbeats use a separate managed
  stdin channel; blob and receiver-result traffic share HTTP/2.
- The plugin coordinates operations; the proposed standalone `send` convenience
  command is not implemented. Existing CLI building blocks support orchestration.
- Credentials are operation-specific and protected on disk. The model assumes
  trusted host users and Docker access, not isolation from other privileged
  local processes.
- Cancellation, lag and transfer leases are covered by automated tests; abrupt
  host power loss, manager SIGKILL, disk exhaustion and sustained slow-receiver
  failure campaigns on the physical cluster remain release qualification tasks.
- No release archive checksums are invented. The development vendored snapshot
  and explicit development binary configuration are clearly labeled. Public
  release/vendoring qualification follows docs/releasing.md.

SSH forwards explicitly disable connection sharing so cancellation cannot
leave listeners attached to an unrelated persistent master. This ownership
change passed an additional one-source/three-receiver forwarded cluster run.
A post-test audit found no test tags, relay processes or temporary transfer
directories on any of the four hosts; version/hash-addressed binary caches
remain available for subsequent runs.

The final arm64 binary (SHA-256
`7c2037e0e5f2dbf48f619e8d1ea5480fab6cf883f95a49321f21650c0d5c2a36`)
passed an additional direct transfer to all three receivers after cancellation
hardening: 33,564,854 acquired bytes, zero replays and 2,097,152 peak ring bytes.
The final static amd64 cross-build has SHA-256
`33f88324504ca8e2d0a4c7565c6b3eb850458c7a7100267ca71e1ec310a669ed`.
These identify local development builds, not release archive checksum pins.

## Subsequent large-image and SDK qualification

DeepSeek image benchmarking supersedes the earlier absence of fast-network
measurements. It also tested fixes
for per-blob push completion, failed-upload reservation release, and remote
process cancellation. The model deployment was stopped at the user's request;
later tests validate image import and identity without model startup.

The Moby client is now v0.6.1 (API types v1.56.1), with automatic Engine API
negotiation. Full Go race tests, including Docker source/export and receiver
integration tests, pass on Docker 29.1.3. Large-image direct imports into two
empty Docker 29.2.1 stores also pass. Fresh 32 MiB transfers from `host-a` to `host-d`
pass on the new binary over direct HTTP/2, forwarded HTTP/2, and SSH stdio,
with zero replays and 2,097,152 peak source ring bytes in every case.
All 16 plugin tests pass, including both
source modes over all three transports locally. Go vet, vulnerability scanning,
Python lint, version/workflow checks, dependency license generation, and
vendored snapshot verification pass after the upgrade.

An opt-in `docker-save` source now supports a full, capped, indexed archive;
this is an explicit additional staging mode, not the initial per-upload model.
Tests verify concurrent section reads, invalid archive entries, cancellation,
budget failure and cleanup. File sources allow concurrent bounded replay for
late readers, with regression coverage for aggregate buffer accounting.
The measured archive path does **not yet reliably beat SSH save/load** for a
fresh source. Existing layout input remains useful when already available.

New local development build SHA-256 values:

- Linux arm64: `6fea7e83a0eecea373517b087dbdbf89433071bef5034bdd1e5daa9f1ed2d679`
- Linux amd64: `bf00f917a47ac4156f04fa0a47d356b1b7bbea4dd98b3f965c0081914ecb13fb`

These supersede the earlier binary identities in this record; they remain
development artifacts, not published release checksum pins.

## Read-only classic-store source and measured improvement

The opt-in `docker-classic` adapter now avoids Docker's export/compression
preparation on rootful Docker 29.1.3/29.2.1 overlay2. It directly reuses upstream
tar-split reconstruction, confined file opens and mandatory full SHA-256 checks.
Its private-store access is explicit, read-only and held under a plugin-owned
image reference. Unsupported versions/drivers/user namespaces fail before pulls.

Native-source benchmarking measured two cold
25 GB image distributions to `host-b`/`host-c`: **120.329 and 122.140 seconds**, versus
prior builtin SSH save/load runs of **237.084 and 239.362 seconds**. Readiness
was about 1.5 seconds; every required unique layer was hash-verified, with zero
replays and exact receiver Docker IDs. Increasing Docker download concurrency
from three to eight shortened downloads but did not improve total time. Only
the disposable test daemons were reconfigured. A fresh 32 MiB child-layer update
reused the cached base and completed in 3.944 seconds, acquiring only that layer.

Additional tests cover exact original tar reconstruction (including links and
padding), preparation without payload reads, concurrent reconstruction,
corrupted file/header bytes, digest/parent mismatches, escaping paths, and
cancellation. Raw descriptors must match config DiffIDs. New metrics count
actual active/peak acquisitions and verified bytes/blobs; receiver phase times
expose the remaining extraction/registration tail.

The native source passed real `host-a` to `host-d` transfers over all three routes,
32 MiB each, zero replays and 2 MiB peak source rings. All 21 plugin tests passed, including a real helper-cancellation test. The new helper lifecycle
also handles indeterminate Docker-create results by tracking its owned name
before invoking Docker. Full Go race/Docker tests, the added parent-chain race
test, vet, both static builds, vulnerability scanning, Python lint and generated
version/workflow checks passed. Linked dependency license texts now include
tar-split v0.12.3.

Current development build SHA-256 values:

- Linux arm64: `b8359341ab1431fa71b98f67511246904d57bb3c226714d3e66eb6f8ccb98f44`
- Linux amd64: `26d4c8d2df8e6d5492eae0930b72a7ccd42cbee60684c43279f24999f5b7d1d3`

These supersede earlier build identities, without creating release checksum
pins. No public repository, commit, push or release was created.

The refreshed plugin snapshot is vendored into Sparkrun `develop-next` and its
recorded hashes verify. The 59 affected Sparkrun provider/plugin/transport/session
tests pass. Final cleanup audits on the controller and all four cluster hosts
found zero relay processes, operation/qualification/archive/benchmark directories,
or test image tags. The source helper containers, isolated benchmark daemons,
their owned volumes and temporary dind image tags were removed. Verified binary
caches remain. User Docker settings and unrelated images were left in place.

## Automatic source selection

The plugin now defaults to `source_mode: auto`, with the permission controls
and explicit source overrides preserved. Selection uses daemon/store/runtime
qualification, supplied-manifest constraints, requested transport/effective
speed, and (only for a possible full export) image size and free space on both
source filesystems. The selected mode and reason are logged before preparation
or destination pulls. See [selection policy](source-selection.md).

All 71 plugin tests pass, including real auto selections for native on all
three transports, archive, push and supplied-manifest modes. Unit tests cover
unsupported engines/stores/security modes, disk/budget boundaries, conservative
unknown/SSH-route behavior, explicit overrides, permission checks and immutable
settings. A simulated native config-digest failure remains fatal rather than
triggering another source. The dependency-isolated CI-style run passes 53 tests
and skips 18 tests requiring patched Sparkrun and/or Docker. Python lint and
generated version/workflow checks pass. No Go engine or protocol changed.

Fresh 32 MiB `host-a` to `host-d` transfers with `--source-mode auto` select
`docker-classic` on all three routes, verify the final Docker IDs, acquire
33,556,480 bytes each with zero replays, and stay at 2,097,152 peak source ring
bytes. This confirms policy/orchestration behavior; it does not rerun or change
the previous 25 GB throughput benchmark.

The auto-selection snapshot is vendored into Sparkrun `develop-next`; hash
verification and all 59 affected provider/plugin/transport/session tests pass.
Final audits on the controller and the two qualification hosts found no relay
processes, owned operation directories, test image tags or source helpers.
Verified executable caches remain; normal Docker settings were not changed.

## Startup optimization and receiver profiling (2026-10-04)

The plugin now overlaps independent host setup (bounded to eight workers),
acquires releases once per architecture, combines verified binary cache checks,
and batches host discovery into two commands while preserving cgroup ancestor
limits. Workers settle before failure cleanup and all routes precede pulls.
Normal logs include full provider phase/cleanup times and receiver download/
extraction milestones. No Go engine or integrity-boundary changes were needed.

Startup benchmarking measured setup-only
A/B measurements of 15.170–15.367 s before and 5.327–5.518 s after: about ten
seconds saved. The complete cold 25 GB/two-receiver provider call takes 120.692 s
with default Docker settings, now including binary checks, discovery, preflight
and cleanup. Exact image IDs match on both destinations. Five source replays
occurred; actual byte counts and phase boundaries were recorded.

Disposable-daemon CPU profiles expose receiver filesystem/hash/scheduling work.
GOMAXPROCS=2 trials reduce receiver import by roughly 19–26 seconds, but this
daemon-wide experiment is not an automatic plugin setting. Default settings
were restored for the final cold run; normal host daemons were not changed.

All 78 plugin tests pass, including real source/transport cases; the isolated
CI-style suite reports 60 passed / 18 skipped. New coverage includes setup
concurrency and failure barriers, cgroup ancestor caps with unknown routes,
literal shell arguments, and refusing to execute a tampered cached binary.
Python lint and repo-tools generated version/workflow checks pass.

The refreshed `develop-next` snapshot verifies and all 59 affected Sparkrun
tests pass. Real `host-a` → `host-d` 32 MiB transfers pass direct, forwarded and
stdio routes with native auto selection, exact IDs, zero replays and 2 MiB
peak rings. Final audits on the controller and all four cluster hosts show
zero relay processes, operation/test directories or fixture tags. Owned test
daemons, volumes and helpers were removed; executable caches remain.

## Ordered optimization experiments (2026-10-04)

Implemented and measured source coalescing, cache-aware import, relay-only
slot/CPU tuning and full native streaming load in the requested 3 → 2 → 4 → 1
order. All are opt-in; normal Docker settings and existing defaults remain
unchanged.

A 250 ms source join window reduced source acquisition from 42.95 GB to almost
exactly 25.00 GB, but did not improve total cold latency. Selective load reused
a compressed parent chain while receiving uncompressed layers: 34.61 MB fell
to 1.05 MB for the tested child, with exact final identity. Four relay slots
completed in 117.87 s vs 120.25 s for the control, an exploratory single-sample
difference; relay GOMAXPROCS=4 took 119.92 s. Full quiet native load took
135.06 s and does not justify replacing registry pull for cold copies.

Load modes require an explicit outer-archive byte cap and exact uncompressed
OCI layers; initial receiver qualification is Linux overlay2 Docker 29.1.3 /
29.2.1. Cached-chain detection uses bounded public APIs and a temporary base
tag. Missing cached chains, corrupt bytes and budget violations fail closed.
The relay streams the archive, but Docker stages it with buffered disk I/O;
the cap does not cover extracted storage or RSS. Tests preserve original config
bytes, enforce the archive cap, reject corruption and prove a missing cached
chain cannot produce a destination image.

Complete real-Docker Go race tests, vet, formatting, 88 plugin tests, Python
lint, generated version/workflow checks and linked-license verification pass.
The isolated CI-style Python run passes 70 tests / skips 18 integrations.
`govulncheck v1.1.4` on Go 1.25.14 reports no vulnerabilities.
Fresh final-binary native load copies pass direct HTTP/2, forwarded SSH and
stdio from `host-a` to `host-d`, with exact IDs and zero replays. Static development
build identities (superseding earlier local builds, not release pins):

- arm64: `92349cc3350a808b50653164628800b11c0a43ece5553df104ae2849db6bad5d`
- amd64: `43a544d419bb40959fdc1b5eb2eed692d6fd7b01beafffb06921494197b817ae`

The adapter is re-vendored into Sparkrun `develop-next`; 60 affected tests pass
and snapshot hashes verify. Content SHA-256:
`05c2165fa88aac2c4373065acca1b58bc7ceabe5310c5e51ff795e8f6f1750b4`.
Owned benchmark engines/volumes/helpers, fixture images and temporary operation
directories were removed. Final controller/four-host audits find zero relay
processes, operation directories or fixture tags. No model startup, normal
daemon tuning, commit, push or publication occurred.

## Independent plugin development (2026-10-04)

Removed the OCI Relay vendor snapshot, feature/loader binding, package metadata,
lint exclusion and vendor-specific CI/script from Sparkrun develop-next. The
generic image-distribution API and managed session support remain. A built
Sparkrun 0.4.0 wheel contains that API and no OCI Relay files. The unrelated
MODEL_CACHE_VALIDATION_DESIGN document and all other preexisting work remain.

`source dev.sh` now creates this repository's .venv, installs the selected
Sparkrun checkout and plugin editably, builds the relay with Go 1.25.14, and
verifies the installed entry point is enabled and registered. A private
.dev/config snapshot selects the provider; ordinary user config is untouched.
Initial config, cluster, registry and recipe snapshots retain local edits on
subsequent setup. Native source permission is explicit in that dev config, and
builtin fallback is disabled so tests cannot silently use a different provider.

Actual initial and repeated source/deactivate cycles pass, including restoration
of preexisting config and installed-plugin-disable environment variables. The
selected checkout is verified against the imported Sparkrun package, preventing
PYTHONPATH from silently selecting another host. Outside the dev config the
installed integration remains unselected and unregistered. Dev-configured
sparkrun status finds benchmark-cluster and its four idle hosts; no workloads launched.

91 plugin tests pass in the new environment, including real Docker lifecycle
checks and tests for private config preservation and explicit vendoring. 79
affected Sparkrun distribution/session/plugin/inventory tests pass without
the bundled relay. The dependency-isolated CI run passes 71 / skips 19. Shell
syntax, Python lint and relay generated version/workflow checks pass. The
rebuilt arm64 binary retains SHA-256
`92349cc3350a808b50653164628800b11c0a43ece5553df104ae2849db6bad5d`.

Future bundling stays in scripts/vendor-plugin.py in this repository. It adds
the snapshot plus disabled feature/loader bindings, package data and a Ruff
exclusion only when invoked explicitly; --check validates hashes and bindings.
Repeated vendoring and tamper rejection pass against disposable targets, and
the current develop-next file layouts also pass an isolated vendoring check.
The actual Sparkrun checkout remains unvendored. No commits or publication.

## Plugin permission defaults and initial repository publication

The plugin now defaults allow_native_store and allow_preparation_read to true,
including when no plugin settings are supplied. Validation returns a fresh
settings mapping, and source selection plus engine plans use those same values.
Explicit false values and strict boolean validation remain effective. Native
store qualification, full-archive budget requirements, integrity checks and
the standalone Go CLI's preparation opt-in are unchanged.

Real plugin lifecycle tests omit both permissions across all three source modes
and three transports. Policy tests cover omitted values, explicit independent
opt-outs, both permissions disabled, supplied manifests, immutable config and
propagation into provider execution. Sparkrun remains unvendored; development
continues through source dev.sh and the installed entry point.

Pre-publication validation passed: 93 plugin tests including real Docker,
72 passing / 20 skipped dependency-isolated CI tests, complete real-Docker Go
race tests, vet, formatting, Python lint, shell syntax, generated workflows /
versions and all 24 linked dependency license texts. Local dev configuration,
binaries, editor recovery files and private work notes are excluded from Git.

## Multiple TCP paths and transfer-only mode (2026-10-04)

Added explicitly bound data paths, per-connection scheduling/counters, safe
whole-blob failover, plugin route qualification and an import-free VERIFIED mode.
Benchmarking covered eight comparisons
of the 25 GB recipe image. Two connections reduced verification from 35.7 to
about 21–22 seconds whether using one NIC or two. Two-receiver verification
fell from 32.5 to about 22 seconds. All bytes retained source/receiver SHA-256
verification; no large-image Docker import timing claim is made for these runs.

Complete Go race tests with real Docker, vet, static arm64/amd64 builds, all
112 Python tests including real integration, Ruff, generated CI/version checks
and dependency notices passed. After batching route probes, the 19 affected
route tests and actual remote plugin import passed again. Fresh 32 MiB plugin
qualification verified the exact Docker image ID. Controller/four-host cleanup
audits found zero owned operations, helpers, test tags or relay processes.

## Cold import and transport profiles (2026-10-04)

Follow-up measurements covered four real plugin imports in A–B–B–A order,
resetting two isolated Docker stores before
every run. One versus two connections averaged 121.29 versus 113.58 seconds,
including provider setup and cleanup. Every target completed with the pinned
image ID. Source/receiver hashing remained enabled and no normal daemon settings
changed. Separate diagnostic profiles locate substantial receiver HTTP/2
flow-control write contention; global defaults remain unchanged.

After profiling and fixture removal, a controller/four-host audit found zero
relay processes, operation directories, helper containers, benchmark volumes or
test tags. No models ran. This follow-up changed documentation only; the binary
was the same build as the preceding tested multi-path implementation. The
[registry-source proposal](registry-source.md) records the pre-pull plugin hook,
Dragonfly reuse assessment and upstream-cache constraints for future work.


## Registry-backed source (2026-10-04)

Implemented registry metadata resolution, authenticated on-demand compressed
blob streaming, optional bounded disk retention, and the generic Sparkrun
pre-pull hook on develop-next. Existing image-copy providers remain compatible;
OCI Relay remains an installed plugin, not vendored source. Registry qualification checked
upstream byte counts, missing-layer reuse, warm skips and a fetcher that also
imports. Full validation includes real Docker over all transports, race tests,
credential/token/redirect and corruption tests, and a public Docker Hub download.
The initial direct Dragonfly OCI import was replaced with attributed parser
reuse to avoid its flagged legacy Docker dependency; the final vulnerability
scan is clean.

## Large registry image performance (2026-10-04)

Twelve distribution trials used one pinned public GHCR manifest on two physical
receivers, with isolated Docker 29.2.1 overlay2 stores. Every receiver matched
the exact config ID and all 30 RootFS DiffIDs. Measurements included
complete provider times, upstream bytes, partial-cache construction and baseline
timing boundaries. Fresh relay operations took 238–263 seconds versus 381
seconds for source pull followed by SSH save/load. A 313 MB missing-layer update
took 11–15 seconds versus 159 seconds for save/load from an already-ready source.
Direct pulls were comparable in latency; single trials and WAN variation limit
stronger claims. Disk retention had no hits and defaults remain unchanged.

All owned test daemons, volumes, spools and added fixture images were removed.
A controller/four-host audit found zero remaining owned runtime resources.
Normal daemon settings and workloads were untouched; no models ran. This
follow-up changes benchmark documentation only, using the previously tested
registry binary without rebuilding or modifying production code.


## Mixed overlay2 / containerd qualification (2026-10-05)

Native containerd content sources and per-receiver representation negotiation
are implemented for qualified Docker 29.2.1 containerd overlayfs stores,
alongside existing 29.1.3/29.2.1 classic overlay2 support. Config identity is now
separate from Docker's backend-dependent image ID. Public push/save paths also
handle containerd platform manifests correctly.

Four-way benchmarking completed 32 successful
trials on two physical Spark hosts, using isolated Docker daemons and a 1 GiB
four-layer workload. Both methods used the same fast interface. Relay reduced
cold time by 23–53% and partial-cache time by 49–70% versus SSH save/load.
Every partial trial transferred exactly one missing layer; cached layers used
zero local payload reads and zero network payload, including mixed stores.

Additional checks exercised actual plugin source/receiver helpers over direct
HTTP/2 and SSH stdio, a registry update into a containerd store with a raw cached
base, and a warm skip with differing source/installed manifest digests. Both
backends rejected valid compressed hashes paired with false DiffIDs and kept
the previous destination tag. A cleanup audit exposed and fixed a redundant
base-tag pin during moving-tag updates; the helper container now supplies that
pin, with a permanent integration regression assertion.

Go race tests (including real-Docker fixtures), Go vet, Python lint, and all 135
plugin tests passed; the 20 real plugin integration cases were rerun after the
pin-lifecycle fix. All four isolated daemons and their labeled volumes and
operation directories were removed. No model/application was started and no
normal Docker daemon was reconfigured. See [storage compatibility](storage-compatibility.md)
for exact store/version qualification, identity semantics and limits.

## Docker 29+ admission policy (2026-10-05)

Replaced exact Docker patch-version allowlists with a major-version baseline
of 29 or newer in plugin native/automatic archive selection, both Go native
adapters, and the optional load importer. Vendor/build suffixes are accepted;
older, unknown or malformed versions remain ineligible. Backend, native layout,
read-only access and content-integrity checks are unchanged. The receiver still
checks its supplied daemon facts against its actual Docker daemon.

Go race tests and vet pass. Plugin tests: 145 passed, 20 real-Docker integration
cases skipped for this policy-only change; Python lint and diff checks pass.
Regression tests exercise 29.0.0, later patches, future majors, vendor suffixes,
older/malformed versions, source adapters, receiver helper selection and the
load-importer gate. Static Linux arm64/amd64 development binaries were rebuilt.
These version fixtures do not expand the real-engine test matrix above.

## Expanded cache discovery and per-layer reuse (2026-10-05)

The Go inventory command now backs plugin and standalone discovery: all image
records are deduplicated and inspected in batches of 32 with eight concurrent
requests, bounded by a metadata time budget rather than the previous 128-entry
cutoff. The default budget is 10 seconds; plugin `cache_discovery_seconds` accepts
1–300. Partial discovery is reported explicitly and cannot authorize unknown
cache skips. Candidate metadata is revalidated against the actual source.

Native receiver reuse now combines layers from multiple images, including
matching DiffIDs under different parents. Classic store readers scan only
selected tar-split metadata and retain donor images with never-started owned
containers. Containerd probes requested blob paths and retains open files,
including bytes without an image record. Negotiation distinguishes a matching
parent chain, locally available blob bytes, and missing bytes, and requires an
explicit acknowledgement of cache schema 2. Config and DiffID identity remain
unchanged; cached bytes still pass full SHA verification when read.

Cache benchmarking completed 16 passing
trials on isolated overlay2/containerd receivers with raw/gzip sources. Native
reuse fetched one missing layer and reused two from other parent chains. It
reduced source bytes by roughly two thirds except when containerd ordinary pull
already reused the exact gzip blobs. Small loopback transfers sometimes took
longer due to discovery/retention overhead; this does not establish a universal
speedup. All owned test daemons, volumes and retention containers were removed.

Go race tests, including real-Docker pull/load/integrity fixtures, and Go vet
pass. Plugin regression coverage includes two new real registry tests over
direct HTTP/2 and SSH stdio, plus discovery-budget validation. Tests exercise
late inventory matches, duplicate IDs, bounded concurrency, partial discovery,
GC unlink retention, selected metadata reads, corruption, confinement, protocol
validation and cleanup after cache-retention failures.

## Default-visible image-transfer progress (2026-10-06)

The plugin reports preparation, receiver cache discovery, per-host layer bytes,
throughput and reuse, import, verification, cleanup, and a final summary through
Sparkrun's existing `PROGRESS` level. A 30-second heartbeat covers silent setup
and preparation. Registry upstream bytes remain separate from receiver totals.
Expected transfer size is an upper bound because Docker can discover additional
cache hits; byte progress never changes success or integrity decisions.

Receivers send bounded advisory snapshots over the existing authenticated
HTTP/2 connection. Reporting has its own cancellable goroutine and a two-second
request deadline, with no unbounded event queue or data-worker dependency.
Counters distinguish unique received offsets from retry/replay payload traffic.

Validation passed:

- Full Go race suite, including real Docker pull/load and integrity checks;
  Go vet and Python lint.
- Full plugin suite, followed by a focused rerun of progress and all 25 real
  plugin integration cases after final rendering/cleanup refinements.
- Deliberately slowed fresh registry pulls on direct HTTP/2, SSH forwarding,
  and SSH stdio: default-visible nonzero byte updates arrived before image
  verification, with exact final config/DiffIDs and separate registry status.
- Fresh, partial-cache, and already-present registry cases on all three
  transports: summaries count only transferred payloads, report a reused base
  for partial updates, and zero transferred bytes for warm images.
- Concurrent replays and partial retries do not inflate unique-byte progress;
  spoofed, malformed, oversized and stale progress cannot change final results;
  cancellation stops a stalled reporter without waiting for its timeout.
- Rendering distinguishes import/verification from transfer, throttles updates,
  preserves cleanup warnings, and avoids reporting unknown totals as zero.

Linux arm64 and amd64 development binaries were rebuilt. Disposable test image
tags are removed by the fixtures; no model/application is launched and no Docker
daemon configuration changes are needed.
