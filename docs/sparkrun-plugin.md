<!--
SPDX-FileCopyrightText: 2026 Scitrera LLC
SPDX-License-Identifier: AGPL-3.0-only
SPDX-FileComment: The Sparkrun additional permission in LICENSE_EXCEPTION applies.
-->

# Sparkrun plugin

The plugin handles local-image copies and can overlap registry downloads with
multi-host distribution through the pre-pull hook. With `source_mode: auto`, a
missing source image or explicit fresh pull uses the registry directly; existing
local images retain core's source/refresh policy. Core retains build/offline,
Coldsnap materialization and launch ordering. Explicit per-node pull mode and
local-only ensure operations retain builtin behavior. See
[registry sources](registry-source.md) for selection, authentication and cache limits.

Sparkrun `develop-next` can bundle the release adapter using its
`vendor-oci-relay.py` script. Its `plugins.oci_relay` feature defaults on for
alpha and off for stable/beta; explicit feature overrides take precedence.
With the bundled provider enabled, normal `container_distribution_provider: auto`
selects it. To opt in on stable/beta (or set `false` to opt out on alpha):

```yaml
features:
  plugins.oci_relay: true
```

The independently installed plugin remains available. `source dev.sh` enables
the editable installed adapter and disables the bundled gate in private dev
configuration. For a manual installed adapter, disable the bundled gate and use:

```yaml
features:
  plugins.oci_relay: false  # Avoid loading the bundled copy as well.
integrations:
  oci-relay: true
container_distribution_provider: oci-relay
plugins:
  oci-relay:
    source_mode: auto  # Default; choose a permitted source from detected facts.
    allow_native_store: true  # Default; set false to prohibit native-store access.
    allow_preparation_read: true  # Default; set false to prohibit full preparation.
    transport: auto
    registry_source: true  # Default; overlap missing-image pulls with distribution.
    registry_cache_bytes: 0  # Optional separate disk budget for retained compressed blobs.
    # Optional development override; releases download verified binaries:
    # development_binary: /absolute/path/to/oci-relay
```

The installed distribution exposes the `sparkrun.plugins` entry point named
`oci-relay`. Future bundling is an explicit operation described in [development](development.md#versions-ci-and-vendoring); choose
one registration path and do not enable installed and bundled copies together.

## Binary distribution

For a locally supplied production binary, configure `binary_paths` and
`binary_sha256` maps keyed by `amd64`/`arm64`. The controller verifies the ELF
architecture and hash, stages it to each execution host, verifies the remote
hash, and checks the version/protocol before use. Hosts do not need internet
access. Offline mode never downloads a binary. `releases.json` pins the published archives for each supported architecture.
The adapter is pinned after the binary release, without moving the engine tag.

## Transports

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

## Multiple data paths

Optional `plugins.oci-relay.data_paths` assigns explicit data addresses to
execution hosts. For example (documentation addresses; replace with your own):

```yaml
data_paths:
  - controller: 192.0.2.1
    host-a: 192.0.2.2
    host-b: 192.0.2.3
  - controller: 198.51.100.1
    host-a: 198.51.100.2
    host-b: 198.51.100.3
```

Keys must match the source/target execution hosts passed by Sparkrun; `controller`
means a controller-local source. Include remote source hosts in each map too.
This requires `auto` or `http2-direct`. The plugin checks directly connected
routes, assigned local addresses and distinct interfaces at both ends, then
preflights every route with the session credentials. Unavailable paths degrade
to surviving links; `auto` can use SSH when none pass. It does not change routes,
configure bonding, or infer a second data network from nearby IP addresses.

Each path gets a separate, locally bound TCP/HTTP2 transport. New **whole-layer**
requests go to the connection with the fewest assigned in-flight bytes, sharing the
existing memory/concurrency budget. A broken path is disabled for that operation.
Failure before any bytes are written can use another path immediately; a partial
blob fails the acquisition and requires a whole-blob retry. TLS identity errors,
protocol errors and receiver digest mismatches fail validation. Source and
receiver SHA-256 checks remain enabled. Per-path payload bytes, requests,
failures and peak active requests are included in receiver results and logs.
This is layer-level distribution, not within-layer piece striping or RDMA.
`connections_per_path: 1` is the default; values 2–4 open additional independent
HTTP/2 connections on each configured path without increasing acquisition or
buffer budgets. This also works with a single configured path and lets you
separate connection contention from the benefit of another NIC. Counters include
the connection number; a network failure disables that connection for the session.

## Setup and adaptive resource limits

Independent host setup runs concurrently, with at most eight setup workers.
Binary verification is retained on every operation; release acquisition is
shared per architecture. Discovery batches host facts into two commands per
host, including cgroup ancestor caps. All routes must pass preflight before
any receiver pull starts. Failed setup settles its workers before cleanup.
The plugin logs binary/discovery/session/preparation/preflight/import/cleanup
times and a total including cleanup, plus each receiver's download/import
timings.

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
With explicit, qualified `data_paths`, it sums the per-path effective hints for
budget sizing, still applying CPU/memory limits. Preflight fallback may leave
the already allocated source budget larger than the surviving route needs.
Explicit `max_buffer_bytes` and `source_streams` override the
policy. `max_spool_bytes` defaults to 8 GiB, and `max_upload_bytes` defaults to
that total. A push-source blob must fit the upload cap; metadata preparation
is separately measured. Image groups are serialized within the plugin process.
Separate Sparkrun invocations do not share a global resource governor.

## Failure and fallback policy

Provider failures are surfaced. Archive fallback requires both
`container_distribution_provider: auto` and
`container_distribution_fallback: true`, and is allowed only when a provider
reports that it cannot support a request before destination transfer starts.

## Progress output

The Sparkrun plugin shows image-transfer status at default verbosity, using
Sparkrun's `PROGRESS` logging level. It reports preparation, cache discovery,
per-host received layer bytes and throughput, known reused layers, Docker import,
verification, and cleanup. Byte updates are limited to about once every five
seconds per host; observed phase changes appear immediately. Quiet preparation
steps emit a heartbeat after 30 seconds. Registry download bytes are displayed
separately from bytes sent to receivers. Detailed metrics remain available at
`-v`. No Sparkrun API change or extra plugin configuration is required.

The byte total is labeled **up to**: Docker can find additional cached layers
after the relay's inventory. Unique received bytes do not double-count retries;
transfer rates and final transferred totals include retried/replayed layer
payloads, excluding metadata and transport overhead. A full byte counter never
means the image is ready: success still requires Docker import, image identity
checks, final tag verification, and reported cleanup. Preparation and extraction
have phase/elapsed status rather than a fabricated percentage or ETA.


See [standalone progress events](standalone.md#progress-events) for the event fields.

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

`load-cached` uses bounded parallel discovery across deduplicated receiver
images, including dangling/intermediate records, with a 10-second metadata
budget. It looks for an exact prefix of the requested DiffID chain, including
every parent.
It pins the best matching base with a temporary tag, streams only missing raw
layers into a Docker load archive, and preserves the original image config.
This can reuse layers across compressed and uncompressed representations when
registry pull would download them again. A timed-out scan reports incomplete discovery and may miss unexamined images.
With no usable prefix, an unsupported store or compressed input, selection
returns to normal pull **before transfer**. Errors after import begins are
fatal; they do not trigger another importer or weaken verification.

`load` streams all raw layers to the same importer without a cache probe.
Both load modes require Linux overlay2 Docker 29 or newer, exact uncompressed
OCI descriptors, and an explicit `max_import_bytes` (4 MiB–1 PiB).
The cap covers the complete outer tar archive,
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
