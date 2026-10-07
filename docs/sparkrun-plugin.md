<!--
SPDX-FileCopyrightText: 2026 Scitrera LLC
SPDX-License-Identifier: AGPL-3.0-only
SPDX-FileComment: The Sparkrun additional permission in LICENSE_EXCEPTION applies.
-->

# Sparkrun plugin

The plugin handles local-image copies and can overlap registry downloads with
multi-host distribution through the pre-pull hook. With `source_mode: auto`, a
missing source image, explicit fresh pull, or controller-local `:latest` refresh
uses the registry directly. Existing versioned/local-build images and unforced
delegated sources retain core's local-image policy. Core retains build/offline,
Coldsnap materialization and launch ordering. Explicit per-node pull mode and
local-only ensure operations retain builtin behavior. See
[registry sources](registry-source.md) for selection, authentication and cache limits.

Digest-pinned recipes (`image@sha256:...` or `image:tag@sha256:...`) also use
OCI Relay, including index pins. Updated `develop-next` hosts expose
`IMAGE_RUNTIME_API_VERSION = 1`, which lets the plugin pass verified per-host
Docker IDs to the launcher while preserving the requested recipe pin. Warm
and offline runs can recover those bindings from private host receipts. See
[digest-pinned recipes](registry-source.md#digest-pinned-recipes).

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
container_distribution_provider: auto
plugins:
  oci-relay:
    source_mode: auto  # Default; choose a permitted source from detected facts.
    allow_native_store: true  # Default; set false to prohibit native-store access.
    allow_preparation_read: true  # Default; set false to prohibit full preparation.
    transport: auto
    registry_source: true  # Default; overlap missing-image pulls with distribution.
    registry_cache_bytes: 17179869184  # Up to 16 GiB disk; keep 16 GiB free. Zero disables.
    # Optional development override; releases download verified binaries:
    # development_binary: /absolute/path/to/oci-relay
```

The installed distribution exposes the `sparkrun.plugins` entry point named
`oci-relay`. Future bundling is an explicit operation described in [development](development.md#versions-ci-and-vendoring); choose
one registration path and do not enable installed and bundled copies together.

## Binary distribution

Execution hosts currently require Linux amd64/arm64. A Mac controller can
coordinate delegated execution on Linux cluster nodes without running a relay
locally; see [platforms and controller placement](platforms.md). The standalone
macOS arm64 archive does not yet enable a controller-local plugin source.

For a locally supplied production binary, configure `binary_paths` and
`binary_sha256` maps keyed by `amd64`/`arm64`. The controller verifies the ELF
architecture and hash, stages it to each execution host, verifies the remote
hash, and checks the version/protocol before use. Hosts do not need internet
access. Offline mode never downloads a binary. `releases.json` pins the published archives for each supported architecture.
The adapter is pinned after the binary release, without moving the engine tag.

Source binaries from v0.1.2 advertise `registry-range-v1` and automatically parallelize
large upstream layer downloads. Plugin settings can adjust concurrency, range
size, threshold and the shared buffer budget; see
[parallel upstream ranges](registry-source.md#parallel-upstream-ranges).
This is separate from receiver link striping and requires v0.1.2 or newer.

With updated Sparkrun `develop-next`, automatic provider selection falls back to
builtin Docker distribution if the relay release is unavailable (including
network errors, timeouts, or offline mode without a cached release), or the
provider cannot support the request before transfer. A warning explains the
fallback. A verified controller cache works without internet; nodes receive
binaries over SSH. Offline fallback still requires an available local image.

Set `container_distribution_fallback: false` to disable fallback, or explicitly
select `container_distribution_provider: oci-relay` to require the relay.
`source dev.sh` keeps these strict settings for testing. Invalid configuration,
checksum/certificate failures, and started transfers remain errors. Older hosts
require `container_distribution_fallback: true` to enable fallback in `auto` mode.

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
`connections_per_path: 1` is the default; values 2–4 open additional independent
HTTP/2 connections on each configured path without increasing acquisition or
buffer budgets. This also works with a single configured path and lets you
separate connection contention from the benefit of another NIC. Counters include
the connection number; a network failure disables that connection for the session.

With a stripe-capable binary at both ends, layers **at least 256 MiB** use
within-layer striping whenever two or more qualified connections are available.
Alternating 1 MiB pieces travel over up to four distinct connections, preferring
different links. One source acquisition supplies every lane, including live
registry streams and reconstructed classic-store tar streams: striping does not
fetch the layer separately for each connection or stage another payload file.
Receivers reassemble pieces in order into the existing cache and SHA-256 verifier.
Source verification still withholds the layer tail; corruption cannot complete
an import. A failed lane cancels that layer acquisition and retries the whole
layer on surviving connections. Piece-level resume is not implemented.

`stripe_threshold_bytes` overrides the threshold (0 disables; otherwise 8 MiB
to 1 PiB). `stripe_streams` sets the per-layer connection cap (2–8, default 4).
`stripe_piece_bytes` sets the piece size (powers of two from 1–64 MiB, default
1 MiB). The source advertises its supported maximum; a source with the original
striping implementation uses 1 MiB even when a larger size is requested. The
receiver limits lane count to the number of full pieces in the layer and uses
an ordinary stream when fewer than two fit. Results include effective
`stripe_piece_bytes` on each used connection.
The cap does not create connections: use both configured paths, or increase
`connections_per_path` on a single path. Small layers, one remaining connection,
SSH transports without explicit paths, and older sources use ordinary streams.
Stripe and receive-window overrides require a supporting binary; default plugin settings
remain compatible with older releases. Results expose `stripe_requests` per
connection. No RDMA transport is included.

The existing managed payload-memory and acquisition limits still apply. Stripe
readers share source cache frames and the receiver needs no separate reassembly
ring; HTTP/2/TLS/socket buffers remain additional bounded transport overhead,
as with ordinary transfers. Slow lanes backpressure the source and eventually
fail under the existing lag/write deadlines. Incomplete lane groups expire and
release their readers; session cleanup also cancels outstanding groups.

For controlled transport experiments, `http2_stream_window_bytes` overrides the
HTTP/2 receive credit **per stream** (0 preserves Go's default; otherwise powers
of two from 1–64 MiB). The pinned Go toolchain defaults to 4 MiB per stream.
This is not a total-memory budget: larger windows permit more buffered data on
each active stripe, in addition to the managed relay cache and other overhead.
For example, four active lanes with 32 MiB windows can buffer up to 128 MiB for
one layer; several concurrent layers multiply that allowance. The plugin does
not automatically enlarge these windows. Per-path results expose an explicit
override as `http2_stream_window_bytes`. Benchmark piece size and receive window
together before changing either; pieces are portions of persistent streams,
not separate TCP packets or HTTP requests.

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
| 100–199 Gbps | 1 GiB | 16 |
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
verification, and cleanup. Byte updates are limited to about once every 30
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

## Bundled receiver decoder

New native release bundles include a private, verified `unpigz` helper. The
plugin automatically selects it for missing gzip layers on overlay2 receivers
when disk headroom permits. See [selection, budgets and settings](bundled-decoder.md).
