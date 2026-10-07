# Automatic source selection

Source selection is Sparkrun plugin policy. The Go relay still receives an
explicit mode, buffer limits and staging limits for one operation. The plugin
now defaults to `source_mode: auto`; existing explicit modes remain overrides.
No global Docker configuration or storage format is changed.

Typical configuration on the qualified Spark cluster:

```yaml
plugins:
  oci-relay:
    source_mode: auto
    allow_native_store: true
    allow_preparation_read: true
    transport: auto
```

The access/preparation booleans are permissions, independent of source choice.
Both `allow_native_store` and `allow_preparation_read` default to `true` in the
plugin; explicit `false` values are preserved. Disabling native access leaves
public-API source selection available. Disabling `allow_preparation_read` allows
native-only operation when available. For local-image copying, disabling both requires a supplied
manifest or reports unsupported before transfer. Registry sourcing needs neither
permission. These defaults do not change
the Go CLI's explicit `--allow-preparation-read` flag. Supplying
an exact manifest permits the push source without a full preparation pass.
A dry run logs the requested policy and defers host-dependent selection until
execution, without opening a host session or starting any processes.

## Registry pre-pull selection

Before local source preparation, the optional Sparkrun pre-pull hook lets auto
use a registry source for a missing source image or an explicit fresh pull.
Existing local images keep core's established refresh/local-copy path. Offline
mode never uses the registry. The fetcher need not import the image unless it is
also a destination; delegated mode includes the head among receivers. Set
`registry_source: false` to disable automatic selection, or `source_mode: registry`
to explicitly choose it. Credentials stay on the fetcher and
`registry_cache_bytes` is a separate compressed-blob disk budget, defaulting to
16 GiB in the plugin with a 16 GiB free-space floor; zero disables retention.
See [registry sources](registry-source.md) for compatibility and limits.

## Local-image decision order

| Circumstance | Choice |
|---|---|
| Explicit `source_mode` | Honor it; validate its existing permission/manifest requirements |
| Supplied manifest under auto | `docker`; preserve exact manifest bytes and compressed representation |
| Native access allowed, Docker 29+, Linux overlay2, rootful/no user namespace, `runc`, bindable store path | `docker-classic` |
| Native access allowed, Docker 29+, Linux containerd overlayfs, rootful/no user namespace, `runc`, qualified content directory | `docker-containerd` |
| Native unavailable/disallowed, qualified archive engine/store, permitted preparation and explicit budget, fast forced-direct route, adequate disk | `docker-save` |
| Archive not preferred/feasible, full preparation allowed | `docker` push source |
| No permitted source | Unsupported before pulls; let Sparkrun's configured builtin fallback policy decide |

For native qualification the source Docker daemon is queried through its
explicit local Unix socket, independently of the user's Docker CLI context.
The helper rechecks qualification before accessing the store. The plugin reads
only selected daemon fields and image size for policy; it does not log image
config/env or credentials. Explicit source modes and supplied manifests skip
auto-selection probes. Settings are copied per operation, so a choice for one
host does not become the choice for another host or later operation.

Docker version checks require major version 29 or newer, with no patch allowlist
or upper bound. Vendor/build suffixes are accepted. This baseline does not replace
backend, layout or integrity checks; measured versions are recorded separately in
[storage compatibility](storage-compatibility.md).

Archive auto-selection is intentionally conservative:

- Limited to Docker 29+ overlay2 exporters. Explicit
  `docker-save` remains available for separate qualification elsewhere.
- Requires `allow_preparation_read: true` and an explicit `max_spool_bytes`.
- Requires **requested** `transport: http2-direct` and an effective route hint
  of at least 25 Gbps, from discovery or `network_gbps`. A hint is not measured
  throughput; SSH/unknown/unnegotiated-auto routes do not qualify for auto staging.
- Estimates archive bytes as Docker image Size plus the greater of 25% or 4 MiB.
  This must fit the explicit cap. The exact tar size can differ; runtime cap
  enforcement remains mandatory.
- Uses read-only free-space probes on the source spool filesystem and Docker
  data-root filesystem. Each must have at least twice the estimated archive
  size, conservatively allowing for relay staging and Docker export scratch
  even when the two directories share a filesystem. Failed or unknown disk/size
  probes rule out auto staging. Free space is not reserved and can change.

## Bulk reads and source pipelining

On Linux, bulk local-file reads prefer aligned `O_DIRECT` I/O, including native
overlay2 payloads, containerd content, OCI layouts, staged archive sections and
retained registry blobs. Native overlay2 payload files below **1 MiB** remain
buffered: direct I/O for each small file substantially slowed reconstruction in
the real-image cutoff sweep. Larger native files use direct reads regardless of
page-cache warmth. Metadata reads and all staging writes remain buffered.
The reader opens an independent descriptor for the already validated
inode, preserving root confinement, pinned files after unlink, and concurrent
section reads. It does not change Docker's descriptors, daemon settings or
global page cache. Unsupported filesystems, alignment or descriptor reopening
fall back to buffered reads; genuine I/O errors still fail the acquisition.
Other operating systems use buffered reads.

Direct reads use at most 1 MiB plus alignment padding (less than 64 KiB) of
scratch space per active file reader; small files use smaller aligned buffers.
This bounded scratch space is separate from the managed transfer ring, like
other source, TLS and runtime allocations. Native reconstruction opens payload
files sequentially within each active layer acquisition.

The source overlaps reads and tar reconstruction/CRC checks with SHA-256 and
cache writes using four 64 KiB queue frames per acquisition. Those frames are
reserved **inside** `max_buffer_bytes`. Budgets below 1 MiB per acquisition use
the synchronous path. Receiver caches retain their existing synchronous path.
Every full-layer SHA check and native tar-split CRC check remains enabled, and
the final frame remains withheld until both source completion and SHA verification
succeed. This requires an updated relay binary; plugin settings alone do not
enable it in older releases.

## Failures, identity and observations

The chosen mode and its reason are logged before helper creation/preparation
and before any destination pull. A permitted public mode may be selected when
native capability checks reject the store. Unsupported selection can use
Sparkrun's builtin fallback only under the existing explicit configuration:
`container_distribution_provider: auto` and
`container_distribution_fallback: true`.

This is selection, not a retry ladder. Failure after selecting a source,
including inaccessible/corrupt metadata, config or layer hash mismatch, archive
cap exhaustion, helper failure and transfer failure, remains an error. Auto
never discards a supplied manifest to obtain another representation, and never
switches modes in the middle of a transfer. All existing integrity checks remain.
A failed Docker-daemon probe is surfaced, rather than treated as proof that
another Docker source is usable.

The initial native preference follows the measured 120–122-second cold copy
versus 237–239-second builtin save/load comparison; archive staging also avoided
the much longer push-compression path on this image. These measurements do not
establish that uncompressed native transfer wins on every slow link, or that
the staged archive beats builtin save/load. Explicit overrides remain available.

Future improvements can incorporate measured throughput/preparation history,
receiver missing-layer estimates and already-prepared OCI layouts. The current policy does not scan for layouts, migrate image
stores, benchmark live images during selection, or weaken qualification for a
new Docker version. Existing whole-image matches are still skipped by Sparkrun
before provider invocation. Qualified receivers now inventory matching DiffID
chains and negotiate cached-layer representations across stores; see
[storage compatibility](storage-compatibility.md). Containerd native access and
receiver helpers follow the same explicit `allow_native_store` opt-out.
