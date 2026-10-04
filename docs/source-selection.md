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
native-only operation when available. Disabling both requires a supplied
manifest or reports unsupported before transfer. These defaults do not change
the Go CLI's explicit `--allow-preparation-read` flag. Supplying
an exact manifest permits the push source without a full preparation pass.
A dry run logs the requested policy and defers host-dependent selection until
execution, without opening a host session or starting any processes.

## Decision order

| Circumstance | Choice |
|---|---|
| Explicit `source_mode` | Honor it; validate its existing permission/manifest requirements |
| Supplied manifest under auto | `docker`; preserve exact manifest bytes and compressed representation |
| Native access allowed, Docker 29.1.3/29.2.1, Linux overlay2, rootful/no user namespace, `runc`, bindable store path | `docker-classic` |
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

Archive auto-selection is intentionally conservative:

- Limited to the tested Docker 29.1.3/29.2.1 overlay2 exporters. Explicit
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
the much longer push-compression path on this image. See the
[benchmark](benchmarks/native-2026-10-03.md). These measurements do not establish
that uncompressed native transfer wins on every slow link, or that the staged
archive beats builtin save/load. Explicit overrides remain available.

Future improvements can incorporate measured throughput/preparation history,
receiver missing-layer estimates, already-prepared OCI layouts and native
containerd content. The current policy does not scan for layouts, migrate image
stores, benchmark live images during selection, or weaken qualification for a
new Docker version. Existing whole-image matches are still skipped by Sparkrun
before provider invocation; partial compressed-to-uncompressed cache behavior
is unchanged.
