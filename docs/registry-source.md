# Registry-backed source

Implemented for the standalone relay and the separately installed Sparkrun
plugin, targeting `develop-next` / v0.4.0 with image-copy API 1 and the optional
pre-pull API 1. Digest-pinned recipes also use the additive image-runtime API 1.
The adapter can be installed independently or bundled through Sparkrun's vendor script.

## Data path

```text
Registry -> source fetcher + bounded cache -> receiver relay -> Docker pull
                                         -> receiver relay -> Docker pull
                                         -> fetcher Docker, only if also a target
```

The source resolves a tag or verifies a requested digest, selects one platform, pins the exact manifest
and config, then fetches requested blobs by digest. It streams the registry's
compressed bytes without first pulling into source Docker, unpacking, exporting
or recompressing. Docker receivers still verify and extract their layers.
These reads use the [OCI pull API](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pull).

The existing bounded cache, authenticated peer transport and loopback receiver
registry provide the data path. `--source registry` uses the same source
interface as local Docker and OCI-layout adapters. Source and receiver SHA-256
checks remain enabled. Manifest/config metadata is limited to 4 MiB, index
resolution to eight nesting levels, and descriptor counts remain bounded.
Compressed blob hashes remain distinct from config diffIDs. Final stream
completion is withheld until verification succeeds.

## Sparkrun selection

Core's additive `try_image_pull()` hook runs before the controller/head source
pull in `distribute_image_from_local()` and `distribute_image_from_head()`.
Providers may implement `pull(ImagePullRequest)`; providers with only the
existing `copy()` method continue through the previous path. `None` declines
before transfer. A started operation's partial failure raises
`ImageDistributionFailed`, preventing an outer delegated fallback from silently
selecting another mutable-tag identity.

The relay plugin's default `source_mode: auto` uses registry sourcing when the
fetcher has no local copy, a fresh pull is explicitly requested, or Sparkrun
would refresh a controller-local pullable `:latest`/untagged image. That refresh
does not first download and import the image into the controller's Docker
store: OCI Relay resolves the manifest and streams needed layers directly to
the receivers, with status from source selection onward.

Existing versioned tags and local-build names retain core's local-image path.
Delegated sources also keep their existing local copy unless a fresh pull is
requested. Explicit local source modes and supplied manifests retain their
meaning. Offline mode never contacts the registry.
`source_mode: registry` explicitly chooses the registry when online.

A best-effort controller `:latest` refresh may reuse the original cached image
if registry metadata preparation fails **before any receiver starts**. The
fallback pins the inspected Docker image ID, reports the reason, and shares the
original operation deadline. Failures after source readiness, including partial
transfers and corrupt payloads, do not switch to an older image. Forced pulls,
explicit registry mode, and missing source images never use this cached fallback.

Routine receiver and registry byte updates use a 30-second cadence; phase
changes and completion remain immediate, with a 30-second heartbeat during
silent work. Explicit builtin Docker pulls also announce their start and emit
Sparkrun's normal 30-second heartbeat.

In controller distribution, only the requested targets import the image; the
controller acts as a fetcher without a Docker import. In delegated distribution,
the head is both fetcher and receiver and imports alongside the workers. A
colocated receiver bypasses distinct-interface qualification. The plugin chooses
the target architecture, even when the controller architecture differs, and
requires one target platform per operation. Build/Coldsnap ordering is unchanged.
Explicit per-host `pull` mode and local-only `ensure_image` operations retain
Sparkrun's builtin behavior.

Receivers using `--skip-present` compare the exact pinned config ID and platform
with the existing destination tag before importing. Matches report COMPLETE
with `already_present: true`. Docker's ordinary pull handles partial layer
reuse; the source fetches only blobs actually requested. The plugin does not
assume the Docker API can enumerate all reusable compressed blobs.

```yaml
plugins:
  oci-relay:
    source_mode: auto
    registry_source: true       # Default: automatic pre-pull registry selection.
    registry_cache_bytes: 0     # Default: no payload disk cache.
    # Optional retained compressed blobs, e.g. 8 GiB:
    # registry_cache_bytes: 8589934592
    # registry_config: /absolute/path/to/docker/config.json  # On the fetcher.
    # registry_plain_http: true  # Explicitly trusted HTTP registries only.
```

Set `registry_source: false` to disable automatic pre-pull selection; an explicit
`source_mode: registry` still wins. Existing adaptive concurrency and memory
settings apply. Registry source does not require native-store access or a full
preparation read, and requires `receiver_import: pull`.

## Upstream sharing and disk budget

The memory ring shares overlapping readers but evicts old prefixes. A late
receiver can require another acquisition; `ConcurrentReplay` remains disabled
for registry sources, but that alone cannot eliminate later refetches.

An optional `registry_cache_bytes` budget retains compressed blobs for the
operation. The first requester streams while the same bytes are written using
ordinary buffered file I/O. A retained blob becomes readable by later
acquisitions only after exact size and digest verification. Files are private
and removed on cleanup. This adds no full-image preparation barrier.

Reservations cover whole blobs before writing and never exceed the byte budget.
Completed blobs remain until operation cleanup. A blob that does not fit the
remaining budget streams without retention, increments `disk_cache_bypasses`,
and may be downloaded again. This is a bounded first-fit cache, not an LRU or
persistent cache. A filesystem write failure fails the acquisition and removes
its incomplete file; it does not silently continue as a successful cache fill.

The disk budget is separate from `max_buffer_bytes` and Docker push staging's
`max_spool_bytes`. OS page cache, TLS and runtime memory remain outside the
managed ring allocation. Size retention to cover the required compressed blobs
when avoiding refetch is important; there is no unconditional single-download
guarantee under failures or an insufficient cache budget.

Source events and plugin logs expose `registry_metrics`: upstream blob bytes,
blob download count, retained-cache hits/bypasses, current/peak reserved disk
bytes, and metadata bytes. These are application byte counts, excluding HTTP/TLS
overhead. Source cache acquisition bytes can exceed upstream bytes when a
retained file satisfies a later receiver.

## Credentials and protocol behavior

Credentials stay on the selected fetcher. The source reads its Docker
`config.json` from `DOCKER_CONFIG`, the default user directory, or an explicit
`registry_config` path. It supports per-registry credential helpers, the default
credential store, inline auth, identity refresh tokens and registry tokens.
Helper output is bounded and helpers have a deadline. Receiver mTLS credentials
are separate; no registry secret appears in command arguments or relay events.

HTTPS verifies certificates using the host trust store. The CLI has no
skip-verification switch. Plain HTTP requires explicit opt-in. Bearer token
requests use repository pull scope; refresh is coalesced across concurrent
requests. Challenge parsing reuses Distribution's parser. Token responses are
bounded; auth-service redirects are refused. Blob redirects strip credentials
when leaving the original origin, including chains of CDN redirects. Errors
omit response bodies and signed URLs. HTTP content decompression is disabled so
hashes cover the exact manifest representation.

HTTP 429/503 and authentication retries are bounded. Partial bodies are not
appended to a new attempt; the existing acquisition/receiver retry mechanism
starts the blob again. HTTP Range resume is not implemented.

## Dragonfly reuse

`registry_reference.go` adapts reference normalization and URL construction from
Dragonfly's [`pkg/oci`](https://github.com/dragonflyoss/dragonfly/tree/41b312389c90e4f3c44adde8bbd8aa11888076d4/pkg/oci)
at the existing pinned revision, retaining Apache-2.0 licensing and attribution.
An attempted direct import linked its legacy Docker daemon dependency and
failed `govulncheck`; the final build avoids that dependency and passes the scan.
The existing direct Dragonfly rolling-window reuse is unchanged.

The high-level upstream resolver also uses an unverified default TLS client and
unbounded manifest reads, so relay supplies its own bounded resolver, transport,
authentication flow and cache. The Distribution challenge parser is directly
imported. Details and redistributed licenses are in
[third-party notices](../THIRD_PARTY_NOTICES.md).

## CLI and limits

```sh
oci-relay prepare --source registry --image docker.io/library/alpine:3.22 \
  --platform linux/arm64 --output manifest.json

# After creating a private session and distributing receiver credentials:
oci-relay run --source registry --image example.org/team/image:tag \
  --platform linux/arm64 --session-dir /private/session \
  --listen 0.0.0.0:9443 --advertise SOURCE_IP \
  --registry-cache-bytes 8589934592 --spool-dir /private/cache

oci-relay peer --endpoint https://SOURCE_IP:9443 \
  --session /private/node1.json --tag example.org/team/image:tag \
  --replace-tag --skip-present
```

The standalone source accepts a tag or SHA-256 digest; a receiver still needs a
writable destination tag. The Sparkrun plugin supplies that local tag and hands
verified per-host IDs to the launcher, as described below. Full multi-platform
index distribution, foreign layer URLs, artifacts, signatures/referrers, range
resume and a persistent payload cache are outside this implementation. Hash
verification is not publisher signature verification.

## Digest-pinned recipes

Both `repository@sha256:...` and `repository:tag@sha256:...` go through OCI Relay.
The digest always wins over the tag. A pin can name a platform manifest or a
multi-platform index: the source verifies the root bytes and every selected
child descriptor before transferring the chosen platform's layers. It never
substitutes a mutable tag when the pin is missing, unavailable, or invalid.

Docker's tag API cannot attach an upstream `RepoDigest` to a relay import.
Instead, the plugin imports under a deterministic `oci-relay/pinned:...`
retention tag, verifies the source root digest and receiver results, and returns
each host's **actual immutable Docker image ID** through `ImageCopyResult.runtime_images`.
Sparkrun uses those IDs for image probes, content-ID materialization and Docker
launches (`--pull=never`). The original pin remains in the recipe, image plan
and job metadata. Different store IDs do not make an otherwise identical
multi-node recipe heterogeneous.

Successful imports also publish small private JSON receipts under
`remote_cache_dir/pins/` (default `~/.cache/oci-relay/pins/`). Each binds the
requested reference and registry digest to the verified config and local
runtime ID. Subsequent operations inspect that immutable ID and its native
platform. They never trust the retention tag alone. A moved tag cannot change
the binding; a removed image makes the receipt unusable. Receipts trust the
management user's host cache and Docker access, just like the locally staged
relay executable; they are not independently signed provenance documents.

If every receiver already has the verified pin, the plugin reports no transfer
and needs no registry access. Missing targets use the exact registry source
when online; native cache negotiation still avoids downloading existing layers.
With `--offline` or a configured local source mode, a verified local pin can
serve as the relay source. Offline preflight recognizes prior relay imports.
A forced pull re-verifies the registry source; offline mode rejects forced pulls.
Deleting receipts loses the upstream binding for images that Docker cannot
inspect by their original pin; an online relay operation can recreate it.

Before a pinned transfer starts, the plugin checks that each receiver can write
pin metadata. This is a writability check, not a disk reservation: Docker import
can still exhaust space afterward. A failed final receipt write reports failure
instead of a misleading completion message, identifies that the images were
already verified, and advises retrying after restoring cache storage. On retry,
the registry pin is re-verified and an intact installed image can be reused
without downloading its layers again.

The engine remains compatible with the v0.1.0 binaries. The plugin requires
Sparkrun's image-runtime API 1 for this handoff and reports an explicit
compatibility error on older hosts instead of silently dropping the digest.

Qualification tests cover two physical
receivers, shared layers, exact-image skipping, source-as-receiver, all three
transports locally, and a public Docker Hub download. Large-image performance
tests compare fresh and partial registry pulls with pull-then-SSH-save/load and
independent receiver pulls. Relay took 238–263 seconds versus 381 seconds for
fresh pull-then-save/load; a 313 MB missing-layer update took 11–15 seconds
versus 159 seconds for save/load from an already-populated source. Each needed
blob was fetched once for both receivers, including the diskless trials.
Direct pulls had comparable latency in these single trials. Disk retention had
no hits, so these results do not justify changing its zero-byte default.
A matched pull-then-native-relay comparison remains unmeasured.

## Mixed Docker stores

Registry payloads retain their exact upstream compressed hashes. Qualified native
receivers can match the config's ordered DiffIDs against an existing classic or
containerd base and negotiate local representations for those cached positions.
Discovery also combines matching layers from multiple local images, even after
parent changes, and containerd probes exact retained CAS blobs. Blob reuse can
avoid download while still requiring unpacking under the new parent chain.
Only missing layers need upstream acquisition. Config bytes remain identical;
the receiver reports both canonical and installed manifest digests. See
[storage compatibility](storage-compatibility.md) for qualification, identity
checks, opt-outs, and signature/referrer limitations.
