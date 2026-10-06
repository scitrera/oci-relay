# Docker storage compatibility and layer reuse

OCI Relay supports local native sources and Docker receivers using classic
`overlay2` or the containerd `overlayfs` image store. Registry sources feed the
same receiver path. The plugin detects each daemon independently; mixed clusters
require no storage migration or daemon restart.

Native access accepts rootful Linux **Docker 29 and newer** with classic
**overlay2** or **containerd overlayfs**, `runc`, and no user namespace. There is
no patch-release allowlist or upper version limit; vendor/build suffixes are
accepted. Older or unparseable versions and other snapshotters do not select
native access automatically. The Go adapters enforce the same version baseline.
Backend detection, read-only confinement, native metadata/layout validation,
and all hash checks still apply. Incompatible native layouts fail rather than
silently falling back after native selection.

The version baseline is an admission policy, not a claim of testing every
release. Real-engine validation covers overlay2 on 29.1.3/29.2.1 and containerd
overlayfs on 29.2.1. See [validation](validation.md) for measured scope.

## Identities that must remain separate

| Identity | Meaning | Reuse rule |
|---|---|---|
| Blob digest | SHA-256 of exact transferred bytes, compressed or raw | Verify full size and digest on acquisition; different encodings can have different digests |
| DiffID | SHA-256 of the uncompressed layer tar, including headers and padding | Compare exact values; equivalent extracted files alone do not prove equal DiffIDs |
| ChainID / ordered DiffID prefix | Layer plus its complete parent history | Required for reuse of an unpacked layer chain |
| Config digest | SHA-256 of exact OCI config bytes | Stable image identity across representation changes |
| Manifest/index digest | SHA-256 of that exact metadata document | Changes when layer descriptors/encodings change |
| Docker image ID | Backend-specific identifier | Tested classic store returns config digest; tested containerd store can return manifest or index digest |

The distinction between DiffID and ChainID follows the
[OCI image config specification](https://github.com/opencontainers/image-spec/blob/v1.1.1/config.md#layer-diffid).
Docker's tested containerd inspector returns the target descriptor digest as its
ID, and platform-specific inspection resolves an index to a platform manifest;
see [Moby 29.2.1 inspection](https://github.com/moby/moby/blob/docker-v29.2.1/daemon/containerd/image_inspect.go).
Containerd also retains compressed content alongside unpacked snapshots; see
[Docker's containerd store documentation](https://docs.docker.com/engine/storage/containerd/).

Matching one DiffID at a different layer position or after a different parent
is insufficient to skip an unpacked layer. The relay uses a verified common
prefix. Docker may independently reuse exact CAS blobs outside that prefix.

## Native sources

- `docker-classic` reconstructs exact uncompressed tar streams from read-only
  tar-split metadata and overlay2 files, with metadata checks, tar-split CRCs,
  and full transfer SHA-256 verification.
- `docker-containerd` reads exact retained CAS blobs. It verifies the pinned
  root index/manifest, selects one platform, verifies its config, bounds
  metadata traversal to 64 MiB in aggregate (4 MiB per document), and checks blob file types and sizes before serving.
  Layer payloads are acquired and SHA-256 verified on demand. Preparation
  performs no export, recompression, payload scan, or source disk staging.

The plugin pins each source with an owned helper container and mounts its store
read-only. It executes the static relay entrypoint, never the image application.
Source helpers receive no Docker socket. Native selection is automatic when
`allow_native_store: true` (the plugin default).

For Docker's embedded containerd, the plugin uses
`<DockerRootDir>/containerd/daemon/io.containerd.content.v1.content`. For the standard external
`/run/containerd/containerd.sock` (or `/var/run/containerd/containerd.sock`), it
uses `/var/lib/containerd/io.containerd.content.v1.content`. These are default
path conventions, not discovery of arbitrary containerd configuration. An explicitly
configured `containerd_content_root` can identify a different content directory.
It must name the **content directory containing `blobs/sha256`**, not a snapshot
root or a containerd socket. A missing/incompatible path or missing selected
platform content fails before transfer; the relay never searches arbitrary
private stores or falls back after a native integrity failure.

Standalone callers provide that read-only content root with `--docker-root`, a
pinned manifest/index ID with `--image`, `--source docker-containerd`, and
`--engine-version 29.2.1`. They are responsible for retaining the source image
throughout the operation. The plugin owns that lifecycle automatically.

Public Docker push and save sources also distinguish config IDs from containerd
manifest IDs. Push explicitly selects the pinned platform. Save selects that
platform's exact manifest from the bounded archive even when the exported index
also contains attestations. Full save staging still needs an explicit budget;
it is not auto-preferred for containerd.

## Receiver inventory and negotiation

The plugin and standalone receiver share Go discovery. `oci-relay inventory`
reads `{"diff_ids":[...],"platform":{"os":"linux","architecture":"arm64"}}`
from stdin and emits bounded candidate hints and metrics. It lists all Docker
images, including dangling/intermediate records, deduplicates IDs **before**
inspection, and inspects the requested platform with eight concurrent requests
in batches of 32. There is no 128-image cutoff. Discovery stops when all required
DiffIDs are covered or every listed image has been examined. It does not promise
the globally best unpacked prefix after finding complete blob coverage.

The default metadata-discovery budget is 10 seconds. The plugin setting
`cache_discovery_seconds` and inventory CLI `--timeout-seconds` accept 1–300.
A deadline returns the matches found so far with `complete: false` and
`stop_reason: time_budget`; unknown layers remain remote misses. Missing images
are tolerated as stale hints; daemon failures and operation cancellation surface
as errors. Discovery timing excludes process/SSH startup and native preparation.
The standalone receiver and optional load importer use the 10-second default.

1. The source advertises its platform and ordered DiffIDs. Discovery selects a
   matching base and, when necessary, additional images supplying other DiffIDs.
   The plugin passes the hints through a private `--cache-inventory` JSON file.
   The receiver binds that inventory to its actual source DiffIDs/platform and
   verifies the selected native metadata; hints alone never authorize a skip.
2. A qualified receiver uses a helper with read-only store mounts and the local
   Docker socket. Overlay2 needs a matching image. Containerd can use any local
   image of the requested platform to bootstrap its helper, even without matching
   image metadata, then probe exact incoming blob paths. With no suitable helper
   image, the plugin uses ordinary Docker pull. Standalone callers can supply a
   native root directly without that helper-image requirement.
3. For overlay2, the helper retains its own image (`--native-base`). Additional
   donor images are retained by owned, never-started containers, removed after
   import. The receiver verifies config hashes, calculates ChainIDs and validates
   layerdb parent/DiffID metadata. It reads tar-split metadata only for needed
   layers, combining references under two open store-directory handles. An equal
   DiffID under a different parent provides an exact local tar stream but does
   **not** establish a reusable unpacked chain. Unreferenced overlay2 layers with
   no retainable Docker image are not reused.
4. For containerd, exact incoming SHA-256 blob paths are probed directly; this
   does not require a discoverable image reference. Selected image manifests can
   also map incoming DiffIDs to a different retained encoding. Open file handles
   retain advertised bytes across concurrent GC unlink, without private metadata
   writes or content leases. At most 256 distinct blob handles are retained per
   receiver; additional blobs use the source and set `cache_probe_limited`.
5. The receiver negotiates per-layer availability over authenticated HTTP/2:
   `chain` for a verified matching parent prefix, `blob` for locally available
   bytes outside that prefix, and `missing` otherwise. Config bytes, DiffIDs,
   layer order and every missing descriptor stay unchanged. A chain match allows
   Docker to reuse its unpacked state; Docker still controls actual unpacking.
   A blob match avoids network transfer but may require decompression/unpacking.
6. `POST /relay/v1/negotiate` acknowledges cache schema version 2, validates the
   prefix and per-layer states, and computes unique missing blobs/bytes. Old
   prefix-only requests remain accepted; new peers reject an old source that
   does not acknowledge the extension before import. The plugin stages the same
   engine build throughout an operation. Agreement and completion are bound to
   the exact negotiated manifest.
7. Docker imports through the loopback registry. Cached bytes requested by Docker
   are served locally through the verifying relay cache. Missing bytes come from
   the source. Final config/manifest identity, platform and ordered RootFS DiffIDs
   are checked before assigning the destination tag. Retention containers, file
   handles and operation directories are released, with cleanup errors reported.

Cached content relies on Docker's immutable, previously verified image store;
we do not rehash every existing snapshot file before each operation. Bytes
actually read through the relay still undergo full size/SHA-256 verification.
Corrupt metadata/payloads fail the operation. There are no fabricated blob
hashes, empty registry blobs, or writes to private Docker/containerd metadata.

A receiver view can have a different manifest digest from the source. The
original config stays byte-identical. This is single-platform tagged image
installation; it does not preserve an index, signatures or referrers for a
rewritten destination manifest. Set `allow_native_store: false` to disable both
native source access and receiver representation adaptation. Ordinary Docker
pull reuse remains available.

## Results and limits

Protocol 1 retains `image_id` as its config-digest identity for coordinator
compatibility. New receiver fields make the distinctions explicit:

- `config_digest`: stable config digest, equal to protocol-1 `image_id`.
- `docker_image_id`: Docker's inspected platform image ID.
- `manifest_digest`: canonical source manifest digest.
- `installed_manifest_digest`: negotiated receiver view digest, when importing.
- `store`: detected local backend.
- `reused_layers`: verified matching parent-prefix length.
- `cached_blob_layers`: other layer positions available locally (not unique blobs).
- `discovery_seconds`, `discovery_images`, `discovery_inspected`: metadata probe
  time, unique IDs listed, and inspection count.
- `discovery_complete`, `discovery_stop_reason`: full coverage/exhausted search
  versus a time-budget cutoff; `all_layers_found` may stop before every image.
- `discovery_stale`: candidate images lost before retention/native metadata access.
- `cache_probe_seconds`: native metadata/probing and retention setup time.
- `cache_probe_limited`: containerd retained-file budget prevented further probes.
- `local_bytes`: cached payload bytes actually reconstructed/read locally.

Receiver cache metrics include local reads when required; source acquisition
metrics show the layer bytes that actually crossed from the source. A cache
hit does not imply a payload read. Already-present skips do not import or
negotiate a new manifest. Updated peers require the negotiation endpoint;
the plugin deploys its matching engine build to every participant.

No disk staging budget is added for this feature. Metadata reads use buffered
filesystem I/O. Relay memory limits, OS page cache, and Docker download/unpack
storage remain distinct. Different encodings can trade network bytes for CPU;
these capabilities do not establish that relay wins for every image or link.
