<!--
SPDX-FileCopyrightText: 2026 Scitrera LLC
SPDX-License-Identifier: Apache-2.0
-->

# Source modes and manifest input

The plugin chooses a source according to the [automatic selection policy](source-selection.md).
The standalone Go CLI takes an explicit source mode. Native store eligibility
and layer reuse are covered in [storage compatibility](storage-compatibility.md).

## External manifest input

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
the source does not silently substitute another manifest. Qualified native
receivers can negotiate a different cached-layer representation while preserving
the exact config; both manifest digests are reported. See
[storage compatibility](storage-compatibility.md).

## Docker push

With no supplied manifest, `--allow-preparation-read` opts into a classic-store
preparation push that reads/hashes the whole image while discarding blob bodies.
Runtime push rounds then request only currently needed blobs and spool
unidentified uploads under a hard reservation limit. This avoids full-image
staging but still stages each active unidentified blob. Supplying a manifest
avoids that full preparation pass; an exact config acquisition is still needed.

## OCI layouts and registry sources

An existing OCI-layout directory is also supported and can stream blobs larger
than memory without Docker upload staging. A [registry source](registry-source.md)
resolves bounded metadata and streams compressed blobs directly to receivers,
with optional retained disk caching. Diskless identity-first Docker uploads,
HTTP Range resume, full index distribution and peer-to-peer sourcing remain
unimplemented.

## Docker archive

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
manifest differs from the registry's compressed manifest; the config digest
must remain identical. Containerd exports can retain compressed blobs, and its
Docker image ID may instead be a manifest or index digest. These are distinct, reported representations.
Staging uses ordinary buffered file I/O and is deleted on completion or failure.
The cap bounds relay-owned archive bytes, excluding Docker's own export scratch
space and Linux page cache. Preparation time, time to first export byte, and
archive bytes are reported separately. The source can still spend substantial
time preparing before emitting any bytes. Measure preparation and transfer time
for your workload before choosing a mode; this is not an automatic switch or a
guaranteed speedup.

## Classic native store

A `docker-classic` source removes the Docker export/push preparation
step on qualified **rootful Docker 29+ overlay2** stores:

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
and user-namespace stores, other drivers, and Docker versions below 29 or with
unparseable version strings are rejected before receiver pulls. Existing
provider fallback rules apply.

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
cleanup.

## Containerd store and layer reuse

A `docker-containerd` source and store-aware receivers now support qualified
Docker 29+ containerd overlayfs stores. Receivers match ordered DiffID chains
and negotiate local representations across mixed stores. Discovery covers all
image records within a configurable time budget; layers can come from multiple
images, including matching DiffIDs under different parents. Containerd also
probes exact cached blob paths directly. Only unavailable layers cross the
network; local blob reuse can still require unpacking. Config, blob, manifest and Docker backend IDs are checked
separately. Source helpers have no Docker socket; native receiver helpers need
the local socket for import. See [storage compatibility](storage-compatibility.md).
The version baseline has no patch allowlist or upper bound; actual storage
layout and integrity checks still apply. Real-engine tests cover overlay2 on
29.1.3/29.2.1 and containerd overlayfs on 29.2.1.

## Transfer metrics

Metrics now include active/peak source acquisitions, verified bytes and blobs
(including replays), and the last verified completion time. Receiver results
also separate Docker pull duration, last download completion and last layer
registration. Time origins are per process/phase, not synchronized wall clocks.
The configured stream count is a ceiling; Docker's download limit and actual
missing layers control demand. Each blob still uses one stream; 64 KiB buffer
frames are not independent parallel pieces.

Direct TCP supports explicitly qualified multiple paths and multiple connections
per path. RoCE/RDMA remains unimplemented; authenticated TCP is the portable path.
