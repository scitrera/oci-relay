# Bundled receiver decoding

OCI Relay release bundles include `oci-relay`, a native `unpigz` executable,
`bundle.json`, and `UNPIGZ_LICENSES.txt`. No additional repository, package
installation, Docker daemon configuration, or daemon restart is required.
The decoder stays in the plugin's private cache; it is not installed into
Docker's PATH. Verification, decoding and staging run in Go and the native
helper. Direct HTTP/2 payloads bypass Python; the optional SSH stdio fallback
forwards opaque streams through the Python orchestrator.

## Selection and limits

The plugin defaults to `receiver_decoder: auto`. It checks the relay's
`receiver-unpigz-v1` capability and installs the verified helper on receivers
that support it. Older releases keep using ordinary compressed imports.
Automatic decoding currently applies only to overlay2 receivers using the
`pull` importer. Containerd stores retain their normal import path.

Cache discovery happens first. Only missing gzip layers are decoded; cached
layers and native uncompressed representations are left alone. A completely
resident image needs no decoder work. The compressed input digest is verified
by the transfer cache, and decoded bytes must match the config's DiffID before
being offered to Docker. The image config, ordered DiffIDs and platform remain
unchanged. The receiver negotiates the new local manifest with its source.

Decoded tar streams are staged in a private operation directory. The plugin
checks free space for both scratch and Docker's store, budgets for scratch,
Docker's download copy and unpacked data, and leaves a reserve. This is a
conservative estimate, not an exact prediction of filesystem allocation.
Go enforces the exact aggregate raw-byte limit and checks scratch headroom as
it writes. Staging is removed after import and on error/cancellation; the
plugin also removes the owned directory during cleanup. An abrupt host crash
can leave an operation directory under `/tmp/oci-relay.*`.

Defaults can be overridden under `plugins.oci-relay`:

```yaml
receiver_decoder: auto       # auto | none | unpigz (require the helper)
# decode_workers: 8          # otherwise min(8, relay streams, available CPUs)
max_decode_bytes: 68719476736 # 64 GiB maximum; reduced to available disk budget
decode_reserve_bytes: 17179869184
```

`decode_reserve_bytes` defaults to 16 GiB. The plugin leaves another 1 GiB
margin when calculating its cap. `decode_workers` accepts 1–16. In automatic
mode, an unavailable helper or insufficient staging budget retains the
compressed path; failed integrity checks always fail the operation. Explicit
`unpigz` mode reports resource/availability failures instead of falling back.

Progress reports a receiving/decoding phase before Docker import. Receiver
results include decoder seconds, raw bytes, layer count and a fallback reason
when applicable. Raw scratch can be considerably larger than compressed input.
The earlier full-image benchmark improved receiver time by roughly 17%, but
that is not a guarantee for another image, disk, or number of receivers.

## Development and standalone use

`source dev.sh` builds and checks the native helper along with the Go binary.
A C compiler, make and Python 3.12+ are needed; source downloads use pinned
HTTPS URLs and SHA-256 checksums. Verified development helpers are reused until
the build script or dependency pins change. The config sets
`development_unpigz` explicitly; a neighboring executable is never trusted just
because it exists. Explicit deployments can instead configure `decoder_paths`
and trusted `decoder_sha256` maps by Linux architecture.

Standalone receivers default to no extra decoder. To opt in, add:

```sh
--decoder auto --unpigz /absolute/private/unpigz \
  --decode-spool-dir /absolute/private/scratch --decode-workers 8 \
  --max-decode-bytes 68719476736 --decode-reserve-bytes 17179869184
```

The scratch parent must already exist. The initial Go check requires room for
three times the specified cap plus the reserve; choose a smaller cap on smaller
disks. Native-cache helper containers mount the decoder read-only and only the
owned scratch directory writable. Native Docker storage remains read-only.

## Native builds

Release jobs use `ubuntu-24.04` for Linux AMD64, `ubuntu-24.04-arm` for Linux
ARM64, and `macos-15` for Apple Silicon. Build and execution tests run on the
target architecture; QEMU is not used. The build script rejects a host/target
mismatch. Linux helpers statically link pinned musl and zlib. macOS helpers
statically link zlib and use the system runtime; they are ad-hoc signed, not
Developer ID signed or notarized. Windows helpers are not supported.

Native source pins live in `scripts/native-dependencies.json`. The build
verifies source archives, runs zlib tests, checks helper linkage, and exercises
valid concatenated gzip streams plus damaged/truncated input. `bundle.json`
binds the version, platform, capabilities and executable hashes. The plugin
checks that manifest against the checksum-pinned release archive before
installing the helper, including offline cache reuse.
