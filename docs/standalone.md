<!--
SPDX-FileCopyrightText: 2026 Spark Arena
SPDX-License-Identifier: Apache-2.0
-->

# Standalone commands

The plugin is the multi-host coordinator. The executable exposes composable
commands; there is no standalone SSH `send` command yet.
See [platform support](platforms.md) for macOS source configuration and the
distinction between a controller and an execution host.

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

## Progress events

Receiver snapshots travel over authenticated HTTP/2 control requests, including
when HTTP/2 is carried by SSH stdio or forwarding. Reporting is best effort,
bounded, and independent of data workers. Source JSONL `progress` events include
`receiver_progress` snapshots and already reported `receivers`; each final
receiver result includes a `progress` snapshot. These fields are advisory and
cannot authorize a cache hit or successful completion. Import-free
`--transfer-only` runs retain their existing aggregate metrics.

## Transfer-only validation

For an import-free benchmark, use `peer --transfer-only`. It downloads each
unique config/layer descriptor through the bounded cache, verifies exact byte
counts and SHA-256, and discards the bytes without opening a receiver Docker
connection. It reports **VERIFIED**, with no image ID or destination tag;
Sparkrun does not accept that as a completed image copy. A source whose receivers
all verify exits successfully with state VERIFIED; normal imports use COMPLETE.
To bind and distribute across paths, replace `--endpoint` with repeated flags:

```sh
oci-relay peer --session /private/node1.json --transfer-only \
  --path https://192.0.2.1:9443,192.0.2.2 \
  --path https://198.51.100.1:9443,198.51.100.2 \
  --connections-per-path 2
```

With a capable source, layers at least 256 MiB are striped into alternating
1 MiB pieces over up to four available connections. `--stripe-threshold-bytes 0`
disables this for comparisons; nonzero thresholds must be 8 MiB–1 PiB.
`--stripe-streams` sets the connection cap per layer (2–8, default 4).
`--stripe-piece-bytes` selects a power-of-two size from 1–64 MiB (default 1 MiB).
It negotiates the effective size with the source; original stripe-capable sources
fall back to 1 MiB. Per-path `stripe_piece_bytes` reports the effective value.
The experimental `--http2-stream-window-bytes` changes receive credit per stream
(0 preserves Go's default, otherwise powers of two from 1–64 MiB). It permits
additional transport buffering beyond `--max-buffer-bytes`; account for every
concurrent layer and lane. The benchmark accepts the same flag for comparisons.
One source acquisition feeds all pieces; both source and receiver still verify
the full layer digest. Small layers and peers without striping support retain
whole-layer transfers. A lost lane requires a whole-layer retry, not piece resume.

`scripts/benchmark-image.py transfer` coordinates this on explicit hosts, with
`--source-mode docker-classic` for the existing native source. It requires no
isolated receiver daemon and accepts `--data-paths /private/paths.json` containing
the same list of host-to-IP maps as the plugin. Use identical buffer/concurrency
settings when comparing one and two links. Results include per-path counters
(`--connections-per-path` controls independent connections per link)
and interface RX/TX snapshots; they contain private host/network identifiers and
should be kept outside the repository. Transfer time still includes source reads,
reconstruction, TLS and hashing; it is not a raw network-capacity measurement.
The benchmark also accepts `--stripe-threshold-bytes`, `--stripe-streams`, `--stripe-piece-bytes`, and
`--max-buffer-bytes` for matched comparisons. See
[plugin transport details](sparkrun-plugin.md#multiple-data-paths) for memory
accounting, source compatibility, and automatic fallback.
