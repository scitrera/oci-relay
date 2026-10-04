# Third-party notices

OCI Relay's original implementation is copyright 2026 Scitrera LLC, licensed
under AGPL-3.0-only with the Sparkrun permission in LICENSE_EXCEPTION.

The executable directly imports Dragonfly's `pkg/stats.RollingWindow` from
`d7y.io/dragonfly/v2`, pinned to commit `41b312389c90` through the exact
pseudo-version in go.mod. Dragonfly is Apache-2.0 software from the Dragonfly
Authors. No Dragonfly daemon, scheduler, manager or database is embedded.

It also directly uses Scitrera's `go-backpressure` semaphore, the Moby Engine
client, OCI image/digest types, and their linked dependencies. Exact versions
are in go.mod/go.sum; linked module license/notice texts are collected in
THIRD_PARTY_LICENSES.txt by scripts/dependency-licenses.py. Regenerate that
file when imports or dependencies change.

The opt-in classic-store adapter directly uses `github.com/vbatts/tar-split`
v0.12.3 (BSD-3-Clause) for exact tar reconstruction and per-file CRC checks.
This is the upstream library used by Moby, not copied Moby private-store code.
The relay supplies its own confined, read-only store adapter and full SHA-256
verification. Its license is included in THIRD_PARTY_LICENSES.txt.

Nydus informed the metadata-readiness and demand-coalescing design. No Nydus
source code is copied or linked. There is no Nydus filesystem or snapshotter
dependency and no Nydus-compatible on-disk format.

The Go compiler/runtime and standard library use the Go Authors' BSD-style
license. Their license text is included in THIRD_PARTY_LICENSES.txt. The
shared CI generator is scitrera/repo-tools at the revision in versions.yaml,
under its own BSD-3-Clause license; it is not linked into the executable.
