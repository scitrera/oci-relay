<!--
SPDX-FileCopyrightText: 2026 Spark Arena
SPDX-License-Identifier: Apache-2.0
-->

# Platforms and controller placement

The relay runs on the **source/fetcher and receivers**. A Sparkrun controller
that only coordinates remote hosts does not execute the Go binary. It downloads
and verifies the Linux archives, stages binaries over SSH, and manages the
operation. Direct HTTP/2 keeps image bytes between the execution hosts; SSH
forwarding and stdio may carry those bytes through the controller.

For a Mac controller with Linux Spark nodes, use Sparkrun's
`transfer_mode: delegated`. The head is the source/fetcher and the cluster nodes
are receivers. Normal `auto` selection chooses delegated when the controller
has no local IB link, but explicit delegated mode also avoids a fallback to a
controller-local source. Registry credentials must be available on the fetcher.

The updated plugin also selects the first Linux receiver as registry fetcher
when a non-Linux controller has no explicit source host. That host must have the
registry credentials; controller credentials are not silently forwarded.

`push` and controller-local source paths do need a relay on the controller.
The v0.1.3 plugin accepts **Linux execution hosts only**: architecture
detection, binary verification/staging, native Docker helpers and route probes
assume Linux. A macOS binary does not by itself enable that plugin path.

| Release binary | v0.1.3 qualification / role |
|---|---|
| Linux arm64 | Native build, Go/helper tests, real Docker and multi-host transfers; Spark execution hosts |
| Linux amd64 | Native build and Go/helper tests; real Docker qualification remains open |
| macOS arm64 | Native build and Go/helper tests; Docker Desktop qualification remains open |

The macOS archive can be used for standalone registry or OCI-layout sources
and transfer-only receivers, subject to that qualification limit. Set
`--platform linux/arm64` (or the receiver's actual platform) for registry sources:
the source's default platform otherwise follows the machine running it. Docker
operations require an explicit local Unix socket; Docker contexts and
`DOCKER_HOST` are not resolved automatically. Native Linux store access is not
supported against Docker Desktop's VM from the macOS process. Release binaries
are not Developer ID signed or notarized.

## Windows

Both `windows/amd64` and `windows/arm64` cross-compile, but native Windows
binaries are not published as supported v0.1.3 artifacts. Runtime portability
still requires work:

- Protect session files using Windows ACLs; current checks require POSIX
  private-directory/file permissions.
- Support Docker named pipes; the engine client currently permits only local
  Unix sockets.
- Qualify the optional Unix-socket attachment transport or supply a Windows
  alternative. Direct TCP/HTTP/2 itself is portable.
- Replace the plugin's Linux shell utilities and binary/host probes for native
  Windows execution.

For Windows control machines, running Sparkrun under WSL with delegated Linux
execution avoids the need for a native Windows relay. This is an architectural
path, not a claim of completed Windows controller qualification. Native Windows
Sparkrun/SSH process support is a separate concern from Go cross-compilation.

The v0.1.3 bundle workflows build and execute tests natively on Linux AMD64, Linux
ARM64 and macOS ARM64. This supersedes the v0.1.0 cross-build arrangement; it
does not imply Docker Desktop or native Windows execution qualification.
See [bundled decoder builds](bundled-decoder.md).
