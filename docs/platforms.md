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

## Development support

The working tree adds Windows x64/ARM64 runtime support. These changes are
**not included in the published v0.1.3 binaries**. Use a candidate built from
this tree and its matching editable plugin during qualification.

| Execution host | Runtime path | Qualification |
|---|---|---|
| Linux arm64 | Native overlay2/containerd and existing registry importer | Existing real-engine and cluster coverage |
| Linux amd64 | Same implementation | Native CI; broader real-engine coverage remains open |
| macOS arm64 | Standalone registry/layout sources; Docker context plus archive import | Native build/tests; Desktop runtime qualification remains open |
| Windows x64/ARM64 | Registry/layout sources, Docker named pipe or TCP/TLS, archive import | Candidate builds and native CI tests; Desktop end-to-end qualification remains open |
| Windows + WSLC | Experimental public CLI adapter, bounded archive import/export | CLI contract tests; real WSLC qualification remains open |

The Windows qualification workflow runs tests on `windows-2025` and
`windows-11-arm`, without QEMU, including a real local named-pipe HTTP server,
protected credential-directory ACLs, binary subprocess pipes, and the transfer
protocol. It does not install or qualify Docker Desktop or WSLC. Candidate
artifacts are separate from the repo-tools-generated release pipeline.

## Docker connections

Standalone commands accept `--docker-host` (`unix://`, local `npipe://`, or
`tcp://`) and `--docker-context`. With neither supplied, selection follows
`DOCKER_CONTEXT`, then `DOCKER_HOST`, then the current Docker context. The default
context uses the platform default: a named pipe on Windows, Unix socket on Unix.
An explicitly selected host overrides the environment's context.

For TCP, `--docker-tls`, `--docker-ca`, `--docker-cert`, and `--docker-key` enable
verified TLS; `DOCKER_TLS_VERIFY`, `DOCKER_CERT_PATH`, and named-context TLS files
are recognized. Skipping certificate verification is unsupported. Non-loopback
plaintext TCP requires `--docker-allow-plain-http`. Nothing enables a Docker
TCP listener or changes daemon settings automatically.

A Windows controller can run Docker Desktop without enabling port 2375. The
named pipe / selected context is the default; configured TCP remains an option.
See [Docker's connection FAQ](https://docs.docker.com/desktop/troubleshoot-and-support/faqs/general/).

## VM-safe imports and local sources

Use `peer --import archive --max-import-bytes BUDGET` when the daemon is remote
or inside a VM. Verified layer blobs stream into a combined OCI/Docker archive
through Docker's ImageLoad API. The relay needs no receiver registry port or
VM-to-host loopback route. Docker may still stage/unpack data internally.

The archive preserves the original OCI manifest and config for containerd;
classic Docker checks the config and ordered uncompressed layer hashes. An
exact image already present can skip transfer with `--skip-present`. The initial
portable path sends a full archive when import is required: it does **not** yet
provide the Linux native-store path's minimal partial-layer transfer. Windows
and remote-daemon native filesystem access is deliberately unqualified.

For a local Desktop/WSLC source, use `--source docker-save --allow-preparation-read
--max-spool-bytes BUDGET`. This stages a bounded export once and then serves its
layers through the normal verified parallel relay. Both OCI-layout and classic
Docker-save archives are accepted. The relay does not assume access to the VM's
image-store files. Registry sources avoid that export step; their default image
OS is Linux, independently of the control host's OS. Select `--platform
linux/arm64` explicitly when an x64 controller serves ARM receivers.

## WSLC

Select `--runtime wslc`, optionally `--wslc-session NAME` and
`--wslc-executable PATH`. The adapter uses public `image inspect/list/save/load/tag/rm`
commands with argument arrays, not a shell or private VM protocol. Metadata
failures remain failures; a structured missing-image result is a cache miss.

WSLC's current CLI `image load --input` requires a seekable file. The adapter
stages one private archive, checks its declared budget and available space plus
1 GiB reserve, verifies the complete input before invoking WSLC, and removes it
on success or failure. `--import-spool-dir` selects the staging parent. This
allowance covers relay staging, not the runtime's additional unpacked storage.
Docker Desktop imports stream through its API instead of this extra staging
file. This is based on the public implementation at
[WSL revision 341f1dd](https://github.com/microsoft/WSL/blob/341f1ddf76f089ff6351590b03f92bad30041725/src/windows/wslc/services/ImageService.cpp).

## Sparkrun integration

A Windows controller can coordinate Linux SSH nodes without running a local Go
relay. The plugin continues to delegate registry fetching to a Linux receiver
by default. For a Windows-local Desktop or WSLC image source, the updated plugin
supports a native Windows relay and per-host runtime settings. See
[configuration examples](windows.md).

Remote execution hosts managed through Sparkrun SSH are still Linux. A Windows
receiver can use the standalone Go CLI; native Windows SSH target orchestration
and a WSLC workload executor are separate follow-up work. No GB10 CPU affinity
mask is applied to N1X: its Windows scheduler and core topology require their
own qualification. Windows uses buffered file reads; Linux keeps its existing
direct-read policy. Windows bundles do not require or advertise `unpigz`.

Before a Windows release, run real Desktop and WSLC cold/warm imports, a mixed
Windows/Linux transfer, cancellation and disk-full cases. Qualification must
include actual runtime image identities and cleanup, not only compilation.
