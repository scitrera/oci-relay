<!--
SPDX-FileCopyrightText: 2026 Scitrera LLC
SPDX-License-Identifier: Apache-2.0
-->

# Windows development examples

Use the working-tree Go executable and editable plugin. Windows runtime support
is undergoing qualification; v0.1.3 release binaries do not contain these changes.
See [platform support](platforms.md) for the tested scope and remaining work.

## Standalone receiver

Create the session on the source, then copy that receiver's credential JSON over
an authenticated management channel into a private directory on Windows. Use
`oci-relay.exe private-directory --path C:\relay-operation` before placing
credentials there. Never copy the full session or another peer's key to a receiver.

Docker Desktop, using its selected context or default named pipe:

```powershell
oci-relay.exe peer --endpoint https://SOURCE:PORT `
  --session C:\relay-operation\peer.json --tag example/image:tag `
  --skip-present --import archive --max-import-bytes 68719476736
```

WSLC, with an explicit session and scratch parent:

```powershell
oci-relay.exe peer --endpoint https://SOURCE:PORT `
  --session C:\relay-operation\peer.json --tag example/image:tag `
  --runtime wslc --wslc-session sparkrun --skip-present `
  --import archive --max-import-bytes 68719476736 `
  --import-spool-dir D:\relay-scratch
```

The example cap is 64 GiB. It limits the archive; leave additional disk space for
Docker/WSLC extraction. WSLC adds a relay staging file, while Docker uses a
streaming API request. Transfers retain mutual TLS, SHA-256 verification,
backpressure and striping. The Windows archive path currently sends all layers
when import is needed; exact warm-image skips are supported.

For an already configured TCP daemon, append `--docker-host tcp://HOST:2376
--docker-tls --docker-ca C:\certs\ca.pem --docker-cert C:\certs\cert.pem
--docker-key C:\certs\key.pem`. Do not enable an unauthenticated daemon listener
as part of installation. A loopback TCP endpoint can also represent a separately
managed secure tunnel.

## Native Windows control node with Linux receivers

Use Sparkrun `develop-next` with the Windows process-cancellation update and
install the plugin editably using the Windows Python environment:

```powershell
uv venv --python 3.13 .venv
uv pip install --python .venv/Scripts/python.exe -e ../oss-sparkrun -e .
. .venv/Scripts/Activate.ps1
```

Configure Sparkrun as shown below. Ordinary
delegated Linux distribution downloads Linux binaries on the controller and
stages them over SSH; it needs no Windows relay binary.

For a Windows-local image source, configure both Windows and Linux candidate
binaries from the same working tree. Hashes below are placeholders; calculate
the actual hashes with `Get-FileHash -Algorithm SHA256`. Do not substitute
unverified download hashes.

```yaml
container_distribution_provider: oci-relay
container_distribution_fallback: false  # Surface experimental-runtime failures.
features:
  plugins.oci_relay: false  # Use the editable installed adapter.
integrations:
  oci-relay: true
plugins:
  oci-relay:
    binary_paths:
      windows/arm64: C:/relay/oci-relay.exe
      linux/arm64: C:/relay/oci-relay-linux-arm64
    binary_sha256:
      windows/arm64: ACTUAL_WINDOWS_BINARY_SHA256
      linux/arm64: ACTUAL_LINUX_BINARY_SHA256
    runtime_connections:
      controller:
        docker_context: desktop-linux
    max_spool_bytes: 68719476736
    source_mode: auto
    allow_preparation_read: true
```

Use `windows/amd64` for an x64 controller. Executable architecture and image
platform are separate: Linux ARM64 receivers still receive Linux ARM64 images.
The context name must exist locally (`docker context ls`); omit
`runtime_connections.controller` to use the controller's normal Docker default.
The plugin chooses bounded `docker-save` for a VM source and preserves native
store reuse on qualified Linux receivers.

Replace the controller connection with this to select WSLC for a local source:

```yaml
runtime_connections:
  controller:
    runtime: wslc
    wslc_session: sparkrun
```

Runtime connection keys can also be Linux SSH hostnames/IPs for a nondefault
daemon there. Certificate and context paths belong to the host running the
relay. Such remote/VM receiver daemons automatically use archive import, with
`max_import_bytes` defaulting to 64 GiB unless explicitly configured.

Windows-local source operation directories use Windows ACLs. Linux remote paths
remain POSIX paths even when the controller runs Windows. For SSH stdio fallback,
the Windows source attachment uses loopback TCP carrying the same end-to-end TLS
stream; it does not use a Windows Unix socket. Direct HTTP/2 and SSH forwarding
are also available.

Windows release archives and hashes are not published yet. The native Windows
qualification workflow produces candidate executables; a public release must
wait for runtime qualification. Its Windows artifacts contain no Linux decoder.
