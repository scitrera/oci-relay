<!--
SPDX-FileCopyrightText: 2026 Scitrera LLC
SPDX-License-Identifier: AGPL-3.0-only
SPDX-FileComment: The Sparkrun additional permission in LICENSE_EXCEPTION applies.
-->

# OCI Relay

OCI Relay distributes container images from a local Docker store or remote OCI
registry to multiple Docker hosts. The Go relay handles verified transfers;
the Sparkrun plugin coordinates hosts, selects sources and transports, tunes
resource limits, and reports progress.

- Stream registry pulls directly to receivers without importing on the source.
- Reuse existing layers across classic overlay2 and containerd image stores.
- Transfer over authenticated HTTP/2, directly or through SSH forwarding/stdio.
- Use multiple TCP data paths with bounded memory and concurrency.

Native access supports qualified rootful Linux Docker 29+ stores. Real-engine
qualification currently covers Linux arm64; Linux amd64 binaries are
cross-built. See [storage compatibility](docs/storage-compatibility.md) and
[validation](docs/validation.md). A public release is not yet available.

## Quick start with Sparkrun

Requires Bash, `uv`, Go 1.25+, Python 3.12+, and a Sparkrun `develop-next` / 0.4.0
checkout with image-distribution and pre-pull API 1. The default checkout path
is `../oss-sparkrun`; set `SPARKRUN_CHECKOUT` to use another location.

```bash
source dev.sh
sparkrun run YOUR_RECIPE
deactivate
```

The setup builds the relay, installs both projects editably, and enables the
plugin in a private development configuration. It preserves normal user
configuration and keeps the plugin outside Sparkrun's source tree. Image-transfer
progress is visible by default. See [development setup](docs/development.md)
for details or [plugin configuration](docs/sparkrun-plugin.md) for manual setup.

To build the standalone executable:

```sh
CGO_ENABLED=0 go build -trimpath -o bin/oci-relay ./cmd/oci-relay
```

## Documentation

- [Sparkrun plugin](docs/sparkrun-plugin.md): configuration, transports, multiple
  links, adaptive limits, progress, and optional tuning.
- [Source modes and manifests](docs/source-modes.md) and
  [automatic selection](docs/source-selection.md).
- [Registry sources](docs/registry-source.md): authentication, caching, and limits.
- [Storage compatibility](docs/storage-compatibility.md): native access, layer
  discovery, mixed stores, and hash identities.
- [Standalone commands](docs/standalone.md): sessions, events, and transfer-only validation.
- [Development](docs/development.md): tests, CI, versions, and vendoring;
  [release procedure](docs/releasing.md).
- [Validation and limitations](docs/validation.md).

## License and contributions

Copyright 2026 Scitrera LLC. OCI Relay is [AGPL-3.0-only](LICENSE), with a
[Sparkrun additional permission](LICENSE_EXCEPTION) allowing combination and
vendoring while Sparkrun's Apache-licensed portions retain Apache-2.0.
OCI Relay remains AGPL, including applicable corresponding-source obligations.
Ship both license documents with binaries and plugin copies.

See [third-party notices](THIRD_PARTY_NOTICES.md) and
[dependency licenses](THIRD_PARTY_LICENSES.txt). Contributions require the
[CLA](CLA.md); see [CONTRIBUTING.md](CONTRIBUTING.md).
