<!--
SPDX-FileCopyrightText: 2026 Scitrera LLC
SPDX-License-Identifier: AGPL-3.0-only
SPDX-FileComment: The sparkrun additional permission in LICENSE_EXCEPTION applies.
-->

# Contributing to OCI Relay

Describe the proposed change and provide relevant validation. Use the checks
in README.md and the compatibility matrix in docs/validation.md. Keep relay
mechanisms in Go and Sparkrun-specific orchestration and tuning in the plugin.

Before a contribution can be merged, its contributor must accept the
[OCI Relay Contributor License Agreement](CLA.md) through a contribution
process designated by Scitrera LLC. Contributors retain ownership; the CLA
grants Scitrera LLC copyright and patent licenses, including the rights for
open-source, commercial, and proprietary licensing described in the agreement.
It also preserves the agreement's open-source availability commitment for
contributions incorporated into the publicly available project.

The repository does not yet designate an electronic CLA acceptance service.
Until one is documented, coordinate acceptance with Scitrera LLC and retain
an explicit acceptance record before merging. Submitting a pull request or
adding a commit sign-off alone is not the designated acceptance process.
General discussion, feature requests, bug reports, and ideas that are not
intended as submissions of copyrightable material do not require acceptance.
The CLA is a contribution agreement, not an additional condition on exercising
rights under the project's software license.

Submit only work you authored or are authorized to contribute, including any
required employer authorization. Identify third-party material and its source
and license. Preserve its original notices; do not attribute imported code
solely to Scitrera LLC or assume our Sparkrun exception changes its license.

For new Scitrera-authored Go files, use:

```go
// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
```

Use equivalent comments in Python and other source formats. Adjust the year
and attribution to the actual authorship; retain contributor and upstream
notices. Keep the exception reference valid when code is packaged or vendored,
and ship copies of both `LICENSE` and `LICENSE_EXCEPTION` with the plugin and
Go release archives. Record licensing separately for formats without comments.

The CLA follows the common Version 1.0 agreement used by Coldsnap and
Sparkroute, adapted for OCI Relay with Scitrera LLC as the sole Project Owner
and an explicit reference to the Sparkrun additional permission. It preserves
the existing copyright and patent grants, commercial licensing terms,
contributor protections, and Texas/Harris County governing-law provisions.
