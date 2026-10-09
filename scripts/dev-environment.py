#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0
"""Prepare private dev configuration and verify installed-plugin loading."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import tempfile


def configure(destination: Path, base: Path, binary: Path) -> Path:
    import yaml

    destination = destination.absolute()
    if destination.is_symlink():
        raise ValueError('development config directory must not be a symlink')
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination.chmod(0o700)
    config = destination / 'config.yaml'
    if config.is_symlink():
        raise ValueError('development config file must not be a symlink')
    first = not config.exists()
    original = base / 'config.yaml' if first else config
    data = yaml.safe_load(original.read_text()) if original.is_file() else {}
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError('Sparkrun config must be a mapping')
    for key in ('integrations', 'plugins', 'features'):
        if not isinstance(data.setdefault(key, {}), dict):
            raise ValueError(key + ' must be a mapping')
    settings = data['plugins'].setdefault('oci-relay', {})
    if not isinstance(settings, dict):
        raise ValueError('plugins.oci-relay must be a mapping')
    data['integrations']['oci-relay'] = True
    # Prefer the editable installed adapter even on alpha with a bundled copy.
    data['features']['plugins.oci_relay'] = False
    data['container_distribution_provider'] = 'oci-relay'
    # A dev failure must not silently benchmark builtin copy instead.
    data['container_distribution_fallback'] = False
    for name, value in {'source_mode': 'auto', 'allow_native_store': True,
                        'allow_preparation_read': True, 'transport': 'auto'}.items():
        settings.setdefault(name, value)
    settings['development_binary'] = str(binary.resolve())
    if binary.with_name('unpigz').is_file():
        settings['development_unpigz'] = str(binary.with_name('unpigz').resolve())
    if first:
        # Snapshot distribution/recipe config, not auth tokens or service state.
        # Copies prevent development commands from mutating the user's originals.
        for name in ('clusters', 'recipes', 'registries.yaml'):
            source, target = base / name, destination / name
            if source.exists() and not target.exists():
                if source.is_dir():
                    shutil.copytree(source, target)
                else:
                    shutil.copyfile(source, target)
    fd, temporary = tempfile.mkstemp(prefix='.config-', dir=destination)
    try:
        with os.fdopen(fd, 'w') as stream:
            yaml.safe_dump(data, stream, sort_keys=False)
        os.replace(temporary, config)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return config


def verify() -> None:
    import sparkrun.plugins as api
    import sparkrun_oci_relay
    from sparkrun.application import initialize
    from sparkrun.core.image_distribution import _PROVIDERS
    from sparkrun.core.installed_plugins import installed_plugin_inventory

    if api.IMAGE_DISTRIBUTION_API_VERSION != 1 or getattr(api, "IMAGE_PULL_API_VERSION", None) != 1:
        raise RuntimeError('Sparkrun checkout needs image-distribution API 1 and pre-pull API 1')
    initialize()
    rows = [row for row in installed_plugin_inventory() if row.name == 'oci-relay']
    if len(rows) != 1 or not rows[0].selected or not rows[0].loaded or rows[0].failure:
        raise RuntimeError('OCI Relay installed-plugin registration failed: ' + str(rows))
    if _PROVIDERS.get('oci-relay') is not sparkrun_oci_relay.provider.PROVIDER:
        raise RuntimeError('OCI Relay provider did not come from the installed plugin')
    print('OCI Relay installed, enabled and registered from ' + str(Path(sparkrun_oci_relay.__file__).parent))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    setup = commands.add_parser('configure')
    setup.add_argument('--destination', type=Path, required=True)
    setup.add_argument('--base', type=Path, required=True)
    setup.add_argument('--binary', type=Path, required=True)
    commands.add_parser('verify')
    args = parser.parse_args()
    if args.command == 'configure':
        print('Development config: ' + str(configure(args.destination, args.base, args.binary)))
    else:
        verify()


if __name__ == '__main__':
    main()
