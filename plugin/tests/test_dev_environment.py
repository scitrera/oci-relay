# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
import importlib.util
from pathlib import Path

import pytest

yaml = pytest.importorskip('yaml')
ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('dev_environment', ROOT / 'scripts/dev-environment.py')
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


def test_private_config_preserves_user_settings_and_local_edits(tmp_path):
    base, destination = tmp_path / 'user', tmp_path / 'dev'
    base.mkdir()
    original = b'plugins:\n  oci-relay:\n    source_streams: 7\nssh_user: fixture\n'
    (base / 'config.yaml').write_bytes(original)
    (base / 'clusters').mkdir()
    (base / 'clusters/.default').write_text('test-cluster')
    (base / 'registries.yaml').write_text('registries: []\n')
    (base / 'arena_token').write_text('must-not-copy')
    config = dev.configure(destination, base, tmp_path / 'relay')
    data = yaml.safe_load(config.read_text())
    assert data['integrations']['oci-relay'] is True
    assert data['container_distribution_provider'] == 'oci-relay'
    assert data['container_distribution_fallback'] is False
    assert data['plugins']['oci-relay']['source_streams'] == 7
    assert (destination / 'clusters/.default').read_text() == 'test-cluster'
    assert not (destination / 'arena_token').exists()
    assert destination.stat().st_mode & 0o777 == 0o700
    assert config.stat().st_mode & 0o777 == 0o600
    data['plugins']['oci-relay']['source_streams'] = 4
    config.write_text(yaml.safe_dump(data))
    (destination / 'clusters/.default').write_text('dev-cluster')
    dev.configure(destination, base, tmp_path / 'new-relay')
    data = yaml.safe_load(config.read_text())
    assert data['plugins']['oci-relay']['source_streams'] == 4
    assert data['plugins']['oci-relay']['development_binary'] == str(tmp_path / 'new-relay')
    assert (destination / 'clusters/.default').read_text() == 'dev-cluster'
    assert (base / 'clusters/.default').read_text() == 'test-cluster'
    assert (base / 'config.yaml').read_bytes() == original


def test_config_symlink_cannot_overwrite_user_file(tmp_path):
    user = tmp_path / 'user.yaml'
    user.write_text('ssh_user: original\n')
    destination = tmp_path / 'dev'
    destination.mkdir()
    (destination / 'config.yaml').symlink_to(user)
    with pytest.raises(ValueError, match='symlink'):
        dev.configure(destination, tmp_path, tmp_path / 'relay')
    assert user.read_text() == 'ssh_user: original\n'
