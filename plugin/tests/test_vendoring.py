# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]


def test_explicit_vendoring_installs_bindings_and_refuses_tampering(tmp_path):
    core = tmp_path / 'src/sparkrun/core'
    core.mkdir(parents=True)
    (core / 'image_distribution.py').write_text('IMAGE_DISTRIBUTION_API_VERSION = 1\nIMAGE_PULL_API_VERSION = 1\n')
    (core / 'features.py').write_text('registered = []\ndef FeatureFlag(**kw): return kw\ndef register_feature(f): registered.append(f)\n')
    (core / 'in_tree_plugins.py').write_text('IN_TREE_PLUGIN_FEATURES = {}\n')
    (tmp_path / 'pyproject.toml').write_text('[project]\nname = "fixture"\n[tool.setuptools.package-data]\n[tool.ruff]\nextend-exclude = ["other"]\n')
    command = [sys.executable, str(ROOT / 'scripts/vendor-plugin.py'), '--sparkrun', str(tmp_path)]
    subprocess.run([*command, '--development'], check=True, capture_output=True)
    subprocess.run([*command, '--check'], check=True, capture_output=True)
    state = {}
    exec((core / 'features.py').read_text(), state)
    assert state['registered'] == [{'name': 'plugins.oci_relay', 'description': 'OCI Relay image distribution',
                                    'channel_defaults': {'alpha': True}, 'default': False}]
    exec((core / 'in_tree_plugins.py').read_text(), state)
    assert state['IN_TREE_PLUGIN_FEATURES']['oci_relay'] == 'plugins.oci_relay'
    metadata = tomllib.loads((tmp_path / 'pyproject.toml').read_text())
    assert 'LICENSE' in metadata['tool']['setuptools']['package-data']['sparkrun.plugins.oci_relay']
    vendored = tmp_path / 'src/sparkrun/plugins/oci_relay'
    assert (vendored / 'LICENSE').read_bytes() == (ROOT / 'LICENSE').read_bytes()
    assert not (vendored / 'LICENSE_EXCEPTION').exists()
    assert metadata['tool']['ruff']['extend-exclude'] == ['other', 'src/sparkrun/plugins/oci_relay']
    before = {str(p): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    subprocess.run([*command, '--development'], check=True, capture_output=True)
    assert before == {str(p): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    plugin = tmp_path / 'src/sparkrun/plugins/oci_relay/__init__.py'
    plugin.write_text('# local work\n')
    assert subprocess.run([*command, '--check'], capture_output=True).returncode != 0
    assert subprocess.run([*command, '--development'], capture_output=True).returncode != 0
    assert plugin.read_text() == '# local work\n'
