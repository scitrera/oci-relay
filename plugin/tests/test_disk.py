# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace

import pytest

from sparkrun_oci_relay.disk import registry_cache_budget


@pytest.mark.parametrize('available,settings,expected', [
    (300, {}, 16), (24, {}, 8), (16, {}, 0), (2, {}, 0),
    (300, {'registry_cache_bytes': 4 << 30}, 4),
    (20, {'registry_cache_bytes': 64 << 30}, 4),
])
def test_retention_preserves_user_disk_headroom(available, settings, expected):
    commands = []
    def execute(host, args):
        commands.append((host, args))
        return f'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/sda 999999999 0 {available << 20} 0% /space with spaces\n'.encode()
    assert registry_cache_budget(SimpleNamespace(execute=execute), 'host', '/space with spaces', settings) == expected << 30
    assert commands == [('host', ['env', 'LC_ALL=C', 'df', '-Pk', '/space with spaces'])]


@pytest.mark.parametrize('output', [b'', b'bad', b'a b c -1 d e', b'a b c wrong d e'])
def test_unknown_headroom_disables_optional_cache(output):
    runner = SimpleNamespace(execute=lambda *args: output)
    assert registry_cache_budget(runner, None, '/tmp', {}) == 0


def test_explicit_zero_does_not_probe():
    runner = SimpleNamespace(execute=lambda *args: pytest.fail('unexpected probe'))
    assert registry_cache_budget(runner, None, '/tmp', {'registry_cache_bytes': 0}) == 0
