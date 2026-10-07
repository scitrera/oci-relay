# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
import pytest

from sparkrun_oci_relay import decoder
from sparkrun_oci_relay.source_policy import validate


class Runner:
    def directory(self, host):
        return '/tmp/owned'

    def execute(self, host, args):
        return b'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/a 400000000 100000000 300000000 25% /\n'


def test_auto_selection_caps_workers_and_scratch(monkeypatch):
    monkeypatch.setattr(decoder, 'docker_facts', lambda *a: {'driver': 'overlay2', 'root': '/var/lib/docker'})
    args = decoder.arguments(Runner(), 'host', '/private/unpigz', {}, {'cpus': 20}, {'source_streams': 4})
    assert args[args.index('--decode-workers') + 1] == '4'
    assert int(args[args.index('--max-decode-bytes') + 1]) == 64 << 30
    assert args[args.index('--decoder') + 1] == 'auto'
    assert decoder.arguments(Runner(), 'host', None, {}, {}, {}) == []
    with pytest.raises(decoder.OperationError):
        decoder.arguments(Runner(), 'host', None, {'receiver_decoder': 'unpigz'}, {}, {})
    monkeypatch.setattr(decoder, 'docker_facts', lambda *a: {'driver': 'overlayfs'})
    assert decoder.arguments(Runner(), 'host', '/private/unpigz', {}, {}, {}) == []


def test_auto_disables_on_unknown_or_full_disk(monkeypatch):
    monkeypatch.setattr(decoder, 'docker_facts', lambda *a: {'driver': 'overlay2', 'root': '/var/lib/docker'})
    runner = Runner()
    for reply in (b'unknown', b'/dev/a 10000 9000 1000 90% /\n'):
        runner.execute = lambda *a: reply
        assert decoder.arguments(runner, 'host', '/private/unpigz', {}, {}, {}) == []


@pytest.mark.parametrize('settings', [
    {'receiver_decoder': 'bad'}, {'decode_workers': True}, {'decode_workers': 17},
    {'max_decode_bytes': 100}, {'decode_reserve_bytes': -1},
    {'receiver_decoder': 'unpigz', 'receiver_import': 'load'},
])
def test_decoder_configuration_is_bounded(settings):
    with pytest.raises(ValueError):
        validate(settings)
