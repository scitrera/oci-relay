# SPDX-FileCopyrightText: 2026 Spark Arena
# SPDX-License-Identifier: Apache-2.0
"""Exercise real acquisition -> provider -> host dispatch without SSH or Docker."""

import io
import json
import urllib.error

import pytest

pytest.importorskip('sparkrun.plugins')
from sparkrun.core import image_distribution as api
from sparkrun_oci_relay import provider, release


@pytest.fixture
def dispatch(tmp_path, monkeypatch):
    closed = []
    # Acquisition failure tests must also run before a new engine's artifact
    # checksums have been published. Never depend on the release manifest.
    monkeypatch.setattr(release, '__file__', str(tmp_path / 'release.py'))
    (tmp_path / 'releases.json').write_text(json.dumps({release.__version__: {'linux/arm64': {
        'url': 'https://example.invalid/release.tar.gz', 'sha256': 'a' * 64,
    }}}))

    class Config(dict):
        def plugin_settings(self, name):
            return {'cache_dir': str(tmp_path / 'cache')}

    class Runner:
        def __init__(self, *args):
            pass

        def architecture(self, host):
            return 'arm64'

        def stage_binary(self, *args):
            pytest.fail('binary staged despite failed acquisition')

        def close(self):
            closed.append(True)
            return []

    monkeypatch.setattr(provider, 'Runner', Runner)
    monkeypatch.setattr(provider.platform, 'system', lambda: 'Linux')
    monkeypatch.setattr(provider.pins, 'preflight', lambda *a: None)
    relay = provider.RelayProvider()
    monkeypatch.setattr(api, '_PROVIDERS', {'oci-relay': relay})

    def invoke(kind='copy', config=None, offline=False):
        token = api._CONFIG.set(Config(config or {}))
        arguments = dict(image='registry.test/image:tag', source_host=None, targets=['node'],
                         transfer_hosts=None, ssh_user=None, ssh_key=None, ssh_options=None,
                         timeout=30, dry_run=False, offline=offline, session=object())
        try:
            if kind == 'copy':
                return api.try_image_copy(**arguments)
            if kind == 'pin':
                arguments['image'] += '@sha256:' + 'a' * 64
            return api.try_image_pull(**arguments, force_pull=True)
        finally:
            api._CONFIG.reset(token)

    return invoke, relay, closed


@pytest.mark.parametrize('kind,offline', [('copy', False), ('copy', True), ('pull', False), ('pin', False)])
@pytest.mark.parametrize('config,fallback', [
    ({}, True), ({'container_distribution_provider': 'auto'}, True),
    ({'container_distribution_fallback': True}, True),
    ({'container_distribution_fallback': False}, False),
    ({'container_distribution_provider': 'oci-relay', 'container_distribution_fallback': True}, False),
])
def test_download_unavailable_uses_builtin_only_when_policy_allows(
    dispatch, monkeypatch, caplog, kind, offline, config, fallback,
):
    invoke, relay, closed = dispatch
    downloads = []

    def unavailable(*a, **kw):
        downloads.append(True)
        raise urllib.error.URLError('network unreachable')

    monkeypatch.setattr(release.urllib.request, 'urlopen', unavailable)
    if fallback:
        assert invoke(kind, config, offline) is None  # Host hands off to builtin.
        assert 'fallback' in caplog.text
    elif kind == 'copy':
        assert invoke(kind, config, offline) == ['node']
        assert 'fallback' not in caplog.text
    else:
        with pytest.raises(api.ImageDistributionFailed):
            invoke(kind, config, offline)
        assert 'fallback' not in caplog.text
    assert len(downloads) == (0 if offline else 1)
    assert closed and not relay._operations.locked()


@pytest.mark.parametrize('kind', ['copy', 'pull', 'pin'])
def test_download_checksum_failure_never_falls_back(dispatch, monkeypatch, caplog, kind):
    invoke, _, _ = dispatch
    response = io.BytesIO(b'incorrect archive bytes')
    response.url = 'https://example.invalid/archive'
    monkeypatch.setattr(release.urllib.request, 'urlopen', lambda *a, **kw: response)
    with pytest.raises(release.BinaryInvalid, match='checksum'):
        invoke(kind, {'container_distribution_fallback': True})
    assert 'fallback' not in caplog.text


@pytest.mark.parametrize('kind', ['copy', 'pull'])
def test_started_transfer_never_falls_back(dispatch, monkeypatch, caplog, kind):
    invoke, relay, _ = dispatch

    def failed(*a, **kw):
        raise provider.OperationError('receiver failed after startup')

    monkeypatch.setattr(relay, '_copy', failed)
    with pytest.raises(provider.OperationError, match='after startup'):
        invoke(kind, {'container_distribution_fallback': True})
    assert 'fallback' not in caplog.text
