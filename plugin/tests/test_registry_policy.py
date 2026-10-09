# SPDX-FileCopyrightText: 2026 Spark Arena
# SPDX-License-Identifier: Apache-2.0
from dataclasses import replace
from types import SimpleNamespace

import pytest

from sparkrun_oci_relay.source_policy import validate


def request(settings=None, **extra):
    api = pytest.importorskip('sparkrun.plugins')
    if not hasattr(api, 'ImagePullRequest'):
        pytest.skip('requires Sparkrun optional pre-pull API')
    return api.ImagePullRequest(
        image='example.test/repo/image:latest', source_host=None, targets=('node',), transfer_hosts=('192.0.2.2',),
        config=SimpleNamespace(plugin_settings=lambda _: settings or {}), **extra,
    )


def fake_probe(monkeypatch, present):
    from sparkrun_oci_relay import provider
    commands = []
    def execute(*args, **kwargs):
        commands.append(args)
        return SimpleNamespace(returncode=0 if present else 1, stdout=('sha256:' + 'a' * 64 + '\n').encode())
    monkeypatch.setattr(provider, 'Runner', lambda *a: SimpleNamespace(
        connection=lambda _: SimpleNamespace(execute=execute), close=lambda: [],
    ))
    return commands


@pytest.mark.parametrize('present,forced,expected', [(False, False, True), (True, False, True), (True, True, True)])
def test_pre_pull_only_when_needed_or_forced(monkeypatch, present, forced, expected):
    from sparkrun_oci_relay.provider import RelayProvider
    commands = fake_probe(monkeypatch, present)
    provider = RelayProvider()
    calls = []
    monkeypatch.setattr(provider, 'copy', lambda req, **kw: calls.append((req, kw)) or 'copied')
    assert provider.pull(request(force_pull=forced)) == ('copied' if expected else None)
    assert bool(calls) == expected
    if calls:
        assert calls[0][1] == {'registry': True}
    assert len(commands) == (0 if forced else 1)


@pytest.mark.parametrize('image,source_host', [
    ('example.test/image:v1', None), ('local-build:latest', None),
    ('example.test/image:latest', 'head'),
])
def test_present_non_refreshing_images_keep_existing_source_policy(monkeypatch, image, source_host):
    from sparkrun_oci_relay.provider import RelayProvider
    fake_probe(monkeypatch, True)
    provider = RelayProvider()
    monkeypatch.setattr(provider, 'copy', lambda *a, **k: pytest.fail('unexpected registry refresh'))
    assert provider.pull(replace(request(), image=image, source_host=source_host)) is None


def test_existing_implicit_latest_refreshes_before_builtin_pull(monkeypatch, caplog):
    from sparkrun_oci_relay.provider import PROGRESS, RelayProvider
    caplog.set_level(PROGRESS)
    fake_probe(monkeypatch, True)
    provider = RelayProvider()
    calls = []
    def copy(req, **kw):
        assert 'checking source image' in caplog.text
        assert 'refreshing latest image from registry' in caplog.text
        calls.append((req, kw))
        return 'copied'
    monkeypatch.setattr(provider, 'copy', copy)
    assert provider.pull(replace(request(), image='example.test:5000/image')) == 'copied'
    assert calls[0][0].image == 'example.test:5000/image:latest'
    assert calls[0][1] == {'registry': True}


def test_latest_refresh_metadata_failure_uses_pinned_cached_source(monkeypatch, caplog):
    from sparkrun_oci_relay.provider import RegistrySourceUnavailable, RelayProvider
    fake_probe(monkeypatch, True)
    provider = RelayProvider()
    calls = []
    def copy(req, **kw):
        calls.append((req, kw))
        if kw.get('registry'):
            raise RegistrySourceUnavailable('registry unavailable')
        return 'cached'
    monkeypatch.setattr(provider, 'copy', copy)
    req = request(timeout=90)
    assert provider.pull(req) == 'cached'
    assert calls[1][0].image == req.image
    assert 0 < calls[1][0].timeout <= calls[0][0].timeout < 90
    assert calls[1][1] == {'source_image': 'sha256:' + 'a' * 64}
    assert 'using cached image' in caplog.text


@pytest.mark.parametrize('failure', ['transfer', 'partial', 'forced', 'explicit', 'missing', 'expired'])
def test_registry_fallback_is_only_for_pretransfer_best_effort_refresh(monkeypatch, failure):
    from sparkrun_oci_relay import provider as module
    fake_probe(monkeypatch, failure != 'missing')
    provider = module.RelayProvider()
    calls = []
    clock = [10.0]
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock[0])
    def copy(req, **kw):
        calls.append(kw)
        if failure == 'partial':
            return 'partial-result'
        if failure == 'transfer':
            raise module.OperationError('receiver failed after startup')
        if failure == 'expired':
            clock[0] += 100
        raise module.RegistrySourceUnavailable('registry unavailable')
    monkeypatch.setattr(provider, 'copy', copy)
    settings = {'source_mode': 'registry'} if failure == 'explicit' else {}
    req = request(settings, force_pull=failure == 'forced', timeout=90)
    if failure == 'partial':
        assert provider.pull(req) == 'partial-result'
    else:
        with pytest.raises(module.OperationError):
            provider.pull(req)
    assert calls == [{'registry': True}]


@pytest.mark.parametrize('settings,offline', [({}, True), ({'registry_source': False}, False), ({'source_mode': 'docker-classic'}, False)])
def test_pre_pull_declines_without_probing(monkeypatch, settings, offline):
    from sparkrun_oci_relay.provider import RelayProvider
    commands = fake_probe(monkeypatch, False)
    assert RelayProvider().pull(request(settings, offline=offline)) is None
    assert not commands


def test_explicit_registry_and_implicit_latest(monkeypatch):
    from sparkrun_oci_relay.provider import RelayProvider
    commands = fake_probe(monkeypatch, True)
    provider = RelayProvider()
    calls = []
    monkeypatch.setattr(provider, 'copy', lambda req, **kw: calls.append(req) or 'copied')
    settings = {'source_mode': 'registry'}
    assert provider.pull(replace(request(settings), image='example.test:5000/repo/image')) == 'copied'
    assert calls[0].image == 'example.test:5000/repo/image:latest'
    assert settings == {'source_mode': 'registry'} and not commands


@pytest.mark.parametrize('suffix', ['@sha256:', ':canary@sha256:'])
def test_digest_runtime_reference_uses_exact_registry_pin(monkeypatch, suffix):
    from sparkrun_oci_relay import provider as module
    fake_probe(monkeypatch, False)
    monkeypatch.setattr(module.pins, 'resolve', lambda *a: None)
    req = replace(request(), image='example.test/image' + suffix + '1' * 64)
    provider = module.RelayProvider()
    calls = []
    monkeypatch.setattr(provider, 'copy', lambda r, **kw: calls.append((r, kw)) or 'copied')
    assert provider.pull(req) == 'copied'
    assert calls[0][0].image == req.image
    assert calls[0][1] == {'registry': True}


def test_digest_pin_requires_host_runtime_capability(monkeypatch):
    api = pytest.importorskip("sparkrun.plugins")
    from sparkrun_oci_relay.provider import RelayProvider
    monkeypatch.delattr(api, 'IMAGE_RUNTIME_API_VERSION')
    with pytest.raises(api.ImageDistributionUnsupported, match='image-runtime API'):
        RelayProvider().pull(replace(request(), image='example.test/image@sha256:' + '1' * 64))


@pytest.mark.parametrize('offline', [False, True])
def test_cached_pin_returns_per_host_runtime_ids_without_registry(monkeypatch, offline):
    from sparkrun_oci_relay import provider as module
    fake_probe(monkeypatch, False)
    runtime_id = 'sha256:' + 'a' * 64
    monkeypatch.setattr(module.pins, 'resolve', lambda *a: runtime_id)
    provider = module.RelayProvider()
    monkeypatch.setattr(provider, 'copy', lambda *a, **kw: pytest.fail('warm pin transferred'))
    req = replace(request(offline=offline), image='example.test/image@sha256:' + '1' * 64)
    result = provider.pull(req)
    assert result.outcomes == {'node': 'already_present'}
    assert result.runtime_images == {'node': runtime_id}


def test_partial_offline_pin_uses_verified_receiver_as_source(monkeypatch):
    from sparkrun_oci_relay import provider as module
    fake_probe(monkeypatch, False)
    runtime_id = 'sha256:' + 'a' * 64
    monkeypatch.setattr(module.pins, 'resolve', lambda runner, host, image: runtime_id if host == 'node' else None)
    provider = module.RelayProvider()
    calls = []
    monkeypatch.setattr(provider, 'copy', lambda r, **kw: calls.append((r, kw)) or 'copied')
    req = replace(request(offline=True), image='example.test/image@sha256:' + '1' * 64,
                  targets=('node', 'missing'), transfer_hosts=('node', 'missing'))
    assert provider.pull(req) == 'copied'
    assert calls[0][0].source_host == 'node'
    assert calls[0][1] == {'registry': False}


def test_pin_registry_failure_never_uses_mutable_tag(monkeypatch):
    from sparkrun_oci_relay import provider as module
    fake_probe(monkeypatch, True)
    monkeypatch.setattr(module.pins, 'resolve', lambda *a: None)
    provider = module.RelayProvider()
    calls = []
    def copy(r, **kw):
        calls.append((r, kw))
        raise module.RegistrySourceUnavailable('unavailable')
    monkeypatch.setattr(provider, 'copy', copy)
    req = replace(request(), image='example.test/image:latest@sha256:' + '1' * 64)
    with pytest.raises(module.RegistrySourceUnavailable):
        provider.pull(req)
    assert len(calls) == 1 and calls[0][0].image == req.image
    assert calls[0][1] == {'registry': True}


@pytest.mark.parametrize('settings', [
    {'registry_cache_bytes': -1}, {'registry_cache_bytes': True}, {'registry_cache_bytes': 1 << 51},
    {'registry_plain_http': 'true'}, {'registry_source': 'true'}, {'registry_config': 'relative'},
    {'source_mode': 'registry', 'manifest': '/manifest.json'},
    {'source_mode': 'registry', 'receiver_import': 'load'},
])
def test_registry_policy_rejects_invalid_settings(settings):
    with pytest.raises(ValueError):
        validate(settings)


def test_registry_requires_no_native_or_preparation_permission():
    settings = {'source_mode': 'registry', 'allow_native_store': False, 'allow_preparation_read': False,
                'registry_cache_bytes': 0}
    assert validate(settings) == settings
