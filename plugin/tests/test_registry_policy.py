# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
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
        return SimpleNamespace(returncode=0 if present else 1)
    monkeypatch.setattr(provider, 'Runner', lambda *a: SimpleNamespace(
        connection=lambda _: SimpleNamespace(execute=execute), close=lambda: [],
    ))
    return commands


@pytest.mark.parametrize('present,forced,expected', [(False, False, True), (True, False, False), (True, True, True)])
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


def test_digest_runtime_reference_keeps_builtin_path(monkeypatch):
    from sparkrun_oci_relay.provider import RelayProvider
    commands = fake_probe(monkeypatch, False)
    req = replace(request(), image='example.test/image@sha256:' + '1' * 64)
    assert RelayProvider().pull(req) is None
    assert not commands


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
