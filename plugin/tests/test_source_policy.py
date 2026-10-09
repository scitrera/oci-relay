# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from sparkrun_oci_relay.source_policy import LOCAL_DOCKER, SourceUnavailable, detect, select, store_version_supported, validate

GIB = 1 << 30


def store(**extra):
    return dict(version='29.1.3', driver='overlay2', os='linux', root='/var/lib/docker',
                security=['name=seccomp,profile=builtin'], runtimes={'runc': {}},
                image_size=25 * GIB, spool_free_bytes=100 * GIB, docker_free_bytes=100 * GIB, **extra)


def policy(**extra):
    return dict(allow_native_store=True, allow_preparation_read=True, **extra)


def archive_policy():
    return dict(allow_native_store=False, allow_preparation_read=True, max_spool_bytes=32 * GIB, transport='http2-direct')


@pytest.mark.parametrize('version', [
    '29.0.0', '29.1.3', '29.2.1', '29.3.0', '30.0.0', '99.0.0',
    'v29.1.3', '29.0.0-rc.1', '29.1.3+vendor.1', '29.1.3-0ubuntu1~24.04.1',
])
def test_docker29_and_later_automatically_select_native_and_archive(version):
    assert store_version_supported(version)
    facts = dict(store(), version=version)
    assert select({}, facts, {}).mode == 'docker-classic'
    assert select(archive_policy(), facts, {'network_gbps': 100}).mode == 'docker-save'
    facts.update(driver='overlayfs', driver_status=[['driver-type', 'io.containerd.snapshotter.v1']])
    assert select({}, facts, {}).mode == 'docker-containerd'


@pytest.mark.parametrize('version', [
    None, 29, [], '', 'unknown', '28.99.99', 'v28.1.0', '29', '29.2', '29.x.1',
    '29.1.3garbage', '29.1.3\n', '29.1.3-', '+29.1.3',
])
def test_older_or_malformed_versions_use_public_source(version):
    assert not store_version_supported(version)
    facts = dict(store(), version=version)
    assert select({}, facts, {}).mode == 'docker'
    assert select(archive_policy(), facts, {'network_gbps': 100}).mode == 'docker'
    facts.update(driver='overlayfs', driver_status=[['driver-type', 'io.containerd.snapshotter.v1']])
    assert select({}, facts, {}).mode == 'docker'


def test_default_auto_prefers_native_with_preparation_disabled():
    settings = {'allow_preparation_read': False}
    assert select(settings, store(), {}).mode == 'docker-classic'
    assert settings == {'allow_preparation_read': False}


@pytest.mark.parametrize('mode', ['docker', 'docker-save', 'docker-classic'])
def test_explicit_mode_wins_over_auto_heuristics(mode):
    settings = policy(source_mode=mode, max_spool_bytes=4 << 20)
    choice = select(settings, {}, {})
    assert choice.mode == mode and 'explicit' in choice.reason


def test_exact_manifest_wins_even_with_native_enabled():
    choice = select({'manifest': '/prepared/manifest.json', 'allow_native_store': True}, {}, {})
    assert choice.mode == 'docker' and 'exact representation' in choice.reason


@pytest.mark.parametrize('changed', [
    {'version': '28.5.2'}, {'driver': 'overlayfs'}, {'os': 'windows'},
    {'security': ['name=rootless']}, {'security': ['name=userns']},
    {'security': None}, {'security': [None]}, {'runtimes': {}}, {'runtimes': None},
    {'root': 'relative'}, {'root': '/store,bad'},
])
def test_unqualified_native_store_uses_permitted_public_source(changed):
    facts = store()
    facts.update(changed)
    choice = select(policy(), facts, {})
    assert choice.mode == 'docker'
    assert 'using bounded push' in choice.reason
    with pytest.raises(SourceUnavailable):
        select({'allow_preparation_read': False}, facts, {})


def test_permissions_default_on_without_mutating_config():
    settings = {}
    assert validate(settings) == {'allow_native_store': True, 'allow_preparation_read': True}
    assert select(settings, store(), {}).mode == 'docker-classic'
    assert select(settings, dict(store(), version='unqualified'), {}).mode == 'docker'
    assert settings == {}


def test_explicit_permission_opt_outs_are_preserved():
    assert select({'allow_native_store': False}, store(), {}).mode == 'docker'
    with pytest.raises(SourceUnavailable):
        select({'allow_native_store': False, 'allow_preparation_read': False}, store(), {})
    with pytest.raises(SourceUnavailable):
        select({'source_mode': 'docker-classic', 'allow_native_store': False}, store(), {})
    with pytest.raises(SourceUnavailable):
        select({'source_mode': 'docker', 'allow_preparation_read': False}, store(), {})
    assert select({'manifest': '/m', 'allow_native_store': False, 'allow_preparation_read': False}, store(), {}).mode == 'docker'


def test_provider_passes_defaults_to_execution_without_mutation(monkeypatch):
    from types import SimpleNamespace
    api = pytest.importorskip('sparkrun.plugins')
    from sparkrun_oci_relay.provider import RelayProvider

    original = {}
    request = SimpleNamespace(image="fixture:tag", config=SimpleNamespace(plugin_settings=lambda _: original), dry_run=False, timeout=30)
    provider = RelayProvider()
    observed = []
    def copy(request, settings, timeout, *, source_image=None):
        observed.append(settings)
        return api.ImageCopyResult({})
    monkeypatch.setattr(provider, '_copy', copy)
    provider.copy(request)
    assert observed == [{'allow_native_store': True, 'allow_preparation_read': True}]
    assert original == {}


def test_fast_archive_fallback_requires_budget_and_both_filesystems():
    settings = archive_policy()
    assert select(settings, store(), {'network_gbps': 100}).mode == 'docker-save'
    estimate = 25 * GIB + (25 * GIB) // 4
    for key in ('spool_free_bytes', 'docker_free_bytes'):
        facts = store()
        facts[key] = 2 * estimate - 1
        assert select(settings, facts, {'network_gbps': 100}).mode == 'docker'
        facts[key] += 1
        assert select(settings, facts, {'network_gbps': 100}).mode == 'docker-save'
    assert select(dict(settings, max_spool_bytes=estimate-1), store(), {'network_gbps': 100}).mode == 'docker'


@pytest.mark.parametrize('settings_change,facts_change,route', [
    ({'transport': 'ssh-stdio'}, {}, {'network_gbps': 100}),
    ({'transport': 'http2-ssh'}, {}, {'network_gbps': 100}),
    ({'transport': 'auto'}, {}, {'network_gbps': 100}),
    ({}, {}, {}), ({}, {}, {'network_gbps': 10}),
    ({}, {}, {'network_gbps': float('nan')}),
    ({}, {'image_size': -1}, {'network_gbps': 100}),
    ({}, {'image_size': None}, {'network_gbps': 100}),
    ({}, {'spool_free_bytes': None}, {'network_gbps': 100}),
    ({}, {'docker_free_bytes': None}, {'network_gbps': 100}),
    ({}, {'version': '28.0.0'}, {'network_gbps': 100}),
    ({}, {'driver': 'overlayfs'}, {'network_gbps': 100}),
])
def test_archive_is_not_automatically_staged_on_weak_evidence(settings_change, facts_change, route):
    settings, facts = archive_policy(), store()
    settings.update(settings_change)
    facts.update(facts_change)
    assert select(settings, facts, route).mode == 'docker'


def test_explicit_effective_speed_hint():
    assert select(dict(archive_policy(), network_gbps=100), store(), {}).mode == 'docker-save'


@pytest.mark.parametrize('settings', [
    {'cache_discovery_seconds': 0}, {'cache_discovery_seconds': 301},
    {'cache_discovery_seconds': True}, {'cache_discovery_seconds': 0.5},
    {'source_mode': []}, {'source_mode': 'unknown'},
    {'allow_native_store': 'true'}, {'allow_preparation_read': 1},
    {'source_mode': 'docker-classic', 'manifest': '/m'},
    {'source_mode': 'docker-save', 'manifest': '/m'},
    {'max_spool_bytes': True}, {'max_spool_bytes': 1},
])
def test_invalid_settings_are_not_hidden_by_fallback(settings):
    with pytest.raises(ValueError):
        validate(settings)


class Probes:
    def __init__(self, facts=None, fail_disk=False):
        self.facts = store() if facts is None else facts
        self.fail_disk = fail_disk
        self.calls = []

    def execute(self, host, arguments):
        self.calls.append((host, arguments))
        if arguments == LOCAL_DOCKER + ['image', 'inspect', '--format={{.Size}}', 'fixture:tag']:
            return str(25 * GIB).encode()
        if arguments[:4] == LOCAL_DOCKER + ['info']:
            return json.dumps(self.facts).encode()
        if arguments[:3] == ['df', '-P', '-B1']:
            if self.fail_disk:
                raise RuntimeError('filesystem probe failed')
            return f'Filesystem 1-blocks Used Available Capacity Mounted on\n/dev/test 0 0 {100 * GIB} 0% /\n'.encode()
        raise AssertionError('unexpected or mutating probe: ' + repr(arguments))


def test_detection_skips_probes_for_explicit_mode_and_manifest():
    runner = Probes()
    assert detect(runner, None, 'fixture:tag', policy(source_mode='docker'), {}).mode == 'docker'
    assert detect(runner, None, 'fixture:tag', {'manifest': '/m'}, {}).mode == 'docker'
    assert not runner.calls


def test_native_detection_needs_no_export_or_disk_probe():
    runner = Probes()
    assert detect(runner, 'source-host', 'fixture:tag', {}, {}).mode == 'docker-classic'
    assert len(runner.calls) == 1 and runner.calls[0][0] == 'source-host'


def test_archive_detection_probes_source_filesystems_without_writes():
    for fail_disk, expected in [(False, 'docker-save'), (True, 'docker')]:
        runner = Probes(fail_disk=fail_disk)
        # Probe-derived fields must not come from the daemon info fixture.
        for key in ('image_size', 'spool_free_bytes', 'docker_free_bytes'):
            runner.facts.pop(key)
        settings = dict(archive_policy(), spool_dir='/custom stage')
        choice = detect(runner, 'source-host', 'fixture:tag', settings, {'network_gbps': 100})
        assert choice.mode == expected
        assert any(args == ['df', '-P', '-B1', '--', '/custom stage'] for _, args in runner.calls)
        assert all(host == 'source-host' for host, _ in runner.calls)


def test_daemon_failure_is_not_mistaken_for_source_capability():
    class Failed:
        def execute(self, *args):
            raise RuntimeError('daemon unavailable')
    with pytest.raises(RuntimeError, match='daemon unavailable'):
        detect(Failed(), None, 'fixture:tag', policy(), {})

@pytest.mark.parametrize('extra', [
    {'source_join_milliseconds': True}, {'source_join_milliseconds': 2001},
    {'relay_gomaxprocs': True}, {'relay_gomaxprocs': -1}, {'relay_gomaxprocs': 65},
    {'receiver_import': 'unknown'}, {'receiver_import': 'load-cached'},
    {'receiver_import': 'load', 'max_import_bytes': True},
])
def test_receiver_experiment_settings_require_bounded_explicit_values(extra):
    from sparkrun_oci_relay.source_policy import validate
    with pytest.raises(ValueError):
        validate(dict(allow_native_store=True, **extra))


def test_receiver_experiment_settings_accept_explicit_budgets():
    from sparkrun_oci_relay.source_policy import validate
    validate({'allow_native_store': True, 'receiver_import': 'load-cached',
              'max_import_bytes': 8 << 20, 'source_join_milliseconds': 250, 'relay_gomaxprocs': 4})


def test_containerd_native_selection_qualification_and_opt_out():
    from sparkrun_oci_relay.source_policy import containerd_store_reason, native_mounts

    facts = dict(store(), version='29.2.1', driver='overlayfs',
                 driver_status=[['driver-type', 'io.containerd.snapshotter.v1']])
    assert containerd_store_reason(facts) is None
    assert select({}, facts, {}).mode == 'docker-containerd'
    assert select({'allow_native_store': False}, facts, {}).mode == 'docker'
    assert containerd_store_reason(dict(facts, driver_status=[])) is not None
    assert containerd_store_reason(dict(facts, version='28.5.2')) is not None
    assert containerd_store_reason(dict(facts, security=['name=rootless'])) is not None
    assert native_mounts(facts, {}) == [('/var/lib/docker/containerd/daemon/io.containerd.content.v1.content', '/oci-relay-store', True)]
    assert native_mounts(dict(facts, containerd_address='/run/containerd/containerd.sock'), {}) == [('/var/lib/containerd/io.containerd.content.v1.content', '/oci-relay-store', True)]
    assert native_mounts(facts, {'containerd_content_root': '/custom/content'}) == [('/custom/content', '/oci-relay-store', True)]
    with pytest.raises(SourceUnavailable):
        validate({'source_mode': 'docker-containerd', 'allow_native_store': False})
    for path in ['relative', '/bad,bind', '/bad\npath']:
        with pytest.raises(ValueError):
            validate({'containerd_content_root': path})
