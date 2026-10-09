# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
import hashlib
import json
from types import SimpleNamespace

import pytest

from sparkrun_oci_relay import runtime, release
from sparkrun_oci_relay.source_policy import select


@dataclass
class Result:
    returncode: int = 0
    stdout: bytes = b''
    stderr: bytes = b''


def test_controller_context_does_not_change_linux_receivers(monkeypatch):
    monkeypatch.setattr(runtime.platform, 'system', lambda: 'Windows')
    settings = {'runtime_connections': {'controller': {'docker_context': 'desktop-linux'}}}
    runtime.validate(settings)
    assert runtime.config(settings, None) == {'docker_context': 'desktop-linux'}
    assert runtime.config(settings, 'spark') == {'docker_host': 'unix:///var/run/docker.sock'}
    assert runtime.portable(settings, None)
    assert not runtime.portable(settings, 'spark')
    assert runtime.arguments(settings, None) == ['--docker-context', 'desktop-linux']


def test_remote_docker_info_cannot_qualify_as_local_storage():
    calls = []
    def execute(host, arguments, **kwargs):
        calls.append(arguments)
        return Result(stdout=json.dumps({'os': 'linux', 'driver': 'overlay2', 'version': '29.4.0'}).encode())
    settings = {'runtime_connections': {'node': {'docker_host': 'tcp://127.0.0.1:1234'}},
                'max_spool_bytes': 16 << 30}
    wrapped = runtime.RuntimeSession(SimpleNamespace(execute=execute), settings, 'node')
    facts = json.loads(wrapped.execute('node', runtime.DEFAULT_DOCKER + ['info', '--format', 'json']).stdout)
    assert calls[0][:3] == ['docker', '--host', 'tcp://127.0.0.1:1234']
    assert facts['native_local'] is False
    assert select(settings, facts, {}).mode == 'docker-save'


def test_wslc_metadata_uses_selected_session_without_shell():
    calls = []
    info = {'Id': 'sha256:' + 'a' * 64, 'Os': 'linux', 'Architecture': 'arm64'}
    def execute(host, arguments, **kwargs):
        calls.append(arguments)
        return Result(stdout=json.dumps([info]).encode())
    settings = {'runtime_connections': {'controller': {'runtime': 'wslc', 'wslc_session': 'with spaces'}}}
    wrapped = runtime.RuntimeSession(SimpleNamespace(execute=execute), settings, None)
    result = wrapped.execute('localhost', runtime.DEFAULT_DOCKER + ['image', 'inspect', '--format={{.Id}}|{{.Os}}|{{.Architecture}}', 'image:tag'])
    assert calls == [['wslc.exe', '--session', 'with spaces', 'image', 'inspect', 'image:tag']]
    assert result.stdout == (info['Id'] + '|linux|arm64\n').encode()
    with pytest.raises(RuntimeError, match='metadata adapter'):
        wrapped.execute('localhost', runtime.DEFAULT_DOCKER + ['image', 'pull', 'image:tag'])


@pytest.mark.parametrize('config', [
    {'runtime': 'other'}, {'docker_host': 'tcp://localhost:1234', 'docker_context': 'desktop'},
    {'runtime': 'wslc', 'docker_host': 'tcp://localhost:1234'}, {'docker_tls': 'false'},
    {'wslc_session': 'default'}, {'typo': 'value'},
])
def test_runtime_configuration_rejects_ambiguous_options(config):
    with pytest.raises(ValueError):
        runtime.validate({'runtime_connections': {'controller': config}})


@pytest.mark.parametrize('arch,machine', [('amd64', 0x8664), ('arm64', 0xaa64)])
def test_windows_executable_hash_and_architecture(tmp_path, arch, machine):
    payload = bytearray(512)
    payload[:2] = b'MZ'
    payload[60:64] = (128).to_bytes(4, 'little')
    payload[128:132] = b'PE\0\0'
    payload[132:134] = machine.to_bytes(2, 'little')
    payload[152:154] = b'\x0b\x02'
    path = tmp_path / 'oci-relay.exe'
    path.write_bytes(payload)
    key = 'windows/' + arch
    digest = hashlib.sha256(payload).hexdigest()
    settings = {'binary_paths': {key: str(path)}, 'binary_sha256': {key: digest}}
    assert release.acquire(arch, settings, offline=True, os_name='windows') == (path, digest)
    with pytest.raises(release.BinaryInvalid, match='PE32'):
        release.verify_executable(path, 'arm64' if arch == 'amd64' else 'amd64', 'windows')
    path.write_bytes(b'tampered')
    with pytest.raises(release.BinaryInvalid, match='checksum'):
        release.acquire(arch, settings, offline=True, os_name='windows')
