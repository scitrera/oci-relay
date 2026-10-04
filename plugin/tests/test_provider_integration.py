# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
"""Opt-in, real Docker and Sparkrun process/deployment integration."""
import json
import os
import subprocess
from types import SimpleNamespace
import uuid

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("OCI_RELAY_PLUGIN_TESTS") != "1",
    reason="requires patched Sparkrun develop-next, Docker and OCI_RELAY_BINARY",
)


@pytest.mark.parametrize("transport", ["http2-direct", "http2-ssh", "ssh-stdio"])
@pytest.mark.parametrize("source_mode", ["docker", "docker-save", "docker-classic"])
def test_real_provider_local_lifecycle(tmp_path, transport, source_mode):
    from sparkrun.plugins import ImageCopyRequest
    from sparkrun.transports.session import SshHostSession
    from sparkrun_oci_relay.provider import RelayProvider

    binary = os.environ["OCI_RELAY_BINARY"]
    tag = "oci-relay-plugin-test:" + uuid.uuid4().hex
    (tmp_path / "Dockerfile").write_text("FROM scratch\nCOPY payload /payload\n")
    (tmp_path / "payload").write_bytes(os.urandom(128 << 10))
    subprocess.run(["docker", "build", "-q", "-t", tag, str(tmp_path)], check=True, capture_output=True)
    settings = {
        "development_binary": binary,
        "remote_cache_dir": str(tmp_path / "cache"),
        "transport": transport,
        "source_mode": source_mode,
        "max_buffer_bytes": 8 << 20,
        "max_spool_bytes": 4 << 20,
    }
    session = SshHostSession()
    provider = RelayProvider()
    try:
        request = ImageCopyRequest(
            image=tag, source_host=None, targets=("localhost",), transfer_hosts=("127.0.0.1",),
            timeout=90, offline=True, session=session,
            config=SimpleNamespace(plugin_settings=lambda name: settings),
        )
        result = provider.copy(request)
        assert result.outcomes == {"localhost": "complete"}
        assert not result.errors
        image_id = subprocess.check_output(["docker", "image", "inspect", "--format={{.Id}}", tag], text=True).strip()
        assert image_id.startswith("sha256:")
        # Check this unique fixture, without mistaking another live operation's
        # source tag for a leak from this completed transfer.
        tags = json.loads(subprocess.check_output(
            ["docker", "image", "inspect", "--format={{json .RepoTags}}", image_id], text=True,
        ))
        assert all("/relay/" not in name for name in tags)
    finally:
        session.close()
        subprocess.run(["docker", "image", "rm", tag], capture_output=True)


def test_native_source_cancellation_removes_helper(tmp_path):
    import time

    from sparkrun.transports.session import SshHostSession
    from sparkrun_oci_relay.host import Lines, Runner

    binary = os.environ['OCI_RELAY_BINARY']
    tag = 'oci-relay-plugin-test:' + uuid.uuid4().hex
    (tmp_path / 'Dockerfile').write_text('FROM scratch\nCOPY payload /payload\n')
    (tmp_path / 'payload').write_bytes(os.urandom(1024))
    subprocess.run(['docker', 'build', '-q', '-t', tag, str(tmp_path)], check=True, capture_output=True)
    session = SshHostSession()
    runner = Runner(session, {})
    try:
        directory = runner.directory(None)
        runner.execute(None, [binary, 'session', '--out', directory, '--peers', 'unstarted'])
        process = runner.start_source(None, binary, directory, {
            'version': 1, 'source': 'docker-classic', 'image': tag,
            'session_dir': directory, 'socket': directory + '/source.sock',
            'listen': '127.0.0.1:0', 'managed_stdin': True, 'timeout_seconds': 60,
            'max_buffer_bytes': 8 << 20, 'source_streams': 2,
        })
        events = Lines(process.stdout, events=True)
        Lines(process.stderr)
        assert events.event(time.monotonic() + 15)['type'] == 'ready'
        process.stdin.write(b'{"type":"cancel"}\n')
        assert events.event(time.monotonic() + 10)['state'] == 'CANCELLED'
        assert process.wait(timeout=10) != 0
        assert runner.close() == []
        assert subprocess.run(['docker', 'container', 'inspect', runner.containers[0][1]], capture_output=True).returncode != 0
        assert not os.path.exists(directory)
    finally:
        runner.close()
        session.close()
        subprocess.run(['docker', 'image', 'rm', tag], capture_output=True)


@pytest.mark.parametrize('choice,transport', [
    ('native', 'http2-direct'), ('native', 'http2-ssh'), ('native', 'ssh-stdio'),
    ('archive', 'http2-direct'), ('push', 'http2-direct'), ('manifest', 'http2-direct'),
])
def test_real_auto_source_choice(tmp_path, caplog, choice, transport):
    from sparkrun.plugins import ImageCopyRequest
    from sparkrun.transports.session import SshHostSession
    from sparkrun_oci_relay.provider import RelayProvider

    binary = os.environ['OCI_RELAY_BINARY']
    tag = 'oci-relay-plugin-test:' + uuid.uuid4().hex
    (tmp_path / 'Dockerfile').write_text('FROM scratch\nCOPY payload /payload\n')
    (tmp_path / 'payload').write_bytes(os.urandom(128 << 10))
    subprocess.run(['docker', 'build', '-q', '-t', tag, str(tmp_path)], check=True, capture_output=True)
    settings = {
        'development_binary': binary, 'remote_cache_dir': str(tmp_path / 'cache'),
        # Deliberately omit source_mode: auto is the default.
        'transport': transport, 'allow_native_store': choice in {'native', 'manifest'},
        'allow_preparation_read': choice in {'archive', 'push'},
        'max_buffer_bytes': 8 << 20, 'network_gbps': 100,
    }
    if choice == 'archive':
        settings['max_spool_bytes'] = 32 << 20
    session = SshHostSession()
    try:
        if choice == 'manifest':
            manifest = tmp_path / 'manifest.json'
            subprocess.run([binary, 'prepare', '--image', tag, '--allow-preparation-read',
                            '--output', str(manifest)], check=True, capture_output=True)
            settings['manifest'] = str(manifest)
        expected = {'native': 'docker-classic', 'archive': 'docker-save', 'push': 'docker', 'manifest': 'docker'}[choice]
        request = ImageCopyRequest(
            image=tag, source_host=None, targets=('localhost',), transfer_hosts=('127.0.0.1',),
            timeout=90, offline=True, session=session,
            config=SimpleNamespace(plugin_settings=lambda name: settings),
        )
        with caplog.at_level('INFO', logger='sparkrun_oci_relay.provider'):
            result = RelayProvider().copy(request)
        assert result.outcomes == {'localhost': 'complete'} and not result.errors
        assert f'requested=auto selected={expected} reason=' in caplog.text
        assert 'source_mode' not in settings
    finally:
        session.close()
        subprocess.run(['docker', 'image', 'rm', tag], capture_output=True)


def test_auto_does_not_hide_source_integrity_failure(tmp_path, monkeypatch):
    from sparkrun.plugins import ImageCopyRequest
    from sparkrun.transports.session import SshHostSession
    from sparkrun_oci_relay.host import OperationError, Runner
    from sparkrun_oci_relay.provider import RelayProvider

    binary = os.environ['OCI_RELAY_BINARY']
    starts = []

    def fail_source(self, host, source_binary, directory, plan):
        starts.append(plan['source'])
        raise OperationError('native config digest does not match pinned image ID')

    monkeypatch.setattr(Runner, 'start_source', fail_source)
    session = SshHostSession()
    settings = {'allow_native_store': True, 'allow_preparation_read': True,
                'development_binary': binary, 'remote_cache_dir': str(tmp_path / 'cache')}
    try:
        request = ImageCopyRequest(
            image='fixture:never-pulled', source_host=None, targets=('localhost',), transfer_hosts=('127.0.0.1',),
            timeout=60, offline=True, session=session,
            config=SimpleNamespace(plugin_settings=lambda name: settings),
        )
        with pytest.raises(OperationError, match='digest'):
            RelayProvider().copy(request)
        assert starts == ['docker-classic']
    finally:
        session.close()
