# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
"""Real Docker receiver pulling registry-only fixtures through the actual core hook."""
import base64
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
import platform
import subprocess
import tarfile
import threading
import time
import uuid

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('OCI_RELAY_PLUGIN_TESTS') != '1', reason='requires real Docker and OCI_RELAY_BINARY')


def digest(data):
    return 'sha256:' + hashlib.sha256(data).hexdigest()


class RegistryFixture:
    def __init__(self, size=128 << 10, layer_count=2, chunk_delay=0):
        self.repository = 'relay-test/' + uuid.uuid4().hex
        self.blobs = {}
        self.descriptors = []
        self.diff_ids = []
        self.downloads = []
        self.requests_lock = threading.Lock()
        for number in range(layer_count):
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode='w') as tar:
                entry = tarfile.TarInfo(f'payload-{number}')
                payload = os.urandom(size)
                entry.size = len(payload)
                tar.addfile(entry, io.BytesIO(payload))
            raw = buffer.getvalue()
            blob = gzip.compress(raw, mtime=0)
            self.blobs[digest(blob)] = blob
            self.descriptors.append({'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip', 'digest': digest(blob), 'size': len(blob)})
            self.diff_ids.append(digest(raw))
        self.update(1)
        fixture = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                if self.headers.get('Authorization') != 'Basic ' + base64.b64encode(b'fixture:password').decode():
                    self.send_response(401)
                    self.send_header('WWW-Authenticate', 'Basic realm="fixture"')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                prefix = '/v2/' + fixture.repository + '/'
                if self.path.startswith(prefix + 'manifests/'):
                    data = fixture.manifest
                elif self.path.startswith(prefix + 'blobs/'):
                    key = self.path[len(prefix + 'blobs/'):]
                    data = fixture.blobs.get(key)
                    with fixture.requests_lock:
                        fixture.downloads.append(key)
                else:
                    data = None
                if data is None:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                if chunk_delay and self.path.rsplit('/', 1)[-1] in {d['digest'] for d in fixture.descriptors}:
                    for offset in range(0, len(data), 64 << 10):
                        self.wfile.write(data[offset:offset + (64 << 10)])
                        self.wfile.flush()
                        time.sleep(chunk_delay)
                else:
                    self.wfile.write(data)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.host = '127.0.0.1:' + str(self.server.server_port)
        self.image = self.host + '/' + self.repository + ':test'

    def update(self, layers):
        arch = {'aarch64': 'arm64', 'x86_64': 'amd64'}[platform.machine()]
        config = json.dumps({'architecture': arch, 'os': 'linux', 'config': {},
                             'rootfs': {'type': 'layers', 'diff_ids': self.diff_ids[:layers]}}, separators=(',', ':')).encode()
        self.config_id = digest(config)
        self.blobs[self.config_id] = config
        self.manifest = json.dumps({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
            'config': {'mediaType': 'application/vnd.oci.image.config.v1+json', 'digest': self.config_id, 'size': len(config)},
            'layers': self.descriptors[:layers]}, separators=(',', ':')).encode()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


@pytest.mark.parametrize('transport', ['http2-direct', 'http2-ssh', 'ssh-stdio'])
def test_registry_progress_visible_during_transfer(tmp_path, transport, caplog, monkeypatch):
    from types import SimpleNamespace

    from sparkrun.core.progress import PROGRESS
    from sparkrun.plugins import ImagePullRequest
    from sparkrun.transports.session import SshHostSession
    from sparkrun_oci_relay.provider import RelayProvider
    from sparkrun_oci_relay import provider
    from sparkrun_oci_relay.progress import Progress

    # Exercise live transport events without turning the default 30s display
    # cadence into a long-running integration fixture. Cadence has clock tests.
    monkeypatch.setattr(provider, 'Progress', lambda image: Progress(image, interval=1))

    fixture = RegistryFixture(size=1 << 20, layer_count=1, chunk_delay=0.25)
    config_file = tmp_path / 'docker-config.json'
    config_file.write_text(json.dumps({'auths': {fixture.host: {'auth': base64.b64encode(b'fixture:password').decode()}}}))
    settings = {
        'development_binary': os.environ['OCI_RELAY_BINARY'], 'remote_cache_dir': str(tmp_path / 'binaries'),
        'transport': transport, 'registry_plain_http': True, 'registry_config': str(config_file),
        'registry_cache_bytes': 4 << 20, 'max_buffer_bytes': 8 << 20,
    }
    session = SshHostSession()
    try:
        request = ImagePullRequest(
            image=fixture.image, source_host=None, targets=('localhost',), transfer_hosts=('127.0.0.1',),
            timeout=90, force_pull=True, session=session,
            config=SimpleNamespace(plugin_settings=lambda name: settings),
        )
        with caplog.at_level(PROGRESS):
            result = RelayProvider().pull(request)
        assert result.outcomes == {'localhost': 'complete'} and not result.errors
        lines = [r.getMessage() for r in caplog.records if r.name == 'sparkrun_oci_relay.progress']
        active = [i for i, line in enumerate(lines) if 'received (up to' in line and '0 B received' not in line]
        verified = next(i for i, line in enumerate(lines) if 'localhost: image verified' in line)
        assert active and min(active) < verified, lines
        assert any('registry download:' in line for line in lines)
        assert 'OCI Relay: complete in' in lines[-1]
        assert 'transferred to receivers' in lines[-1]
        assert 'fixture:password' not in caplog.text and str(config_file) not in caplog.text
        observed = json.loads(subprocess.check_output(['docker', 'image', 'inspect', fixture.image]))[0]
        assert observed['RootFS']['Layers'] == fixture.diff_ids
        assert observed['Id'] == fixture.config_id
    finally:
        session.close()
        fixture.close()
        subprocess.run(['docker', 'image', 'rm', fixture.image], capture_output=True)


@pytest.mark.parametrize('transport', ['http2-direct', 'http2-ssh', 'ssh-stdio'])
@pytest.mark.parametrize('latest_refresh', [False, True])
def test_registry_core_hook_import_reuse_and_warm_skip(tmp_path, monkeypatch, transport, caplog, latest_refresh):
    from sparkrun.core import image_distribution as api
    from sparkrun.core.progress import PROGRESS
    from sparkrun.containers import distribute
    from sparkrun_oci_relay.provider import RelayProvider
    from sparkrun_oci_relay.progress import size

    caplog.set_level(PROGRESS)
    fixture = RegistryFixture()
    if latest_refresh:
        fixture.image = fixture.image.removesuffix(':test') + ':latest'
    config_file = tmp_path / 'docker-config.json'
    config_file.write_text(json.dumps({'auths': {fixture.host: {'auth': base64.b64encode(b'fixture:password').decode()}}}))
    settings = {
        'development_binary': os.environ['OCI_RELAY_BINARY'], 'remote_cache_dir': str(tmp_path / 'binaries'),
        'transport': transport, 'registry_plain_http': True, 'registry_config': str(config_file),
        'registry_cache_bytes': 4 << 20, 'max_buffer_bytes': 8 << 20,
    }
    class Config(dict):
        def plugin_settings(self, name):
            return settings
    token = api._CONFIG.set(Config(container_distribution_provider='oci-relay'))
    monkeypatch.setattr(api, '_PROVIDERS', {'oci-relay': RelayProvider()})
    monkeypatch.setattr(distribute, 'ensure_image', lambda *a, **k: pytest.fail('builtin source pull was called'))
    old_ids = []
    try:
        # Fixture exists only in the test registry, never built/pulled on source.
        assert subprocess.run(['docker', 'image', 'inspect', fixture.image], capture_output=True).returncode != 0
        for count in (1, 2, 2):
            caplog.clear()
            previous = fixture.config_id
            fixture.update(count)
            if previous != fixture.config_id:
                old_ids.append(previous)
            fixture.downloads.clear()
            assert distribute.distribute_image_from_local(fixture.image, ['localhost'], transfer_hosts=['127.0.0.1'],
                                                          force_pull=count == 2 and not latest_refresh, timeout=90) == []
            observed = subprocess.check_output(['docker', 'image', 'inspect', '--format={{.Id}}', fixture.image], text=True).strip()
            assert observed == fixture.config_id
            # The native helper already pins the previous image. It must not
            # leave a last temporary base tag that Docker cannot remove while
            # the helper is alive during a moving-tag update.
            for candidate in [fixture.config_id, *old_ids]:
                probe = subprocess.run(['docker', 'image', 'inspect', '--format={{json .RepoTags}}', candidate],
                                       capture_output=True, text=True)
                if probe.returncode == 0:
                    assert not any(tag.startswith('oci-relay-cache/') for tag in (json.loads(probe.stdout) or []))
            layer_reads = [key for key in fixture.downloads if key in {d['digest'] for d in fixture.descriptors}]
            if count == 1:
                assert layer_reads == [fixture.descriptors[0]['digest']]
                assert f"{size(fixture.descriptors[0]['size'])} transferred to receivers" in caplog.text
            elif previous != fixture.config_id:
                assert layer_reads == [fixture.descriptors[1]['digest']], 'existing base layer was downloaded again'
                assert f"{size(fixture.descriptors[1]['size'])} transferred to receivers; 1 layer reused" in caplog.text
            else:
                assert layer_reads == [], 'warm target downloaded layers'
                assert "image already present — verified; 0 B transferred" in caplog.text
    finally:
        api._CONFIG.reset(token)
        fixture.close()
        subprocess.run(['docker', 'image', 'rm', fixture.image], capture_output=True)
        for image_id in old_ids:
            subprocess.run(['docker', 'image', 'rm', image_id], capture_output=True)


def test_latest_metadata_failure_reuses_local_image_without_builtin_pull(tmp_path, monkeypatch, caplog):
    from sparkrun.core import image_distribution as api
    from sparkrun.containers import distribute
    from sparkrun_oci_relay.provider import RelayProvider
    from sparkrun_oci_relay.progress import PROGRESS

    caplog.set_level(PROGRESS)
    fixture = RegistryFixture(layer_count=1)
    fixture.image = fixture.image.removesuffix(':test') + ':latest'
    config_file = tmp_path / 'docker-config.json'
    config_file.write_text(json.dumps({'auths': {fixture.host: {'auth': base64.b64encode(b'fixture:password').decode()}}}))
    settings = {
        'development_binary': os.environ['OCI_RELAY_BINARY'], 'remote_cache_dir': str(tmp_path / 'binaries'),
        'registry_plain_http': True, 'registry_config': str(config_file), 'max_buffer_bytes': 8 << 20,
    }
    class Config(dict):
        def plugin_settings(self, name):
            return settings
    token = api._CONFIG.set(Config(container_distribution_provider='oci-relay'))
    monkeypatch.setattr(api, '_PROVIDERS', {'oci-relay': RelayProvider()})
    monkeypatch.setattr(distribute, 'ensure_image', lambda *a, **k: pytest.fail('builtin source pull was called'))
    try:
        assert distribute.distribute_image_from_local(fixture.image, ['localhost'], transfer_hosts=['127.0.0.1'], timeout=90) == []
        fixture.downloads.clear()
        caplog.clear()
        # Lose registry access after caching latest. No receiver starts during
        # the failed metadata resolution; the original local identity is safe.
        config_file.write_text('{}')
        assert distribute.distribute_image_from_local(fixture.image, ['localhost'], transfer_hosts=['127.0.0.1'], timeout=90) == []
        observed = json.loads(subprocess.check_output(['docker', 'image', 'inspect', fixture.image]))[0]
        assert observed['Id'] == fixture.config_id
        assert observed['RootFS']['Layers'] == fixture.diff_ids
        assert not fixture.downloads
        assert 'using cached image' in caplog.text
        assert 'registry refresh unavailable before transfer' in caplog.text
    finally:
        api._CONFIG.reset(token)
        fixture.close()
        subprocess.run(['docker', 'image', 'rm', fixture.image], capture_output=True)


@pytest.mark.parametrize('transport', ['http2-direct', 'ssh-stdio'])
def test_registry_reuses_layers_from_multiple_images_after_parent_change(tmp_path, monkeypatch, caplog, transport):
    from sparkrun.core import image_distribution as api
    from sparkrun.containers import distribute
    from sparkrun_oci_relay.provider import RelayProvider

    fixture = RegistryFixture(layer_count=5)
    descriptors, diffs = fixture.descriptors.copy(), fixture.diff_ids.copy()
    config_file = tmp_path / 'docker-config.json'
    config_file.write_text(json.dumps({'auths': {fixture.host: {'auth': base64.b64encode(b'fixture:password').decode()}}}))
    settings = {'development_binary': os.environ['OCI_RELAY_BINARY'], 'remote_cache_dir': str(tmp_path / 'binaries'),
                'transport': transport, 'registry_plain_http': True, 'registry_config': str(config_file),
                'max_buffer_bytes': 8 << 20}
    class Config(dict):
        def plugin_settings(self, name):
            return settings
    token = api._CONFIG.set(Config(container_distribution_provider='oci-relay'))
    monkeypatch.setattr(api, '_PROVIDERS', {'oci-relay': RelayProvider()})
    monkeypatch.setattr(distribute, 'ensure_image', lambda *a, **k: pytest.fail('builtin source pull was called'))
    tags = []
    try:
        for name, indices in [('donor-a', [0, 1]), ('donor-b', [2, 3]), ('target', [4, 1, 3])]:
            fixture.image = fixture.host + '/' + fixture.repository + ':' + name
            tags.append(fixture.image)
            fixture.descriptors = [descriptors[i] for i in indices]
            fixture.diff_ids = [diffs[i] for i in indices]
            fixture.update(len(indices))
            fixture.downloads.clear()
            caplog.clear()
            with caplog.at_level('INFO', logger='sparkrun_oci_relay.provider'):
                assert distribute.distribute_image_from_local(fixture.image, ['localhost'], transfer_hosts=['127.0.0.1'],
                                                             force_pull=True, timeout=90) == []
            observed = json.loads(subprocess.check_output(['docker', 'image', 'inspect', '--format={{json .RootFS.Layers}}', fixture.image]))
            assert observed == fixture.diff_ids
            if name == 'target':
                layer_reads = [key for key in fixture.downloads if key in {d['digest'] for d in descriptors}]
                assert layer_reads == [descriptors[4]['digest']], 'cached non-prefix layers crossed the network'
                assert "'cached_blob_layers': 2" in caplog.text
                assert "'reused_layers': 0" in caplog.text
                assert "'discovery_complete': True" in caplog.text
                assert "'cache_probe_limited': False" in caplog.text
        assert subprocess.check_output(['docker', 'ps', '-aq', '--filter', 'label=com.scitrera.oci-relay.cache-pin=true']).strip() == b''
    finally:
        api._CONFIG.reset(token)
        fixture.close()
        for tag in reversed(tags):
            subprocess.run(['docker', 'image', 'rm', tag], capture_output=True)
