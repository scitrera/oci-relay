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
        self.manifests = {}
        self.manifest_requests = []
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
                    key = self.path[len(prefix + 'manifests/'):]
                    fixture.manifest_requests.append(key)
                    data = fixture.manifests.get(key, fixture.manifest)
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


@pytest.mark.parametrize("bundled_decoder", [False, True])
def test_striped_registry_import_downloads_once_and_preserves_reuse(tmp_path, monkeypatch, caplog, bundled_decoder):
    from types import SimpleNamespace

    from sparkrun.plugins import ImagePullRequest
    from sparkrun.transports.session import SshHostSession
    from sparkrun_oci_relay.host import Runner
    from sparkrun_oci_relay.provider import RelayProvider

    fixture = RegistryFixture(size=12 << 20)
    config_file = tmp_path / 'docker-config.json'
    config_file.write_text(json.dumps({'auths': {fixture.host: {'auth': base64.b64encode(b'fixture:password').decode()}}}))
    settings = {'development_binary': os.environ['OCI_RELAY_BINARY'], 'remote_cache_dir': str(tmp_path / 'binaries'),
                'transport': 'http2-direct', 'registry_plain_http': True, 'registry_config': str(config_file),
                'registry_cache_bytes': 0, 'max_buffer_bytes': 8 << 20,
                'stripe_threshold_bytes': 8 << 20, 'stripe_streams': 4}
    if bundled_decoder:
        helper = os.environ.get('OCI_RELAY_UNPIGZ')
        if not helper:
            pytest.skip('requires native OCI_RELAY_UNPIGZ')
        settings.update(development_unpigz=helper, receiver_decoder='unpigz', max_decode_bytes=64 << 20)
    original = Runner.start_receiver

    def local_connections(self, host, binary, directory, arguments, inventory):
        # Route qualification has separate tests. Exercise four real TLS/HTTP2
        # connections on loopback, without depending on CI's physical NICs.
        arguments = list(arguments)
        index = arguments.index('--endpoint')
        endpoint = arguments[index + 1]
        arguments[index:index + 2] = ['--path', endpoint + ',127.0.0.1', '--connections-per-path', '4']
        return original(self, host, binary, directory, arguments, inventory)

    monkeypatch.setattr(Runner, 'start_receiver', local_connections)
    session = SshHostSession()
    old_ids = []
    try:
        for count in (1, 2, 2):
            previous = fixture.config_id
            fixture.update(count)
            if previous != fixture.config_id:
                old_ids.append(previous)
            fixture.downloads.clear()
            caplog.clear()
            request = ImagePullRequest(image=fixture.image, source_host=None, targets=('localhost',),
                transfer_hosts=('127.0.0.1',), timeout=90, force_pull=True, session=session,
                config=SimpleNamespace(plugin_settings=lambda _: settings))
            with caplog.at_level('INFO', logger='sparkrun_oci_relay.provider'):
                outcome = RelayProvider().pull(request)
            assert not outcome.errors and outcome.outcomes['localhost'] in {'complete', 'already_present'}
            observed = json.loads(subprocess.check_output(['docker', 'image', 'inspect', fixture.image]))[0]
            assert observed['Id'] == fixture.config_id
            assert observed['RootFS']['Layers'] == fixture.diff_ids[:count]
            downloads = [d for d in fixture.downloads if d in {v['digest'] for v in fixture.descriptors}]
            if count == 1 or previous != fixture.config_id:
                assert downloads == [fixture.descriptors[count - 1]['digest']], 'striping repeated upstream layer download'
                registry = next(record.args for record in caplog.records if record.msg == 'OCI Relay registry: %s')
                assert registry['shared_sha256_verifications'] == 1
                assert registry['local_sha256_verifications'] == 0
                timings = next(record.args for record in caplog.records if record.msg == 'OCI Relay receiver timings: %s')
                assert len(timings['localhost']['paths']) == 4
                assert all(p['stripe_requests'] == 1 and p['bytes'] > 0 for p in timings['localhost']['paths'])
                assert timings['localhost']['reused_layers'] == count - 1
                if bundled_decoder:
                    assert timings['localhost']['decoder']['layers'] == 1
                    assert timings['localhost']['decoder']['bytes'] >= 12 << 20
                    assert not timings['localhost']['decoder'].get('fallback')
            else:
                assert downloads == [], 'warm target downloaded layers'
    finally:
        session.close()
        fixture.close()
        subprocess.run(['docker', 'image', 'rm', fixture.image], capture_output=True)
        for image_id in old_ids:
            subprocess.run(['docker', 'image', 'rm', image_id], capture_output=True)


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


@pytest.mark.parametrize('index_pin', [False, True])
@pytest.mark.parametrize('transport', ['http2-direct', 'http2-ssh', 'ssh-stdio'])
def test_pinned_registry_to_docker_runtime_and_offline_cache(tmp_path, monkeypatch, caplog, index_pin, transport):
    """Exact registry identity -> selective import -> runnable immutable ID.

    No workload is started. Docker create validates that launch can use the
    bound ID even after the registry is unavailable and the retention tag moves.
    """
    from sparkrun.containers import distribute
    from sparkrun.core import image_distribution as api
    from sparkrun.core.image_preparation import resolve_content_images
    from sparkrun.orchestration.executors.docker import DockerExecutor
    from sparkrun_oci_relay import pins
    from sparkrun_oci_relay.provider import RelayProvider
    from sparkrun_oci_relay.progress import PROGRESS

    caplog.set_level(PROGRESS)
    fixture = RegistryFixture()
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
    config = Config(container_distribution_provider='oci-relay')
    token = api._CONFIG.set(config)
    monkeypatch.setattr(api, '_PROVIDERS', {'oci-relay': RelayProvider()})
    monkeypatch.setattr(distribute, 'ensure_image', lambda *a, **k: pytest.fail('builtin source pull was called'))
    monkeypatch.setattr(distribute, '_check_remote_image_identities', lambda *a, **k: pytest.fail('builtin source check was called'))
    tags = []
    runtime_ids = []
    container = None
    closed = False

    @api.image_distribution_operation
    def transfer(config, image, *, offline=False):
        if transport == 'http2-ssh':
            assert distribute.distribute_image_from_head(image, ['localhost'], timeout=90, offline=offline) == []
        else:
            assert distribute.distribute_image_from_local(image, ['localhost'], transfer_hosts=['127.0.0.1'],
                                                          timeout=90, offline=offline) == []
        resolved = api.resolve_distributed_image(image, 'localhost')
        assert resolved == fixture.config_id
        assert resolve_content_images([image], ['localhost']) == (resolved,)
        executor = DockerExecutor()
        executor.bind_image_references({image: resolved})
        command = executor.run_cmd(image, command='true')
        assert '--pull=never' in command and resolved in command and image not in command
        return resolved

    try:
        for count in (1, 2):
            fixture.update(count)
            leaf_digest = digest(fixture.manifest)
            fixture.manifests[leaf_digest] = fixture.manifest
            root = fixture.manifest
            if index_pin:
                root = json.dumps({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.index.v1+json',
                                   'manifests': [{'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                                       'digest': leaf_digest, 'size': len(root),
                                       'platform': {'os': 'linux', 'architecture': platform.machine().replace('aarch64', 'arm64').replace('x86_64', 'amd64')}}]},
                                  separators=(',', ':')).encode()
            root_digest = digest(root)
            fixture.manifests[root_digest] = root
            image = fixture.image + '@' + root_digest
            tags.append(pins.retention_tag(image))
            fixture.downloads.clear()
            fixture.manifest_requests.clear()
            resolved = transfer(config, image)
            runtime_ids.append(resolved)
            assert fixture.manifest_requests[0] == root_digest
            assert 'test' not in fixture.manifest_requests
            assert [d for d in fixture.downloads if d in {item['digest'] for item in fixture.descriptors}] == [fixture.descriptors[count - 1]['digest']]
            observed = json.loads(subprocess.check_output(['docker', 'image', 'inspect', resolved]))[0]
            assert observed['RootFS']['Layers'] == fixture.diff_ids[:count]
            # Import didn't fabricate an upstream Docker RepoDigest.
            assert image not in (observed.get('RepoDigests') or [])
        # A fresh provider instance/process must recover the verified binding.
        fixture.close()
        closed = True
        fixture.downloads.clear()
        monkeypatch.setattr(api, '_PROVIDERS', {'oci-relay': RelayProvider()})
        assert transfer(config, image, offline=True) == runtime_ids[-1]
        assert not fixture.downloads
        assert 'pinned image already verified' in caplog.text
        if transport == 'http2-direct':
            # Exercise local-source preparation using only the receipt: the
            # upstream pin is absent from Docker and the registry is stopped.
            from sparkrun.transports.session import SshHostSession
            session = SshHostSession()
            try:
                result = RelayProvider().copy(api.ImageCopyRequest(
                    image=image, source_host=None, targets=('localhost',), transfer_hosts=('127.0.0.1',),
                    config=config, session=session, offline=True, timeout=90,
                ))
                assert result.outcomes == {'localhost': 'already_present'}
                assert result.runtime_images == {'localhost': runtime_ids[-1]}
            finally:
                session.close()
        # Move the retention alias to an unrelated earlier image. A pin must
        # still resolve to its recorded immutable ID and be runnable locally.
        subprocess.run(['docker', 'tag', runtime_ids[0], tags[-1]], check=True, capture_output=True)
        assert api.resolve_distributed_image(image, 'localhost') == runtime_ids[-1]
        container = subprocess.check_output(['docker', 'create', '--pull=never', '--entrypoint', '/fixture-command',
                                             runtime_ids[-1]], text=True).strip()
        assert subprocess.check_output(['docker', 'inspect', '--format={{.Image}}', container], text=True).strip() == runtime_ids[-1]
        # Removing the immutable image invalidates its receipt even if another
        # image now occupies the retention tag. Never run that replacement.
        subprocess.run(['docker', 'rm', container], check=True, capture_output=True)
        container = None
        subprocess.run(['docker', 'image', 'rm', runtime_ids[-1]], check=True, capture_output=True)
        assert api.resolve_distributed_image(image, 'localhost') == image
        with pytest.raises(api.ImageDistributionFailed, match='not resident'):
            transfer(config, image, offline=True)
    finally:
        api._CONFIG.reset(token)
        if not closed:
            fixture.close()
        if container:
            subprocess.run(['docker', 'rm', '-f', container], capture_output=True)
        for tag in tags:
            subprocess.run(['docker', 'image', 'rm', tag], capture_output=True)
        for image_id in runtime_ids:
            subprocess.run(['docker', 'image', 'rm', image_id], capture_output=True)


def test_corrupt_registry_pin_never_transfers_or_falls_back(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from sparkrun.plugins import ImagePullRequest
    from sparkrun.transports.session import SshHostSession
    from sparkrun_oci_relay.host import OperationError, Runner
    from sparkrun_oci_relay.provider import RelayProvider

    fixture = RegistryFixture(layer_count=1)
    config_file = tmp_path / 'docker-config.json'
    config_file.write_text(json.dumps({'auths': {fixture.host: {'auth': base64.b64encode(b'fixture:password').decode()}}}))
    settings = dict(development_binary=os.environ['OCI_RELAY_BINARY'], remote_cache_dir=str(tmp_path / 'binaries'),
                    registry_config=str(config_file), registry_plain_http=True, transport='http2-direct')
    monkeypatch.setattr(Runner, 'start_receiver', lambda *a, **k: pytest.fail('invalid pin reached receiver'))
    session = SshHostSession()
    try:
        request = ImagePullRequest(image=fixture.image + '@sha256:' + '0' * 64, source_host=None,
                                   targets=('localhost',), transfer_hosts=('127.0.0.1',), timeout=90, session=session,
                                   config=SimpleNamespace(plugin_settings=lambda _: settings))
        with pytest.raises(OperationError):
            RelayProvider().pull(request)
        assert not fixture.downloads
        assert not list((tmp_path / 'binaries' / 'pins').glob('*.json'))
    finally:
        session.close()
        fixture.close()


@pytest.mark.parametrize('failure_phase', ['preflight', 'record'])
def test_pin_metadata_failure_is_not_reported_complete_and_retry_reuses_import(tmp_path, monkeypatch, caplog, failure_phase):
    from types import SimpleNamespace
    from sparkrun.plugins import ImagePullRequest
    from sparkrun.transports.session import SshHostSession
    from sparkrun_oci_relay import pins
    from sparkrun_oci_relay.host import OperationError, Runner
    from sparkrun_oci_relay.provider import RelayProvider
    from sparkrun_oci_relay.progress import PROGRESS

    fixture = RegistryFixture(layer_count=1)
    image = fixture.image + '@' + digest(fixture.manifest)
    config_file = tmp_path / 'config.json'
    config_file.write_text(json.dumps({'auths': {fixture.host: {'auth': base64.b64encode(b'fixture:password').decode()}}}))
    settings = dict(development_binary=os.environ['OCI_RELAY_BINARY'], remote_cache_dir=str(tmp_path / 'cache'),
                    registry_config=str(config_file), registry_plain_http=True, transport='http2-direct', max_buffer_bytes=8 << 20)
    starts = []
    original_start = Runner.start_source
    def start(*args, **kwargs):
        starts.append(True)
        return original_start(*args, **kwargs)
    monkeypatch.setattr(Runner, 'start_source', start)
    original = getattr(pins, failure_phase)
    def disk_full(*args, **kwargs):
        raise OperationError('localhost: No space left on device')
    monkeypatch.setattr(pins, failure_phase, disk_full)
    session = SshHostSession()
    request = ImagePullRequest(image=image, source_host=None, targets=('localhost',), transfer_hosts=('127.0.0.1',),
                               timeout=90, session=session, config=SimpleNamespace(plugin_settings=lambda _: settings))
    caplog.set_level(PROGRESS)
    try:
        with pytest.raises(OperationError, match='No space left on device') as error:
            RelayProvider().pull(request)
        assert 'OCI Relay: failed in' in caplog.text
        assert 'OCI Relay: complete in' not in caplog.text
        assert not list((tmp_path / 'cache' / 'pins').glob('*.json'))
        if failure_phase == 'preflight':
            assert not starts and not fixture.downloads
        else:
            assert starts and fixture.descriptors[0]['digest'] in fixture.downloads
            assert 'images imported and verified' in str(error.value)
            actual = subprocess.check_output(['docker', 'image', 'inspect', '--format={{.Id}}', pins.retention_tag(image)], text=True).strip()
            assert actual == fixture.config_id
        monkeypatch.setattr(pins, failure_phase, original)
        fixture.downloads.clear()
        caplog.clear()
        result = RelayProvider().pull(request)
        assert not result.errors and result.runtime_images == {'localhost': fixture.config_id}
        assert 'OCI Relay: complete in' in caplog.text
        if failure_phase == 'record':
            assert result.outcomes == {'localhost': 'already_present'}
            assert fixture.descriptors[0]['digest'] not in fixture.downloads
        receipts = list((tmp_path / 'cache' / 'pins').glob('*.json'))
        assert len(receipts) == 1 and json.loads(receipts[0].read_text())['runtime_image'] == fixture.config_id
        assert not list((tmp_path / 'cache' / 'pins').glob('.preflight.*'))
    finally:
        session.close()
        fixture.close()
        subprocess.run(['docker', 'image', 'rm', pins.retention_tag(image)], capture_output=True)
