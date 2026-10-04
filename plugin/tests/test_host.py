# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
import io
import time

import pytest

from sparkrun_oci_relay.host import Lines, OperationError, pump


def test_events_are_bounded_and_require_objects():
    lines = Lines(io.BytesIO(b'{"type":"ready"}\n{"type":"result"}\n'), events=True)
    assert lines.event(time.monotonic() + 2)["type"] == "ready"
    assert lines.event(time.monotonic() + 2)["type"] == "result"
    with pytest.raises(OperationError, match="required result"):
        lines.event(time.monotonic() + 2)
    invalid = Lines(io.BytesIO(b"[]\n"), events=True)
    with pytest.raises(OperationError, match="object"):
        invalid.event(time.monotonic() + 2)
    too_long = Lines(io.BytesIO(b"x" * (65536 + 1)))
    assert too_long.finished.wait(2)
    assert isinstance(too_long.error, OperationError)


def test_pipe_handles_short_writes():
    class Partial:
        def __init__(self):
            self.data = bytearray()
            self.closed = False

        def write(self, data):
            self.data.extend(data[:7])
            return min(7, len(data))

        def close(self):
            self.closed = True

    data = b"test payload" * 10000
    target = Partial()
    pump(io.BytesIO(data), target)
    assert target.closed and target.data == data


def test_native_source_qualification_and_cleanup():
    import json
    from types import SimpleNamespace

    ImageDistributionUnsupported = pytest.importorskip("sparkrun.plugins").ImageDistributionUnsupported
    from sparkrun_oci_relay.host import LOCAL_DOCKER, Runner

    runner = Runner.__new__(Runner)
    runner.settings = {}
    runner.containers, runner.directories, runner.processes = [], [], []
    runner.local = SimpleNamespace(close=lambda: None)
    calls = []
    source_id = 'sha256:' + 'a' * 64
    info = {'version': '29.1.3', 'driver': 'overlay2', 'root': '/var/lib/docker', 'security': [], 'os': 'linux', 'runtimes': {'runc': {}}}

    def execute(host, args, **kwargs):
        calls.append(args)
        if args[:4] == LOCAL_DOCKER + ['info']:
            return json.dumps(info).encode()
        if args[:5] == LOCAL_DOCKER + ['image', 'inspect']:
            return source_id.encode()
        if args[0] == 'id':
            return b'1000'
        if args[:4] == LOCAL_DOCKER + ['create']:
            # Exercise the indeterminate-create cleanup path.
            raise OperationError('connection lost after create')
        return b''

    runner.execute = execute
    runner.json = lambda *args: None
    runner.start = lambda *args: None
    with pytest.raises(OperationError, match='connection lost'):
        runner.start_source(None, '/verified/relay', '/tmp/oci-relay.0123456789',
                            {'source': 'docker-classic', 'image': 'fixture:latest'})
    assert len(runner.containers) == 1
    create = next(args for args in calls if args[:4] == LOCAL_DOCKER + ['create'])
    assert '--read-only' in create and '--privileged' not in create
    mounts = [create[i + 1] for i, value in enumerate(create) if value == '--mount']
    assert len(mounts) == 4 and all('docker.sock' not in value for value in mounts)
    assert all(value.endswith(',readonly') for value in mounts[:3])
    assert source_id in create
    assert runner.close() == []
    assert calls[-1] == LOCAL_DOCKER + ['rm', '--force', '--volumes', runner.containers[0][1]]
    calls.clear()
    info['version'] = '99.0.0'
    with pytest.raises(ImageDistributionUnsupported):
        runner.start_source(None, '/verified/relay', '/tmp/oci-relay.0123456789',
                            {'source': 'docker-classic', 'image': 'fixture:latest'})
    assert not any(args[:4] == LOCAL_DOCKER + ['create'] for args in calls)


def test_binary_cache_rechecks_hash_before_execution(tmp_path):
    import hashlib
    import shutil
    import subprocess

    from sparkrun_oci_relay.host import Runner

    class Session:
        uploads = 0
        calls = 0

        def execute(self, host, args, **kwargs):
            self.calls += 1
            return subprocess.run(args, capture_output=True, timeout=kwargs.get("timeout", 30))

        def upload(self, host, sources, destination):
            self.uploads += 1
            shutil.copyfile(sources[0], destination)

    binary = tmp_path / "binary"
    binary.write_text('#!/bin/sh\nprintf \'{"version":"0.1.0","protocol":1}\\n\'\n')
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    session = Session()
    runner = Runner.__new__(Runner)
    runner.settings = {"remote_cache_dir": str(tmp_path / "cache ' $() ;")}
    runner.connection = lambda host: session
    destination = runner.stage_binary("host", binary, digest, "0.1.0")
    assert session.uploads == 1
    before = session.calls
    assert runner.stage_binary("host", binary, digest, "0.1.0") == destination
    assert session.uploads == 1 and session.calls - before == 1
    # If the checksum were skipped, the corrupt cache entry would execute and
    # create this marker. Instead it must be replaced with the pinned bytes.
    from pathlib import Path
    import shlex

    cached = Path(destination)
    cached.chmod(0o755)
    marker = tmp_path / "must-not-execute"
    cached.write_text('#!/bin/sh\ntouch ' + shlex.quote(str(marker)) + '\nexit 1\n')
    runner.stage_binary("host", binary, digest, "0.1.0")
    assert not marker.exists()
    assert session.uploads == 2
    assert hashlib.sha256(cached.read_bytes()).hexdigest() == digest


def test_relay_runtime_tuning_does_not_wrap_docker_cli():
    from types import SimpleNamespace
    from sparkrun_oci_relay.host import LOCAL_DOCKER, Runner

    calls = []
    runner = Runner.__new__(Runner)
    runner.settings = {'relay_gomaxprocs': 4}
    runner.processes = []
    runner.connection = lambda host: SimpleNamespace(open_process=lambda host, args: calls.append(args))
    runner.start('host', ['/verified/relay', 'peer'])
    runner.start('host', LOCAL_DOCKER + ['start', '--attach', 'owned-container'])
    assert calls[0] == ['env', 'GOMAXPROCS=4', '/verified/relay', 'peer']
    assert calls[1] == LOCAL_DOCKER + ['start', '--attach', 'owned-container']
