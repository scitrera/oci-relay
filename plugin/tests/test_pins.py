# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0
"""Pin receipts never turn a mutable alias or an absent image into provenance."""
import json
from types import SimpleNamespace

import pytest

from sparkrun_oci_relay import pins
from sparkrun_oci_relay.host import OperationError

IMAGE = 'registry.test:5000/image:canary@sha256:' + '1' * 64
RUNTIME = 'sha256:' + '2' * 64
CONFIG = 'sha256:' + '3' * 64


@pytest.mark.parametrize('image', ['x@sha256:short', 'x@sha512:' + '1' * 128, '@sha256:' + '1' * 64, 'x@y@sha256:' + '1' * 64])
def test_invalid_pins_fail_closed(image):
    with pytest.raises(OperationError, match='full sha256'):
        pins.digest(image)


def test_alias_is_valid_deterministic_and_distinct():
    assert pins.retention_tag(IMAGE) == pins.retention_tag(IMAGE)
    assert '@' not in pins.retention_tag(IMAGE)
    assert pins.retention_tag(IMAGE) != pins.retention_tag(IMAGE[:-1] + '4')


def runner_for(receipt, *, present=True, native=False, arch='arm64'):
    commands = []
    def execute(host, args, **kwargs):
        commands.append(args)
        if args[0] == 'head':
            return SimpleNamespace(returncode=0, stdout=receipt)
        reference = args[-1]
        success = (reference == RUNTIME and present) or (reference == IMAGE and native)
        return SimpleNamespace(returncode=0 if success else 1, stdout=(RUNTIME + '|linux|' + arch).encode())
    return SimpleNamespace(connection=lambda host: SimpleNamespace(execute=execute),
                           cache_directory=lambda host: '/private/cache', settings={}, architecture=lambda host: 'arm64'), commands


def receipt(**extra):
    return json.dumps(dict(version=1, image=IMAGE, registry_digest=pins.digest(IMAGE),
                           runtime_image=RUNTIME, config_digest=CONFIG, **extra)).encode()


def test_receipt_resolves_immutable_id_even_when_alias_moves():
    runner, commands = runner_for(receipt())
    assert pins.resolve(runner, 'node', IMAGE) == RUNTIME
    assert commands[-1][-1] == RUNTIME
    assert all(pins.retention_tag(IMAGE) not in args for args in commands)


@pytest.mark.parametrize('data,present,arch', [
    (b'{broken', True, 'arm64'), (b'[]', True, 'arm64'), (b'x' * 16385, True, 'arm64'),
    (receipt().replace(IMAGE.encode(), b'wrong-reference'), True, 'arm64'),
    (receipt(), False, 'arm64'), (receipt(), True, 'amd64'),
])
def test_stale_malformed_wrong_pin_or_platform_receipt_is_not_trusted(data, present, arch):
    runner, commands = runner_for(data, present=present, arch=arch)
    assert pins.resolve(runner, 'node', IMAGE) is None
    assert commands[-1][-1] == IMAGE
    assert all(pins.retention_tag(IMAGE) not in args for args in commands)


def test_docker_native_digest_binding_can_supply_local_source():
    runner, commands = runner_for(b'', native=True)
    assert pins.resolve(runner, 'node', IMAGE) == RUNTIME
    assert commands[-1][-1] == IMAGE


def test_receipt_publish_is_private_atomic_and_shell_quoted(tmp_path):
    import subprocess
    from sparkrun_oci_relay.host import Runner
    runner = Runner.__new__(Runner)
    runner.settings = {'remote_cache_dir': str(tmp_path / "cache ' $(touch SHOULD_NOT_EXIST)")}
    runner.connection = lambda host: SimpleNamespace(execute=lambda host, args, **kw: subprocess.run(
        args, input=kw.get('input_data'), timeout=kw.get('timeout'), capture_output=True,
    ))
    pins.record(runner, 'node', IMAGE, RUNTIME, CONFIG)
    files = list(tmp_path.rglob('*.json'))
    assert len(files) == 1
    assert json.loads(files[0].read_text())['runtime_image'] == RUNTIME
    assert files[0].stat().st_mode & 0o777 == 0o600
    assert files[0].parent.stat().st_mode & 0o777 == 0o700
