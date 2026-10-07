#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
"""Compare streaming registry imports with staged source decompression.

Requires separately provisioned, empty, isolated Docker stores. Measures source
preparation, plugin orchestration and image import; never starts a model or
changes a daemon. See docs/registry-source.md for the experiment's limitations.
"""

import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

from sparkrun.plugins import ImageCopyRequest
from sparkrun.transports.session import SshHostSession
from sparkrun_oci_relay.host import Runner
from sparkrun_oci_relay.provider import RelayProvider


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['compressed', 'uncompressed'])
    parser.add_argument('--image', required=True, help='exact SHA-256-pinned registry reference')
    parser.add_argument('--platform', default='linux/arm64')
    parser.add_argument('--host', action='append', required=True)
    parser.add_argument('--data-paths', required=True, help='plugin data_paths JSON file')
    parser.add_argument('--receiver-docker-host', required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--decompressor', type=Path)
    parser.add_argument('--workers', type=int, default=3)
    parser.add_argument('--max-buffer-bytes', type=int, default=512 << 20)
    parser.add_argument('--source-streams', type=int, default=8)
    parser.add_argument('--connections-per-path', type=int, choices=range(1, 5), default=2)
    parser.add_argument('--stripe-threshold-bytes', type=int, default=256 << 20)
    parser.add_argument('--stripe-streams', type=int, choices=range(2, 9), default=4)
    parser.add_argument('--stripe-piece-bytes', type=int, default=1 << 20)
    parser.add_argument('--http2-stream-window-bytes', type=int, default=0)
    parser.add_argument('--max-bytes', type=int, default=64 << 30)
    parser.add_argument('--spool-dir', type=Path, default=Path('/tmp'))
    parser.add_argument('--cache-bytes', type=int, help='omit for plugin default')
    parser.add_argument('--timeout', type=int, default=1800)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if '@sha256:' not in args.image or not args.receiver_docker_host.startswith('unix:///tmp/oci-relay-benchmark.'):
        parser.error('requires a digest pin and an explicitly isolated benchmark Docker socket')
    if args.mode == 'uncompressed' and not args.decompressor:
        parser.error('--decompressor is required for uncompressed mode')
    if args.output.exists():
        parser.error('refusing to overwrite an existing measurement')

    class Session(SshHostSession):
        def open_process(self, host, arguments):
            if host in args.host and len(arguments) > 1 and arguments[1] == 'peer' and '--check' not in arguments:
                arguments = [*arguments, '--docker-host', args.receiver_docker_host]
            return super().open_process(host, arguments)

    with args.binary.open('rb') as stream:
        binary_sha = hashlib.file_digest(stream, 'sha256').hexdigest()
    result = {'mode': args.mode, 'image': args.image, 'platform': args.platform, 'hosts': args.host, 'workers': args.workers,
              'max_buffer_bytes': args.max_buffer_bytes, 'source_streams': args.source_streams,
              'connections_per_path': args.connections_per_path,
              'stripe_threshold_bytes': args.stripe_threshold_bytes, 'stripe_streams': args.stripe_streams, 'stripe_piece_bytes': args.stripe_piece_bytes, 'http2_stream_window_bytes': args.http2_stream_window_bytes,
              'utc_started': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'binary_sha256': binary_sha}

    class Observations(logging.Handler):
        def emit(self, record):
            keys = {'OCI Relay registry: %s': 'registry_metrics', 'OCI Relay receiver timings: %s': 'receivers',
                    'OCI Relay timings: %s': 'provider_timings', 'OCI Relay transfer metrics: %s': 'transfer_metrics'}
            if record.msg in keys:
                result[keys[record.msg]] = dict(record.args)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logger = logging.getLogger('sparkrun_oci_relay.provider')
    observer = Observations()
    logger.addHandler(observer)
    session = Session()
    runner = Runner(session, {})
    docker = ['docker', '--host', args.receiver_docker_host]
    started = None
    try:
        result['engines'] = {}
        for host in args.host:
            info = json.loads(runner.execute(host, docker + ['info', '--format={{json .}}']))
            if info['Images'] or info['Containers'] or info['Driver'] not in {'overlay2', 'overlayfs'}:
                raise RuntimeError('benchmark requires empty isolated stores')
            result['engines'][host] = {k: info[k] for k in ('ServerVersion', 'Driver', 'Images', 'Containers')}
        with tempfile.TemporaryDirectory(prefix='oci-relay-import-bench-', dir=args.spool_dir) as spool:
            started = time.monotonic()
            layout = None
            if args.mode == 'uncompressed':
                stat = os.statvfs(spool)
                if stat.f_bavail * stat.f_frsize < args.max_bytes + (16 << 30):
                    raise RuntimeError('insufficient source headroom for the explicit export budget plus 16 GiB reserve')
                output = subprocess.check_output([
                    str(args.decompressor.resolve()), '--image', args.image, '--spool-dir', spool,
                    '--max-bytes', str(args.max_bytes), '--workers', str(args.workers), '--platform', args.platform,
                ], timeout=args.timeout)
                result['export'] = json.loads(output)
                layout = result['export']['layout']
                result['export_disk_bytes'] = sum(p.stat().st_size for p in Path(layout).rglob('*') if p.is_file())
            old = Runner.start_source

            def source(self, host, binary, directory, plan):
                if layout:
                    plan = dict(plan, source='oci-layout', layout=layout, image='', registry_cache_bytes=0)
                return old(self, host, binary, directory, plan)

            settings = {'development_binary': str(args.binary.resolve()), 'source_mode': 'registry',
                        'platform': args.platform,
                        'transport': 'http2-direct', 'allow_native_store': False,
                        'data_paths': json.loads(Path(args.data_paths).read_text()),
                        'connections_per_path': args.connections_per_path,
                        'stripe_threshold_bytes': args.stripe_threshold_bytes, 'stripe_streams': args.stripe_streams, 'stripe_piece_bytes': args.stripe_piece_bytes, 'http2_stream_window_bytes': args.http2_stream_window_bytes,
                        'source_streams': args.source_streams, 'max_buffer_bytes': args.max_buffer_bytes}
            if args.cache_bytes is not None:
                settings['registry_cache_bytes'] = args.cache_bytes
            # A disposable destination tag avoids publishing upstream pin receipts
            # for the experimental rewritten manifest. Exact config is checked below.
            request = ImageCopyRequest(image='oci-relay-benchmark:registry-import', source_host=None,
                targets=tuple(args.host), transfer_hosts=tuple(args.host),
                timeout=max(1, int(args.timeout - (time.monotonic() - started))), session=session,
                config=SimpleNamespace(plugin_settings=lambda _: settings))
            with patch.object(Runner, 'start_source', source):
                outcome = RelayProvider().copy(request, source_image=args.image)
            if outcome.errors or not all(v == 'complete' for v in outcome.outcomes.values()):
                raise RuntimeError(str(outcome))
            result['operation_seconds'] = time.monotonic() - started
            result['verified'] = {}
            for host in args.host:
                inspected = json.loads(runner.execute(host, docker + ['image', 'inspect', request.image]))[0]
                if inspected['Id'] != result['receivers'][host]['config_digest']:
                    raise RuntimeError('imported config mismatch')
                if layout and inspected['Id'] != result['export']['config_digest']:
                    raise RuntimeError('export changed the pinned config')
                result['verified'][host] = {'id': inspected['Id'], 'diff_ids': inspected['RootFS']['Layers']}
            # Charge removal of experiment staging to the total as well.
            if layout:
                shutil.rmtree(layout)
            result['total_seconds'] = time.monotonic() - started
            result['state'] = 'COMPLETE'
    except BaseException as error:
        result.update(state='FAILED', error=str(error))
        raise
    finally:
        if started is not None:
            result['elapsed_seconds'] = time.monotonic() - started
        result['cleanup_errors'] = runner.close()
        session.close()
        logger.removeHandler(observer)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
