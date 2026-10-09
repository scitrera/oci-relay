#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0
"""Opt-in qualification: only the explicitly listed Docker hosts are modified."""
import argparse
import json
import logging
from pathlib import Path
from types import SimpleNamespace
import uuid

from sparkrun.plugins import ImageCopyRequest
from sparkrun.transports.session import SshHostSession
from sparkrun_oci_relay.provider import RelayProvider

parser = argparse.ArgumentParser()
parser.add_argument("--hosts", required=True, help="comma-separated source,receiver,...")
parser.add_argument("--binary", required=True)
parser.add_argument("--source-mode", choices=["auto", "docker", "docker-save", "docker-classic"], default="docker")
parser.add_argument("--receiver-import", choices=["pull", "load-cached", "load"], default="pull")
parser.add_argument("--max-import-bytes", type=int, default=0)
parser.add_argument("--transport", action="append", choices=["http2-direct", "http2-ssh", "ssh-stdio"])
parser.add_argument("--data-paths", help="private JSON file of plugin host-to-IP maps (direct transport only)")
parser.add_argument("--connections-per-path", type=int, choices=range(1, 5), default=1)
args = parser.parse_args()
transports = args.transport or (["http2-direct"] if args.data_paths else ["http2-direct", "http2-ssh", "ssh-stdio"])
if args.data_paths and any(transport != "http2-direct" for transport in transports):
    parser.error("data_paths qualification requires http2-direct")
if args.connections_per_path != 1 and not args.data_paths:
    parser.error("connections_per_path requires data_paths")
hosts = args.hosts.split(",")
if len(hosts) < 2 or len(set(hosts)) != len(hosts):
    parser.error("at least two distinct hosts are required")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
binary = str(Path(args.binary).resolve())
session = SshHostSession()
provider = RelayProvider()
observations = []
try:
    for transport in transports:
        tag = "oci-relay-qualification:" + uuid.uuid4().hex
        source_dir = None
        try:
            result = session.execute(hosts[0], ["mktemp", "-d", "/tmp/oci-relay-qualification.XXXXXXXXXX"])
            if result.returncode:
                raise RuntimeError(result.stderr.decode())
            source_dir = result.stdout.decode().strip()
            # A fresh incompressible layer guarantees every receiver needs bytes.
            setup = (
                "set -eu\n"
                'dd if=/dev/urandom of="$1/payload" bs=1048576 count=32 status=none\n'
                'tar -C "$1" -cf - payload | docker import - "$2"\n'
            )
            result = session.execute(hosts[0], ["sh", "-c", setup, "qualification", source_dir, tag], timeout=120)
            if result.returncode:
                raise RuntimeError(result.stderr.decode())
            source_id = session.execute(hosts[0], ["docker", "image", "inspect", "--format={{.Id}}", tag]).stdout.strip()
            settings = {
                "source_mode": args.source_mode, "allow_native_store": args.source_mode in {"auto", "docker-classic"},
                "development_binary": binary, "transport": transport, "allow_preparation_read": True,
                "receiver_import": args.receiver_import, "max_import_bytes": args.max_import_bytes,
                "max_buffer_bytes": 8 << 20, "max_spool_bytes": 64 << 20, "max_upload_bytes": 64 << 20,
            }
            if args.data_paths:
                settings["data_paths"] = json.loads(Path(args.data_paths).read_text())
                settings["connections_per_path"] = args.connections_per_path
            request = ImageCopyRequest(
                image=tag, source_host=hosts[0], targets=tuple(hosts[1:]), transfer_hosts=tuple(hosts[1:]),
                timeout=180, offline=True, session=session,
                config=SimpleNamespace(plugin_settings=lambda name: settings),
            )
            outcome = provider.copy(request)
            if any(value != "complete" for value in outcome.outcomes.values()):
                raise RuntimeError(outcome)
            for host in hosts[1:]:
                result = session.execute(host, ["docker", "image", "inspect", "--format={{.Id}}", tag])
                if result.returncode or result.stdout.strip() != source_id:
                    raise RuntimeError("destination identity mismatch on " + host)
            observations.append({"transport": transport, "source": hosts[0], "receivers": hosts[1:],
                                 "image_id": source_id.decode(), "receiver_import": args.receiver_import, "state": "COMPLETE"})
            print(json.dumps(observations[-1]), flush=True)
        finally:
            for host in hosts:
                result = session.execute(host, ["docker", "image", "rm", tag], timeout=30)
                if result.returncode:
                    logging.warning("test-image cleanup on %s: %s", host, result.stderr.decode().strip())
            if source_dir:
                session.execute(hosts[0], ["rm", "-rf", "--", source_dir])
finally:
    session.close()
