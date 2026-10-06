#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
"""Compare a local image copy into an explicitly supplied empty test Docker store.

Use the dev environment with Sparkrun develop-next and this plugin. The caller owns
creation/removal of the isolated receiver daemon. Never point this at a normal
host daemon. No image removal or cache pruning is performed by this script.
The transfer method verifies all blobs without using a receiver Docker daemon.
"""

import argparse
import hashlib
import json
import logging
from pathlib import Path
import shlex
import subprocess
import threading
import time
from types import SimpleNamespace

from sparkrun.transports.session import SshHostSession
from sparkrun_oci_relay import __version__
from sparkrun_oci_relay.host import Lines, Runner
from sparkrun_oci_relay.paths import arguments as path_arguments, qualify as qualify_paths
from sparkrun_oci_relay.tuning import limits, probe


class BenchmarkSession(SshHostSession):
    """Redirect only receiver imports, keeping provider orchestration unchanged."""

    def __init__(self, hosts, docker_host):
        super().__init__()
        self.receiver_hosts = hosts
        self.receiver_docker_host = docker_host

    def open_process(self, host, arguments):
        command = arguments[2:] if arguments[:1] == ["env"] and arguments[1].startswith("GOMAXPROCS=") else arguments
        if self.receiver_docker_host and host in self.receiver_hosts and len(command) > 1 and command[1] == "peer" and "--check" not in command:
            arguments = [*arguments, "--docker-host", self.receiver_docker_host]
        return super().open_process(host, arguments)


class ProviderObservations(logging.Handler):
    def __init__(self, result):
        super().__init__()
        self.result = result

    def emit(self, record):
        # These messages contain counts/timings only, never session credentials.
        keys = {"OCI Relay timings: %s": "provider_timings", "OCI Relay completed: %s": "source_metrics",
                "OCI Relay receiver timings: %s": "receiver_timings"}
        if record.msg in keys:
            self.result[keys[record.msg]] = dict(record.args)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("method", choices=["builtin", "relay", "provider", "transfer"],
                        help="provider includes plugin binary checks, discovery, preflight and cleanup")
    parser.add_argument("--image", required=True, help="local image reference; this script never pulls it")
    parser.add_argument("--host", required=True, action="append", help="receiver management address; repeat for fan-out")
    parser.add_argument("--transfer-host", required=True, action="append", help="one data address per --host, in order")
    parser.add_argument("--receiver-docker-host", help="isolated unix:///tmp/.../docker.sock; omit for transfer")
    parser.add_argument("--data-paths", help="private JSON file containing plugin data_paths host-to-IP maps")
    parser.add_argument("--connections-per-path", type=int, choices=range(1, 5), default=1)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--manifest", help="omit to measure full preparation read")
    parser.add_argument("--layout", help="pre-exported OCI layout instead of the Docker push source")
    parser.add_argument("--source-mode", choices=["auto", "docker", "docker-save", "docker-classic"], default="docker")
    parser.add_argument("--max-spool-bytes", type=int, default=8 << 30)
    parser.add_argument("--network-gbps", type=float, help="effective bandwidth hint for plugin sizing")
    parser.add_argument("--source-join-milliseconds", type=int, default=0, help="bounded source prefix coalescing window")
    parser.add_argument("--receiver-import", choices=["pull", "load-cached", "load"], default="pull")
    parser.add_argument("--max-import-bytes", type=int, default=0, help="explicit Docker load archive scratch budget")
    parser.add_argument("--source-streams", type=int, help="explicit relay acquisition slots per process")
    parser.add_argument("--relay-gomaxprocs", type=int, default=0, help="relay processes only; does not change dockerd")
    parser.add_argument("--output", required=True)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--warm", action="store_true", help="permit cached images in the isolated benchmark store")
    args = parser.parse_args()
    if args.method != "transfer" and not (args.receiver_docker_host or "").startswith("unix:///tmp/oci-relay-benchmark."):
        parser.error("receiver must be an explicitly isolated benchmark Docker socket")
    if args.method == "transfer" and args.receiver_docker_host:
        parser.error("transfer-only does not use a receiver Docker socket")
    if args.method == "builtin" and args.data_paths:
        parser.error("builtin does not support data_paths")
    if len(args.host) != len(args.transfer_host) or len(set(args.host)) != len(args.host):
        parser.error("provide unique management hosts and an equal number of transfer addresses")
    if args.source_mode == "auto" and args.method != "provider":
        parser.error("auto source policy requires the provider method")
    if args.layout and args.method == "provider":
        parser.error("the provider does not accept pre-exported layouts")
    if args.connections_per_path != 1 and args.method != "transfer" and not args.data_paths:
        parser.error("multiple connections require explicit data_paths (or transfer mode)")
    operation_started = time.monotonic()
    session = BenchmarkSession(args.host, args.receiver_docker_host)
    runner = Runner(session, {"relay_gomaxprocs": args.relay_gomaxprocs})
    receiver_docker = ["docker", "--host", args.receiver_docker_host]
    source_id = subprocess.check_output(
        ["docker", "image", "inspect", "--format={{.Id}}", args.image], text=True,
    ).strip()
    infos = {}
    for host in ([] if args.method == "transfer" else args.host):
        info = json.loads(runner.execute(host, receiver_docker + ["info", "--format={{json .}}"] ))
        if (info["Images"] and not args.warm) or info["Containers"] or info["Driver"] != "overlay2":
            raise RuntimeError("receiver must be an empty overlay2 benchmark store (or explicitly --warm)")
        infos[host] = {key: info[key] for key in ("ServerVersion", "Driver", "Images", "Containers")}
    result = {
        "method": args.method, "image": args.image, "source_id": source_id,
        "host": args.host, "transfer_host": args.transfer_host,
        "receiver_engines": infos, "supplied_manifest": bool(args.manifest),
        "source_mode": "oci-layout" if args.layout else args.source_mode,
        "cache_state": "not-imported" if args.method == "transfer" else "warm" if args.warm else "cold",
        "source_join_milliseconds": args.source_join_milliseconds,
        "receiver_import": "none" if args.method == "transfer" else args.receiver_import,
        "relay_gomaxprocs": args.relay_gomaxprocs, "source_streams_override": args.source_streams,
        "connections_per_path": args.connections_per_path,
    }
    maps = json.loads(Path(args.data_paths).read_text()) if args.data_paths else None
    stop_heartbeat = threading.Event()
    started = None
    try:
        if args.method == "builtin":
            from sparkrun.orchestration.ssh import run_pipeline_to_remotes_parallel

            started = time.monotonic()
            # Call the same pipeline helper used by core distribution.
            outcomes = run_pipeline_to_remotes_parallel(
                args.transfer_host, shlex.join(["docker", "save", args.image]),
                shlex.join(receiver_docker + ["load"]), timeout=args.timeout,
            )
            if len(outcomes) != len(args.host) or not all(outcome.success for outcome in outcomes):
                raise RuntimeError(str(outcomes))
            result["copy_seconds"] = time.monotonic() - started
        elif args.method == "provider":
            from sparkrun.plugins import ImageCopyRequest
            from sparkrun_oci_relay.provider import RelayProvider

            settings = {
                "development_binary": str(Path(args.binary).resolve()),
                "transport": "http2-direct", "source_mode": args.source_mode,
                "allow_native_store": args.source_mode in {"auto", "docker-classic"},
                "allow_preparation_read": not bool(args.manifest),
                "max_spool_bytes": args.max_spool_bytes,
                "source_join_milliseconds": args.source_join_milliseconds,
                "receiver_import": args.receiver_import, "max_import_bytes": args.max_import_bytes,
                "relay_gomaxprocs": args.relay_gomaxprocs,
            }
            if args.network_gbps is not None:
                settings["network_gbps"] = args.network_gbps
            if args.source_streams is not None:
                settings["source_streams"] = args.source_streams
            if args.manifest:
                settings["manifest"] = str(Path(args.manifest).resolve())
            if maps is not None:
                settings["data_paths"] = maps
                settings["connections_per_path"] = args.connections_per_path
            request = ImageCopyRequest(
                image=args.image, source_host=None, targets=tuple(args.host), transfer_hosts=tuple(args.transfer_host),
                timeout=args.timeout, offline=True, session=session,
                config=SimpleNamespace(plugin_settings=lambda name: settings),
            )
            logger = logging.getLogger("sparkrun_oci_relay.provider")
            observations = ProviderObservations(result)
            previous_level = logger.level
            logger.setLevel(logging.INFO)
            logger.addHandler(observations)
            started = time.monotonic()
            try:
                outcome = RelayProvider().copy(request)
            finally:
                logger.removeHandler(observations)
                logger.setLevel(previous_level)
            result["outcomes"] = outcome.outcomes
            if set(outcome.outcomes) != set(args.host) or any(value != "complete" for value in outcome.outcomes.values()):
                raise RuntimeError(str(outcome))
        else:
            binary = Path(args.binary).resolve()
            sha = hashlib.sha256(binary.read_bytes()).hexdigest()
            # Deployment and route probes precede timing, matching a cached install.
            remote_binaries = {host: runner.stage_binary(host, binary, sha, __version__) for host in args.host}
            source_facts = probe(runner, None, args.transfer_host[0])
            address = source_facts["source_address"]
            target_facts = {host: probe(runner, host, address) for host in args.host}
            data_paths = {}
            if maps is not None:
                all_facts = {None: source_facts, **target_facts}
                for host in args.host:
                    data_paths[host] = qualify_paths(runner, None, host, maps, all_facts)
                    if len(data_paths[host]) != len(maps):
                        raise RuntimeError("benchmark requires every configured path to qualify")
            elif args.method == "transfer":
                data_paths = {host: [{"source": address, "local": target_facts[host]["source_address"],
                                     "source_device": source_facts["interface"],
                                     "target_device": target_facts[host]["interface"]}] for host in args.host}

            def interface_counters():
                devices = {None: set(), **{host: set() for host in args.host}}
                for host, paths in data_paths.items():
                    for path in paths:
                        devices[None].add(path["source_device"])
                        devices[host].add(path["target_device"])
                counters = {}
                for host, names in devices.items():
                    counters[host or "controller"] = {}
                    for device in sorted(names):
                        raw = runner.execute(host, ["cat", *[f"/sys/class/net/{device}/statistics/{kind}_bytes"
                                                            for kind in ("rx", "tx")]])
                        rx, tx = map(int, raw.split())
                        counters[host or "controller"][device] = {"rx_bytes": rx, "tx_bytes": tx}
                return counters

            result["interface_counters_before"] = interface_counters()
            tuning = {"network_gbps": args.network_gbps} if args.network_gbps is not None else {}
            if args.source_streams is not None:
                tuning["source_streams"] = args.source_streams
            source_limits = limits(tuning, source_facts, "http2-direct")
            target_limits = {host: limits(tuning, facts, "http2-direct") for host, facts in target_facts.items()}
            result.update(source_facts=source_facts, target_facts=target_facts,
                          source_limits=source_limits, target_limits=target_limits)
            started = time.monotonic()
            source_dir = runner.directory(None)
            identities = {f"receiver-{index}": host for index, host in enumerate(args.host)}
            runner.execute(None, [str(binary), "session", "--out", source_dir,
                                  "--peers", ",".join(identities), "--duration", f"{args.timeout + 60}s"])
            credentials = json.loads(runner.execute(None, ["cat", source_dir + "/session.json"]))
            credential_files = {}
            for identity, host in identities.items():
                credential = runner.directory(host) + "/peer.json"
                runner.json(host, credential, credentials["peers"][identity])
                credential_files[host] = credential
            plan = {
                "version": 1, "source": "oci-layout" if args.layout else args.source_mode, "image": "" if args.layout else args.image,
                "layout": str(Path(args.layout).resolve()) if args.layout else "",
                "manifest": str(Path(args.manifest).resolve()) if args.manifest else "",
                "allow_preparation_read": not bool(args.manifest),
                "session_dir": source_dir, "socket": source_dir + "/source.sock",
                "listen": "0.0.0.0:0", "advertise": address,
                "max_buffer_bytes": source_limits["max_buffer_bytes"],
                "source_streams": source_limits["source_streams"],
                "source_join_milliseconds": args.source_join_milliseconds,
                "max_spool_bytes": args.max_spool_bytes, "max_upload_bytes": args.max_spool_bytes,
                "spool_dir": source_dir,
                "timeout_seconds": args.timeout, "lease_seconds": 45, "managed_stdin": True,
            }
            preparation_start = time.monotonic()
            source = runner.start_source(None, str(binary), source_dir, plan)
            events = Lines(source.stdout, events=True)
            source_errors = Lines(source.stderr)

            def heartbeat():
                while not stop_heartbeat.is_set():
                    try:
                        source.stdin.write(b'{"type":"heartbeat"}\n')
                    except (OSError, ValueError):
                        return
                    stop_heartbeat.wait(5)

            threading.Thread(target=heartbeat, daemon=True).start()
            deadline = started + args.timeout
            try:
                ready = events.event(deadline)
                if ready.get("type") != "ready":
                    raise RuntimeError("source did not become ready")
                result["preparation_seconds"] = time.monotonic() - preparation_start
                print(json.dumps({"phase": "ready", "preparation_seconds": result["preparation_seconds"]}), flush=True)
                copy_start = time.monotonic()
                receivers = {}
                for host in args.host:
                    receiver = runner.start(host, [
                        remote_binaries[host], "peer", *(path_arguments(data_paths[host],
                            int(ready["endpoint"].rsplit(":", 1)[1])) if host in data_paths else ["--endpoint", ready["endpoint"]]),
                        "--session", credential_files[host],
                        *(["--connections-per-path", str(args.connections_per_path)] if host in data_paths else []),
                        *(["--transfer-only"] if args.method == "transfer" else ["--docker-host", args.receiver_docker_host, "--tag", args.image]),
                        "--max-buffer-bytes", str(target_limits[host]["max_buffer_bytes"]),
                        "--max-source-streams", str(target_limits[host]["source_streams"]),
                        "--timeout-seconds", str(args.timeout),
                        "--import", args.receiver_import, "--max-import-bytes", str(args.max_import_bytes),
                    ])
                    Lines(receiver.stdout)
                    receivers[host] = (receiver, Lines(receiver.stderr))

                def health():
                    for host, (receiver, receiver_errors) in receivers.items():
                        if receiver.poll() not in (None, 0):
                            raise RuntimeError(host + " receiver failed: " + "\n".join(receiver_errors.tail))

                while True:
                    final = events.event(deadline, health)
                    if final.get("type") != "progress":
                        break
                    print(json.dumps(final), flush=True)
                result["copy_seconds"] = time.monotonic() - copy_start
                result["relay_result"] = final
                codes = [receiver.wait(timeout=15) for receiver, _ in receivers.values()]
                expected = "VERIFIED" if args.method == "transfer" else "COMPLETE"
                if final.get("state") != expected or any(codes) or source.wait(timeout=15):
                    raise RuntimeError("relay did not complete: " + json.dumps(final))
            except Exception as error:
                raise RuntimeError(str(error) + "\n" + "\n".join(source_errors.tail)) from error
        result["total_seconds"] = time.monotonic() - started
        if args.method not in {"builtin", "provider"}:
            result["interface_counters_after"] = interface_counters()
        result["received_ids"] = {}
        for host in ([] if args.method == "transfer" else args.host):
            received = runner.execute(host, receiver_docker + [
                "image", "inspect", "--format={{.Id}}", args.image,
            ]).decode().strip()
            result["received_ids"][host] = received
            if received != source_id:
                raise RuntimeError("receiver identity does not match source")
        result["state"] = "VERIFIED" if args.method == "transfer" else "COMPLETE"
    except BaseException as error:
        result["state"] = "FAILED"
        result["error"] = str(error)
        if started is not None:
            result["elapsed_seconds"] = time.monotonic() - started
        raise
    finally:
        stop_heartbeat.set()
        result["cleanup_errors"] = runner.close()
        session.close()
        result["operation_seconds"] = time.monotonic() - operation_started
        Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
