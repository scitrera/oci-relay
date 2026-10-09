# SPDX-FileCopyrightText: 2026 Spark Arena
# SPDX-License-Identifier: Apache-2.0
import json

import pytest

from sparkrun_oci_relay.paths import arguments, qualify, validate_paths
from sparkrun_oci_relay.source_policy import validate


MAPS = [{"controller": "192.0.2.1", "receiver": "192.0.2.2"},
        {"controller": "198.51.100.1", "receiver": "198.51.100.2"}]


class Routes:
    def __init__(self, *, same_device=False, failed=False, unassigned=False):
        self.same_device, self.failed, self.unassigned = same_device, failed, unassigned

    def execute(self, host, args):
        assert args[:2] == ["sh", "-c"]
        local, remote, _ = args[4:]
        if self.failed and remote.startswith("198."):
            raise RuntimeError("link unavailable")
        device = "nic-a" if self.same_device or remote.startswith("192.") else "nic-b"
        route = [{"dev": device, "src": local}]
        suffix = ".1" if host is None else ".2"
        addresses = [{"ifname": device, "addr_info": [{"local": network + suffix} for network in
                      (["203.0.113"] if self.unassigned else ["192.0.2", "198.51.100"])]}]
        return b"\0".join([json.dumps(route).encode(), json.dumps(addresses).encode(), b"200000", b""])


def test_dual_routes_and_spark_bandwidth_cap():
    facts = {host: {"product_name": "NVIDIA DGX Spark"} for host in (None, "receiver")}
    routes = qualify(Routes(), None, "receiver", MAPS, facts)
    assert len(routes) == 2
    assert sum(route["network_gbps"] for route in routes) == 200
    assert arguments(routes, 1234) == ["--path", "https://192.0.2.1:1234,192.0.2.2",
                                     "--path", "https://198.51.100.1:1234,198.51.100.2"]


@pytest.mark.parametrize("option", ["same_device", "failed"])
def test_single_link_degradation(option):
    assert len(qualify(Routes(**{option: True}), None, "receiver", MAPS, {})) == 1


def test_unassigned_address_rejected():
    assert qualify(Routes(unassigned=True), None, "receiver", MAPS, {}) == []


@pytest.mark.parametrize("value", [[], {}, [{}], [{"controller": "0.0.0.0"}], [{"controller": "127.0.0.1"}],
                                   [{"controller": "example.com"}], [{1: "192.0.2.1"}], MAPS * 5])
def test_invalid_maps(value):
    with pytest.raises(ValueError):
        validate_paths(value)


def test_map_settings_and_missing_host():
    assert validate({"data_paths": MAPS})["data_paths"] == MAPS
    with pytest.raises(ValueError, match="transport"):
        validate({"data_paths": MAPS, "transport": "ssh-stdio"})
    with pytest.raises(ValueError, match="source"):
        qualify(Routes(), "unlisted-source", "receiver", MAPS, {})


def test_ipv6_cli_authority():
    assert arguments([{"source": "2001:db8::1", "local": "2001:db8::2"}], 443) == [
        "--path", "https://[2001:db8::1]:443,2001:db8::2",
    ]


@pytest.mark.parametrize("value", [0, 5, True, "2"])
def test_connection_bounds(value):
    with pytest.raises(ValueError, match="connections_per_path"):
        validate({"data_paths": MAPS, "connections_per_path": value})


def test_connections_require_explicit_paths():
    with pytest.raises(ValueError, match="data_paths"):
        validate({"connections_per_path": 2})
    assert validate({"data_paths": MAPS, "connections_per_path": 2})["connections_per_path"] == 2


@pytest.mark.parametrize("key,value", [("stripe_threshold_bytes", True), ("stripe_threshold_bytes", -1),
                                     ("stripe_threshold_bytes", 1 << 20), ("stripe_threshold_bytes", 1 << 51),
                                     ("stripe_streams", True), ("stripe_streams", 1), ("stripe_streams", 9)])
def test_stripe_bounds(key, value):
    with pytest.raises(ValueError, match=key):
        validate({key: value})


def test_stripe_overrides_and_disable():
    assert validate({"stripe_threshold_bytes": 0})["stripe_threshold_bytes"] == 0
    settings = {"stripe_threshold_bytes": 256 << 20, "stripe_streams": 8}
    assert all(validate(settings)[k] == v for k, v in settings.items())


@pytest.mark.parametrize("value", [0, True, "16777216", 512 << 10, 3 << 20, 128 << 20])
def test_stripe_piece_bounds(value):
    with pytest.raises(ValueError, match="stripe_piece_bytes"):
        validate({"stripe_piece_bytes": value})


@pytest.mark.parametrize("mib", [1, 8, 16, 32, 64])
def test_stripe_piece_sizes(mib):
    assert validate({"stripe_piece_bytes": mib << 20})["stripe_piece_bytes"] == mib << 20


@pytest.mark.parametrize("value", [True, -1, "16777216", 3 << 20, 128 << 20])
def test_http2_window_bounds(value):
    with pytest.raises(ValueError, match="http2_stream_window_bytes"):
        validate({"http2_stream_window_bytes": value})


@pytest.mark.parametrize("value", [0, 4 << 20, 16 << 20, 32 << 20, 64 << 20])
def test_http2_window_sizes(value):
    assert validate({"http2_stream_window_bytes": value})["http2_stream_window_bytes"] == value
