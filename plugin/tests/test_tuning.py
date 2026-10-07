# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
import pytest
from sparkrun_oci_relay.tuning import limits, probe, MIB


def test_cx7_hints_increase_budgets_but_ssh_does_not():
    facts = {"network_gbps": 200, "available_memory": 128 << 30, "cpus": 32}
    assert limits({}, facts, "http2-direct") == {"max_buffer_bytes": 1024 * MIB, "source_streams": 32}
    assert limits({}, facts, "ssh-stdio") == {"max_buffer_bytes": 128 * MIB, "source_streams": 4}
    facts["network_gbps"] = 100
    assert limits({}, facts, "http2-direct")["max_buffer_bytes"] == 1024 * MIB


def test_auto_limits_respect_memory_cpu_and_unknown_hosts():
    assert limits({}, {}, "auto") == {"max_buffer_bytes": 128 * MIB, "source_streams": 4}
    assert limits({}, {"network_gbps": 200}, "auto")["max_buffer_bytes"] == 128 * MIB
    facts = {"network_gbps": 200, "available_memory": 256 * MIB, "cpus": 2}
    assert limits({}, facts, "auto") == {"max_buffer_bytes": 16 * MIB, "source_streams": 2}
    assert limits({}, facts, "auto", local_roles=2)["max_buffer_bytes"] == 8 * MIB


def test_explicit_limits_win_and_invalid_values_fail():
    assert limits({"max_buffer_bytes": 64 * MIB, "source_streams": 3}, {}, "auto") == {
        "max_buffer_bytes": 64 * MIB, "source_streams": 3,
    }
    for settings in ({"source_streams": True}, {"network_gbps": -1}, {"max_buffer_bytes": 1}):
        with pytest.raises(ValueError):
            limits(settings, {}, "auto")


def test_spark_probe_uses_effective_single_port_bandwidth():
    class Runner:
        def execute(self, host, args):
            if args[-1] == "192.168.12.16":
                return b"\0".join([b"", b"", b"", b"20", b"NVIDIA_DGX_Spark\n",
                                   b'[{"dev":"enP2p1s0f0np0","prefsrc":"192.168.12.11"}]', b""])
            assert args[-1] == "/sys/class/net/enP2p1s0f0np0/speed"
            return b"200000\n\0"

    facts = probe(Runner(), None, "192.168.12.16")
    assert facts["link_gbps"] == 200
    assert facts["network_gbps"] == 100
    assert facts["source_address"] == "192.168.12.11"
    facts.update(available_memory=128 << 30, cpus=20)
    assert limits({}, facts, "http2-direct") == {"max_buffer_bytes": 1024 * MIB, "source_streams": 16}


def test_spark_larger_buffer_still_shares_available_memory_between_roles():
    facts = {"network_gbps": 100, "available_memory": 8 << 30, "cpus": 20}
    assert limits({}, facts, "http2-direct") == {"max_buffer_bytes": 512 * MIB, "source_streams": 16}
    assert limits({}, facts, "http2-direct", local_roles=2) == {"max_buffer_bytes": 256 * MIB, "source_streams": 16}


def test_batched_probe_keeps_ancestor_memory_caps_when_route_is_unknown():
    calls = []

    class Runner:
        def execute(self, host, args):
            calls.append(args)
            if len(calls) == 1:
                return b"\0".join([b"host", b"MemAvailable: 100000000 kB\n", b"0::/parent/child\n", b"20", b"", b"[]", b""])
            assert args[4:] == ["/dev/null", "/sys/fs/cgroup/parent/child", "/sys/fs/cgroup/parent", "/sys/fs/cgroup"]
            return b"\0".join([b"", b"max\n100\n", b"1073741824\n536870912\n", b"max\n200\n", b""])

    facts = probe(Runner(), "host", "unreachable")
    assert len(calls) == 2
    assert facts["available_memory"] == 512 * MIB
    assert facts["cpus"] == 20
    assert "network_gbps" not in facts
    assert limits({}, facts, "http2-direct")["max_buffer_bytes"] == 32 * MIB


def test_probe_failure_does_not_use_uncapped_host_memory():
    class Runner:
        count = 0

        def execute(self, host, args):
            self.count += 1
            if self.count == 1:
                return b"\0".join([b"host", b"MemAvailable: 100000000 kB\n", b"0::/\n", b"", b"", b"invalid", b""])
            raise RuntimeError("connection failed")

    facts = probe(Runner(), "host", "destination")
    assert "available_memory" not in facts
    assert limits({"network_gbps": 100}, facts, "auto")["max_buffer_bytes"] == 128 * MIB


def test_host_probe_scripts_keep_arguments_literal_and_fields_separate(tmp_path):
    import subprocess

    from sparkrun_oci_relay.tuning import _FACTS, _LIMITS, _sections

    class Runner:
        def execute(self, host, args):
            return subprocess.check_output(args)

    # Shell metacharacters in host-owned paths stay positional data.
    directory = tmp_path / "a ; $() ' space"
    directory.mkdir()
    (directory / "memory.max").write_text("1000\n")
    (directory / "memory.current").write_text("40\n")
    assert _sections(Runner(), None, _LIMITS, ["/dev/null", str(directory), str(tmp_path / "missing")], 3) == [
        b"", b"1000\n40\n", b"",
    ]
    fields = _sections(Runner(), None, _FACTS, ["127.0.0.1"], 6)
    assert b"MemTotal:" in fields[1]
    assert int(fields[3]) >= 1
