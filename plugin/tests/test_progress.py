# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

import logging
import threading

from sparkrun_oci_relay.progress import PROGRESS, Progress


def event(sequence=1, phase="transferring", **fields):
    return {"receiver_progress": {"receiver-0": dict(
        sequence=sequence, phase=phase, inventory_ready=True, expected_bytes=8 << 20,
        received_bytes=4 << 20, wire_bytes=5 << 20, bytes_per_second=2 << 20,
        elapsed_seconds=10, reused_layers=3, **fields,
    )}}


def test_default_visible_progress_throttling_and_terminal_authority(caplog):
    caplog.set_level(PROGRESS)
    p = Progress("fixture:tag", interval=60)
    p.bind({"receiver-0": "spark-a"})
    try:
        p.event(event())
        assert "spark-a: transferring / importing" in caplog.text
        assert "4.0 MiB received (up to 8.0 MiB needed), 2.0 MiB/s; 3 layers reused" in caplog.text
        count = len(caplog.records)
        p.event(event(2))
        assert len(caplog.records) == count
        p.event(event(3, "verifying"))
        assert "verifying image" in caplog.text
        p.event(event(2, "checking"))
        assert len(caplog.records) == count + 1
        assert "image verified" not in caplog.text and "complete" not in caplog.text
        # Even a full byte count cannot override a failed import/identity check.
        p.outcome("receiver-0", {"progress": {"wire_bytes": 8 << 20}}, False)
    finally:
        p.close(False, False)
    assert "spark-a: failed" in caplog.text
    assert "failed in" in caplog.text and "complete in" not in caplog.text
    assert all(r.levelno == PROGRESS for r in caplog.records)
    assert not p.thread.is_alive()


def test_registry_download_separate_from_receiver_totals(caplog):
    caplog.set_level(PROGRESS)
    p = Progress("fixture:tag")
    p.bind({"receiver-0": "spark-a", "receiver-1": "spark-b"})
    try:
        p.event({"registry_metrics": {"upstream_blob_bytes": 8 << 20}})
        for peer in p.hosts:
            p.outcome(peer, {"progress": {"wire_bytes": 8 << 20}, "cached_blob_layers": 2}, True)
    finally:
        p.close(True, False)
    assert "registry download: 8.0 MiB" in caplog.text
    assert "16.0 MiB transferred to receivers; 4 layers reused; registry downloaded 8.0 MiB" in caplog.text


def test_warm_target_and_cleanup_warning(caplog):
    caplog.set_level(PROGRESS)
    p = Progress("fixture:tag")
    p.bind({"receiver-0": "spark-a"})
    try:
        p.outcome("receiver-0", {"already_present": True, "progress": {"wire_bytes": 0}, "cleanup_error": "fixture"}, True)
    finally:
        p.close(True, False)
    assert "image already present — verified; cleanup incomplete; 0 B transferred" in caplog.text
    assert "complete; cleanup incomplete" in caplog.text


def test_missing_final_counters_are_not_reported_as_zero(caplog):
    caplog.set_level(PROGRESS)
    p = Progress("fixture:tag")
    p.bind({"receiver-0": "spark-a"})
    p.close(False, False)
    assert "receiver byte total unavailable" in caplog.text
    assert "0 B transferred" not in caplog.text


def test_finished_receiver_waits_for_final_checks_without_stale_updates(caplog):
    caplog.set_level(PROGRESS)
    p = Progress("fixture:tag")
    p.bind({"receiver-0": "spark-a"})
    try:
        update = event()
        update["receivers"] = {"receiver-0": {"state": "COMPLETE"}}
        p.event(update)
        p.event(event(2))
        assert "import finished; awaiting final checks" in caplog.text
        assert "received (up to" not in caplog.text
        assert "image verified" not in caplog.text
    finally:
        p.close(False, False)


def test_heartbeat_during_silent_preparation(caplog):
    caplog.set_level(PROGRESS)
    seen = threading.Event()

    class Observe(logging.Handler):
        def emit(self, record):
            if "preparing source; running" in record.getMessage():
                seen.set()

    handler = Observe()
    logger = logging.getLogger("sparkrun_oci_relay.progress")
    logger.addHandler(handler)
    p = Progress("fixture:tag", heartbeat=0.02)
    try:
        p.phase("preparing source")
        assert seen.wait(2)
    finally:
        p.close(False, False)
        logger.removeHandler(handler)
    assert not p.thread.is_alive()
