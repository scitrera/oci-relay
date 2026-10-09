# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0
import threading

import pytest

from sparkrun_oci_relay.parallel import parallel


def test_setup_overlaps_hosts_and_keeps_input_order():
    barrier = threading.Barrier(3, timeout=5)

    def operation(host):
        barrier.wait()
        return host.upper()

    assert list(parallel(["c", "a", "b"], operation).items()) == [("c", "C"), ("a", "A"), ("b", "B")]
    assert parallel([], operation) == {}


def test_failure_settles_workers_before_cleanup_can_start():
    started = threading.Barrier(2, timeout=5)
    failed = threading.Event()
    released = threading.Event()
    returned = threading.Event()
    resources = []
    errors = []

    def operation(host):
        started.wait()
        if host == "failure":
            failed.set()
            raise RuntimeError("setup failed")
        assert released.wait(5)
        resources.append(host)

    def run():
        try:
            parallel(["failure", "late-resource"], operation)
        except RuntimeError as error:
            errors.append(str(error))
        finally:
            returned.set()

    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert failed.wait(5)
        assert not returned.wait(0.05)
    finally:
        released.set()
        worker.join(timeout=5)
    assert returned.is_set()
    assert errors == ["setup failed"]
    assert resources == ["late-resource"]


def test_setup_propagates_interrupts():
    def interrupted(host):
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        parallel(["host"], interrupted)
