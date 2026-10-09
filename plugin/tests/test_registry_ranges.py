# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: Apache-2.0
import pytest

from sparkrun_oci_relay.registry_ranges import LIMITS, plan
from sparkrun_oci_relay.source_policy import validate


@pytest.mark.parametrize('key', LIMITS)
@pytest.mark.parametrize('bad', [True, '4', 0, -1, 1 << 51])
def test_range_limits_reject_invalid_settings(key, bad):
    with pytest.raises(ValueError, match=key):
        validate({key: bad})


def test_range_overrides_require_capability_and_preserve_explicit_disable():
    assert plan({}, []) == {}
    assert plan({}, ['registry-range-v1']) == {}
    settings = {'registry_range_concurrency': 1, 'registry_range_chunk_bytes': 16 << 20,
                'registry_range_threshold_bytes': 256 << 20, 'registry_range_buffer_bytes': 128 << 20}
    with pytest.raises(ValueError, match='registry-range-v1'):
        plan(settings, ['receiver-unpigz-v1'])
    assert plan(settings, ['registry-range-v1']) == settings
    assert all(validate(settings)[key] == value for key, value in settings.items())
