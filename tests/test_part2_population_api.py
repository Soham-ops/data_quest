from datetime import UTC, datetime

import pytest

from rearc_data_quest.jobs.part2_population_api import _build_s3_keys, _normalize_s3_prefix, _validate_payload


def test_normalize_s3_prefix_adds_trailing_slash():
    assert _normalize_s3_prefix("raw/datausa") == "raw/datausa/"
    assert _normalize_s3_prefix("raw/datausa/") == "raw/datausa/"


def test_build_s3_keys_contains_latest_and_timestamped_snapshot():
    now = datetime(2026, 2, 27, 10, 30, 1, tzinfo=UTC)
    latest_key, snapshot_key = _build_s3_keys("raw/datausa", now)

    assert latest_key == "raw/datausa/population_latest.json"
    assert snapshot_key == "raw/datausa/population_20260227T103001Z.json"


def test_validate_payload_returns_row_count():
    payload = {"data": [{"Year": "2013"}, {"Year": "2014"}]}
    assert _validate_payload(payload) == 2


def test_validate_payload_raises_when_data_missing():
    with pytest.raises(ValueError):
        _validate_payload({"source": "datausa"})

