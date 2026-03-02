from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from rearc_data_quest.aws_utils import s3_client
from rearc_data_quest.config import Settings
from rearc_data_quest.http_utils import get_json
from rearc_data_quest.logging_utils import configure_logging, get_run_id, set_run_id

logger = logging.getLogger(__name__)


def _normalize_s3_prefix(prefix: str) -> str:
    return prefix if prefix.endswith("/") else f"{prefix}/"

#function that creates 2 S3 file 
#One name is always latest
#One name has time stamp so old copies are saved too.
def _build_s3_keys(prefix: str, fetched_at: datetime) -> tuple[str, str]:
    normalized_prefix = _normalize_s3_prefix(prefix)
    timestamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    latest_key = f"{normalized_prefix}population_latest.json"
    snapshot_key = f"{normalized_prefix}population_{timestamp}.json"
    return latest_key, snapshot_key


def _validate_payload(payload: dict) -> int:
    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError("Population API response is missing a list 'data' field")
    return len(data)


def _fetch_and_upload_population(settings: Settings) -> tuple[str, str, int]:
    payload = get_json(settings.population_api_url, timeout=30)
    row_count = _validate_payload(payload)

    fetched_at = datetime.now(UTC)
    latest_key, snapshot_key = _build_s3_keys(settings.s3_api_prefix, fetched_at)
    payload_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    s3 = s3_client(settings.aws_region)
    metadata = {
        "source_system": "datausa",
        "source_url": settings.population_api_url,
        "fetched_at_utc": fetched_at.isoformat(),
        "row_count": str(row_count),
        "pipeline_run_id": get_run_id(),
    }

    for key in (latest_key, snapshot_key):
        s3.put_object(
            Bucket=settings.s3_bucket_name,
            Key=key,
            Body=payload_bytes,
            ContentType="application/json",
            Metadata=metadata,
        )

    return latest_key, snapshot_key, row_count


def run(*, run_id: str | None = None) -> None:
    configure_logging()
    if run_id:
        set_run_id(run_id)
    settings = Settings.from_env()
    latest_key, snapshot_key, row_count = _fetch_and_upload_population(settings)
    logger.info(
        "Population API ingestion complete | row_count=%s latest_key=%s snapshot_key=%s",
        row_count,
        latest_key,
        snapshot_key,
    )


if __name__ == "__main__":
    run()
