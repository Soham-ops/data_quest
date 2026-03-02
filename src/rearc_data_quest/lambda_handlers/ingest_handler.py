from __future__ import annotations

import logging
from uuid import uuid4

from rearc_data_quest.jobs import part1_bls_sync, part2_population_api
from rearc_data_quest.logging_utils import configure_logging, set_run_id

logger = logging.getLogger(__name__)


def _resolve_run_id(event) -> str:
    if isinstance(event, dict):
        event_id = event.get("id")
        if isinstance(event_id, str) and event_id.strip():
            return event_id
    return uuid4().hex


def handler(event, context):
    configure_logging()
    pipeline_run_id = _resolve_run_id(event)
    set_run_id(pipeline_run_id)
    logger.info("Starting ingest lambda")

    part1_bls_sync.run(run_id=pipeline_run_id)
    part2_population_api.run(run_id=pipeline_run_id)

    logger.info("Ingest lambda complete")
    return {
        "statusCode": 200,
        "message": "Ingest jobs executed",
        "pipeline_run_id": pipeline_run_id,
    }
