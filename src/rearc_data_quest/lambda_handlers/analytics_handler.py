from __future__ import annotations

import json
import logging
import os
import re
from uuid import uuid4
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import boto3
from rearc_data_quest.logging_utils import configure_logging, set_run_id

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"(dapi[a-zA-Z0-9]+)")
_BEARER_RE = re.compile(r"(Bearer\s+)([A-Za-z0-9._\-]+)", re.IGNORECASE)


def _pipeline_run_id_from_destination_event(event) -> str | None:
    if not isinstance(event, dict):
        return None

    # Lambda destination payload usually includes the source function return body here.
    response_payload = event.get("responsePayload")
    if isinstance(response_payload, str):
        try:
            response_payload = json.loads(response_payload)
        except json.JSONDecodeError:
            response_payload = None
    if isinstance(response_payload, dict):
        run_id = response_payload.get("pipeline_run_id")
        if isinstance(run_id, str) and run_id.strip():
            return run_id

    # Fallback for non-destination/manual invocations that may pass it directly.
    request_payload = event.get("requestPayload")
    if isinstance(request_payload, str):
        try:
            request_payload = json.loads(request_payload)
        except json.JSONDecodeError:
            request_payload = None
    if isinstance(request_payload, dict):
        run_id = request_payload.get("pipeline_run_id")
        if isinstance(run_id, str) and run_id.strip():
            return run_id

    return None


def _resolve_run_id(event, context) -> str:
    destination_run_id = _pipeline_run_id_from_destination_event(event)
    if destination_run_id:
        return destination_run_id

    if isinstance(event, dict):
        event_id = event.get("id")
        if isinstance(event_id, str) and event_id.strip():
            return event_id
    request_id = getattr(context, "aws_request_id", None)
    if isinstance(request_id, str) and request_id.strip():
        return request_id
    return uuid4().hex


def _sanitize_for_logs(value: str) -> str:
    masked = _TOKEN_RE.sub("dapi***REDACTED***", value)
    masked = _BEARER_RE.sub(r"\1***REDACTED***", masked)
    return masked


def _load_databricks_secret(secret_name: str) -> dict[str, str]:
    client = boto3.client("secretsmanager")
    secret = client.get_secret_value(SecretId=secret_name)
    secret_string = secret.get("SecretString")
    if not secret_string:
        raise ValueError(f"Secret {secret_name} has no SecretString")

    payload = json.loads(secret_string)
    host = str(payload.get("host", "")).strip()
    token = str(payload.get("token", "")).strip()
    job_id = str(payload.get("job_id", "")).strip()
    if not host or not token or not job_id:
        raise ValueError("Secret must contain non-empty host, token, job_id")

    return {"host": host.rstrip("/"), "token": token, "job_id": job_id}


def _trigger_databricks_job(*, host: str, token: str, job_id: str) -> tuple[int, int | None]:
    url = f"{host}/api/2.1/jobs/run-now"
    body = json.dumps({"job_id": int(job_id)}).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    req = Request(url=url, data=body, headers=headers, method="POST")

    try:
        with urlopen(req, timeout=30) as resp:
            status = getattr(resp, "status", resp.getcode())
            response_body = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        safe_error_body = _sanitize_for_logs(error_body)
        logger.error("Databricks run-now failed | status=%s body=%s", exc.code, safe_error_body)
        raise RuntimeError(f"Databricks API error {exc.code}: {safe_error_body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Databricks API network error: {exc}") from exc

    payload = json.loads(response_body) if response_body else {}
    run_id = payload.get("run_id")
    return status, int(run_id) if run_id is not None else None


def handler(event, context):
    configure_logging()
    pipeline_run_id = _resolve_run_id(event, context)
    set_run_id(pipeline_run_id)

    secret_name = os.getenv("DATABRICKS_SECRET_NAME", "rearc/databricks/job-trigger")
    cfg = _load_databricks_secret(secret_name)
    status, run_id = _trigger_databricks_job(
        host=cfg["host"],
        token=cfg["token"],
        job_id=cfg["job_id"],
    )

    logger.info(
        "Triggered Databricks job | job_id=%s databricks_run_id=%s status=%s",
        cfg["job_id"],
        run_id,
        status,
    )
    return {
        "statusCode": 200,
        "message": "Databricks job triggered",
        "job_id": cfg["job_id"],
        "run_id": run_id,
        "api_status": status,
        "pipeline_run_id": pipeline_run_id,
    }
