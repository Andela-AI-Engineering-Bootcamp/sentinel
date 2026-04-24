"""Lambda handler for Planner orchestrator."""

from __future__ import annotations

import json

from common.pipeline import run_job
from common.store import get_database


def _job_id_from_record(record: dict) -> str | None:
    body = record.get("body", "")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None
    return payload.get("job_id")


def lambda_handler(event, context):
    db = get_database()
    results: list[dict] = []

    if "Records" in event:
        for record in event["Records"]:
            job_id = _job_id_from_record(record)
            if not job_id:
                results.append({"status": "failed", "error": "Missing job_id in SQS message"})
                continue
            run_result = run_job(job_id, db)
            results.append(run_result.model_dump())
        return {"results": results}

    job_id = event.get("job_id")
    if not job_id:
        return {"status": "failed", "error": "Missing job_id"}

    run_result = run_job(job_id, db)
    return run_result.model_dump()
