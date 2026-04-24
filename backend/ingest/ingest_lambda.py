"""Incident ingestion lambda compatible with API Gateway proxy events."""

from __future__ import annotations

import json

from common.models import IncidentInput
from common.pipeline import create_incident_and_job
from common.store import get_database


INGEST_SERVICE_USER = "ingest_service"


def lambda_handler(event, context):
    try:
        body = event.get("body")
        payload = json.loads(body) if isinstance(body, str) else event
        incident = IncidentInput.model_validate(payload)
    except Exception as exc:  # noqa: BLE001
        return {"statusCode": 400, "body": json.dumps({"error": f"Invalid payload: {exc}"})}

    db = get_database()
    try:
        incident_id, job_id = create_incident_and_job(incident, db, clerk_user_id=INGEST_SERVICE_USER)
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Incident accepted",
                    "incident_id": incident_id,
                    "job_id": job_id,
                    "status": "pending",
                }
            ),
        }
    finally:
        db.close()
