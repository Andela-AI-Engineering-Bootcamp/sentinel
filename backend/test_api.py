"""API smoke test for Sentinel (Aurora Data API)."""

from __future__ import annotations

import os

from fastapi.testclient import TestClient

from api.main import app
from common.config import aurora_cluster_arn, aurora_database, aurora_secret_arn
from common.store import Database
from database.src.db import load_sql_statements, migration_file


def _ensure_env() -> None:
    if not aurora_cluster_arn() or not aurora_secret_arn():
        raise RuntimeError(
            "Missing Aurora env. Set AURORA_CLUSTER_ARN and AURORA_SECRET_ARN before running tests."
        )


def main() -> None:
    _ensure_env()
    os.environ["AUTH_DISABLED"] = "true"

    db = Database(aurora_database())
    try:
        db.execute_script(load_sql_statements(migration_file()))
    finally:
        db.close()

    client = TestClient(app)

    health = client.get("/health")
    assert health.status_code == 200, health.text

    me = client.get("/api/me")
    assert me.status_code == 200, me.text
    assert me.json()["user_id"] == "dev_user"

    response = client.post(
        "/api/incidents/analyze-sync",
        json={
            "title": "Database instability",
            "source": "manual",
            "text": "ERROR database connection refused and timeout in checkout service",
        },
    )
    assert response.status_code == 200, response.text

    payload = response.json()
    assert payload["status"] == "completed", payload
    assert payload["analysis"]["summary"]["severity"] in {"high", "critical"}

    jobs = client.get("/api/jobs")
    assert jobs.status_code == 200, jobs.text
    assert len(jobs.json()) >= 1

    print("API smoke test passed")


if __name__ == "__main__":
    main()
