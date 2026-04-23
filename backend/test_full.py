"""Extended integration test for multiple incidents (Aurora Data API)."""

from __future__ import annotations

from common.config import aurora_cluster_arn, aurora_database, aurora_secret_arn
from common.models import IncidentInput
from common.pipeline import create_incident_and_job, run_job
from common.store import Database
from database.src.db import load_sql_statements, migration_file


SAMPLES = [
    IncidentInput(
        title="Auth failures",
        source="uploaded",
        text="ERROR 403 forbidden: access denied for service account",
    ),
    IncidentInput(
        title="Memory pressure",
        source="manual",
        text="WARN heap growth\nERROR OOM killed process in worker",
    ),
    IncidentInput(
        title="Unknown issue",
        source="manual",
        text="service seems unstable and users report intermittent issues",
    ),
]


def _ensure_env() -> None:
    if not aurora_cluster_arn() or not aurora_secret_arn():
        raise RuntimeError(
            "Missing Aurora env. Set AURORA_CLUSTER_ARN and AURORA_SECRET_ARN before running tests."
        )


def main() -> None:
    _ensure_env()

    db = Database(aurora_database())
    try:
        db.execute_script(load_sql_statements(migration_file()))

        for sample in SAMPLES:
            _, job_id = create_incident_and_job(sample, db, clerk_user_id="test_full_user")
            result = run_job(job_id, db, clerk_user_id="test_full_user")
            assert result.status == "completed", result
            assert result.analysis is not None

        print("Backend full integration test passed (3/3 incidents)")
    finally:
        db.close()


if __name__ == "__main__":
    main()
