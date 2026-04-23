"""Seed sample incidents for demo/dashboard."""

from __future__ import annotations

from src.pathing import ensure_backend_root_on_path

ensure_backend_root_on_path()

from common.models import IncidentInput
from common.pipeline import create_incident_and_job, run_job
from src.db import get_database


SEED_INCIDENTS = [
    IncidentInput(
        title="Payments timeout spike",
        source="seed",
        text="ERROR timeout contacting payment gateway after 30s",
    ),
    IncidentInput(
        title="Auth token failures",
        source="seed",
        text="403 forbidden and access denied from upstream auth service",
    ),
]


def main() -> None:
    db = get_database()
    try:
        for incident in SEED_INCIDENTS:
            _, job_id = create_incident_and_job(incident, db)
            run_job(job_id, db)
        print(f"Seeded and analyzed {len(SEED_INCIDENTS)} sample incidents.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
