"""Seed sample incidents for demo/dashboard."""

from __future__ import annotations

from src.pathing import ensure_backend_root_on_path

ensure_backend_root_on_path()

import os

from common.models import IncidentInput
from common.pipeline import create_incident_and_job, run_job
from src.db import get_database

SEED_CLERK_USER_ID = os.getenv("SEED_CLERK_USER_ID", "seed_user")
SEED_PREMIUM_CLERK_USER_ID = os.getenv("SEED_PREMIUM_CLERK_USER_ID", SEED_CLERK_USER_ID)
SEED_PREMIUM_LIVE_BOARD = os.getenv("SEED_PREMIUM_LIVE_BOARD", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


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
        db.upsert_user_entitlements(
            SEED_PREMIUM_CLERK_USER_ID,
            subscription_tier="pro" if SEED_PREMIUM_LIVE_BOARD else "free",
            live_incident_board_enabled=SEED_PREMIUM_LIVE_BOARD,
        )
        for incident in SEED_INCIDENTS:
            _, job_id = create_incident_and_job(
                incident, db, clerk_user_id=SEED_CLERK_USER_ID
            )
            run_job(job_id, db)
        print(
            f"Seeded and analyzed {len(SEED_INCIDENTS)} sample incidents for user '{SEED_CLERK_USER_ID}'."
        )
        if SEED_PREMIUM_LIVE_BOARD:
            print(
                "Enabled Live Incident Board entitlement for "
                f"'{SEED_PREMIUM_CLERK_USER_ID}'."
            )
    finally:
        db.close()


if __name__ == "__main__":
    main()
