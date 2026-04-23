"""Simple ingest lambda test (Aurora Data API)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.config import aurora_cluster_arn, aurora_database, aurora_secret_arn
from common.store import Database
from database.src.db import load_sql_statements, migration_file
from ingest.ingest_lambda import lambda_handler


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
    finally:
        db.close()

    event = {
        "body": json.dumps(
            {
                "title": "Checkout outage",
                "source": "manual",
                "text": "ERROR: database connection refused during checkout",
            }
        )
    }
    result = lambda_handler(event, None)
    assert result["statusCode"] == 200, result
    print("Ingest test passed")


if __name__ == "__main__":
    main()
