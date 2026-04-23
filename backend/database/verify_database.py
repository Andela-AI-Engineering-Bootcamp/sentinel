"""Verify basic Sentinel Aurora DB integrity for backend workflows."""

from __future__ import annotations

import json

from src.db import get_database


def main() -> None:
    db = get_database()
    try:
        incidents = db.list_incidents(limit=500)
        jobs = db.list_jobs(limit=500)
        users = db._query("SELECT COUNT(*) AS count FROM users")  # noqa: SLF001
        actions = db._query("SELECT COUNT(*) AS count FROM remediation_actions")  # noqa: SLF001
        follow_ups = db._query("SELECT COUNT(*) AS count FROM follow_ups")  # noqa: SLF001
        print("---")
        print("DATABASE VERIFICATION")
        print("---")
        print(f"Users table rows: {users[0]['count'] if users else 0}")
        print(f"Incidents table rows: {len(incidents)}")
        print(f"Jobs table rows: {len(jobs)}")
        print(f"Remediation actions rows: {actions[0]['count'] if actions else 0}")
        print(f"Follow-up rows: {follow_ups[0]['count'] if follow_ups else 0}")
        if jobs:
            sample = jobs[0]
            print("Sample recent job:")
            print(json.dumps(sample, indent=2))
        print("Schema availability: OK (Aurora Data API)")
        print("---")
        print("Sentinel database is ready.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
