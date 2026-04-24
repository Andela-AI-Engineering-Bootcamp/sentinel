"""List recent Sentinel incidents and derived job summaries."""

from __future__ import annotations

import json

from common.store import get_database


def main() -> None:
    db = get_database()
    try:
        incidents = db.list_incidents(limit=20)
        print(f"Recent incidents: {len(incidents)}")
        for item in incidents:
            print(f"- {item['id']} | {item.get('title') or 'Untitled'} | {item['created_at']}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
