"""Reset SQLite DB file used by Sentinel local development."""

from __future__ import annotations

from pathlib import Path

from src.pathing import ensure_backend_root_on_path

ensure_backend_root_on_path()

from common.config import get_db_path
from common.store import Database


def main() -> None:
    db_path = Path(get_db_path())
    if db_path.exists():
        db_path.unlink()
        print(f"Deleted existing DB: {db_path}")

    db = Database(str(db_path))
    db.close()
    print("Reset complete.")


if __name__ == "__main__":
    main()
