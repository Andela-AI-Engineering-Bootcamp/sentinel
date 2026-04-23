"""Apply Aurora schema migrations for Sentinel."""

from __future__ import annotations

from src.pathing import ensure_backend_root_on_path

ensure_backend_root_on_path()

from src.db import get_database, load_sql_statements, migration_file


def main() -> None:
    db = get_database()
    try:
        path = migration_file()
        statements = load_sql_statements(path)
        db.execute_script(statements)
        print(f"Migrations complete ({len(statements)} statements applied).")
    finally:
        db.close()


if __name__ == "__main__":
    main()
