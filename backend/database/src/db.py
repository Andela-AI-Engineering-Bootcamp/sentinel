"""Database adapter and migration helpers for Sentinel database utilities."""

from __future__ import annotations

from pathlib import Path

try:
    from .pathing import ensure_backend_root_on_path
except ImportError:  # pragma: no cover - script execution fallback
    from src.pathing import ensure_backend_root_on_path

ensure_backend_root_on_path()

from common.store import get_database as _get_database, _SentinelDb


def get_database() -> _SentinelDb:
    """Return a database instance appropriate for the current environment."""

    return _get_database()


def migration_file() -> Path:
    """Absolute path to the baseline Aurora schema migration file."""

    return Path(__file__).resolve().parents[1] / "migrations" / "001_schema.sql"


def load_sql_statements(path: Path) -> list[str]:
    """Load SQL file and split into executable statements."""

    sql_text = path.read_text(encoding="utf-8")
    lines: list[str] = []
    for line in sql_text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)

    cleaned = "\n".join(lines)
    statements = [stmt.strip() for stmt in cleaned.split(";")]
    return [stmt for stmt in statements if stmt]
