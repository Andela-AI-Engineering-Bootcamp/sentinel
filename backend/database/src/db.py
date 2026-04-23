"""Database adapter for Sentinel database utilities."""

from __future__ import annotations

from src.pathing import ensure_backend_root_on_path

ensure_backend_root_on_path()

from common.config import get_db_path
from common.store import Database


def get_database() -> Database:
    """Return a database instance using configured path."""

    return Database(get_db_path())
