"""Path bootstrap helpers for local database utility scripts."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_backend_root_on_path() -> None:
    """Ensure `backend/` is importable so `common` package resolves."""

    backend_root = Path(__file__).resolve().parents[2]
    backend_root_str = str(backend_root)
    if backend_root_str not in sys.path:
        sys.path.insert(0, backend_root_str)
