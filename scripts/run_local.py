"""Run Sentinel backend API and frontend dev servers together."""

from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"
FRONTEND_DIR = ROOT / "frontend"


def _load_dotenv(path: Path) -> dict[str, str]:
    """Parse a simple KEY=VALUE .env file; skips comments and blank lines."""
    result: dict[str, str] = {}
    if not path.exists():
        return result
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        # Strip optional surrounding quotes
        value = value.strip().strip('"').strip("'")
        if key:
            result[key] = value
    return result


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.4)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def main() -> None:
    backend_cmd = ["uv", "run", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
    frontend_cmd = ["npm", "run", "dev"]

    # Start from the shell environment then layer in the project .env file
    base_env = os.environ.copy()
    dotenv_vars = _load_dotenv(ROOT / ".env")

    # .env values only fill in what the shell hasn't already set
    for key, value in dotenv_vars.items():
        base_env.setdefault(key, value)

    backend_env = base_env.copy()
    frontend_env = base_env.copy()

    # Only disable auth if no Clerk config is present at all
    if not backend_env.get("CLERK_JWKS_URL") and not backend_env.get("CLERK_ISSUER"):
        backend_env.setdefault("AUTH_DISABLED", "true")

    if _port_in_use(8000):
        print(
            "WARNING: Something is already listening on port 8000. "
            "Uvicorn will fail to bind; the UI will still talk to that process (often an OLD API).",
            flush=True,
        )
        print("  Free the port:  lsof -ti :8000 | xargs kill -9", flush=True)

    backend = subprocess.Popen(backend_cmd, cwd=BACKEND_DIR, env=backend_env, shell=os.name == "nt")
    frontend = subprocess.Popen(frontend_cmd, cwd=FRONTEND_DIR, env=frontend_env, shell=os.name == "nt")

    print("Sentinel local dev started:")
    print("- Backend:  http://localhost:8000")
    print("- Frontend: http://localhost:3000")
    if backend_env.get("AUTH_DISABLED") == "true":
        print("- Auth mode: AUTH_DISABLED=true (set Clerk env vars in .env to enable)")
    else:
        print("- Auth mode: Clerk enabled")

    try:
        backend.wait()
        frontend.wait()
    except KeyboardInterrupt:
        print("\nStopping services...")
    finally:
        for proc in (backend, frontend):
            if proc.poll() is None:
                proc.terminate()


if __name__ == "__main__":
    main()
