"""Verify auth enforcement when AUTH_DISABLED=false."""

from __future__ import annotations

import os

from fastapi.testclient import TestClient

from api.main import app


def main() -> None:
    os.environ["AUTH_DISABLED"] = "false"
    os.environ.pop("CLERK_JWKS_URL", None)
    os.environ.pop("CLERK_ISSUER", None)

    client = TestClient(app)
    response = client.get("/api/jobs")
    assert response.status_code == 401, response.text
    print("Auth enforcement test passed (missing token rejected).")


if __name__ == "__main__":
    main()
