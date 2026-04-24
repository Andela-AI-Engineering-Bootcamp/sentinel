"""Smoke test for premium feature entitlement enforcement."""

from __future__ import annotations

import os

from fastapi import Depends
from fastapi.testclient import TestClient

import api.auth as auth_mod
from api.main import app


@app.get("/__test/live-gated")
def _live_gated(_user: auth_mod.AuthContext = Depends(auth_mod.require_feature("live_incident_board"))) -> dict[str, bool]:
    return {"ok": True}


def main() -> None:
    os.environ["AUTH_DISABLED"] = "true"

    original = auth_mod.get_user_entitlements
    try:
        client = TestClient(app)

        auth_mod.get_user_entitlements = lambda user: auth_mod.default_entitlements()
        denied = client.get("/__test/live-gated")
        assert denied.status_code == 403, denied.text

        auth_mod.get_user_entitlements = lambda user: {
            "subscription_tier": "pro",
            "features": {"live_incident_board": True},
        }
        allowed = client.get("/__test/live-gated")
        assert allowed.status_code == 200, allowed.text

        print("Entitlement smoke test passed (free denied, pro allowed).")
    finally:
        auth_mod.get_user_entitlements = original


if __name__ == "__main__":
    main()
