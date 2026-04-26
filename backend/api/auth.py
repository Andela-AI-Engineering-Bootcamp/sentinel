"""Clerk JWT authentication helpers for FastAPI."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


bearer_scheme = HTTPBearer(auto_error=False)
_jwks_client: jwt.PyJWKClient | None = None


@dataclass
class AuthContext:
    """Authenticated request context."""

    user_id: str
    email: str | None
    claims: dict


class AuthError(Exception):
    """Authentication verification failure."""


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def auth_disabled() -> bool:
    """Return whether auth is explicitly disabled for local development/tests."""

    return _truthy(os.getenv("AUTH_DISABLED", "false"))


def default_entitlements() -> dict[str, Any]:
    """Return the baseline feature set for users without a paid plan."""

    return {
        "subscription_tier": "free",
        "features": {
            "live_incident_board": False,
        },
    }


def _clerk_jwks_url() -> str:
    jwks = os.getenv("CLERK_JWKS_URL", "").strip()
    if jwks:
        return jwks

    issuer = os.getenv("CLERK_ISSUER", "").strip().rstrip("/")
    if issuer:
        return f"{issuer}/.well-known/jwks.json"

    raise AuthError("Missing CLERK_JWKS_URL or CLERK_ISSUER.")


def _get_jwks_client() -> jwt.PyJWKClient:
    global _jwks_client
    if _jwks_client is None:
        _jwks_client = jwt.PyJWKClient(_clerk_jwks_url())
    return _jwks_client


def verify_clerk_token(token: str) -> dict:
    """Validate Clerk JWT and return claims."""

    try:
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token).key

        decode_kwargs: dict = {
            "algorithms": ["RS256"],
            "options": {"verify_aud": False},
        }

        issuer = os.getenv("CLERK_ISSUER", "").strip().rstrip("/")
        if issuer:
            # JWT `iss` from Clerk does not include a trailing slash; env values often do.
            decode_kwargs["issuer"] = issuer

        payload = jwt.decode(token, signing_key, **decode_kwargs)
    except Exception as exc:  # noqa: BLE001
        raise AuthError(f"Token verification failed: {exc}") from exc

    subject = payload.get("sub")
    if not subject:
        raise AuthError("Missing subject claim in token.")

    return payload


def require_auth(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> AuthContext:
    """FastAPI dependency that enforces Clerk auth."""

    if auth_disabled():
        return AuthContext(user_id="dev_user", email="dev@example.com", claims={"mode": "auth_disabled"})

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Bearer token")

    try:
        claims = verify_clerk_token(credentials.credentials)
    except AuthError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    return AuthContext(user_id=claims["sub"], email=claims.get("email_address"), claims=claims)


def get_user_entitlements(user: AuthContext) -> dict[str, Any]:
    """Load product entitlements for the authenticated user."""

    try:
        from common.store import get_database

        db = get_database()
        try:
            return db.get_user_entitlements(user.user_id)
        finally:
            db.close()
    except Exception:
        return default_entitlements()


def require_feature(feature_name: str):
    """FastAPI dependency that enforces a specific product entitlement."""

    def dependency(user: AuthContext = Depends(require_auth)) -> AuthContext:
        entitlements = get_user_entitlements(user)
        enabled = bool(entitlements.get("features", {}).get(feature_name))
        if not enabled:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Feature '{feature_name}' is not enabled for this account.",
            )
        return user

    return dependency
