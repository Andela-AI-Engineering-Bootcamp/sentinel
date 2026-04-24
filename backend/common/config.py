"""Configuration helpers for Sentinel backend."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional in some runtime contexts
    load_dotenv = None


if load_dotenv is not None:
    # Load repo-level .env for local scripts; harmless in AWS Lambda if absent.
    _repo_env = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(_repo_env, override=False)


def is_local() -> bool:
    """Return True when running outside AWS (no Aurora ARNs configured)."""
    return not (os.getenv("AURORA_CLUSTER_ARN", "").strip() and os.getenv("AURORA_SECRET_ARN", "").strip())


def sqlite_path() -> str:
    """Absolute path to the local SQLite database file."""
    default = str(Path(__file__).resolve().parents[2] / "sentinel.db")
    return os.getenv("LOCAL_DB_PATH", default)


def get_db_path() -> str:
    """Backward-compatible alias: returns Aurora database name or local SQLite path."""
    if is_local():
        return sqlite_path()
    return aurora_database()


def aurora_cluster_arn() -> str:
    return os.getenv("AURORA_CLUSTER_ARN", "").strip()


def aurora_secret_arn() -> str:
    return os.getenv("AURORA_SECRET_ARN", "").strip()


def aurora_database() -> str:
    return (
        os.getenv("AURORA_DATABASE", "").strip()
        or os.getenv("DB_NAME", "").strip()
        or "sentinel"
    )


def aurora_region() -> str:
    return (
        os.getenv("AURORA_REGION", "").strip()
        or os.getenv("DEFAULT_AWS_REGION", "").strip()
        or os.getenv("AWS_REGION", "").strip()
        or "eu-west-1"
    )


def use_bedrock() -> bool:
    return os.getenv("USE_BEDROCK", "false").lower() == "true"


def bedrock_region() -> str:
    return os.getenv("BEDROCK_REGION", os.getenv("DEFAULT_AWS_REGION", "eu-west-1"))


def clerk_secret_key() -> str:
    return os.getenv("CLERK_SECRET_KEY", "")


def model_support() -> str:
    return os.getenv("BEDROCK_MODEL_SUPPORT", "openai.gpt-oss-120b-1:0")


def model_root_cause() -> str:
    return os.getenv("BEDROCK_MODEL_ROOT_CAUSE", "eu.amazon.nova-pro-v1:0")


def model_remediation() -> str:
    return os.getenv("BEDROCK_MODEL_REMEDIATION", "eu.amazon.nova-pro-v1:0")


def use_openrouter() -> bool:
    return os.getenv("USE_OPEN_ROUTER", "false").lower() == "true"


def openrouter_api_key() -> str:
    return os.getenv("OPENROUTER_API_KEY", "")


def openrouter_model() -> str:
    return os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")


def openrouter_base_url() -> str:
    return os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")


def reminder_interval_seconds() -> int:
    """Interval in seconds between background reminder checks."""
    try:
        return int(os.getenv("REMINDER_INTERVAL_SECONDS", "60"))
    except ValueError:
        return 60
