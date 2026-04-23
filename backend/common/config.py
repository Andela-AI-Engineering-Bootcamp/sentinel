"""Configuration helpers for Sentinel backend."""

from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = str(BASE_DIR / "sentinel.db")


def get_db_path() -> str:
    return os.getenv("SENTINEL_DB_PATH", DEFAULT_DB_PATH)


def use_bedrock() -> bool:
    return os.getenv("USE_BEDROCK", "false").lower() == "true"


def bedrock_region() -> str:
    return os.getenv("BEDROCK_REGION", os.getenv("DEFAULT_AWS_REGION", "us-east-1"))


def clerk_secret_key() -> str:
    return os.getenv("CLERK_SECRET_KEY", "")


def model_support() -> str:
    return os.getenv("BEDROCK_MODEL_SUPPORT", "openai.gpt-oss-120b-1:0")


def model_root_cause() -> str:
    return os.getenv("BEDROCK_MODEL_ROOT_CAUSE", "us.amazon.nova-pro-v1:0")


def model_remediation() -> str:
    return os.getenv("BEDROCK_MODEL_REMEDIATION", "us.amazon.nova-pro-v1:0")


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
