"""CloudWatch-backed live incident detection for Sentinel premium users."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from common.config import aurora_region
from common.models import IncidentInput
from common.pipeline import create_incident_and_job, run_job
from common.store import Database
from common.heuristics import summarize_incident

logger = logging.getLogger(__name__)

_SEVERITY_ORDER = {"low": 1, "medium": 2, "high": 3, "critical": 4}

_PATTERNS: list[dict[str, Any]] = [
    {
        "key": "oom",
        "regex": re.compile(r"(oom|out of memory|killed process)", re.I),
        "title": "Memory pressure / OOM burst",
        "severity": "critical",
        "threshold": 1,
    },
    {
        "key": "panic",
        "regex": re.compile(r"(panic|fatal|service down|outage)", re.I),
        "title": "Fatal runtime failure burst",
        "severity": "critical",
        "threshold": 1,
    },
    {
        "key": "auth",
        "regex": re.compile(r"(\b401\b|\b403\b|forbidden|unauthor|access denied|permission denied|invalid token|\bjwt\b)", re.I),
        "title": "Authentication / authorization failure burst",
        "severity": "high",
        "threshold": 3,
    },
    {
        "key": "database",
        "regex": re.compile(r"(database unavailable|db timeout|connection refused|could not connect|sqlstate|postgres)", re.I),
        "title": "Database connectivity burst",
        "severity": "high",
        "threshold": 3,
    },
    {
        "key": "timeout",
        "regex": re.compile(r"(timeout|timed out|deadline exceeded|upstream failure|504|503)", re.I),
        "title": "Timeout / dependency latency burst",
        "severity": "high",
        "threshold": 4,
    },
    {
        "key": "throttle",
        "regex": re.compile(r"(throttl|rate limit|too many requests|429)", re.I),
        "title": "Throttling / quota burst",
        "severity": "high",
        "threshold": 4,
    },
    {
        "key": "exception",
        "regex": re.compile(r"(exception|traceback|error|failed)", re.I),
        "title": "Application error burst",
        "severity": "high",
        "threshold": 5,
    },
]

_NOISE_PATTERNS = [
    re.compile(r"^(START|END|REPORT|INIT_START)\s+RequestId:", re.I),
    re.compile(r"^INIT_START Runtime Version:", re.I),
]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _short_group_name(log_group: str) -> str:
    parts = [part for part in log_group.split("/") if part]
    return parts[-1] if parts else log_group


def _effective_severity(a: str, b: str) -> str:
    return a if _SEVERITY_ORDER.get(a, 1) >= _SEVERITY_ORDER.get(b, 1) else b


def _pattern_match(message: str) -> dict[str, Any] | None:
    for pattern in _PATTERNS:
        if pattern["regex"].search(message):
            return pattern
    return None


def _default_log_groups() -> list[str]:
    raw = os.getenv("LIVE_CLOUDWATCH_LOG_GROUPS", "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _serialize_event(log_group: str, event: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": int(event.get("timestamp") or 0),
        "log_group": log_group,
        "message": str(event.get("message") or "").strip(),
    }


def _is_noise(message: str) -> bool:
    return any(pattern.search(message) for pattern in _NOISE_PATTERNS)


def _load_events(log_groups: list[str], start_ms: int) -> tuple[list[dict[str, Any]], list[str]]:
    client = boto3.client("logs", region_name=aurora_region())
    events: list[dict[str, Any]] = []
    warnings: list[str] = []

    for log_group in log_groups:
        try:
            paginator = client.get_paginator("filter_log_events")
            page_iter = paginator.paginate(
                logGroupName=log_group,
                startTime=start_ms,
                PaginationConfig={"MaxItems": 200, "PageSize": 100},
            )
            for page in page_iter:
                for event in page.get("events") or []:
                    events.append(_serialize_event(log_group, event))
        except client.exceptions.ResourceNotFoundException:
            warnings.append(f"Log group not found: {log_group}")
        except (ClientError, BotoCoreError) as exc:
            warnings.append(f"{log_group}: {exc}")
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"{log_group}: {exc}")

    events.sort(key=lambda item: item["timestamp"])
    return events, warnings


def _bucket_events(events: list[dict[str, Any]], error_threshold: int) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        msg = event["message"]
        if _is_noise(msg):
            continue
        pattern = _pattern_match(msg)
        if not pattern:
            continue
        fingerprint = hashlib.sha1(f"{event['log_group']}|{pattern['key']}".encode("utf-8")).hexdigest()[:20]
        bucket = buckets.setdefault(
            fingerprint,
            {
                "fingerprint": fingerprint,
                "pattern_key": pattern["key"],
                "title": f"{pattern['title']} in {_short_group_name(event['log_group'])}",
                "severity": pattern["severity"],
                "pattern_threshold": max(1, int(error_threshold or pattern["threshold"])),
                "source_log_groups": {event["log_group"]},
                "events": [],
            },
        )
        bucket["events"].append(event)
        bucket["source_log_groups"].add(event["log_group"])

    detections: list[dict[str, Any]] = []
    for bucket in buckets.values():
        if len(bucket["events"]) < bucket["pattern_threshold"]:
            continue
        recent_events = bucket["events"][-12:]
        combined = "\n".join(
            f"[{item['log_group']}] {item['message']}" for item in recent_events if item["message"]
        )
        summary = summarize_incident(combined)
        evidence = recent_events[-6:]
        detections.append(
            {
                "fingerprint": bucket["fingerprint"],
                "title": bucket["title"],
                "severity": _effective_severity(bucket["severity"], summary.severity),
                "source_log_groups": sorted(bucket["source_log_groups"]),
                "events": recent_events,
                "evidence": evidence,
                "event_count": len(bucket["events"]),
                "combined_text": combined,
            }
        )
    detections.sort(key=lambda item: (_SEVERITY_ORDER.get(item["severity"], 1), item["event_count"]), reverse=True)
    return detections


def _job_is_active(db: Database, job_id: str | None, clerk_user_id: str) -> bool:
    if not job_id:
        return False
    row = db.get_job(job_id, clerk_user_id=clerk_user_id)
    return bool(row and row.get("status") in {"pending", "processing"})


def _job_needs_background_run(db: Database, job_id: str | None, clerk_user_id: str) -> bool:
    if not job_id:
        return False
    row = db.get_job(job_id, clerk_user_id=clerk_user_id)
    return bool(row and row.get("status") == "pending")


def _should_rerun_analysis(existing: dict[str, Any], detection: dict[str, Any]) -> bool:
    last_analysis_raw = existing.get("last_analysis_at")
    if not last_analysis_raw:
        return True
    try:
        last_analysis = datetime.fromisoformat(str(last_analysis_raw))
    except ValueError:
        return True
    if (_now() - last_analysis) >= timedelta(minutes=10):
        return True
    return int(detection["event_count"]) >= max(int(existing.get("event_count") or 0) + 5, 10)


def refresh_live_board(clerk_user_id: str, db: Database) -> dict[str, Any]:
    config = db.get_live_monitor_config(clerk_user_id)
    if not config["log_groups"]:
        config["log_groups"] = _default_log_groups()

    if not config["enabled"]:
        return {
            "config": config,
            "incidents": list_live_board_data(clerk_user_id, db),
            "warnings": [],
            "refreshed_at": _now_iso(),
        }

    if not config["log_groups"]:
        return {
            "config": config,
            "incidents": list_live_board_data(clerk_user_id, db),
            "warnings": ["Add at least one CloudWatch log group to start monitoring."],
            "refreshed_at": _now_iso(),
        }

    if config.get("last_polled_at"):
        try:
            start_dt = datetime.fromisoformat(str(config["last_polled_at"]))
        except ValueError:
            start_dt = _now() - timedelta(minutes=config["lookback_minutes"])
    else:
        start_dt = _now() - timedelta(minutes=config["lookback_minutes"])

    start_ms = int(start_dt.timestamp() * 1000)
    events, warnings = _load_events(config["log_groups"], start_ms)
    detections = _bucket_events(events, config["error_threshold"])
    refreshed_at = _now_iso()

    for detection in detections:
        existing = db.get_live_incident_by_fingerprint(clerk_user_id, detection["fingerprint"])
        latest_job_id: str | None = None
        incident_id: str | None = None
        last_analysis_at: str | None = None

        if existing:
            incident_id = existing.get("incident_id")
            latest_job_id = existing.get("latest_job_id")
            if incident_id and not _job_is_active(db, latest_job_id, clerk_user_id) and _should_rerun_analysis(existing, detection):
                db.update_incident_raw_text(incident_id, detection["combined_text"], title=detection["title"])
                latest_job_id = db.create_job(incident_id, clerk_user_id)
                last_analysis_at = refreshed_at
            else:
                last_analysis_at = existing.get("last_analysis_at")
        else:
            payload = IncidentInput(
                title=detection["title"],
                source="cloudwatch_live",
                text=detection["combined_text"],
            )
            incident_id, latest_job_id = create_incident_and_job(payload, db, clerk_user_id=clerk_user_id)
            last_analysis_at = refreshed_at

        if _job_needs_background_run(db, latest_job_id, clerk_user_id):
            # Kick off analysis for new or refreshed incident snapshots.
            import threading

            threading.Thread(
                target=run_job,
                args=(latest_job_id, None, clerk_user_id),
                daemon=True,
            ).start()

        evidence_json = [
            {
                "timestamp": item["timestamp"],
                "log_group": item["log_group"],
                "message": item["message"][:500],
            }
            for item in detection["evidence"]
        ]

        if existing:
            db.update_live_incident(
                existing["id"],
                title=detection["title"],
                severity=detection["severity"],
                source_log_groups=detection["source_log_groups"],
                evidence=evidence_json,
                event_count=detection["event_count"],
                incident_id=incident_id if incident_id is not None else existing.get("incident_id"),
                latest_job_id=latest_job_id if latest_job_id is not None else existing.get("latest_job_id"),
                last_seen_at=refreshed_at,
                last_analysis_at=last_analysis_at,
                status="open",
            )
        else:
            db.create_live_incident(
                clerk_user_id,
                fingerprint=detection["fingerprint"],
                title=detection["title"],
                severity=detection["severity"],
                source_log_groups=detection["source_log_groups"],
                evidence=evidence_json,
                event_count=detection["event_count"],
                incident_id=incident_id,
                latest_job_id=latest_job_id,
                first_seen_at=refreshed_at,
                last_seen_at=refreshed_at,
                last_analysis_at=last_analysis_at,
            )

    db.touch_live_monitor_poll(clerk_user_id, polled_at=refreshed_at)
    config = db.get_live_monitor_config(clerk_user_id)
    return {
        "config": config,
        "incidents": list_live_board_data(clerk_user_id, db),
        "warnings": warnings,
        "refreshed_at": refreshed_at,
    }


def list_live_board_data(clerk_user_id: str, db: Database) -> list[dict[str, Any]]:
    rows = db.list_live_incidents(clerk_user_id, limit=25)
    data: list[dict[str, Any]] = []
    for row in rows:
        evidence: list[dict[str, Any]] = []
        try:
            parsed = json.loads(row.get("evidence_json") or "[]")
            if isinstance(parsed, list):
                evidence = parsed
        except json.JSONDecodeError:
            evidence = []
        source_groups: list[str] = []
        try:
            parsed_groups = json.loads(row.get("source_log_groups_json") or "[]")
            if isinstance(parsed_groups, list):
                source_groups = [str(item) for item in parsed_groups]
        except json.JSONDecodeError:
            source_groups = []

        analysis = None
        latest_job_id = row.get("latest_job_id")
        if latest_job_id:
            job = db.get_job(latest_job_id, clerk_user_id=clerk_user_id)
            if job:
                raw = job.get("analysis_json")
                if raw:
                    try:
                        analysis = json.loads(raw)
                    except json.JSONDecodeError:
                        analysis = None

        data.append(
            {
                "id": row["id"],
                "fingerprint": row["fingerprint"],
                "title": row["title"],
                "status": row.get("status") or "open",
                "severity": row.get("severity") or "medium",
                "event_count": int(row.get("event_count") or 0),
                "source_log_groups": source_groups,
                "evidence": evidence,
                "incident_id": row.get("incident_id"),
                "latest_job_id": latest_job_id,
                "first_seen_at": row.get("first_seen_at"),
                "last_seen_at": row.get("last_seen_at"),
                "last_analysis_at": row.get("last_analysis_at"),
                "analysis": analysis,
            }
        )
    return data
