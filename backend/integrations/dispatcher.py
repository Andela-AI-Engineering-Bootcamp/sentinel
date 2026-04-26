"""Dispatch completed analysis to user-configured integrations (outbound only)."""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlencode

import httpx

from common.models import IncidentAnalysis

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(12.0, connect=5.0)


def _webhook_placeholder_error(url: str) -> str | None:
    """Detect doc-style shortened URLs (302 from Slack if the path is literally '…')."""
    if "\u2026" in url:
        return (
            "webhook_url contains a Unicode ellipsis (…) — that is not part of a real Slack token. "
            "In Slack: Apps → Incoming Webhooks → copy the full URL "
            "(https://hooks.slack.com/services/T…/B…/… with three long segments)."
        )
    return None


def _public_job_url(job_id: str) -> str | None:
    base = (os.getenv("SENTINEL_PUBLIC_URL") or os.getenv("NEXT_PUBLIC_APP_URL") or "").strip().rstrip("/")
    if not base:
        return None
    q = urlencode({"job": job_id})
    return f"{base}/dashboard?{q}"


def _analysis_payload(
    analysis: IncidentAnalysis,
    *,
    incident_title: str = "",
    incident_source: str = "",
) -> dict[str, Any]:
    """JSON-serializable snapshot for generic webhooks.

    ``incident_title`` is the Sentinel incident title (for bulk ZIP this is usually the
    archive member file name, optionally with a title prefix). ``incident_source`` is the
    incident ``source`` field (e.g. ``upload``, ``manual``).

    Title and source are ordered near the top of the payload (after ids) for webhook consumers.
    """
    title = incident_title or ""
    src = incident_source or ""
    out: dict[str, Any] = {
        "event": "sentinel.analysis.completed",
        "incident_id": analysis.incident_id,
        "job_id": analysis.job_id,
        "incident_title": title,
        "incident_source": src,
        "severity": analysis.summary.severity,
        "summary": analysis.summary.summary,
        "severity_reason": analysis.summary.severity_reason,
        "likely_root_cause": analysis.root_cause.likely_root_cause,
        "root_cause_confidence": analysis.root_cause.confidence,
        "recommended_actions": analysis.remediation.recommended_actions,
        "next_checks": analysis.remediation.next_checks,
        "risk_if_unresolved": analysis.remediation.risk_if_unresolved,
        "dashboard_url": _public_job_url(analysis.job_id),
    }
    return out


def _post_slack(
    config: dict[str, Any],
    analysis: IncidentAnalysis,
    *,
    incident_title: str = "",
    incident_source: str = "",
) -> None:
    url = (config.get("webhook_url") or "").strip()
    if not url:
        logger.warning("Slack integration missing webhook_url")
        return
    if (hint := _webhook_placeholder_error(url)) is not None:
        raise ValueError(hint)
    sev = analysis.summary.severity.upper()
    title = f"Sentinel · {sev} · Job `{analysis.job_id[:8]}…`"
    lines = [
        f"*{title}*",
    ]
    if (incident_title or "").strip():
        lines.append(f"*Incident title:* {incident_title.strip()}")
    if (incident_source or "").strip():
        lines.append(f"*Source:* {incident_source.strip()}")
    lines.extend(
        [
        f"*Summary:* {analysis.summary.summary}",
        f"*Likely root cause:* {analysis.root_cause.likely_root_cause}",
        ]
    )
    if analysis.remediation.recommended_actions:
        lines.append("*Recommended actions:*")
        for i, a in enumerate(analysis.remediation.recommended_actions[:8], 1):
            lines.append(f"{i}. {a}")
    if analysis.remediation.next_checks:
        lines.append("*Next checks:*")
        for i, c in enumerate(analysis.remediation.next_checks[:5], 1):
            lines.append(f"{i}. {c}")
    dash = _public_job_url(analysis.job_id)
    if dash:
        lines.append(f"<{dash}|Open in Sentinel dashboard>")
    text = "\n".join(lines)
    body = {"text": text[:15000]}
    with httpx.Client(timeout=_TIMEOUT) as client:
        r = client.post(url, json=body)
        r.raise_for_status()


def _post_generic(
    config: dict[str, Any],
    analysis: IncidentAnalysis,
    *,
    incident_title: str = "",
    incident_source: str = "",
) -> None:
    url = (config.get("webhook_url") or "").strip()
    if not url:
        logger.warning("generic_webhook missing webhook_url")
        return
    if (hint := _webhook_placeholder_error(url)) is not None:
        raise ValueError(hint)
    payload = _analysis_payload(
        analysis,
        incident_title=incident_title,
        incident_source=incident_source,
    )
    headers: dict[str, str] = {}
    auth = (config.get("auth_header_name") or "").strip()
    auth_val = (config.get("auth_header_value") or "").strip()
    if auth and auth_val:
        headers[auth] = auth_val
    with httpx.Client(timeout=_TIMEOUT) as client:
        r = client.post(url, json=payload, headers=headers or None)
        r.raise_for_status()


def _post_pagerduty(
    config: dict[str, Any],
    analysis: IncidentAnalysis,
    *,
    incident_title: str = "",
    incident_source: str = "",
) -> None:
    routing_key = (config.get("routing_key") or "").strip()
    if not routing_key:
        logger.warning("pagerduty integration missing routing_key")
        return
    sev = analysis.summary.severity
    pd_severity = {"critical": "critical", "high": "error", "medium": "warning", "low": "info"}.get(
        sev, "warning"
    )
    summary = f"[Sentinel] {sev.upper()}: {analysis.summary.summary[:1024]}"
    body = {
        "routing_key": routing_key,
        "event_action": "trigger",
        "payload": {
            "summary": summary,
            "severity": pd_severity,
            "source": "sentinel",
            "custom_details": _analysis_payload(
                analysis,
                incident_title=incident_title,
                incident_source=incident_source,
            ),
        },
    }
    with httpx.Client(timeout=_TIMEOUT) as client:
        r = client.post("https://events.pagerduty.com/v2/enqueue", json=body)
        r.raise_for_status()


def dispatch_all(
    integrations: list[dict[str, Any]],
    analysis: IncidentAnalysis,
    *,
    incident_title: str = "",
    incident_source: str = "",
) -> None:
    """Send analysis to each enabled integration; failures are logged, not raised."""
    for row in integrations:
        if not row.get("enabled", True):
            continue
        config = row.get("config") or {}
        itype = row.get("type") or ""
        try:
            if itype == "slack":
                _post_slack(
                    config,
                    analysis,
                    incident_title=incident_title,
                    incident_source=incident_source,
                )
            elif itype == "generic_webhook":
                _post_generic(
                    config,
                    analysis,
                    incident_title=incident_title,
                    incident_source=incident_source,
                )
            elif itype == "pagerduty":
                _post_pagerduty(
                    config,
                    analysis,
                    incident_title=incident_title,
                    incident_source=incident_source,
                )
            elif itype in ("jira", "opsgenie"):
                logger.info("Integration type %s is saved but outbound dispatch is not implemented", itype)
            else:
                logger.warning("Unknown integration type: %s", itype)
        except Exception:
            logger.exception("Integration dispatch failed type=%s", itype)
