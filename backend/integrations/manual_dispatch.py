"""Send a sample completed analysis to a real URL (webhook.site, httpbin, etc.).

Run from ``backend/``::

    WEBHOOK_URL=https://webhook.site/<your-uuid> uv run python -m integrations.manual_dispatch

Optional::

    INTEGRATION_TYPE=slack          # default: generic_webhook
    SENTINEL_PUBLIC_URL=http://localhost:3000   # adds dashboard_url in generic payload
    INCIDENT_TITLE=edge-gateway.txt             # appears as incident_title in JSON / Slack
    INCIDENT_SOURCE=upload
"""

from __future__ import annotations

import os
import sys

from common.models import (
    GuardrailReport,
    IncidentAnalysis,
    IncidentSummary,
    RemediationPlan,
    RootCauseAnalysis,
)
from integrations.dispatcher import dispatch_all


def _sample_analysis() -> IncidentAnalysis:
    return IncidentAnalysis(
        incident_id="inc-manual-test",
        job_id="job-manual-test",
        summary=IncidentSummary(
            summary="Manual integration smoke test",
            severity="high",
            severity_reason="synthetic payload for outbound dispatch",
        ),
        root_cause=RootCauseAnalysis(
            likely_root_cause="Not applicable — test dispatch only",
            confidence="low",
            reasoning="integrations.manual_dispatch",
        ),
        remediation=RemediationPlan(
            recommended_actions=["Verify webhook received this payload", "Check server logs"],
            next_checks=["Confirm 200 from receiver", "Rotate secret if URL was exposed"],
            risk_if_unresolved="None for a test run",
            recommended_severities=["medium", "medium"],
            check_severities=["low", "low"],
        ),
        guardrails=GuardrailReport(),
        models={"support": "manual", "root_cause": "manual", "remediation": "manual"},
    )


def main() -> int:
    url = (os.getenv("WEBHOOK_URL") or "").strip()
    if not url:
        print("Set WEBHOOK_URL to your webhook.site URL (or https://httpbin.org/post).", file=sys.stderr)
        return 1
    itype = (os.getenv("INTEGRATION_TYPE") or "generic_webhook").strip().lower()
    if itype not in ("slack", "generic_webhook"):
        print("INTEGRATION_TYPE must be slack or generic_webhook", file=sys.stderr)
        return 1

    row = {
        "type": itype,
        "enabled": True,
        "config": {"webhook_url": url},
    }
    analysis = _sample_analysis()
    title = (os.getenv("INCIDENT_TITLE") or "").strip()
    source = (os.getenv("INCIDENT_SOURCE") or "").strip()
    print(f"Dispatching {itype} → {url[:60]}…")
    dispatch_all(
        [row],
        analysis,
        incident_title=title,
        incident_source=source,
    )
    print("Done — check your receiver for a new request.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
