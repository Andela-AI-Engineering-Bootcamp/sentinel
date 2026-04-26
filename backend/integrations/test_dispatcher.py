"""Unit tests for outbound integration dispatch."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from common.models import (
    GuardrailReport,
    IncidentAnalysis,
    IncidentSummary,
    RemediationPlan,
    RootCauseAnalysis,
)
from integrations.dispatcher import _post_slack, dispatch_all


def _sample_analysis() -> IncidentAnalysis:
    return IncidentAnalysis(
        incident_id="inc-1",
        job_id="job-1",
        summary=IncidentSummary(
            summary="DB timeouts",
            severity="high",
            severity_reason="customer impact",
        ),
        root_cause=RootCauseAnalysis(
            likely_root_cause="connection pool exhausted",
            confidence="high",
            reasoning="logs show timeouts",
        ),
        remediation=RemediationPlan(
            recommended_actions=["Scale pool", "Add index"],
            next_checks=["Check RDS metrics"],
            risk_if_unresolved="outage continues",
            recommended_severities=["high", "medium"],
            check_severities=["medium"],
        ),
        guardrails=GuardrailReport(),
        models={"support": "x", "root_cause": "y", "remediation": "z"},
    )


@patch("integrations.dispatcher.httpx.Client")
def test_dispatch_slack_posts_json(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.post = MagicMock(return_value=mock_resp)
    mock_client_cls.return_value = mock_client

    integrations = [
        {
            "type": "slack",
            "enabled": True,
            "config": {"webhook_url": "https://hooks.slack.com/services/TEST/TEST/TEST"},
        }
    ]
    dispatch_all(integrations, _sample_analysis())

    mock_client.post.assert_called_once()
    args, kwargs = mock_client.post.call_args
    assert args[0] == "https://hooks.slack.com/services/TEST/TEST/TEST"
    assert "text" in kwargs["json"]
    assert "DB timeouts" in kwargs["json"]["text"]


@patch("integrations.dispatcher.httpx.Client")
def test_dispatch_skips_disabled(mock_client_cls: MagicMock) -> None:
    integrations = [
        {"type": "slack", "enabled": False, "config": {"webhook_url": "https://example.com"}},
    ]
    dispatch_all(integrations, _sample_analysis())
    mock_client_cls.assert_not_called()


def test_dispatch_empty_list_noop() -> None:
    dispatch_all([], _sample_analysis())


@patch("integrations.dispatcher.httpx.Client")
def test_dispatch_generic_includes_incident_title_and_source(mock_client_cls: MagicMock) -> None:
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.post = MagicMock(return_value=mock_resp)
    mock_client_cls.return_value = mock_client

    integrations = [
        {
            "type": "generic_webhook",
            "enabled": True,
            "config": {"webhook_url": "https://example.com/webhook"},
        },
    ]
    dispatch_all(
        integrations,
        _sample_analysis(),
        incident_title="edge-gateway.txt",
        incident_source="upload",
    )
    body = mock_client.post.call_args[1]["json"]
    assert body["incident_id"] == "inc-1"
    assert body["job_id"] == "job-1"
    assert body["incident_title"] == "edge-gateway.txt"
    assert body["incident_source"] == "upload"
    assert list(body.keys())[:6] == [
        "event",
        "incident_id",
        "job_id",
        "incident_title",
        "incident_source",
        "severity",
    ]


def test_slack_rejects_unicode_ellipsis_in_webhook_url() -> None:
    """Doc-style `…` in the URL path is not a token — Slack returns 302 if posted."""
    config = {"webhook_url": "https://hooks.slack.com/services/\u2026"}
    with pytest.raises(ValueError, match="ellipsis"):
        _post_slack(config, _sample_analysis())
