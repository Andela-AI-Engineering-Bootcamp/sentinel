"""Investigator agent for root-cause identification."""

from __future__ import annotations

import json
from collections.abc import Iterator

from common.bedrock import converse_json, converse_stream_text
from common.guardrails import enforce_grounding
from common.heuristics import infer_root_cause
from common.models import IncidentSummary, NormalizedIncident, RemediationPlan, RootCauseAnalysis
from investigator.templates import INVESTIGATOR_INSTRUCTIONS


def _investigator_prompt(normalized: NormalizedIncident, summary: IncidentSummary) -> str:
    return (
        "Return strict JSON with keys: likely_root_cause, confidence, reasoning, supporting_evidence.\n"
        f"Summary: {summary.summary}\n"
        f"Evidence: {normalized.evidence_snippets}\n"
        f"Log text: {normalized.normalized_text[:4000]}"
    )


def investigate_root_cause(normalized: NormalizedIncident, summary: IncidentSummary) -> RootCauseAnalysis:
    """Identify likely root cause with grounded evidence."""

    prompt = _investigator_prompt(normalized, summary)
    result = converse_json(INVESTIGATOR_INSTRUCTIONS, prompt)

    if result:
        try:
            rc = RootCauseAnalysis.model_validate(result)
            rc, _ = enforce_grounding(
                rc,
                RemediationPlan(recommended_actions=[], next_checks=[], risk_if_unresolved=""),
                normalized.evidence_snippets,
            )
            return rc
        except Exception:  # noqa: BLE001
            pass

    rc = infer_root_cause(normalized.normalized_text, normalized.evidence_snippets)
    rc, _ = enforce_grounding(
        rc,
        RemediationPlan(recommended_actions=[], next_checks=[], risk_if_unresolved=""),
        normalized.evidence_snippets,
    )
    return rc


def stream_investigation_text(normalized: NormalizedIncident, summary: IncidentSummary) -> Iterator[str]:
    """Yield streamed model text (JSON) or chunked heuristic JSON for the UI."""

    prompt = _investigator_prompt(normalized, summary)
    stream = converse_stream_text(INVESTIGATOR_INSTRUCTIONS, prompt)
    had_chunk = False
    for chunk in stream:
        had_chunk = True
        yield chunk

    if had_chunk:
        return

    rc = infer_root_cause(normalized.normalized_text, normalized.evidence_snippets)
    rc, _ = enforce_grounding(
        rc,
        RemediationPlan(recommended_actions=[], next_checks=[], risk_if_unresolved=""),
        normalized.evidence_snippets,
    )
    payload = rc.model_dump_json()
    for i in range(0, len(payload), 32):
        yield payload[i : i + 32]


def parse_streamed_root_cause(text: str, normalized: NormalizedIncident) -> RootCauseAnalysis | None:
    """Parse accumulated investigator stream into a RootCauseAnalysis when possible."""

    raw = text.strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
        rc = RootCauseAnalysis.model_validate(data)
        rc, _ = enforce_grounding(
            rc,
            RemediationPlan(recommended_actions=[], next_checks=[], risk_if_unresolved=""),
            normalized.evidence_snippets,
        )
        return rc
    except Exception:  # noqa: BLE001
        return None
