"""Summarizer agent implementation."""

from __future__ import annotations

from common.bedrock import converse_json
from common.heuristics import summarize_incident as heuristic_summarize
from common.models import IncidentSummary, NormalizedIncident

_SUMMARIZER_INSTRUCTIONS = (
    "You are a senior site-reliability engineer. Summarize the incident concisely and classify severity. "
    "Return strict JSON with keys: summary (string, ≤120 words), severity (one of: low, medium, high, critical), "
    "severity_reason (string, ≤60 words). Base your assessment only on the supplied log text."
)


def summarize_incident(normalized: NormalizedIncident) -> IncidentSummary:
    """Create incident summary and severity, preferring Bedrock when available."""

    prompt = (
        "Return strict JSON with keys: summary, severity, severity_reason.\n"
        f"Evidence snippets: {normalized.evidence_snippets}\n"
        f"Log text:\n{normalized.normalized_text[:6000]}"
    )
    result = converse_json(_SUMMARIZER_INSTRUCTIONS, prompt)

    if result:
        try:
            return IncidentSummary.model_validate(result)
        except Exception:  # noqa: BLE001
            pass

    return heuristic_summarize(normalized.normalized_text)
