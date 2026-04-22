"""Remediator agent for next-action recommendations."""

from __future__ import annotations

from common.bedrock import converse_json
from common.config import model_remediation
from common.guardrails import enforce_grounding
from common.heuristics import generate_questions, recommend_actions
from common.models import (
    ClarificationQuestion,
    ClarificationSet,
    IncidentSummary,
    NormalizedIncident,
    RemediationPlan,
    RootCauseAnalysis,
)
from remediator.templates import REMEDIATOR_INSTRUCTIONS


def generate_remediation(
    normalized: NormalizedIncident,
    summary: IncidentSummary,
    root_cause: RootCauseAnalysis,
    clarifications: dict[str, str] | None = None,
) -> RemediationPlan:
    """Generate remediation recommendations with guardrail enforcement.

    When ``clarifications`` are provided the prompt is enriched with operator
    context so the model can produce environment-specific actions instead of
    generic guidance.
    """

    clarification_block = ""
    if clarifications:
        lines = "\n".join(f"  Q({qid}): {ans}" for qid, ans in clarifications.items() if ans.strip())
        if lines:
            clarification_block = f"\nAdditional operator context:\n{lines}"

    prompt = (
        "Return strict JSON with keys: recommended_actions, recommended_severities, "
        "next_checks, check_severities, risk_if_unresolved.\n"
        f"Incident severity: {summary.severity}\n"
        f"Root cause: {root_cause.model_dump_json()}\n"
        f"Evidence: {normalized.evidence_snippets}"
        f"{clarification_block}"
    )
    result = converse_json(model_remediation(), REMEDIATOR_INSTRUCTIONS, prompt)

    if result:
        try:
            rem = RemediationPlan.model_validate(result)
            _, rem = enforce_grounding(root_cause, rem, normalized.evidence_snippets)
            return rem
        except Exception:  # noqa: BLE001
            pass

    rem = recommend_actions(root_cause, summary.severity)
    _, rem = enforce_grounding(root_cause, rem, normalized.evidence_snippets)
    return rem


def build_clarification_set(
    job_id: str,
    root_cause: RootCauseAnalysis,
    evidence: list[str],
    already_answered: bool = False,
) -> ClarificationSet:
    """Build a ClarificationSet from heuristic question generation."""

    questions: list[ClarificationQuestion] = generate_questions(root_cause, evidence)
    urgency = "required" if root_cause.confidence == "low" else "suggested"
    return ClarificationSet(
        job_id=job_id,
        questions=questions,
        urgency=urgency,
        already_answered=already_answered,
    )
