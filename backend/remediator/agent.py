"""Remediator agent for next-action recommendations."""

from __future__ import annotations

import logging

from common.bedrock import converse_json

logger = logging.getLogger(__name__)
from common.guardrails import enforce_grounding
from common.heuristics import generate_questions, recommend_actions
from common.models import (
    ActionEvaluationResult,
    ClarificationQuestion,
    ClarificationSet,
    IncidentAnalysis,
    IncidentSummary,
    NormalizedIncident,
    PostIncidentReview,
    RemediationFollowUp,
    RemediationPlan,
    RootCauseAnalysis,
)
from remediator.templates import (
    EVALUATION_INSTRUCTIONS,
    FOLLOWUP_INSTRUCTIONS,
    PIR_INSTRUCTIONS,
    REMEDIATOR_INSTRUCTIONS,
)


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
    result = converse_json(REMEDIATOR_INSTRUCTIONS, prompt)

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


def evaluate_findings(
    action_text: str,
    analysis: IncidentAnalysis,
    findings: str,
) -> ActionEvaluationResult:
    """Evaluate whether engineer findings satisfy a specific remediation action.

    Returns an ``ActionEvaluationResult`` with a boolean verdict, a short
    response to show the engineer, and an optional next_step sub-action text
    when the findings are insufficient.
    """
    prompt = (
        "Return strict JSON with keys: satisfied (bool), response (string), next_step (string or null).\n"
        f"Incident severity: {analysis.summary.severity}\n"
        f"Root cause: {analysis.root_cause.likely_root_cause}\n"
        f"Remediation action being evaluated: {action_text}\n"
        f"Engineer findings: {findings}"
    )

    result = converse_json(EVALUATION_INSTRUCTIONS, prompt, max_tokens=600)
    logger.info("evaluate_findings LLM result: %s", result)

    if result is None:
        logger.warning("evaluate_findings: LLM returned None")
        return ActionEvaluationResult(
            satisfied=False,
            response="Could not reach the AI model. Please review the findings manually.",
            next_step=None,
        )

    try:
        satisfied = bool(result.get("satisfied", False))
        return ActionEvaluationResult(
            satisfied=satisfied,
            response=str(result.get("response", "")),
            next_step=str(result["next_step"]) if result.get("next_step") and not satisfied else None,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("evaluate_findings: failed to parse LLM result: %s", exc)
        return ActionEvaluationResult(
            satisfied=False,
            response="Could not parse AI response. Please review manually.",
            next_step=None,
        )


def generate_followup_actions(
    analysis: IncidentAnalysis,
    completed_actions: list[dict],
    additional_context: str,
    anchor_action_id: str | None = None,
) -> RemediationFollowUp:
    """Generate follow-up remediation actions based on engineer findings.

    Takes the existing analysis, the current state of all actions (including
    per-action notes), and the engineer's free-text findings from working through
    the initial plan — then produces new actions that build on that context.

    When ``anchor_action_id`` is provided the prompt explicitly calls out which
    action the engineer's findings relate to, producing more targeted follow-ups.
    """
    anchor_text: str = ""
    action_lines_parts: list[str] = []
    for a in completed_actions:
        prefix = "  "
        if anchor_action_id and str(a.get("id", "")) == str(anchor_action_id):
            prefix = "  [ANCHOR — findings relate to this action] "
            anchor_text = a.get("action_text", "")
        line = (
            f"{prefix}[{a.get('status', 'pending').upper()}] {a.get('action_text', '')}"
            + (f" — Findings: {a['notes']}" if a.get("notes") else "")
        )
        action_lines_parts.append(line)
    action_lines = "\n".join(action_lines_parts)

    original_actions = "\n".join(
        f"  • {act}" for act in (
            analysis.remediation.recommended_actions + analysis.remediation.next_checks
        )
    )

    anchor_block = (
        f"The findings specifically relate to this action: {anchor_text}\n"
        if anchor_text else ""
    )

    prompt = (
        "Return strict JSON with keys: followup_actions, followup_severities, "
        "followup_checks, check_severities, updated_risk.\n"
        f"Severity: {analysis.summary.severity}\n"
        f"Root cause: {analysis.root_cause.likely_root_cause}\n"
        f"Original remediation actions (do NOT repeat these):\n{original_actions}\n"
        f"Current action progress:\n{action_lines or '  (no actions started yet)'}\n"
        f"{anchor_block}"
        f"Engineer findings from remediation:\n  {additional_context}"
    )

    result = converse_json(FOLLOWUP_INSTRUCTIONS, prompt, max_tokens=2000)
    logger.info("generate_followup_actions LLM result: %s", result)

    if result is None:
        logger.warning("generate_followup_actions: LLM returned None (not configured or call failed)")
        return RemediationFollowUp(updated_risk=analysis.remediation.risk_if_unresolved)

    try:
        return RemediationFollowUp(
            followup_actions=list(result.get("followup_actions", [])),
            followup_severities=list(result.get("followup_severities", [])),
            followup_checks=list(result.get("followup_checks", [])),
            check_severities=list(result.get("check_severities", [])),
            updated_risk=str(result.get("updated_risk", analysis.remediation.risk_if_unresolved)),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("generate_followup_actions: failed to parse LLM result: %s", exc)
        return RemediationFollowUp(updated_risk=analysis.remediation.risk_if_unresolved)


def generate_pir(
    analysis: IncidentAnalysis,
    completed_actions: list[dict],
) -> PostIncidentReview:
    """Generate a post-incident review from a completed analysis and action log."""

    action_lines = "\n".join(
        f"  [{a.get('status', 'pending').upper()}] {a.get('action_text', '')} "
        f"(assigned: {a.get('assigned_to') or 'unassigned'})"
        f"{(' — ' + a['notes']) if a.get('notes') else ''}"
        for a in completed_actions
    )
    prompt = (
        "Return strict JSON with keys: timeline, what_went_wrong, what_went_right, "
        "action_summary (array of strings), prevention_steps (array of strings), lessons_learned.\n"
        f"Severity: {analysis.summary.severity}\n"
        f"Summary: {analysis.summary.summary}\n"
        f"Root cause: {analysis.root_cause.likely_root_cause}\n"
        f"Reasoning: {analysis.root_cause.reasoning}\n"
        f"Confidence: {analysis.root_cause.confidence}\n"
        f"Risk if unresolved: {analysis.remediation.risk_if_unresolved}\n"
        f"Actions taken:\n{action_lines or '  (no actions recorded)'}"
    )

    result = converse_json(PIR_INSTRUCTIONS, prompt)

    if result:
        try:
            return PostIncidentReview(
                job_id=analysis.job_id,
                timeline=str(result.get("timeline", "")),
                what_went_wrong=str(result.get("what_went_wrong", "")),
                what_went_right=str(result.get("what_went_right", "")),
                action_summary=list(result.get("action_summary", [])),
                prevention_steps=list(result.get("prevention_steps", [])),
                lessons_learned=str(result.get("lessons_learned", "")),
            )
        except Exception:  # noqa: BLE001
            pass

    # Fallback — construct a minimal PIR from existing data
    done = [a for a in completed_actions if a.get("status") == "done"]
    return PostIncidentReview(
        job_id=analysis.job_id,
        timeline=f"Incident detected and analysed. {len(done)} of {len(completed_actions)} remediation actions completed.",
        what_went_wrong=analysis.root_cause.likely_root_cause,
        what_went_right="Incident was detected and a remediation plan was generated.",
        action_summary=[a["action_text"] for a in completed_actions],
        prevention_steps=list(analysis.remediation.recommended_actions[:3]),
        lessons_learned=analysis.root_cause.reasoning,
    )


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
