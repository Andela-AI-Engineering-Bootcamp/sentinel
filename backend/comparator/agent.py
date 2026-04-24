from __future__ import annotations

import json
from typing import Any

from common.bedrock import converse_json
from common.models import IncidentCompareResult
from comparator.templates import COMPARE_INSTRUCTIONS

_MAX_TEXT = 6000


def _compact_workflow(wf: dict[str, Any]) -> dict[str, Any]:
    """Shape sent to the model — keep under token budget."""
    job = wf.get("job") or {}
    inc = (wf.get("incident") or {}) if isinstance(wf.get("incident"), dict) else {}
    analysis = wf.get("analysis")
    nt = (wf.get("normalized_text") or "")[:_MAX_TEXT]
    return {
        "job_id": job.get("job_id") or job.get("id"),
        "title": inc.get("title"),
        "source": inc.get("source"),
        "status": job.get("status"),
        "created_at": job.get("created_at"),
        "completed_at": job.get("completed_at"),
        "analysis": analysis,
        "normalized_text_excerpt": nt,
        "remediation_action_texts": [
            (a.get("action_text") or "")
            for a in (wf.get("remediation_actions") or [])
        ][:20],
    }


def compare_workflows(
    job_id_a: str,
    job_id_b: str,
    workflow_a: dict[str, Any],
    workflow_b: dict[str, Any],
) -> IncidentCompareResult:
    user_prompt = (
        "Incident A:\n"
        + json.dumps(_compact_workflow(workflow_a), default=str)[:_MAX_TEXT * 2]
        + "\n\nIncident B:\n"
        + json.dumps(_compact_workflow(workflow_b), default=str)[:_MAX_TEXT * 2]
    )
    result = converse_json(COMPARE_INSTRUCTIONS, user_prompt, max_tokens=2000)
    if not result:
        return IncidentCompareResult(
            job_id_a=job_id_a,
            job_id_b=job_id_b,
            verdict="unclear",
            confidence="low",
            notes="Comparison model was unavailable; configure OpenRouter or Bedrock.",
        )
    try:
        return IncidentCompareResult(
            job_id_a=job_id_a,
            job_id_b=job_id_b,
            verdict=result.get("verdict", "unclear"),
            confidence=result.get("confidence", "low"),
            overlapping_symptoms=list(result.get("overlapping_symptoms") or []),
            divergences=list(result.get("divergences") or []),
            operator_next_steps=list(result.get("operator_next_steps") or []),
            notes=str(result.get("notes") or ""),
        )
    except Exception:
        return IncidentCompareResult(
            job_id_a=job_id_a,
            job_id_b=job_id_b,
            verdict="unclear",
            confidence="low",
            notes="Failed to parse comparison result.",
        )