"""Orchestration pipeline shared by API and planner lambda."""

from __future__ import annotations

import json
import logging

from common.config import model_remediation, model_root_cause, model_support
from common.models import IncidentAnalysis, IncidentInput, JobRunResponse
from common.similarity import find_similar_incidents
from common.store import Database, get_database
from investigator.agent import investigate_root_cause
from normalizer.agent import normalize_incident
from remediator.agent import generate_remediation
from summarizer.agent import summarize_incident

logger = logging.getLogger(__name__)


def run_job(
    job_id: str, db: Database | None = None, clerk_user_id: str | None = None
) -> JobRunResponse:
    """Execute the full multi-agent workflow for one job."""

    owned_db = False
    if db is None:
        db = get_database()
        owned_db = True

    try:
        row = db.get_job_with_incident(job_id, clerk_user_id=clerk_user_id)
        if not row:
            return JobRunResponse(
                incident_id="", job_id=job_id, status="failed", error="Job not found"
            )

        status = row.get("status")
        if status == "processing":
            return JobRunResponse(
                incident_id=row["incident_id"], job_id=job_id, status="processing"
            )
        if status == "completed" and row.get("analysis_json"):
            try:
                analysis = IncidentAnalysis.model_validate_json(row["analysis_json"])
                return JobRunResponse(
                    incident_id=row["incident_id"],
                    job_id=job_id,
                    status="completed",
                    analysis=analysis,
                )
            except Exception:  # noqa: BLE001
                pass

        db.update_job_status(job_id, "processing")
        db.set_job_stage(job_id, "queued", "Starting pipeline")

        db.set_job_stage(job_id, "normalize", "Sanitizing and structuring input")
        normalized = normalize_incident(row["raw_text"])
        db.update_incident_sanitization(
            row["incident_id"],
            normalized.normalized_text,
            normalized.guardrails.model_dump(),
        )

        try:
            uid = row.get("clerk_user_id") or clerk_user_id or "anonymous"
            past = db.list_incidents(limit=100, clerk_user_id=uid)
            similar = find_similar_incidents(
                normalized.normalized_text,
                past,
                exclude_id=row["incident_id"],
            )
            if similar:
                db.set_similar_incidents(job_id, similar)
        except Exception:  # noqa: BLE001
            logger.warning("Similarity lookup failed; continuing without it")

        db.set_job_stage(job_id, "summarize", "Generating summary and severity")
        summary = summarize_incident(normalized)

        db.set_job_stage(job_id, "root_cause", "Investigating likely root cause")
        root_cause = investigate_root_cause(normalized, summary)

        db.set_job_stage(job_id, "remediate", "Building remediation plan")
        remediation = generate_remediation(normalized, summary, root_cause)

        analysis = IncidentAnalysis(
            incident_id=row["incident_id"],
            job_id=job_id,
            summary=summary,
            root_cause=root_cause,
            remediation=remediation,
            guardrails=normalized.guardrails,
            models={
                "support": model_support(),
                "root_cause": model_root_cause(),
                "remediation": model_remediation(),
            },
        )

        db.save_analysis(job_id, analysis)

        try:
            incident_severity = summary.severity  # critical / high / medium / low
            _lower = {
                "critical": "high",
                "high": "medium",
                "medium": "low",
                "low": "low",
            }
            fallback_check_sev = _lower.get(incident_severity, "medium")

            valid_severities = {"critical", "high", "medium", "low"}

            # Build per-action severity lists, padding with incident severity if LLM omitted entries.
            rec_sevs = list(remediation.recommended_severities)
            while len(rec_sevs) < len(remediation.recommended_actions):
                rec_sevs.append(incident_severity)

            chk_sevs = list(remediation.check_severities)
            while len(chk_sevs) < len(remediation.next_checks):
                chk_sevs.append(fallback_check_sev)

            for text, sev in zip(remediation.recommended_actions, rec_sevs):
                sev = sev if sev in valid_severities else incident_severity
                db.seed_remediation_actions(
                    job_id, [text], action_type="recommended", severity=sev
                )

            for text, sev in zip(remediation.next_checks, chk_sevs):
                sev = sev if sev in valid_severities else fallback_check_sev
                db.seed_remediation_actions(
                    job_id, [text], action_type="check", severity=sev
                )

        except Exception:  # noqa: BLE001
            logger.warning("Failed to seed remediation actions; continuing")

        try:
            _fire_integrations(
                job_id,
                analysis,
                db,
                clerk_user_id or row.get("clerk_user_id") or "anonymous",
            )
        except Exception:  # noqa: BLE001
            logger.warning("Integration dispatch failed; continuing")

        db.set_job_stage(job_id, "completed", "Analysis ready")
        return JobRunResponse(
            incident_id=row["incident_id"],
            job_id=job_id,
            status="completed",
            analysis=analysis,
        )

    except Exception as exc:  # noqa: BLE001
        db.update_job_status(job_id, "failed", str(exc))
        row = db.get_job(job_id, clerk_user_id=clerk_user_id) or {"incident_id": ""}
        try:
            db.set_job_stage(job_id, "failed", str(exc))
        except Exception:  # noqa: BLE001
            pass
        return JobRunResponse(
            incident_id=row.get("incident_id", ""),
            job_id=job_id,
            status="failed",
            error=str(exc),
        )
    finally:
        if owned_db:
            db.close()


def _fire_integrations(
    job_id: str, analysis: "IncidentAnalysis", db: Database, clerk_user_id: str
) -> None:
    """Dispatch configured integrations if analysis severity warrants it."""
    from integrations.dispatcher import dispatch_all

    integrations = db.list_integrations(clerk_user_id)
    if not integrations:
        return
    severity = analysis.summary.severity
    if severity not in ("high", "critical"):
        return
    dispatch_all(integrations, analysis)


def create_incident_and_job(
    payload: IncidentInput,
    db: Database | None = None,
    clerk_user_id: str = "anonymous",
) -> tuple[str, str]:
    """Create incident/job records in pending state."""

    owned_db = False
    if db is None:
        db = get_database()  # type: ignore[assignment]
        owned_db = True

    try:
        incident_id = db.create_incident(
            text=payload.text,
            title=payload.title,
            source=payload.source,
            clerk_user_id=clerk_user_id,
        )
        job_id = db.create_job(incident_id=incident_id, clerk_user_id=clerk_user_id)
        return incident_id, job_id
    finally:
        if owned_db:
            db.close()


def parse_analysis(job_row: dict) -> dict | None:
    """Parse analysis payload from DB row."""

    raw = job_row.get("analysis_json")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None
