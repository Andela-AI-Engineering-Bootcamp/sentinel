"""Sentinel API service."""

from __future__ import annotations

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional for local dev only
    load_dotenv = None

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel as _Base

from api.auth import AuthContext, require_auth
from common.config import get_db_path
from common.log_stats import compute_log_stats
from common.models import (
    ActionChatRequest,
    ActionEvaluationRequest,
    ActionUpdate,
    ClarificationAnswers,
    DigestRequest,
    FollowUpCreate,
    GuardrailReport,
    IncidentInput,
    IncidentResolveRequest,
    IncidentSummary,
    IntegrationCreate,
    InvestigationStreamInput,
    JobCreateResponse,
    NormalizedIncident,
    RemediationFollowUpRequest,
)
from common.audit_pdf import render_audit_classic_pdf
from common.pdf_report import render_job_pdf
from common.pipeline import create_incident_and_job, parse_analysis, run_job
from common.scheduler import ReminderScheduler
from common.store import Database
from investigator.agent import parse_streamed_root_cause, stream_investigation_text

logger = logging.getLogger(__name__)

if load_dotenv is not None:
    load_dotenv()


app = FastAPI(title="Sentinel API", version="0.3.0")

allowed_origins = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]


@app.on_event("startup")
def on_startup() -> None:
    """Wake up the background scheduler when the API starts."""
    ReminderScheduler.get_instance().ensure_running()

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _db() -> Database:
    return Database(get_db_path())


def _job_view(row: dict[str, Any]) -> dict[str, Any]:
    events: list[Any] = []
    if row.get("pipeline_events"):
        try:
            parsed = json.loads(row["pipeline_events"])
            if isinstance(parsed, list):
                events = parsed
        except json.JSONDecodeError:
            events = []
    return {
        "job_id": row["id"],
        "incident_id": row["incident_id"],
        "status": row["status"],
        "error": row.get("error_message"),
        "current_stage": row.get("current_stage"),
        "pipeline_events": events,
        "analysis": parse_analysis(row),
    }


def _enrich_job_view(
    row: dict[str, Any], db: Database, clerk_user_id: str
) -> dict[str, Any]:
    view = _job_view(row)
    inc = db.get_incident(row["incident_id"], clerk_user_id=clerk_user_id)
    text = (inc or {}).get("sanitized_text") or (inc or {}).get("raw_text") or ""
    view["normalized_text"] = text
    view["log_stats"] = compute_log_stats(text)
    sim_raw = row.get("similar_incidents_json")
    if sim_raw:
        try:
            view["similar_incidents"] = json.loads(sim_raw)
        except json.JSONDecodeError:
            view["similar_incidents"] = []
    else:
        view["similar_incidents"] = []
    return view


def _incident_export_block(inc: dict[str, Any] | None) -> dict[str, Any] | None:
    """Snapshot of the incident record for workflow / audit export."""
    if not inc:
        return None
    out: dict[str, Any] = {
        "id": inc.get("id"),
        "clerk_user_id": inc.get("clerk_user_id"),
        "title": inc.get("title"),
        "source": inc.get("source"),
        "status": inc.get("status"),
        "assigned_to": inc.get("assigned_to"),
        "created_at": inc.get("created_at"),
        "resolved_at": inc.get("resolved_at"),
        "resolution_notes": inc.get("resolution_notes"),
        "raw_text": inc.get("raw_text"),
        "sanitized_text": inc.get("sanitized_text"),
    }
    g = inc.get("guardrail_json")
    if g:
        try:
            out["guardrails"] = json.loads(g) if isinstance(g, str) else g
        except json.JSONDecodeError:
            out["guardrails"] = g
    return out


def _remediation_chat_by_action(
    messages: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    by_action: dict[str, list[dict[str, Any]]] = {}
    for m in messages:
        aid = m.get("action_id") or ""
        by_action.setdefault(aid, []).append(
            {
                "id": m.get("id"),
                "role": m.get("role"),
                "content": m.get("content"),
                "created_at": m.get("created_at"),
            }
        )
    return by_action


def _clarification_qa_for_export(
    job_id: str, view: dict[str, Any], db: Database
) -> list[dict[str, Any]] | None:
    """Pair stored answers with the same question text the UI showed (rebuilt heuristically)."""
    answers = db.get_clarification_answers(job_id)
    if not answers:
        return None
    analysis = view.get("analysis") or {}
    rc_data = analysis.get("root_cause")
    if not rc_data:
        return [
            {
                "question_id": qid,
                "question": None,
                "rationale": None,
                "kind": None,
                "answer": ans,
            }
            for qid, ans in answers.items()
        ]

    from common.models import RootCauseAnalysis
    from remediator.agent import build_clarification_set

    try:
        root_cause = RootCauseAnalysis.model_validate(rc_data)
    except Exception:  # noqa: BLE001
        return [
            {
                "question_id": qid,
                "question": None,
                "rationale": None,
                "kind": None,
                "answer": ans,
            }
            for qid, ans in answers.items()
        ]

    evidence = (analysis.get("remediation") or {}).get("recommended_actions") or []
    cset = build_clarification_set(
        job_id=job_id,
        root_cause=root_cause,
        evidence=evidence,
        already_answered=True,
    )
    q_by_id = {q.id: q for q in cset.questions}
    out: list[dict[str, Any]] = []
    used: set[str] = set()
    for q in cset.questions:
        if q.id not in answers:
            continue
        used.add(q.id)
        out.append(
            {
                "question_id": q.id,
                "question": q.question,
                "rationale": q.rationale,
                "kind": q.kind,
                "answer": answers[q.id],
            }
        )
    for qid, ans in answers.items():
        if qid in used:
            continue
        q = q_by_id.get(qid)
        out.append(
            {
                "question_id": qid,
                "question": q.question if q else None,
                "rationale": q.rationale if q else None,
                "kind": q.kind if q else None,
                "answer": ans,
            }
        )
    return out


def _build_workflow_export(
    job_row: dict[str, Any],
    view: dict[str, Any],
    db: Database,
    job_id: str,
    clerk_user_id: str,
) -> dict[str, Any]:
    """Full readonly snapshot: analysis pipeline, remediation, follow-ups, chat, PIR, incident context."""

    inc = db.get_incident(str(view.get("incident_id")), clerk_user_id=clerk_user_id)
    clar_qa = _clarification_qa_for_export(job_id, view, db)
    return {
        "export_version": 2,
        "kind": "sentinel_workflow",
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "job": {
            "job_id": view.get("job_id") or job_row.get("id"),
            "incident_id": view.get("incident_id"),
            "status": view.get("status"),
            "error": view.get("error"),
            "current_stage": view.get("current_stage"),
            "created_at": job_row.get("created_at"),
            "completed_at": job_row.get("completed_at"),
        },
        "pipeline_events": view.get("pipeline_events") or [],
        "similar_incidents": view.get("similar_incidents") or [],
        "analysis": view.get("analysis"),
        "normalized_text": view.get("normalized_text"),
        "log_stats": view.get("log_stats"),
        "clarification_answers": db.get_clarification_answers(job_id),
        "clarification_qa": clar_qa,
        "remediation_actions": db.list_remediation_actions(job_id),
        "follow_ups": db.list_follow_ups(job_id),
        "remediation_chat": _remediation_chat_by_action(
            db.list_chat_messages_for_job(job_id)
        ),
        "post_incident_review": db.get_pir(job_id),
        "incident": _incident_export_block(inc),
    }


def _background_run_job(job_id: str, clerk_user_id: str) -> None:
    run_job(job_id, db=None, clerk_user_id=clerk_user_id)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "sentinel-api"}


@app.get("/api/me")
def me(user: AuthContext = Depends(require_auth)) -> dict[str, Any]:
    return {"user_id": user.user_id, "email": user.email}


@app.get("/api/team/members")
def list_team_members(
    user: AuthContext = Depends(require_auth),
) -> list[dict[str, Any]]:
    """Return Clerk users visible to this instance.

    When CLERK_SECRET_KEY is set the Clerk Backend API is queried.
    Falls back to returning the current user only so the UI always has
    at least one assignee option.
    """
    import urllib.error
    import urllib.request

    from common.config import clerk_secret_key

    secret = clerk_secret_key()

    if secret:
        try:
            req = urllib.request.Request(
                "https://api.clerk.com/v1/users?limit=100&order_by=-created_at",
                headers={
                    "Authorization": f"Bearer {secret}",
                    "Content-Type": "application/json",
                },
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=8) as resp:  # noqa: S310
                raw = json.loads(resp.read().decode())
            members: list[dict[str, Any]] = []
            # Clerk may return a list directly or a dict with a "data" key
            user_list = raw if isinstance(raw, list) else raw.get("data", [])
            for u in user_list:
                primary_email = ""
                for addr in u.get("email_addresses") or []:
                    if addr.get("id") == u.get("primary_email_address_id"):
                        primary_email = addr.get("email_address", "")
                        break
                if not primary_email and u.get("email_addresses"):
                    primary_email = u["email_addresses"][0].get("email_address", "")
                first = u.get("first_name") or ""
                last = u.get("last_name") or ""
                name = (
                    f"{first} {last}".strip()
                    or u.get("username")
                    or primary_email
                    or u["id"]
                )
                members.append(
                    {
                        "user_id": u["id"],
                        "name": name,
                        "email": primary_email,
                        "avatar_url": u.get("image_url") or u.get("profile_image_url"),
                    }
                )
            return members
        except urllib.error.HTTPError as exc:
            raise HTTPException(
                status_code=502, detail=f"Clerk API error: {exc.code}"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=502, detail=f"Could not reach Clerk API: {exc}"
            ) from exc

    # Auth disabled or no secret key — return the current user only
    return [
        {
            "user_id": user.user_id,
            "name": user.email or user.user_id,
            "email": user.email or "",
            "avatar_url": None,
        }
    ]


@app.post("/api/incidents", response_model=JobCreateResponse)
def create_incident(
    payload: IncidentInput,
    background_tasks: BackgroundTasks,
    user: AuthContext = Depends(require_auth),
) -> JobCreateResponse:
    db = _db()
    try:
        incident_id, job_id = create_incident_and_job(
            payload, db, clerk_user_id=user.user_id
        )
        background_tasks.add_task(_background_run_job, job_id, user.user_id)
        return JobCreateResponse(
            incident_id=incident_id, job_id=job_id, status="pending"
        )
    finally:
        db.close()


@app.post("/api/incidents/analyze-sync")
def analyze_sync(
    payload: IncidentInput, user: AuthContext = Depends(require_auth)
) -> dict[str, Any]:
    db = _db()
    try:
        incident_id, job_id = create_incident_and_job(
            payload, db, clerk_user_id=user.user_id
        )
        result = run_job(job_id, db, clerk_user_id=user.user_id)
        return result.model_dump()
    finally:
        db.close()


@app.post("/api/jobs/{job_id}/run")
def run_analysis(
    job_id: str, user: AuthContext = Depends(require_auth)
) -> dict[str, Any]:
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        result = run_job(job_id, db, clerk_user_id=user.user_id)
        return result.model_dump()
    finally:
        db.close()


@app.get("/api/jobs")
def list_jobs_endpoint(
    limit: int = 25, user: AuthContext = Depends(require_auth)
) -> list[dict[str, Any]]:
    """Recent analysis runs (for dashboard — no need to paste internal ids)."""

    db = _db()
    try:
        rows = db.list_jobs(limit=limit, clerk_user_id=user.user_id)
        return [
            {
                "job_id": r["job_id"],
                "incident_id": r["incident_id"],
                "title": r.get("title"),
                "source": r.get("source"),
                "status": r["status"],
                "created_at": r["created_at"],
                "completed_at": r.get("completed_at"),
            }
            for r in rows
        ]
    finally:
        db.close()


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, user: AuthContext = Depends(require_auth)) -> dict[str, Any]:
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        return _enrich_job_view(row, db, user.user_id)
    finally:
        db.close()


@app.get("/api/jobs/{job_id}/workflow")
def get_workflow_snapshot(
    job_id: str, user: AuthContext = Depends(require_auth)
) -> dict[str, Any]:
    """Readonly full workflow: analysis stages, remediation actions, follow-ups, chat, PIR, incident context."""

    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        view = _enrich_job_view(row, db, user.user_id)
        return _build_workflow_export(row, view, db, job_id, user.user_id)
    finally:
        db.close()


@app.get("/api/jobs/{job_id}/audit/pdf")
def get_audit_pdf(
    job_id: str, user: AuthContext = Depends(require_auth)
) -> Any:
    """Download full workflow as a traditional (Classic) black-on-white audit PDF."""
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        view = _enrich_job_view(row, db, user.user_id)
        wf = _build_workflow_export(row, view, db, job_id, user.user_id)
        pdf = render_audit_classic_pdf(wf)
        name = f"sentinel-audit-{str(job_id)[:8]}.pdf"
        return Response(
            content=bytes(pdf),
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )
    finally:
        db.close()


@app.get("/api/jobs/{job_id}/export")
def export_job(
    job_id: str,
    export_format: Literal["json", "pdf"] = Query(
        "json", alias="format", description="json or pdf"
    ),
    user: AuthContext = Depends(require_auth),
) -> Any:
    """Download full workflow JSON (audit) or a printable PDF summary."""
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        view = _enrich_job_view(row, db, user.user_id)
        if export_format == "json":
            body = _build_workflow_export(row, view, db, job_id, user.user_id)
            name = f"sentinel-workflow-{str(job_id)[:8]}.json"
            return JSONResponse(
                content=body,
                headers={"Content-Disposition": f'attachment; filename="{name}"'},
            )
        view["remediation_actions"] = db.list_remediation_actions(job_id)
        pdf = render_job_pdf(view)
        name = f"sentinel-export-{str(job_id)[:8]}.pdf"
        return Response(
            content=bytes(pdf),
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )
    finally:
        db.close()


@app.get("/api/jobs/{job_id}/stream")
async def stream_job_events(
    job_id: str, user: AuthContext = Depends(require_auth)
) -> StreamingResponse:
    """Server-Sent Events for pipeline stage updates (polls SQLite; use Authorization header with fetch streams)."""

    async def event_source() -> Any:
        last_len = 0
        while True:
            db = _db()
            try:
                row = db.get_job(job_id, clerk_user_id=user.user_id)
                if not row:
                    yield f"data: {json.dumps({'error': 'not_found'})}\n\n"
                    return
                events_raw = row.get("pipeline_events") or "[]"
                try:
                    evs = json.loads(events_raw)
                except json.JSONDecodeError:
                    evs = []
                if isinstance(evs, list) and len(evs) > last_len:
                    for ev in evs[last_len:]:
                        yield f"data: {json.dumps({'event': ev})}\n\n"
                    last_len = len(evs)
                st = row["status"]
                if st in ("completed", "failed"):
                    view = _enrich_job_view(row, db, user.user_id)
                    payload = {"terminal": True, "status": st, "job": view}
                    yield f"data: {json.dumps(payload)}\n\n"
                    return
            finally:
                db.close()
            await asyncio.sleep(0.28)

    return StreamingResponse(event_source(), media_type="text/event-stream")


@app.post("/api/stream/investigate")
def stream_investigation(
    body: InvestigationStreamInput,
    _user: AuthContext = Depends(require_auth),
) -> StreamingResponse:
    """Stream investigator model output as SSE chunks; ends with parsed JSON when valid."""

    normalized = NormalizedIncident(
        normalized_text=body.normalized_text,
        evidence_snippets=body.evidence_snippets,
        guardrails=GuardrailReport(),
    )
    summary = IncidentSummary(
        summary=body.summary,
        severity="medium",
        severity_reason="Streaming replay",
    )

    def gen() -> Any:
        parts: list[str] = []
        for chunk in stream_investigation_text(normalized, summary):
            parts.append(chunk)
            yield f"data: {json.dumps({'chunk': chunk})}\n\n"
        full = "".join(parts)
        parsed = parse_streamed_root_cause(full, normalized)
        done: dict[str, Any] = {"done": True, "raw_length": len(full)}
        if parsed:
            done["root_cause"] = parsed.model_dump()
        yield f"data: {json.dumps(done)}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/api/analytics/mttr")
def get_mttr(user: AuthContext = Depends(require_auth)) -> dict[str, Any]:
    """Mean time to resolve (job created → completed) broken down by severity and source."""
    db = _db()
    try:
        rows = db.list_jobs(limit=500, clerk_user_id=user.user_id)
        from datetime import datetime

        completed = [
            r for r in rows if r.get("completed_at") and r.get("status") == "completed"
        ]
        if not completed:
            return {
                "total": 0,
                "mean_minutes": None,
                "by_source": {},
                "critical_count": 0,
                "high_count": 0,
            }

        durations: list[float] = []
        by_source: dict[str, list[float]] = {}
        critical_count = 0
        high_count = 0

        for r in completed:
            try:
                t0 = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
                t1 = datetime.fromisoformat(r["completed_at"].replace("Z", "+00:00"))
                mins = (t1 - t0).total_seconds() / 60
                durations.append(mins)
                src = r.get("source") or "unknown"
                by_source.setdefault(src, []).append(mins)
                analysis_raw = r.get("analysis_json") or ""
                if (
                    '"severity":"critical"' in analysis_raw
                    or '"severity": "critical"' in analysis_raw
                ):
                    critical_count += 1
                elif (
                    '"severity":"high"' in analysis_raw
                    or '"severity": "high"' in analysis_raw
                ):
                    high_count += 1
            except Exception:  # noqa: BLE001
                continue

        mean = round(sum(durations) / len(durations), 1) if durations else None
        by_source_mean = {
            src: round(sum(vs) / len(vs), 1) for src, vs in by_source.items()
        }
        return {
            "total": len(completed),
            "mean_minutes": mean,
            "by_source": by_source_mean,
            "critical_count": critical_count,
            "high_count": high_count,
        }
    finally:
        db.close()


# ── Remediation action tracking ───────────────────────────────────────────────


@app.get("/api/jobs/{job_id}/actions")
def list_actions(
    job_id: str, user: AuthContext = Depends(require_auth)
) -> list[dict[str, Any]]:
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        return db.list_remediation_actions(job_id)
    finally:
        db.close()


@app.patch("/api/jobs/{job_id}/actions/{action_id}")
def patch_action(
    job_id: str,
    action_id: str,
    body: ActionUpdate,
    user: AuthContext = Depends(require_auth),
) -> dict[str, Any]:
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        valid_statuses = {"pending", "in_progress", "done", "skipped"}
        if body.status and body.status not in valid_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"status must be one of {sorted(valid_statuses)}",
            )
        ok = db.update_remediation_action(
            action_id,
            status=body.status,
            assigned_to=body.assigned_to,
            notes=body.notes,
            severity=body.severity,
            due_date=body.due_date,
        )
        if not ok:
            raise HTTPException(status_code=404, detail="Action not found")
        return {"action_id": action_id, "updated": True}
    finally:
        db.close()


# ── Action chat ───────────────────────────────────────────────────────────────

_ACTION_CHAT_SYSTEM = """\
You are Sentinel's remediation assistant — a senior SRE helping an on-call engineer \
resolve a live incident.

Rules you must follow in every response:
- Be concise. Short, direct sentences. No filler or preamble.
- Do NOT include code blocks or raw commands. Describe actions in plain language.
- You MAY recommend specific tools, packages, or modules by name (e.g. "use kubectl's \
  rollout undo", "the boto3 EC2 client", "PagerDuty's incident API") but always as \
  prose, never as a code snippet.
- Give numbered steps only when the answer is a sequence; otherwise answer directly.
- If you need one critical piece of missing context, ask it — but never ask more than \
  one question at a time.
- Never say "it depends" without immediately following with a concrete default.
"""


@app.get("/api/jobs/{job_id}/actions/{action_id}/chat")
def get_action_chat_history(
    job_id: str,
    action_id: str,
    user: AuthContext = Depends(require_auth),
) -> list[dict[str, Any]]:
    """Return the saved chat history for a specific remediation action."""
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        msgs = db.list_chat_messages(job_id, action_id)
        return [
            {"role": m["role"], "content": m["content"], "created_at": m["created_at"]}
            for m in msgs
        ]
    finally:
        db.close()


@app.post("/api/jobs/{job_id}/actions/{action_id}/chat")
def stream_action_chat(
    job_id: str,
    action_id: str,
    body: ActionChatRequest,
    user: AuthContext = Depends(require_auth),
) -> StreamingResponse:
    """Stream an LLM conversation scoped to a single remediation action and persist both turns."""
    from common.bedrock import converse_stream_chat
    from common.config import model_root_cause

    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")

        actions = db.list_remediation_actions(job_id)
        action = next((a for a in actions if str(a["id"]) == str(action_id)), None)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found")

        view = _job_view(row)
        analysis = view.get("analysis") or {}
        summary_text = (
            analysis.get("summary", {}).get("summary", "")
            if analysis.get("summary")
            else ""
        )
        root_cause_text = (
            analysis.get("root_cause", {}).get("likely_root_cause", "")
            if analysis.get("root_cause")
            else ""
        )
        action_text = action.get("action_text", "")

        context_block = (
            f"INCIDENT SUMMARY: {summary_text}\n"
            f"ROOT CAUSE: {root_cause_text}\n"
            f"TASK THE ENGINEER IS WORKING ON: {action_text}"
        )
        system_prompt = f"{_ACTION_CHAT_SYSTEM}\n\n{context_block}"

        messages = [{"role": m.role, "content": m.content} for m in body.history]
        messages.append({"role": "user", "content": body.message})

        # Persist user message
        db.save_chat_message(job_id, action_id, "user", body.message)

    finally:
        db.close()

    def gen() -> Any:
        parts: list[str] = []
        try:
            for chunk in converse_stream_chat(
                model_root_cause(), system_prompt, messages
            ):
                parts.append(chunk)
                yield f"data: {json.dumps({'chunk': chunk})}\n\n"
        finally:
            # Persist whatever was accumulated, even on early client disconnect
            full_response = "".join(parts)
            if full_response:
                try:
                    persist_db = _db()
                    persist_db.save_chat_message(
                        job_id, action_id, "assistant", full_response
                    )
                    persist_db.close()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Failed to persist assistant chat message: %s", exc)
        yield f"data: {json.dumps({'done': True})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


# ── Follow-up reminders ───────────────────────────────────────────────────────


@app.get("/api/jobs/{job_id}/follow-ups")
def list_follow_ups(
    job_id: str, user: AuthContext = Depends(require_auth)
) -> list[dict[str, Any]]:
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        return db.list_follow_ups(job_id)
    finally:
        db.close()


@app.post("/api/jobs/{job_id}/follow-ups", status_code=201)
def create_follow_up(
    job_id: str,
    body: FollowUpCreate,
    user: AuthContext = Depends(require_auth),
) -> dict[str, Any]:
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        fu_id = db.create_follow_up(
            job_id=job_id,
            clerk_user_id=user.user_id,
            user_email=body.user_email,
            remind_at=body.remind_at,
            action_id=body.action_id,
            user_name=body.user_name,
            message=body.message,
        )

        # Trigger the background scheduler
        ReminderScheduler.get_instance().ensure_running()

        return {"follow_up_id": fu_id, "created": True}
    finally:
        db.close()


@app.delete("/api/jobs/{job_id}/follow-ups/{follow_up_id}", status_code=204)
def delete_follow_up(
    job_id: str,
    follow_up_id: str,
    user: AuthContext = Depends(require_auth),
) -> None:
    db = _db()
    try:
        ok = db.delete_follow_up(follow_up_id, user.user_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Follow-up not found")
    finally:
        db.close()


@app.post("/api/follow-ups/send-pending")
def send_pending_follow_ups(
    user: AuthContext = Depends(require_auth),
) -> dict[str, Any]:
    """Process and send all follow-up reminders that are due now.

    Intended to be called by a cron job or a scheduled task.
    """
    scheduler = ReminderScheduler.get_instance()
    sent, failed = scheduler.process_all_pending()
    return {"sent": sent, "failed": failed}


# ── Clarification Q&A ─────────────────────────────────────────────────────────


@app.get("/api/jobs/{job_id}/clarification-questions")
def get_clarification_questions(
    job_id: str, user: AuthContext = Depends(require_auth)
) -> dict[str, Any]:
    """Return targeted clarification questions for a completed job.

    Questions are generated from the stored root-cause analysis and are
    cached as already-answered once the user submits answers via POST /clarify.
    """
    from common.models import RootCauseAnalysis
    from remediator.agent import build_clarification_set

    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        view = _job_view(row)
        analysis = view.get("analysis")
        if not analysis:
            raise HTTPException(
                status_code=422, detail="Job has no completed analysis yet"
            )

        rc_data = analysis.get("root_cause")
        if not rc_data:
            raise HTTPException(status_code=422, detail="No root cause data available")

        root_cause = RootCauseAnalysis.model_validate(rc_data)
        evidence = analysis.get("summary", {}) and []
        try:
            evidence = analysis["remediation"].get("recommended_actions", [])
        except Exception:  # noqa: BLE001
            evidence = []

        already_answered = db.get_clarification_answers(job_id) is not None
        clarification_set = build_clarification_set(
            job_id=job_id,
            root_cause=root_cause,
            evidence=evidence,
            already_answered=already_answered,
        )
        return clarification_set.model_dump()
    finally:
        db.close()


@app.post("/api/jobs/{job_id}/clarify")
def submit_clarifications(
    job_id: str,
    body: ClarificationAnswers,
    user: AuthContext = Depends(require_auth),
) -> dict[str, Any]:
    """Accept operator answers and return a refined RemediationPlan.

    The refined plan is persisted back into analysis_json and the remediation
    actions table is cleared and re-seeded with the new actions.
    """
    from common.models import IncidentSummary, NormalizedIncident, RootCauseAnalysis
    from remediator.agent import generate_remediation

    db = _db()
    try:
        row = db.get_job_with_incident(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        view = _job_view(row)
        analysis = view.get("analysis")
        if not analysis:
            raise HTTPException(
                status_code=422, detail="Job has no completed analysis yet"
            )

        root_cause = RootCauseAnalysis.model_validate(analysis["root_cause"])
        summary = IncidentSummary.model_validate(analysis["summary"])
        sanitized_text = row.get("sanitized_text") or row.get("raw_text") or ""
        evidence = analysis.get("remediation", {}).get("recommended_actions", [])
        normalized = NormalizedIncident(
            normalized_text=sanitized_text,
            evidence_snippets=evidence,
            guardrails=GuardrailReport(),
        )

        refined = generate_remediation(
            normalized, summary, root_cause, clarifications=body.answers
        )

        db.save_clarification_answers(job_id, body.answers)
        db.delete_remediation_actions(job_id)
        if refined.recommended_actions:
            db.seed_remediation_actions(
                job_id, refined.recommended_actions, action_type="recommended"
            )
        if refined.next_checks:
            db.seed_remediation_actions(
                job_id, refined.next_checks, action_type="check"
            )
        db.update_analysis_remediation(job_id, refined.model_dump_json())

        return {"refined": True, "remediation": refined.model_dump()}
    finally:
        db.close()


# ── Integrations hub ──────────────────────────────────────────────────────────


@app.get("/api/integrations")
def list_integrations(
    user: AuthContext = Depends(require_auth),
) -> list[dict[str, Any]]:
    db = _db()
    try:
        return db.list_integrations(user.user_id)
    finally:
        db.close()


@app.post("/api/integrations", status_code=201)
def create_integration(
    body: IntegrationCreate,
    user: AuthContext = Depends(require_auth),
) -> dict[str, Any]:
    valid_types = {"slack", "jira", "pagerduty", "opsgenie", "generic_webhook"}
    if body.type not in valid_types:
        raise HTTPException(
            status_code=400, detail=f"type must be one of {sorted(valid_types)}"
        )
    db = _db()
    try:
        int_id = db.create_integration(
            user.user_id, body.type, body.config, body.enabled
        )
        return {"integration_id": int_id, "type": body.type, "enabled": body.enabled}
    finally:
        db.close()


@app.delete("/api/integrations/{integration_id}", status_code=204)
def delete_integration(
    integration_id: str, user: AuthContext = Depends(require_auth)
) -> None:
    db = _db()
    try:
        ok = db.delete_integration(integration_id, user.user_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Integration not found")
    finally:
        db.close()


# ── Webhook ingestion ─────────────────────────────────────────────────────────


class _WebhookKey(_Base):
    pass


def _ingest_webhook_payload(
    payload: dict, source_label: str, user_id: str = "webhook"
) -> dict[str, Any]:
    """Map a raw alert payload to IncidentInput and trigger pipeline."""
    title = (
        payload.get("commonAnnotations", {}).get("summary")
        or payload.get("AlarmName")
        or payload.get("title")
        or "Alert from webhook"
    )
    body_parts: list[str] = []
    if payload.get("commonAnnotations"):
        body_parts.append(f"Annotations: {json.dumps(payload['commonAnnotations'])}")
    if payload.get("alerts"):
        for alert in payload["alerts"][:10]:
            body_parts.append(
                f"[{alert.get('status', 'firing')}] {alert.get('labels', {})} — {alert.get('annotations', {})}"
            )
    if payload.get("NewStateReason"):
        body_parts.append(payload["NewStateReason"])
    if payload.get("description"):
        body_parts.append(payload["description"])
    if not body_parts:
        body_parts.append(json.dumps(payload)[:4000])
    text = "\n".join(body_parts) or "No details provided."
    incident_input = IncidentInput(
        text=text, title=str(title)[:200], source=source_label
    )
    db = _db()
    try:
        incident_id, job_id = create_incident_and_job(
            incident_input, db, clerk_user_id=user_id
        )
        import threading

        threading.Thread(
            target=run_job, args=(job_id, None, user_id), daemon=True
        ).start()
        return {"incident_id": incident_id, "job_id": job_id, "status": "pending"}
    finally:
        db.close()


@app.post("/api/ingest/webhook/alertmanager", status_code=202)
def ingest_alertmanager(payload: dict[str, Any]) -> dict[str, Any]:
    """Accept Prometheus Alertmanager webhook payloads."""
    return _ingest_webhook_payload(
        payload, source_label="alertmanager", user_id="webhook_alertmanager"
    )


@app.post("/api/ingest/webhook/cloudwatch", status_code=202)
def ingest_cloudwatch(payload: dict[str, Any]) -> dict[str, Any]:
    """Accept CloudWatch Alarm SNS JSON payloads."""
    return _ingest_webhook_payload(
        payload, source_label="cloudwatch", user_id="webhook_cloudwatch"
    )


@app.post("/api/ingest/webhook", status_code=202)
def ingest_generic_webhook(payload: dict[str, Any]) -> dict[str, Any]:
    """Generic webhook — any JSON alert payload."""
    return _ingest_webhook_payload(
        payload, source_label="webhook", user_id="webhook_generic"
    )


# ── Action findings evaluation ────────────────────────────────────────────────


@app.post("/api/jobs/{job_id}/actions/{action_id}/evaluate", status_code=201)
def evaluate_action_findings(
    job_id: str,
    action_id: str,
    body: ActionEvaluationRequest,
    user: AuthContext = Depends(require_auth),
) -> dict[str, Any]:
    """Evaluate engineer findings against a remediation action.

    - Persists findings to the action's notes field.
    - If the LLM is satisfied: marks the action done.
    - If not satisfied: creates a child trail action with the LLM's next_step text.
    Returns the evaluation result plus the child action id when created.
    """
    from common.models import IncidentAnalysis
    from remediator.agent import evaluate_findings

    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")

        action = db.get_action(action_id)
        if not action or action.get("job_id") != job_id:
            raise HTTPException(status_code=404, detail="Action not found")

        view = _job_view(row)
        analysis_data = view.get("analysis")
        if not analysis_data:
            raise HTTPException(status_code=422, detail="Job has no completed analysis")

        analysis = IncidentAnalysis.model_validate(analysis_data)

        # Persist the findings as the action's notes
        db.update_remediation_action(action_id, notes=body.findings)

        verdict = evaluate_findings(action["action_text"], analysis, body.findings)

        # Persist the LLM response on the action
        db.save_action_eval_response(action_id, verdict.response)

        child_action_id: str | None = None
        resolved_parent_ids: list[str] = []

        if verdict.satisfied:
            db.update_remediation_action(action_id, status="done")
            # Walk up the ancestor chain and mark every parent done too
            cursor_id = action.get("parent_action_id")
            while cursor_id:
                db.update_remediation_action(cursor_id, status="done")
                resolved_parent_ids.append(cursor_id)
                parent_row = db.get_action(cursor_id)
                cursor_id = parent_row.get("parent_action_id") if parent_row else None
        elif verdict.next_step:
            child_action_id = db.seed_trail_action(
                job_id=job_id,
                action_text=verdict.next_step,
                severity=action.get("severity", "medium"),
                action_type=action.get("action_type", "recommended"),
                parent_action_id=action_id,
            )

        return {
            "satisfied": verdict.satisfied,
            "response": verdict.response,
            "next_step": verdict.next_step,
            "child_action_id": child_action_id,
            "action_id": action_id,
            "resolved_parent_ids": resolved_parent_ids,
        }
    finally:
        db.close()


# ── Remediation follow-up ─────────────────────────────────────────────────────


@app.post("/api/jobs/{job_id}/remediation-followup", status_code=201)
def remediation_followup(
    job_id: str,
    body: RemediationFollowUpRequest,
    user: AuthContext = Depends(require_auth),
) -> dict[str, Any]:
    """Accept engineer findings from active remediation and generate follow-up actions.

    The additional_context field captures what the engineer discovered while working
    through the initial plan (e.g. a rollback uncovered a deeper config issue).
    Follow-up actions are appended to the existing action list — the original plan
    is never replaced.
    """
    from common.models import IncidentAnalysis
    from remediator.agent import generate_followup_actions

    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        if row.get("status") != "completed":
            raise HTTPException(
                status_code=422, detail="Follow-up requires a completed analysis"
            )

        view = _job_view(row)
        analysis_data = view.get("analysis")
        if not analysis_data:
            raise HTTPException(status_code=422, detail="Job has no completed analysis")

        analysis = IncidentAnalysis.model_validate(analysis_data)
        current_actions = db.list_remediation_actions(job_id)

        followup = generate_followup_actions(
            analysis,
            current_actions,
            body.additional_context,
            anchor_action_id=body.anchor_action_id,
        )

        incident_severity = analysis.summary.severity
        _lower = {"critical": "high", "high": "medium", "medium": "low", "low": "low"}
        fallback_check_sev = _lower.get(incident_severity, "medium")
        valid_severities = {"critical", "high", "medium", "low"}

        seeded: list[str] = []

        anchor = body.anchor_action_id
        submission = body.additional_context

        if followup.followup_actions:
            sevs = list(followup.followup_severities)
            while len(sevs) < len(followup.followup_actions):
                sevs.append(incident_severity)
            for text, sev in zip(followup.followup_actions, sevs):
                sev = sev if sev in valid_severities else incident_severity
                db.seed_remediation_actions(
                    job_id,
                    [text],
                    action_type="followup",
                    severity=sev,
                    engineer_submission=submission,
                    source_anchor_action_id=anchor,
                )
                seeded.append(text)

        if followup.followup_checks:
            sevs = list(followup.check_severities)
            while len(sevs) < len(followup.followup_checks):
                sevs.append(fallback_check_sev)
            for text, sev in zip(followup.followup_checks, sevs):
                sev = sev if sev in valid_severities else fallback_check_sev
                db.seed_remediation_actions(
                    job_id,
                    [text],
                    action_type="followup_check",
                    severity=sev,
                    engineer_submission=submission,
                    source_anchor_action_id=anchor,
                )
                seeded.append(text)

        return {
            "generated": True,
            "new_actions_count": len(seeded),
            "followup": followup.model_dump(),
        }
    finally:
        db.close()


# ── Incident resolution ───────────────────────────────────────────────────────


@app.patch("/api/incidents/{incident_id}/status")
def update_incident_status_endpoint(
    incident_id: str,
    body: IncidentResolveRequest,
    user: AuthContext = Depends(require_auth),
) -> dict[str, Any]:
    """Mark an incident open / in_progress / resolved with optional notes."""
    valid_statuses = {"open", "in_progress", "resolved"}
    if body.status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"status must be one of {sorted(valid_statuses)}",
        )
    db = _db()
    try:
        ok = db.update_incident_resolution(
            incident_id,
            status=body.status,
            resolution_notes=body.resolution_notes,
            clerk_user_id=user.user_id,
        )
        if not ok:
            raise HTTPException(status_code=404, detail="Incident not found")
        return {"incident_id": incident_id, "status": body.status, "updated": True}
    finally:
        db.close()


# ── Post-Incident Review ───────────────────────────────────────────────────────


@app.get("/api/jobs/{job_id}/pir")
def get_pir(job_id: str, user: AuthContext = Depends(require_auth)) -> dict[str, Any]:
    """Return a previously generated post-incident review, or 404 if none exists."""
    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        pir = db.get_pir(job_id)
        if not pir:
            raise HTTPException(status_code=404, detail="No PIR generated yet")
        return pir
    finally:
        db.close()


@app.post("/api/jobs/{job_id}/pir", status_code=201)
def generate_pir_endpoint(
    job_id: str,
    user: AuthContext = Depends(require_auth),
) -> dict[str, Any]:
    """Generate and persist a post-incident review for a completed job.

    Re-generates a fresh PIR if one already exists (allows regeneration after
    more actions are marked done).
    """
    from common.models import IncidentAnalysis
    from remediator.agent import generate_pir

    db = _db()
    try:
        row = db.get_job(job_id, clerk_user_id=user.user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        if row.get("status") != "completed":
            raise HTTPException(
                status_code=422, detail="PIR can only be generated for completed jobs"
            )
        view = _job_view(row)
        analysis_data = view.get("analysis")
        if not analysis_data:
            raise HTTPException(status_code=422, detail="Job has no completed analysis")

        analysis = IncidentAnalysis.model_validate(analysis_data)
        actions = db.list_remediation_actions(job_id)

        pir = generate_pir(analysis, actions)
        db.save_pir(job_id, pir.model_dump_json())
        return pir.model_dump()
    finally:
        db.close()


# ── Digest reports ────────────────────────────────────────────────────────────


@app.post("/api/reports/digest")
def generate_digest(
    body: DigestRequest, user: AuthContext = Depends(require_auth)
) -> dict[str, Any]:
    """Generate an on-demand incident digest for the last N days."""
    from reports.digest import build_digest

    db = _db()
    try:
        return build_digest(db, user.user_id, days=body.days)
    finally:
        db.close()


@app.get("/api/reports/digest/export")
def export_digest(days: int = 7, user: AuthContext = Depends(require_auth)) -> Any:
    """Export digest as PDF."""
    from reports.digest import build_digest, render_digest_pdf

    db = _db()
    try:
        digest = build_digest(db, user.user_id, days=days)
        pdf = render_digest_pdf(digest)
        name = f"sentinel-digest-{days}d.pdf"
        return Response(
            content=bytes(pdf),
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )
    finally:
        db.close()
