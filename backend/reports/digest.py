"""Incident digest report generation."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from fpdf import FPDF


def _safe(s: str | None, max_len: int = 8000) -> str:
    if not s:
        return ""
    return str(s)[:max_len].encode("latin-1", "replace").decode("latin-1")


def build_digest(db: Any, clerk_user_id: str, days: int = 7) -> dict[str, Any]:
    """Aggregate incidents and jobs from the last N days into a digest dict."""
    from collections import defaultdict

    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=days)).isoformat()

    jobs = db.list_jobs(limit=500, clerk_user_id=clerk_user_id)
    incidents_raw = db.list_incidents(limit=500, clerk_user_id=clerk_user_id)

    recent_jobs = [j for j in jobs if (j.get("created_at") or "") >= since]
    recent_incidents = [i for i in incidents_raw if (i.get("created_at") or "") >= since]

    severity_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    durations: list[float] = []
    total_completed = 0
    total_failed = 0

    daily: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "completed": 0, "failed": 0})

    for j in recent_jobs:
        status = j.get("status", "")
        if status == "completed":
            total_completed += 1
        elif status == "failed":
            total_failed += 1
        src = j.get("source") or "unknown"
        source_counts[src] += 1
        day = (j.get("created_at") or "")[:10]
        if day:
            daily[day]["total"] += 1
            if status == "completed":
                daily[day]["completed"] += 1
            elif status == "failed":
                daily[day]["failed"] += 1

    for j in recent_jobs:
        if j.get("status") == "completed" and j.get("completed_at") and j.get("created_at"):
            try:
                t0 = datetime.fromisoformat(j["created_at"].replace("Z", "+00:00"))
                t1 = datetime.fromisoformat(j["completed_at"].replace("Z", "+00:00"))
                durations.append((t1 - t0).total_seconds() / 60)
            except Exception:  # noqa: BLE001
                pass

    full_jobs = [j for j in recent_jobs if j.get("status") == "completed"]
    for j in full_jobs:
        raw = j.get("analysis_json") or ""
        if not raw:
            continue
        try:
            a = json.loads(raw)
            sev = (a.get("summary") or {}).get("severity", "unknown")
            severity_counts[sev] += 1
        except json.JSONDecodeError:
            pass

    # Build a continuous day-by-day series, filling zeros for empty days
    start_date = (now - timedelta(days=days)).date()
    daily_breakdown = []
    for i in range(days):
        d = (start_date + timedelta(days=i + 1)).isoformat()
        entry = daily.get(d, {"total": 0, "completed": 0, "failed": 0})
        daily_breakdown.append({"date": d, **entry})

    mean_mttr = round(sum(durations) / len(durations), 1) if durations else None

    return {
        "period_days": days,
        "generated_at": now.isoformat(),
        "total_incidents": len(recent_incidents),
        "total_jobs": len(recent_jobs),
        "completed": total_completed,
        "failed": total_failed,
        "mean_mttr_minutes": mean_mttr,
        "severity_breakdown": dict(severity_counts),
        "source_breakdown": dict(source_counts),
        "daily_breakdown": daily_breakdown,
    }


def render_digest_pdf(digest: dict[str, Any]) -> bytes:
    """Render a digest dict to PDF bytes."""
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.add_page()
    w = float(pdf.w - pdf.l_margin - pdf.r_margin)

    def cell(text: str, bold: bool = False, size: int = 10) -> None:
        pdf.set_font("Helvetica", "B" if bold else "", size)
        pdf.set_x(pdf.l_margin)
        pdf.multi_cell(w, 5, _safe(text))

    cell("Sentinel Incident Digest", bold=True, size=16)
    cell(f"Period: last {digest['period_days']} days   Generated: {digest['generated_at']}", size=8)
    pdf.ln(4)

    cell("Summary", bold=True, size=13)
    cell(
        f"Total incidents: {digest['total_incidents']}   "
        f"Jobs run: {digest['total_jobs']}   "
        f"Completed: {digest['completed']}   Failed: {digest['failed']}"
    )
    mttr = digest.get("mean_mttr_minutes")
    cell(f"Average MTTR: {f'{mttr} min' if mttr is not None else 'N/A'}")
    pdf.ln(3)

    sev = digest.get("severity_breakdown") or {}
    if sev:
        cell("Severity Breakdown", bold=True, size=12)
        for s, count in sorted(sev.items()):
            cell(f"  {s}: {count}")
        pdf.ln(2)

    src = digest.get("source_breakdown") or {}
    if src:
        cell("Source Breakdown", bold=True, size=12)
        for s, count in sorted(src.items()):
            cell(f"  {s}: {count}")
        pdf.ln(2)

    patterns = digest.get("top_recurring_patterns") or []
    if patterns:
        cell("Top Recurring Patterns", bold=True, size=12)
        for i, p in enumerate(patterns[:5], 1):
            cell(f"  {i}. [{p['incident_hits']} incidents] {p['pattern']}", size=8)
        pdf.ln(2)

    recent = digest.get("recent_incidents") or []
    if recent:
        cell("Recent Incidents", bold=True, size=12)
        for inc in recent:
            sev_label = f"[{inc['severity'].upper()}] " if inc.get("severity") else ""
            cell(f"  {sev_label}{inc['title']} ({inc.get('source', '')})")
            if inc.get("summary"):
                cell(f"    {inc['summary'][:120]}", size=8)
        pdf.ln(2)

    return bytes(pdf.output())
