"""Generate a rich PDF export for an incident job.

Sections:
1. Incident Summary
2. Root Cause Analysis
3. Log Data Overview  (bar-chart visualisations)
4. Remediation TODO   (live status / severity / due-date from remediation_actions table)
5. Immediate Checks
6. Guardrails
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fpdf import FPDF


# ── Colour palette

_SEVERITY_RGB: dict[str, tuple[int, int, int]] = {
    "critical": (220, 38, 38),
    "high": (234, 88, 12),
    "medium": (202, 138, 4),
    "low": (22, 163, 74),
}
_STATUS_RGB: dict[str, tuple[int, int, int]] = {
    "done": (22, 163, 74),
    "in_progress": (234, 138, 4),
    "pending": (148, 163, 184),
    "skipped": (148, 163, 184),
}
_STATUS_LABEL: dict[str, str] = {
    "done": "DONE",
    "in_progress": "IN PROG",
    "pending": "PENDING",
    "skipped": "SKIPPED",
}
_LEVEL_RGB: dict[str, tuple[int, int, int]] = {
    "error": (244, 63, 94),
    "warn": (245, 158, 11),
    "info": (56, 189, 248),
    "debug": (167, 139, 250),
    "other": (100, 116, 139),
}
_HTTP_RGB: dict[str, tuple[int, int, int]] = {
    "2xx": (34, 197, 94),
    "3xx": (56, 189, 248),
    "4xx": (245, 158, 11),
    "5xx": (239, 68, 68),
    "other": (148, 163, 184),
}
_SORDER: dict[str, int] = {"critical": 0, "high": 1, "medium": 2, "low": 3}


def _safe(s: str | None, max_len: int = 12000) -> str:
    if not s:
        return ""
    t = str(s)[:max_len]
    return t.encode("latin-1", "replace").decode("latin-1")


def _epw(pdf: FPDF) -> float:
    return float(pdf.w - pdf.l_margin - pdf.r_margin)


def _multi_cell(pdf: FPDF, text: str, h: float = 4) -> None:
    s = _safe(text)
    if not s.strip():
        return
    pdf.set_x(pdf.l_margin)
    pdf.multi_cell(_epw(pdf), h, s)


def _check_page_break(pdf: FPDF, needed: float = 20) -> None:
    if pdf.get_y() > pdf.h - pdf.b_margin - needed:
        pdf.add_page()


# ── Section / layout helpers


def _section_header(pdf: FPDF, title: str) -> None:
    _check_page_break(pdf, 15)
    pdf.set_fill_color(30, 30, 55)
    pdf.set_text_color(230, 230, 250)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_x(pdf.l_margin)
    pdf.cell(_epw(pdf), 7, _safe(f"  {title}"), ln=True, fill=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(2)


def _subsection(pdf: FPDF, title: str) -> None:
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(50, 50, 80)
    pdf.set_x(pdf.l_margin)
    pdf.cell(_epw(pdf), 5, _safe(title), ln=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(1)


def _draw_bar_row(
    pdf: FPDF,
    label: str,
    value: int,
    max_value: int,
    fill_rgb: tuple[int, int, int],
    label_w: float = 42,
    bar_max_w: float = 100,
    count_w: float = 32,
) -> None:
    """Labelled horizontal bar: [label text][grey track / coloured fill][count (pct%)]."""
    _check_page_break(pdf, 8)
    row_h = 5.5
    bar_h = 3.0
    y = pdf.get_y()
    x = float(pdf.l_margin)

    pdf.set_text_color(50, 50, 60)
    pdf.set_font("Helvetica", size=8)
    pdf.set_xy(x, y)
    pdf.cell(label_w, row_h, _safe(label), ln=False)

    bar_y = y + (row_h - bar_h) / 2
    pdf.set_fill_color(215, 215, 222)
    pdf.rect(x + label_w, bar_y, bar_max_w, bar_h, style="F")

    if max_value > 0 and value > 0:
        bar_w = (value / max_value) * bar_max_w
        r, g, b = fill_rgb
        pdf.set_fill_color(r, g, b)
        pdf.rect(x + label_w, bar_y, bar_w, bar_h, style="F")

    pct = f"{round(value / max_value * 100)}%" if max_value > 0 else "0%"
    pdf.set_text_color(80, 80, 90)
    pdf.set_font("Helvetica", size=8)
    pdf.set_xy(x + label_w + bar_max_w + 2, y)
    pdf.cell(count_w, row_h, _safe(f"{value:,}  ({pct})"), ln=True)


# ── Visualisation sections


def _render_log_charts(pdf: FPDF, stats: dict[str, Any]) -> None:
    if not stats or not stats.get("line_count"):
        return

    _section_header(pdf, "3. Log Data Overview")

    pdf.set_font("Helvetica", size=8)
    pdf.set_text_color(100, 100, 115)
    pdf.set_x(pdf.l_margin)
    pdf.cell(
        _epw(pdf),
        4.5,
        _safe(
            f"Lines: {stats.get('line_count', 0):,}   "
            f"Characters: {stats.get('char_count', 0):,}   "
            f"Timestamped lines: {stats.get('timestamped_points', 0):,}"
        ),
        ln=True,
    )
    pdf.ln(3)
    pdf.set_text_color(0, 0, 0)

    # Severity mix
    _subsection(pdf, "Severity mix (per log line)")
    levels = stats.get("levels") or {}
    max_lv = (
        max(
            (levels.get(k, 0) for k in ("error", "warn", "info", "debug", "other")),
            default=1,
        )
        or 1
    )
    for lv in ("error", "warn", "info", "debug", "other"):
        _draw_bar_row(
            pdf, lv, levels.get(lv, 0), max_lv, _LEVEL_RGB.get(lv, (148, 163, 184))
        )
    pdf.ln(3)

    # HTTP status classes
    http = stats.get("http_class") or {}
    if any(http.get(k, 0) for k in ("2xx", "3xx", "4xx", "5xx")):
        _subsection(pdf, "HTTP status classes")
        max_http = (
            max(
                (http.get(k, 0) for k in ("2xx", "3xx", "4xx", "5xx", "other")),
                default=1,
            )
            or 1
        )
        for cls in ("2xx", "3xx", "4xx", "5xx", "other"):
            if http.get(cls, 0):
                _draw_bar_row(
                    pdf,
                    cls,
                    http.get(cls, 0),
                    max_http,
                    _HTTP_RGB.get(cls, (148, 163, 184)),
                )
        pdf.ln(3)

    # Reliability signals
    signals = stats.get("signal_keywords") or {}
    if signals:
        _subsection(pdf, "Reliability signals")
        for name, val in signals.items():
            _check_page_break(pdf, 8)
            detected = bool(val)
            r, g, b = (22, 163, 74) if detected else (148, 163, 184)
            y = pdf.get_y()
            x = float(pdf.l_margin)
            pdf.set_fill_color(r, g, b)
            pdf.rect(x, y + 1.5, 4, 2.5, style="F")
            style = "B" if detected else ""
            pdf.set_font("Helvetica", style, 8)
            pdf.set_text_color(r, g, b)
            pdf.set_xy(x + 6, y)
            pdf.cell(36, 5, _safe(name), ln=False)
            pdf.set_font("Helvetica", size=8)
            pdf.set_text_color(80, 80, 90)
            pdf.cell(20, 5, "YES" if detected else "no", ln=True)
        pdf.set_text_color(0, 0, 0)
        pdf.ln(3)

    # Error / warning timeline (bucket mini-sparkline)
    buckets = stats.get("buckets") or []
    if len(buckets) > 1:
        _subsection(pdf, "Error / warning distribution across log")
        max_val = (
            max((max(b.get("error", 0), b.get("warn", 0)) for b in buckets), default=1)
            or 1
        )
        bar_max_w = 112.0
        col_h = 4.5
        bar_h = 2.0
        for i, bk in enumerate(buckets[:12]):
            _check_page_break(pdf, 7)
            y = pdf.get_y()
            x = float(pdf.l_margin)
            lbl = _safe(str(bk.get("label", f"B{i + 1}"))[:12])
            pdf.set_font("Helvetica", size=6)
            pdf.set_text_color(100, 100, 110)
            pdf.set_xy(x, y)
            pdf.cell(28, col_h, lbl, ln=False)
            # grey track
            pdf.set_fill_color(215, 215, 222)
            pdf.rect(x + 28, y + (col_h - bar_h) / 2, bar_max_w, bar_h, style="F")
            # error fill
            err_w = min((bk.get("error", 0) / max_val) * bar_max_w, bar_max_w)
            if err_w > 0:
                pdf.set_fill_color(244, 63, 94)
                pdf.rect(x + 28, y + (col_h - bar_h) / 2, err_w, bar_h, style="F")
            # warn fill (slightly narrower, on top)
            wrn_w = min((bk.get("warn", 0) / max_val) * bar_max_w, bar_max_w)
            if wrn_w > 0:
                mid_y = y + (col_h - bar_h) / 2
                pdf.set_fill_color(245, 158, 11)
                pdf.rect(x + 28, mid_y, wrn_w, bar_h * 0.65, style="F")
            pdf.set_text_color(80, 80, 90)
            pdf.set_font("Helvetica", size=6)
            pdf.set_xy(x + 28 + bar_max_w + 2, y)
            pdf.cell(
                28,
                col_h,
                _safe(f"E:{bk.get('error', 0)} W:{bk.get('warn', 0)}"),
                ln=True,
            )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(3)


# ── Remediation action table


def _render_actions(
    pdf: FPDF,
    actions: list[dict[str, Any]],
    section_title: str,
    action_type: str,
) -> None:
    filtered = [a for a in actions if a.get("action_type") == action_type]
    if not filtered:
        return

    filtered.sort(
        key=lambda a: (
            _SORDER.get(a.get("severity", "medium"), 2),
            a.get("action_text", ""),
        )
    )

    done_count = sum(1 for a in filtered if a.get("status") == "done")
    total = len(filtered)
    pct = round(done_count / total * 100) if total else 0

    _section_header(pdf, f"{section_title}  ({done_count}/{total} completed, {pct}%)")

    # Progress bar
    epw = _epw(pdf)
    x = float(pdf.l_margin)
    y = pdf.get_y()
    bar_h = 4.0
    pdf.set_fill_color(215, 215, 222)
    pdf.rect(x, y, epw, bar_h, style="F")
    if pct > 0:
        fill_w = (pct / 100) * epw
        r, g, b = (22, 163, 74) if pct == 100 else (99, 102, 241)
        pdf.set_fill_color(r, g, b)
        pdf.rect(x, y, fill_w, bar_h, style="F")
    pdf.ln(bar_h + 3)

    # Column widths
    status_w = 28.0
    sev_w = 22.0
    due_w = 30.0
    text_w = epw - status_w - sev_w - due_w

    # Column headers
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_text_color(110, 110, 125)
    pdf.set_x(x)
    pdf.cell(status_w, 4.5, "STATUS", ln=False)
    pdf.cell(sev_w, 4.5, "SEVERITY", ln=False)
    pdf.cell(text_w, 4.5, "ACTION", ln=False)
    pdf.cell(due_w, 4.5, "DUE DATE", ln=True)
    pdf.set_draw_color(210, 210, 218)
    pdf.line(x, pdf.get_y(), x + epw, pdf.get_y())
    pdf.ln(2)
    pdf.set_text_color(0, 0, 0)

    for action in filtered:
        _check_page_break(pdf, 14)
        row_y = pdf.get_y()

        status = action.get("status", "pending")
        sev = action.get("severity", "medium")
        text = action.get("action_text", "")
        due_raw = action.get("due_date") or ""
        notes = action.get("notes") or ""

        st_rgb = _STATUS_RGB.get(status, (148, 163, 184))
        st_label = _STATUS_LABEL.get(status, status.upper()[:7])
        sev_rgb = _SEVERITY_RGB.get(sev, (202, 138, 4))

        # Status badge
        r, g, b = st_rgb
        pdf.set_fill_color(min(r + 175, 255), min(g + 175, 255), min(b + 175, 255))
        pdf.rect(x, row_y + 0.5, status_w - 3, 5.0, style="F")
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(r, g, b)
        pdf.set_xy(x, row_y + 0.5)
        pdf.cell(status_w - 3, 5.0, _safe(st_label), align="C", ln=False)

        # Severity badge
        r2, g2, b2 = sev_rgb
        pdf.set_fill_color(min(r2 + 175, 255), min(g2 + 175, 255), min(b2 + 175, 255))
        pdf.rect(x + status_w, row_y + 0.5, sev_w - 2, 5.0, style="F")
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(r2, g2, b2)
        pdf.set_xy(x + status_w, row_y + 0.5)
        pdf.cell(sev_w - 2, 5.0, _safe(sev.upper()), align="C", ln=False)

        # Action text (truncated to fit one line)
        truncated = text[:88] + ("..." if len(text) > 88 else "")
        pdf.set_font("Helvetica", size=8)
        pdf.set_text_color(25, 25, 40)
        pdf.set_xy(x + status_w + sev_w, row_y)
        pdf.cell(text_w, 6.0, _safe(truncated), ln=False)

        # Due date
        due_str = "—"
        is_overdue = False
        if due_raw:
            try:
                dt = datetime.fromisoformat(due_raw.replace("Z", "+00:00"))
                due_str = dt.strftime("%Y-%m-%d")
                is_overdue = dt < datetime.now(timezone.utc) and status not in (
                    "done",
                    "skipped",
                )
            except Exception:
                due_str = due_raw[:10]

        pdf.set_font("Helvetica", "B" if is_overdue else "", 8)
        if is_overdue:
            pdf.set_text_color(220, 38, 38)
        else:
            pdf.set_text_color(80, 80, 95)
        pdf.set_xy(x + status_w + sev_w + text_w, row_y)
        pdf.cell(due_w, 6.0, _safe(due_str + (" !" if is_overdue else "")), ln=True)
        pdf.set_text_color(0, 0, 0)

        # Inline notes
        if notes:
            pdf.set_x(x + status_w + sev_w)
            pdf.set_font("Helvetica", "I", 7)
            pdf.set_text_color(105, 105, 115)
            note_str = "Note: " + notes[:110] + ("..." if len(notes) > 110 else "")
            pdf.cell(text_w + due_w, 4.0, _safe(note_str), ln=True)
            pdf.set_text_color(0, 0, 0)

        # Row separator
        pdf.set_draw_color(230, 230, 235)
        pdf.line(x, pdf.get_y(), x + epw, pdf.get_y())
        pdf.ln(1.5)

    pdf.ln(3)


def render_job_pdf(job_view: dict[str, Any]) -> bytes:
    """Build PDF bytes from the enriched job view.

    Expects the shape of GET /api/jobs/{id} plus an optional
    ``remediation_actions`` key containing rows from the
    ``remediation_actions`` table (with ``status``, ``severity``,
    ``due_date``, ``notes``, etc.).
    """

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.add_page()

    analysis = job_view.get("analysis") or {}
    summary_data = analysis.get("summary") or {}
    root_cause_data = analysis.get("root_cause") or {}
    rem = analysis.get("remediation") or {}
    guardrails = analysis.get("guardrails") or {}
    stats = job_view.get("log_stats") or {}
    actions = job_view.get("remediation_actions") or []

    severity = summary_data.get("severity", "")
    sev_rgb = _SEVERITY_RGB.get(severity, (80, 80, 100))
    gen_at = analysis.get("generated_at") or ""

    pdf.set_fill_color(25, 25, 50)
    pdf.rect(0, 0, pdf.w, 20, style="F")
    pdf.set_font("Helvetica", "B", 15)
    pdf.set_text_color(230, 230, 255)
    pdf.set_xy(pdf.l_margin, 5)
    pdf.cell(_epw(pdf), 10, "Sentinel Incident Report", ln=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(8)

    if gen_at:
        pdf.set_font("Helvetica", size=8)
        pdf.set_text_color(105, 105, 120)
        pdf.set_x(pdf.l_margin)
        pdf.cell(_epw(pdf), 4.5, _safe(f"Generated: {gen_at}"), ln=True)

    if severity:
        r, g, b = sev_rgb
        pdf.set_font("Helvetica", size=8)
        pdf.set_text_color(105, 105, 120)
        pdf.set_x(pdf.l_margin)
        pdf.cell(22, 4.5, "Severity:", ln=False)
        pdf.set_fill_color(min(r + 175, 255), min(g + 175, 255), min(b + 175, 255))
        cur_x = pdf.get_x()
        cur_y = pdf.get_y()
        pdf.rect(cur_x, cur_y, 24, 4.5, style="F")
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(r, g, b)
        pdf.cell(24, 4.5, _safe(severity.upper()), align="C", ln=True)
        pdf.set_text_color(0, 0, 0)
    pdf.ln(4)

    if summary_data:
        _section_header(pdf, "1. Incident Summary")
        pdf.set_font("Helvetica", size=9)
        _multi_cell(pdf, summary_data.get("summary", ""), 4.5)
        if summary_data.get("severity_reason"):
            pdf.ln(1)
            pdf.set_font("Helvetica", "I", 8)
            pdf.set_text_color(90, 90, 105)
            _multi_cell(pdf, f"Severity reason: {summary_data['severity_reason']}", 4)
            pdf.set_text_color(0, 0, 0)
        pdf.ln(4)

    if root_cause_data:
        _section_header(pdf, "2. Root Cause Analysis")
        pdf.set_font("Helvetica", "B", 9)
        _multi_cell(pdf, root_cause_data.get("likely_root_cause", ""), 4.5)
        if root_cause_data.get("confidence"):
            pdf.set_font("Helvetica", "I", 8)
            pdf.set_text_color(90, 90, 105)
            pdf.set_x(pdf.l_margin)
            pdf.cell(
                _epw(pdf),
                4,
                _safe(f"Confidence: {root_cause_data['confidence']}"),
                ln=True,
            )
            pdf.set_text_color(0, 0, 0)
        if root_cause_data.get("reasoning"):
            pdf.set_font("Helvetica", size=8)
            _multi_cell(pdf, root_cause_data["reasoning"], 4)
        evidence = root_cause_data.get("supporting_evidence") or []
        if evidence:
            pdf.ln(1)
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_x(pdf.l_margin)
            pdf.cell(_epw(pdf), 4, "Supporting evidence:", ln=True)
            pdf.set_font("Helvetica", size=8)
            for i, ev in enumerate(evidence[:10], 1):
                _multi_cell(pdf, f"  {i}. {ev}", 4)
        pdf.ln(4)

    _render_log_charts(pdf, stats)

    if actions:
        _render_actions(pdf, actions, "4. Remediation TODO", "recommended")
        _render_actions(pdf, actions, "5. Immediate Checks", "check")
    else:
        # Fallback: render from raw analysis JSON (no live status)
        _section_header(pdf, "4. Remediation")
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_x(pdf.l_margin)
        pdf.cell(_epw(pdf), 5, "Recommended actions", ln=True)
        pdf.set_font("Helvetica", size=8)
        for i, a in enumerate((rem.get("recommended_actions") or [])[:40], 1):
            _multi_cell(pdf, f"  {i}. {a}", 4)
        pdf.ln(1)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_x(pdf.l_margin)
        pdf.cell(_epw(pdf), 5, "Immediate checks", ln=True)
        pdf.set_font("Helvetica", size=8)
        for i, c in enumerate((rem.get("next_checks") or [])[:40], 1):
            _multi_cell(pdf, f"  {i}. {c}", 4)
        if rem.get("risk_if_unresolved"):
            pdf.ln(1)
            _multi_cell(
                pdf, f"Risk if unresolved: {rem.get('risk_if_unresolved', '')}", 4
            )
        pdf.ln(4)

    # Guardrails
    _section_header(pdf, "6. Guardrails")
    pdf.set_font("Helvetica", size=8)
    _multi_cell(
        pdf,
        f"Prompt injection: {'yes' if guardrails.get('prompt_injection_detected') else 'no'}  |  "
        f"Unsafe content removed: {'yes' if guardrails.get('unsafe_content_removed') else 'no'}  |  "
        f"Input truncated: {'yes' if guardrails.get('input_truncated') else 'no'}",
        4,
    )
    for i, n in enumerate((guardrails.get("notes") or [])[:15], 1):
        _multi_cell(pdf, f"  {i}. {n}", 4)
    pdf.ln(3)

    return bytes(pdf.output())
