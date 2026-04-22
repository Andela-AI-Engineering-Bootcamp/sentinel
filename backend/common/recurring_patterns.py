"""Mine recurring line templates across incidents for dashboard analytics."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Any

# Short lines are usually noise; long lines stay bounded for display
_MIN_TEMPLATE_LEN = 10
_MAX_TEMPLATE_LEN = 420

_MON = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
_DOW = r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)"

_UUID = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.I,
)
# Syslog-style timestamps before digit pass (avoids [Sun Dec # #:#:# #])
_BRACKET_SYSLOG_TS = re.compile(
    rf"\[{_DOW}\s+{_MON}\s+\d{{1,2}}\s+\d{{2}}:\d{{2}}:\d{{2}}(?:\.\d+)?\s+\d{{4}}\]",
    re.I,
)
_SYSLOG_TS = re.compile(
    rf"\b{_DOW}\s+{_MON}\s+\d{{1,2}}\s+\d{{2}}:\d{{2}}:\d{{2}}(?:\.\d+)?\s+\d{{4}}\b",
    re.I,
)
_SHORT_MMDD_TS = re.compile(
    rf"\b{_MON}\s+\d{{1,2}}\s+\d{{2}}:\d{{2}}:\d{{2}}(?:\.\d+)?\b",
    re.I,
)
_ISO_TS = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b",
    re.I,
)
_IPV4 = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d{1,2})\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d{1,2})\b"
)
_KERNEL_PID = re.compile(r"\bkernel\[\d+\]:", re.I)
_RE_DIGITS = re.compile(r"\b\d+\b")
_RE_HEX = re.compile(r"\b0x[0-9a-f]+\b", re.I)
_RE_WS = re.compile(r"\s+")


def normalize_line_template(line: str) -> str:
    """Turn a log line into a stable template (numbers/ids redacted) for cross-run matching."""

    s = (line or "").strip()
    if not s:
        return ""
    s = _BRACKET_SYSLOG_TS.sub("[<timestamp>]", s)
    s = _ISO_TS.sub("<timestamp>", s)
    s = _SYSLOG_TS.sub("<timestamp>", s)
    s = _SHORT_MMDD_TS.sub("<timestamp>", s)
    s = _IPV4.sub("<ip>", s)
    s = _UUID.sub("<uuid>", s)
    s = _KERNEL_PID.sub("kernel[<pid>]:", s)
    s = _RE_HEX.sub("<hex>", s)
    s = _RE_DIGITS.sub("<n>", s)
    s = _RE_WS.sub(" ", s)
    s = s[:_MAX_TEMPLATE_LEN]
    if len(s) < _MIN_TEMPLATE_LEN:
        return ""
    return s


def _templates_for_text(body: str) -> tuple[Counter[str], set[str]]:
    """Count per line + unique templates appearing in this body."""

    c: Counter[str] = Counter()
    seen: set[str] = set()
    for line in (body or "").splitlines():
        t = normalize_line_template(line)
        if not t:
            continue
        c[t] += 1
        seen.add(t)
    return c, seen


def mine_recurring_patterns(
    incident_rows: list[dict[str, Any]],
    *,
    top_n: int = 12,
) -> dict[str, Any]:
    """
    Find line templates that recur across a user's saved incidents.
    `similarity_index` blends cross-incident spread and raw frequency (0..1).
    """

    pattern_line_counts: Counter[str] = Counter()
    pattern_incidents: dict[str, set[str]] = defaultdict(set)

    with_body: set[str] = set()
    for row in incident_rows:
        iid = row.get("id")
        if not iid:
            continue
        body = (row.get("sanitized_text") or row.get("raw_text") or "").strip()
        if not body:
            continue
        with_body.add(iid)
        counts, uniq = _templates_for_text(body)
        for t, n in counts.items():
            pattern_line_counts[t] += n
        for t in uniq:
            pattern_incidents[t].add(iid)

    total_incidents = max(1, len(with_body))
    max_lines = max(pattern_line_counts.values(), default=1)

    entries: list[dict[str, Any]] = []
    for t, line_occ in pattern_line_counts.items():
        inc_hits = len(pattern_incidents.get(t, ()))
        spread = inc_hits / total_incidents
        freq = line_occ / max_lines
        # Weight cross-incident recurrence (institutional "similarity") over raw line spam
        similarity_index = min(1.0, 0.55 * spread + 0.45 * freq)
        entries.append(
            {
                "pattern": t,
                "line_occurrences": line_occ,
                "incident_hits": inc_hits,
                "similarity_index": round(float(similarity_index), 4),
            }
        )

    entries.sort(
        key=lambda e: (e["similarity_index"], e["line_occurrences"], e["incident_hits"]),
        reverse=True,
    )
    return {
        "total_incidents_scanned": len(with_body),
        "patterns": entries[:top_n],
    }


def selected_pattern_overlap(selected_body: str, pattern_strings: list[str]) -> set[str]:
    """Which patterns appear in the selected incident (normalized line set)."""

    if not (selected_body or "").strip() or not pattern_strings:
        return set()
    _, templates = _templates_for_text(selected_body)
    want = set(pattern_strings)
    return templates & want
