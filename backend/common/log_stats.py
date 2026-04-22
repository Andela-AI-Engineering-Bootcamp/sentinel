"""Heuristic stats extracted from raw/sanitized log text for charts and export."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import asdict, dataclass

# Order matters: first match wins per line for primary level
_LEVEL_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(?:FATAL|CRITICAL|CRIT)\b|^\s*\[?CRITICAL\]?[:,\s]", re.I), "error"),
    (re.compile(r"\bERROR\b|^\s*\[?ERROR\]?[:,\s]|\[E\]\s|level[=:]error", re.I), "error"),
    (re.compile(r"exception|traceback|stack\s*trace|oomkilled|out\s*of\s*memory|segfault|panic", re.I), "error"),
    (re.compile(r"\bWARN(?:ING)?\b|^\s*\[?WARN\]?[:,\s]|\[W\]\s|level[=:]warn", re.I), "warn"),
    (re.compile(r"\bINFO\b|^\s*\[?INFO\]?[:,\s]|\[I\]\s|level[=:]info", re.I), "info"),
    (re.compile(r"\bDEBUG\b|^\s*\[?DEBUG\]?[:,\s]|\[D\]\s|level[=:]debug", re.I), "debug"),
]

# Standalone 3-digit HTTP / API status codes
_HTTP_CODE = re.compile(r"(?<![0-9])([1-5][0-9]{2})(?![0-9])")

# Keywords for “signal” bar when HTTP codes are sparse
_SIGNAL_KEYWORDS: list[tuple[str, re.Pattern[str]]] = [
    ("timeout", re.compile(r"\btimeout|timed?\s*out|deadline\s*exceed", re.I)),
    ("connection", re.compile(r"\brefused|econnrefused|connection\s*reset|broken\s*pipe", re.I)),
    ("auth", re.compile(r"\b401\b|\b403\b|unauthor|forbidden|denied|invalid\s*token|jwt", re.I)),
    ("throttle", re.compile(r"throttl|rate[\s-]?limit|429|too many requests|quota", re.I)),
    ("database", re.compile(r"\bpostgres|\bmysql|sql\s*error|deadlock|constraint", re.I)),
]


@dataclass
class LogStats:
    line_count: int
    char_count: int
    levels: dict[str, int]
    http_status: dict[str, int]
    http_class: dict[str, int]
    signal_keywords: dict[str, int]
    buckets: list[dict[str, int | str]]
    timestamped_points: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _classify_status(code: int) -> str:
    if 200 <= code < 300:
        return "2xx"
    if 300 <= code < 400:
        return "3xx"
    if 400 <= code < 500:
        return "4xx"
    if 500 <= code < 600:
        return "5xx"
    return "other"


def _line_level(line: str) -> str:
    for pat, name in _LEVEL_RULES:
        if pat.search(line):
            return name
    return "other"


def compute_log_stats(text: str, *, max_buckets: int = 12) -> dict[str, object]:
    """Derive simple aggregates from free-form incident/log text (best-effort)."""

    if not text or not str(text).strip():
        return LogStats(
            line_count=0,
            char_count=0,
            levels={"error": 0, "warn": 0, "info": 0, "debug": 0, "other": 0},
            http_status={},
            http_class={"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0, "other": 0},
            signal_keywords={},
            buckets=[],
            timestamped_points=0,
        ).as_dict()

    body = str(text)
    lines = body.splitlines()
    n = len(lines)
    char_count = len(body)

    levels: Counter[str] = Counter(
        {
            "error": 0,
            "warn": 0,
            "info": 0,
            "debug": 0,
            "other": 0,
        }
    )
    for line in lines:
        lv = _line_level(line)
        levels[lv] += 1

    http_status: Counter[str] = Counter()
    http_class: Counter[str] = Counter(
        {
            "2xx": 0,
            "3xx": 0,
            "4xx": 0,
            "5xx": 0,
            "other": 0,
        }
    )
    for m in _HTTP_CODE.finditer(body):
        try:
            code = int(m.group(1))
        except ValueError:
            continue
        if code < 100 or code > 599:
            continue
        http_status[str(code)] += 1
        http_class[_classify_status(code)] += 1

    signal_keywords: Counter[str] = Counter()
    for name, pat in _SIGNAL_KEYWORDS:
        if pat.search(body):
            signal_keywords[name] += 1
    for name, _ in _SIGNAL_KEYWORDS:
        if name not in signal_keywords:
            signal_keywords[name] = 0

    # Time-ish tokens (ISO-ish or common syslog); used only to show whether time-like data exists
    time_hint = re.compile(
        r"(?:\d{4}-\d{2}-\d{2}T|\d{4}-\d{2}-\d{2} |\d{2}/[A-Za-z]{3}/\d{4}|\[?\d{2}/[A-Za-z]+/\d{4}:\d{2}:\d{2})"
    )
    ts_lines = sum(1 for line in lines if time_hint.search(line))
    timestamped_points = ts_lines

    # Segment the file into buckets: errors and warns per line range (x = position in file)
    nb = min(max_buckets, max(1, n))
    bucket_size = max(1, (n + nb - 1) // nb)
    buckets: list[dict[str, int | str]] = []
    idx = 0
    bnum = 0
    while idx < n and bnum < nb:
        end = min(n, idx + bucket_size)
        chunk = lines[idx:end]
        err = 0
        wrn = 0
        for line in chunk:
            lv = _line_level(line)
            if lv == "error":
                err += 1
            elif lv == "warn":
                wrn += 1
        bnum += 1
        start_line = idx + 1
        end_line = end
        label = f"L{start_line}–L{end_line}"
        buckets.append(
            {
                "label": label,
                "line_start": start_line,
                "line_end": end_line,
                "error": err,
                "warn": wrn,
            }
        )
        idx = end

    return LogStats(
        line_count=n,
        char_count=char_count,
        levels=dict(levels),
        http_status=dict(http_status),
        http_class=dict(http_class),
        signal_keywords=dict(signal_keywords),
        buckets=buckets,
        timestamped_points=timestamped_points,
    ).as_dict()
