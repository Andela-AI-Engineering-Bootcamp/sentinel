"""Guardrails for prompt injection, XSS, and grounded responses."""

from __future__ import annotations

import json
import re

from common.models import GuardrailReport, RemediationPlan, RootCauseAnalysis


PROMPT_INJECTION_PATTERNS: list[str] = [
    # Classic override phrases
    r"ignore\s+previous\s+instructions",
    r"disregard\s+all\s+prior",
    r"forget\s+everything",
    r"new\s+instructions\s*:",
    r"^system\s*:",
    r"^assistant\s*:",
    r"<\s*tool\s*>",
    r"prompt\s*injection",
    # Persona / roleplay hijacking
    r"act\s+as\s+(if\s+you(\s+are)?|a\s+new|an?\s+\w)",
    r"you\s+are\s+now\s+(a|an|the)\s+",
    r"pretend\s+(you\s+are|to\s+be)",
    r"roleplay\s+as",
    r"\bDAN\s*:",
    r"do\s+anything\s+now",
    r"jailbreak",
    # Instruction extraction
    r"(print|reveal|show|output|repeat)\s+(your\s+)?(system\s+)?(prompt|instructions|rules|guidelines)",
    r"what\s+are\s+your\s+(instructions|rules|guidelines|directives)",
    r"bypass\s+(safety|filter|restriction|guardrail|policy)",
    r"override\s+(instructions|rules|system|policy)",
    # Token/boundary smuggling
    r"<\s*/?(human|user|assistant|system|context|instructions)\s*>",
    r"\[\s*INST\s*\]|\[\s*/INST\s*\]",  # Llama instruction tags
    r"###\s*(instruction|system|human|prompt)",
    r"<\|im_start\|>|<\|im_end\|>",  # ChatML tokens
    r"\bSTOP\s*\.\s*New\s+task\b",
    # Additional model-specific boundary tokens
    r"<\|system\|>|<\|user\|>|<\|assistant\|>",  # Phi / Mistral variants
    r"###\s*(Human|Assistant)",                    # Claude raw-prompt format
    # Additional jailbreak aliases
    r"\b(AIM|STAN|DUDE|KEVIN|DAVE)\s*:",           # Named jailbreak personas
    r"developer\s+mode\s+enabled",
    r"maintenance\s+mode\s+activated",
    r"sudo\s+mode",
    # Base64 payload smuggling (20+ chars of base64 following the keyword)
    r"base64\s*[,;:\s]\s*[A-Za-z0-9+/]{20,}={0,2}",
    # Markdown-heading injection: ### <anything> INSTRUCTIONS/DIRECTIVES/TASK/PROMPT
    # Catches variants like "### NEW INSTRUCTIONS" or "### SYSTEM TASK"
    r"###\s+\S.*\b(instructions?|directives?|task|prompt)\b",
    # Priority-escalation signals used to frame injected instructions
    r"\b(highest|top|maximum|absolute)\s+priority\b",
    # Credential / environment exfiltration attempts
    r"(print|show|output|reveal|return|list|display|dump)\s+(all\s+)?"
    r"(env(ironment)?\s+var(iable)?s?|connection\s+string|credentials?|secrets?|api[_\s]?keys?|passwords?)",
]

_XSS_SUBS: list[tuple[str, re.Pattern[str], str]] = [
    (
        "script tag",
        re.compile(r"<script\b[^>]*>[\s\S]*?</script\s*>", re.IGNORECASE),
        "[SCRIPT_REMOVED]",
    ),
    (
        "unclosed script tag",
        re.compile(r"<script\b[^>]*>", re.IGNORECASE),
        "[SCRIPT_TAG_REMOVED]",
    ),
    (
        "javascript: URI",
        re.compile(r"javascript\s*:", re.IGNORECASE),
        "[JS_URI_REMOVED]",
    ),
    # HTML entity-encoded javascript: (&#106;avascript: and similar)
    (
        "entity-encoded javascript: URI",
        re.compile(r"&#x?0*6[aA];|&#x?0*6[aA]\b", re.IGNORECASE),
        "[ENTITY_JS_REMOVED]",
    ),
    (
        "data:text/html URI",
        re.compile(r"data\s*:\s*text/html\b[^,\"'>\s]*", re.IGNORECASE),
        "[DATA_URI_REMOVED]",
    ),
    (
        "inline event handler",
        re.compile(r"\bon\w{2,}\s*=\s*(?:\"[^\"]*\"|'[^']*'|\S+)", re.IGNORECASE),
        "[EVENT_HANDLER_REMOVED]",
    ),
    (
        "unsafe HTML tag",
        re.compile(
            r"<\/?\s*(iframe|frame|object|embed|applet|base|form|meta|link|svg|math)"
            r"(\s[^>]*)?>",
            re.IGNORECASE,
        ),
        "[UNSAFE_TAG_REMOVED]",
    ),
    # SVG-based XSS via event attributes
    (
        "SVG XSS vector",
        re.compile(r"<svg\b[^>]*\bon\w+\s*=", re.IGNORECASE),
        "[SVG_XSS_REMOVED]",
    ),
    # CSS url() pointing to javascript: or data:text/html
    (
        "CSS url() injection",
        re.compile(
            r"url\s*\(\s*['\"]?\s*(javascript|data\s*:\s*text/html)",
            re.IGNORECASE,
        ),
        "[CSS_URL_REMOVED]",
    ),
    (
        "document.cookie / document.write",
        re.compile(r"document\s*\.\s*(cookie|write\s*\()", re.IGNORECASE),
        "[DOM_ACCESS_REMOVED]",
    ),
    (
        "eval()",
        re.compile(r"\beval\s*\(", re.IGNORECASE),
        "[EVAL_REMOVED]",
    ),
    (
        "window.location / window.open",
        re.compile(r"\bwindow\s*\.\s*(location|open)\s*[=(]", re.IGNORECASE),
        "[WINDOW_ACCESS_REMOVED]",
    ),
    (
        "innerHTML / outerHTML assignment",
        re.compile(r"\b(inner|outer)HTML\s*=", re.IGNORECASE),
        "[INNERHTML_REMOVED]",
    ),
    (
        "expression() CSS injection",
        re.compile(r"\bexpression\s*\(", re.IGNORECASE),
        "[CSS_EXPRESSION_REMOVED]",
    ),
    (
        "vbscript: URI",
        re.compile(r"vbscript\s*:", re.IGNORECASE),
        "[VBSCRIPT_URI_REMOVED]",
    ),
]

# ---------------------------------------------------------------------------
# Log format detection patterns
# ---------------------------------------------------------------------------

# ISO 8601 / RFC 3339 timestamps (e.g. 2024-04-23T08:12:44Z or 2024-04-23 08:12:44)
_TS_ISO = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
)
# Syslog-style timestamps (e.g. Apr 24 08:12:44 or Apr  4 08:12:44)
_TS_SYSLOG = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\b",
)
# Bracketed / slash-delimited timestamps (e.g. [2024/04/23 08:12:44] or [24/Apr/2024])
_TS_BRACKETED = re.compile(
    r"\[\d{2}[/\-]\w+[/\-]\d{2,4}[:\s]|\[\d{4}[/\-]\d{2}[/\-]\d{2}",
)
# Unix epoch timestamps (10+ digit integers alone or in JSON values)
_TS_EPOCH = re.compile(
    r'(?:^|[\s,{"\':])(1[0-9]{9,12})(?=$|[\s,}"\'\]])',
)
# Explicit log level keywords
_LOG_LEVEL = re.compile(
    r"\b(DEBUG|INFO|WARN(?:ING)?|ERROR|CRITICAL|FATAL|NOTICE|TRACE|SEVERE)\b",
    re.IGNORECASE,
)
# Stack trace markers (Python, Java, JavaScript, Go, Rust)
_STACK_TRACE = re.compile(
    r"Traceback \(most recent call last\)"
    r"|^\s+at\s+[\w.$<>]+\s*\("  # Java / JS
    r"|^\s+at\s+\w.*:\d+$"        # Node.js / Go
    r"|File \"[^\"]+\", line \d+" # Python
    r"|goroutine \d+ \[",          # Go panic
    re.MULTILINE,
)
# Newline-delimited JSON log lines (line starts with '{')
_JSON_LOG_LINE = re.compile(r'^\s*\{.*"(?:level|severity|log_level|msg|message|timestamp|time|ts)"', re.MULTILINE | re.IGNORECASE)
# HTTP access log pattern (e.g. GET /path HTTP/1.1 200)
_HTTP_ACCESS = re.compile(
    r"\b(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+/\S*\s+HTTP/\d"
    r"|\bHTTP/\d[.\d]*\s+[45]\d{2}\b",
)
# Common error / incident keywords (keep as a broad fallback)
_ERROR_KEYWORDS = re.compile(
    r"\b(error|exception|traceback|timeout|timed\s*out|denied|refused|panic|oom"
    r"|segfault|crash|killed|out\s+of\s+memory|connection\s+reset|503|500|502|504)\b",
    re.IGNORECASE,
)

# Minimum fraction of non-empty lines that must look like log lines.
# 0.5 requires at least half of the content to carry a log signal, so
# payloads that mix a few real log lines with large blobs of HTML, prose,
# or injection preamble cannot pass the format gate.
_MIN_LOG_LINE_FRACTION = 0.5


_JSON_LOG_TIME_KEYS = frozenset(
    {"timestamp", "time", "ts", "@timestamp", "datetime", "date"},
)
_JSON_LOG_LEVEL_MSG_KEYS = frozenset(
    {"level", "severity", "log_level", "message", "msg", "text", "log"},
)


def _dict_looks_like_log_record(obj: object) -> bool:
    """True if a JSON object has typical structured-log field names."""
    if not isinstance(obj, dict) or not obj:
        return False
    keys_lower = {str(k).lower() for k in obj}
    has_time = bool(keys_lower & _JSON_LOG_TIME_KEYS)
    has_level_or_msg = bool(keys_lower & _JSON_LOG_LEVEL_MSG_KEYS)
    return has_time and has_level_or_msg


def _json_text_is_log_export_array(text: str) -> bool:
    """
    True when *text* is a JSON array of log-like objects (pretty-printed export).

    Line-based heuristics fail on this format: most lines are braces, commas,
    or property names — only a minority contain inline timestamps or levels.
    """
    raw = text.strip()
    if not raw.startswith("["):
        return False
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return False
    if not isinstance(data, list) or len(data) == 0:
        return False
    ok = sum(1 for item in data if _dict_looks_like_log_record(item))
    if ok == 0:
        return False
    # Allow a few bad rows (partial export, header row) without failing the whole paste.
    return ok / len(data) >= 0.75


def _line_is_log_like(line: str) -> bool:
    """Return True if the line carries at least one log-format signal."""
    return bool(
        _TS_ISO.search(line)
        or _TS_SYSLOG.search(line)
        or _TS_BRACKETED.search(line)
        or _TS_EPOCH.search(line)
        or _LOG_LEVEL.search(line)
        or _STACK_TRACE.search(line)
        or _HTTP_ACCESS.search(line)
        or _ERROR_KEYWORDS.search(line)
    )


def validate_log_format(text: str) -> tuple[bool, list[str]]:
    """
    Validate that *text* resembles structured log or incident data.

    Accepts:
    - Lines with ISO 8601 / syslog / bracketed / epoch timestamps
    - Lines containing log-level keywords (DEBUG … FATAL)
    - Stack traces (Python, Java, JS, Go)
    - Newline-delimited JSON with log-semantic keys (NDJSON)
    - A JSON array of objects with timestamp/time + level/message fields (pretty-printed export)
    - HTTP access log lines
    - Lines containing common error/incident keywords

    Returns ``(True, [])`` when the input passes, or
    ``(False, [reason, ...])`` with human-readable reasons when it does not.
    """
    if not text or not text.strip():
        return False, ["Input is empty."]

    # Fast path: JSON log line anywhere in the text (NDJSON)
    if _JSON_LOG_LINE.search(text):
        return True, []

    # Fast path: pretty-printed JSON array of structured log objects
    if _json_text_is_log_export_array(text):
        return True, []

    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return False, ["Input contains no readable content."]

    log_like_count = sum(1 for ln in lines if _line_is_log_like(ln))

    if log_like_count == 0:
        return False, [
            "No log data found. Paste raw log output, a stack trace, or a structured "
            "log file — not prose, configuration, or source code."
        ]

    fraction = log_like_count / len(lines)
    if fraction < _MIN_LOG_LINE_FRACTION:
        return False, [
            f"Too little log content — only {log_like_count} of {len(lines)} lines "
            "contain a timestamp, log level, or error keyword. "
            "Add more log output or remove non-log text before submitting."
        ]

    return True, []

EVIDENCE_HINTS = re.compile(
    r"(error|exception|traceback|timeout|timed\s*out|denied|failed|refused|503|500|panic|oom|throttl)",
    re.IGNORECASE,
)


_HARD_XSS_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("script tag", re.compile(r"<script\b[^>]*>", re.IGNORECASE)),
    ("javascript: URI", re.compile(r"javascript\s*:", re.IGNORECASE)),
    ("entity-encoded javascript: URI", re.compile(r"&#x?0*6[aA];", re.IGNORECASE)),
    ("data:text/html URI", re.compile(r"data\s*:\s*text/html", re.IGNORECASE)),
    ("SVG XSS vector", re.compile(r"<svg\b[^>]*\bon\w+\s*=", re.IGNORECASE)),
    ("vbscript: URI", re.compile(r"vbscript\s*:", re.IGNORECASE)),
]


def detect_hard_xss(text: str) -> list[str]:
    """Return labels for any script-injection patterns found in text.

    Only high-confidence patterns are checked here — ones that can never
    appear in legitimate log data (script tags, javascript: URIs, etc.).
    Softer patterns (inline event handlers in log strings) are left to the
    sanitizer so that false positives don't block real incidents.
    """
    return [label for label, pattern in _HARD_XSS_PATTERNS if pattern.search(text)]


def sanitize_incident_text(
    text: str, max_chars: int = 12000
) -> tuple[str, GuardrailReport]:
    """
    Sanitise input in two passes:

    Pass 1 — XSS / HTML injection (full-text, inline substitution).
      Dangerous HTML/script fragments are replaced with labelled placeholders
      so the surrounding log context is preserved.

    Pass 2 — Prompt injection (line-by-line drop).
      Lines whose entire content is a prompt-injection attempt are removed.
    """

    report = GuardrailReport()
    clean = text.replace("\x00", " ").replace("\r", "")

    if len(clean) > max_chars:
        clean = clean[:max_chars]
        report.input_truncated = True
        report.notes.append(f"Input truncated to {max_chars} characters.")

    for label, pattern, replacement in _XSS_SUBS:
        new_clean, n = pattern.subn(replacement, clean)
        if n:
            clean = new_clean
            report.xss_detected = True
            report.xss_patterns_removed.append(label)

    if report.xss_detected:
        report.unsafe_content_removed = True
        report.notes.append(
            f"XSS / HTML injection fragments removed: "
            f"{', '.join(report.xss_patterns_removed)}."
        )

    kept_lines: list[str] = []
    for line in clean.split("\n"):
        line_stripped = line.strip()
        blocked = False
        for pattern in PROMPT_INJECTION_PATTERNS:
            if re.search(pattern, line_stripped, re.IGNORECASE):
                report.prompt_injection_detected = True
                if pattern not in report.blocked_patterns:
                    report.blocked_patterns.append(pattern)
                blocked = True
                break
        if not blocked:
            kept_lines.append(line)

    if report.prompt_injection_detected:
        report.unsafe_content_removed = True
        report.notes.append("Prompt-injection fragments removed from incident input.")

    sanitized = "\n".join(kept_lines).strip()
    if not sanitized:
        sanitized = "[EMPTY_AFTER_SANITIZATION]"
        report.notes.append("Input became empty after sanitisation.")

    return sanitized, report


def sanitize_chat_message(text: str) -> tuple[str, GuardrailReport]:
    """
    Sanitise a single chat message (smaller budget than a full incident payload).
    Applies both XSS stripping and prompt-injection line removal.
    """
    return sanitize_incident_text(text, max_chars=4000)


def extract_evidence_snippets(text: str, max_snippets: int = 6) -> list[str]:
    """Extract evidence-like log lines to ground downstream reasoning."""

    snippets: list[str] = []
    for line in text.split("\n"):
        candidate = line.strip()
        if not candidate:
            continue
        if EVIDENCE_HINTS.search(candidate):
            snippets.append(candidate[:300])
        if len(snippets) >= max_snippets:
            return snippets

    if not snippets:
        fallback = [line.strip()[:300] for line in text.split("\n") if line.strip()][:3]
        snippets.extend(fallback)

    return snippets


def enforce_grounding(
    root_cause: RootCauseAnalysis,
    remediation: RemediationPlan,
    evidence_snippets: list[str],
) -> tuple[RootCauseAnalysis, RemediationPlan]:
    """Prevent unsupported claims by forcing evidence-aware outputs."""

    if not evidence_snippets:
        root_cause.likely_root_cause = (
            "Insufficient evidence to determine a root cause."
        )
        root_cause.confidence = "low"
        root_cause.reasoning = (
            "No concrete error lines were provided in the incident payload."
        )
        root_cause.supporting_evidence = ["No evidence snippets available"]

    if root_cause.confidence == "low":
        guardrail_action = (
            "Collect additional logs and metrics before applying irreversible fixes."
        )
        if guardrail_action not in remediation.recommended_actions:
            remediation.recommended_actions.insert(0, guardrail_action)

    if not root_cause.supporting_evidence:
        root_cause.supporting_evidence = evidence_snippets[:3] or [
            "No supporting evidence extracted"
        ]

    return root_cause, remediation
