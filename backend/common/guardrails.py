"""Guardrails for prompt injection, XSS, and grounded responses."""

from __future__ import annotations

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

EVIDENCE_HINTS = re.compile(
    r"(error|exception|traceback|timeout|timed\s*out|denied|failed|refused|503|500|panic|oom|throttl)",
    re.IGNORECASE,
)


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
