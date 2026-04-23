/**
 * Security-first content scanner.
 *
 * All security checks (XSS, script injection, prompt injection) run
 * unconditionally on every piece of content — regardless of whether it
 * also contains valid log lines.  The content-type check (code vs logs)
 * runs only as a secondary pass when no log evidence is found.
 */

// ---------------------------------------------------------------------------
// Security checks — always run
// ---------------------------------------------------------------------------

const SECURITY_CHECKS = [
  {
    id: "script_tag",
    severity: "danger",
    label: "Embedded <script> tag",
    pattern: /<script[\s\S]*?>/i,
    detail:
      "Contains <script> tags that could attempt to execute arbitrary JavaScript.",
  },
  {
    id: "html_structural",
    severity: "danger",
    label: "HTML injection",
    pattern:
      /<\/?(html|body|head|iframe|frame|object|embed|form|link|meta|base|style)\b[^>]*/i,
    detail:
      "Contains HTML structural tags (e.g. <iframe>, <form>) that could alter rendering or inject content.",
  },
  {
    id: "inline_event_handler",
    severity: "danger",
    label: "Inline event handler",
    pattern: /\bon\w+\s*=\s*["'][^"']*["']/i,
    detail:
      "Contains inline event handlers (e.g. onclick=, onerror=) commonly used in XSS attacks.",
  },
  {
    id: "js_uri",
    severity: "danger",
    label: "javascript: URI",
    pattern: /javascript\s*:/i,
    detail: "Contains a javascript: URI scheme, a classic cross-site scripting vector.",
  },
  {
    id: "data_uri",
    severity: "danger",
    label: "data: URI",
    pattern: /data\s*:\s*text\/html/i,
    detail: "Contains a data:text/html URI that can embed executable HTML pages.",
  },
  {
    id: "dom_api",
    severity: "warn",
    label: "Browser / DOM API call",
    pattern:
      /document\s*\.\s*(cookie|write|getElementById|querySelector|location)|window\s*\.\s*(location|open|eval)|eval\s*\(/,
    detail:
      "Contains DOM or browser API references (document.cookie, eval, etc.) unexpected in log data.",
  },
  {
    id: "prompt_injection",
    severity: "danger",
    label: "Prompt injection attempt",
    pattern:
      /ignore\s+previous\s+instructions|disregard\s+all\s+prior|forget\s+everything|new\s+instructions\s*:|^\s*system\s*:|^\s*assistant\s*:|<\s*tool\s*>|prompt\s+injection/im,
    detail:
      "Contains patterns that attempt to override or hijack the AI system instructions.",
  },
];

// ---------------------------------------------------------------------------
// Content-type check — only runs when no log evidence is present
// ---------------------------------------------------------------------------

/** Signals that the text contains genuine incident / log content. */
const LOG_EVIDENCE =
  /error|exception|traceback|timeout|timed\s*out|denied|failed|refused|503|500|4\d{2}|panic|oom|throttl|segfault|crash|killed|out\s+of\s+memory/i;

/** Timestamp or log-level prefixes typical of structured logs. */
const LOG_LINE_PATTERN =
  /^\s*(\d{4}-\d{2}-\d{2}[\sT]|\[\s*\d+[-/]|\w{3}\s+\d{1,2}\s+\d{2}:)|\b(DEBUG|INFO|WARN|WARNING|ERROR|CRITICAL|FATAL|NOTICE)\b/m;

/** Patterns that identify source code in various languages. */
const CODE_SIGNALS = [
  /\bfunction\s+\w+\s*\(/,                              // JS/TS function
  /\b(var|let|const)\s+\w+\s*=/,                        // JS/TS variable
  /\bimport\s+.+\s+from\s+['"]/,                        // ES module import
  /\bexport\s+(default|function|class|const|let)\b/,    // ES module export
  /=>\s*[{(]/,                                          // Arrow function
  /\bdef\s+\w+\s*\(/,                                   // Python function
  /\bclass\s+\w+(\s+extends|\s*{)/,                     // JS/Python class
  /^\s*#include\s+[<"]/m,                               // C/C++ include
  /^\s*(public|private|protected)\s+(static\s+)?\w+/m,  // Java/C#
  /^\s*from\s+\w+\s+import/m,                           // Python import
  /console\.(log|error|warn|debug)\s*\(/,               // JS console
  /^\s*<\?php/m,                                        // PHP
  /^\s*package\s+\w+;/m,                                // Java/Go
  /^\s*(fn|pub fn|impl)\s+\w+/m,                        // Rust
  /\bputs\s+['"]|require\s+['"]\w/,                     // Ruby
];

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Chat-specific prompt injection guard
// ---------------------------------------------------------------------------

/**
 * Prompt injection signals relevant to a conversational chat input.
 * Intentionally narrower than the incident-text guard — we don't flag
 * code or markup, only patterns that attempt to hijack the AI.
 */
const CHAT_INJECTION_SIGNALS = [
  { id: "override", pattern: /ignore\s+previous\s+instructions|disregard\s+all\s+prior|forget\s+everything/i, label: "Override attempt" },
  { id: "new_instructions", pattern: /new\s+instructions\s*:|^\s*system\s*:/im, label: "System instruction injection" },
  { id: "persona", pattern: /act\s+as\s+(if\s+you(\s+are)?|a\s+new|an?\s+\w)|you\s+are\s+now\s+(a|an|the)\s+|pretend\s+(you\s+are|to\s+be)|roleplay\s+as/i, label: "Persona hijacking" },
  { id: "jailbreak", pattern: /\bjailbreak\b|\bDAN\s*:|do\s+anything\s+now/i, label: "Jailbreak attempt" },
  { id: "reveal_prompt", pattern: /(print|reveal|show|output|repeat)\s+(your\s+)?(system\s+)?(prompt|instructions|rules|guidelines)/i, label: "System prompt extraction" },
  { id: "bypass", pattern: /bypass\s+(safety|filter|restriction|guardrail|policy)|override\s+(instructions|rules|system|policy)/i, label: "Safety bypass" },
  { id: "token_smuggling", pattern: /<\s*[/]?(human|user|assistant|system|context|instructions)\s*>|\[\s*INST\s*\]|<[|]im_start[|]>/i, label: "Token boundary injection" },
  { id: "script_tag", pattern: /<script[\s\S]*?>/i, label: "Script injection" },
  { id: "event_handler", pattern: /\bon\w{2,}\s*=\s*["'][^"']*["']/i, label: "Inline event handler" },
];

/**
 * Scan a chat message for prompt injection or XSS attempts.
 * Returns a warning object or null if the message looks safe.
 *
 * @param {string} text
 * @returns {{ label: string, detail: string } | null}
 */
export function detectChatInjection(text) {
  if (!text || text.trim().length < 5) return null;
  const hits = CHAT_INJECTION_SIGNALS.filter(({ pattern }) => pattern.test(text));
  if (hits.length === 0) return null;
  return {
    label: hits.map((h) => h.label).join(", "),
    detail:
      hits.length === 1
        ? `Your message appears to contain a ${hits[0].label.toLowerCase()}. It will be sanitized before reaching the AI.`
        : `Your message contains ${hits.length} suspicious patterns (${hits.map((h) => h.label).join("; ")}). They will be sanitized before reaching the AI.`,
  };
}

// ---------------------------------------------------------------------------
// Incident text guard (paste / upload)
// ---------------------------------------------------------------------------

/**
 * Scan content for security threats and content-type issues.
 *
 * @param {string} text
 * @returns {{
 *   issues: Array<{ id: string, severity: "danger"|"warn", label: string, detail: string }>
 * } | null}
 */
export function detectContentIssue(text) {
  if (!text || text.trim().length < 10) return null;

  const issues = [];

  // 1. Unconditional security scan
  for (const check of SECURITY_CHECKS) {
    if (check.pattern.test(text)) {
      issues.push({
        id: check.id,
        severity: check.severity,
        label: check.label,
        detail: check.detail,
      });
    }
  }

  // 2. Content-type check — only when no log evidence found
  const hasLogEvidence = LOG_EVIDENCE.test(text) || LOG_LINE_PATTERN.test(text);
  if (!hasLogEvidence) {
    const codeHits = CODE_SIGNALS.filter((p) => p.test(text)).length;
    if (codeHits >= 1) {
      issues.push({
        id: "wrong_content_type",
        severity: "warn",
        label: "Non-log content",
        detail:
          codeHits >= 3
            ? "Multiple code patterns detected. This does not appear to be incident log data."
            : "Possible code or script content. This does not appear to be incident log data.",
      });
    }
  }

  return issues.length > 0 ? { issues } : null;
}
