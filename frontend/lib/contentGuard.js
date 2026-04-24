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
    id: "entity_js_uri",
    severity: "danger",
    label: "Entity-encoded javascript: URI",
    pattern: /&#x?0*6[aA];/i,
    detail: "Contains HTML-entity encoded 'javascript:' — a bypass technique for XSS filters.",
  },
  {
    id: "data_uri",
    severity: "danger",
    label: "data: URI",
    pattern: /data\s*:\s*text\/html/i,
    detail: "Contains a data:text/html URI that can embed executable HTML pages.",
  },
  {
    id: "svg_xss",
    severity: "danger",
    label: "SVG XSS vector",
    pattern: /<svg\b[^>]*\bon\w+\s*=/i,
    detail: "Contains an SVG element with an inline event handler, a known XSS vector.",
  },
  {
    id: "css_url_injection",
    severity: "danger",
    label: "CSS url() injection",
    pattern: /url\s*\(\s*['"]?\s*(javascript|data\s*:\s*text\/html)/i,
    detail: "Contains a CSS url() pointing to javascript: or data:text/html — used to bypass content filters.",
  },
  {
    id: "dom_api",
    severity: "danger",
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
      /ignore\s+previous\s+instructions|disregard\s+all\s+prior|forget\s+everything|new\s+instructions\s*:|^\s*system\s*:|^\s*assistant\s*:|<\s*tool\s*>|prompt\s+injection|<\|system\||<\|user\||<\|assistant\||###\s*(Human|Assistant)|\b(AIM|STAN|DUDE|KEVIN|DAVE)\s*:|developer\s+mode\s+enabled|maintenance\s+mode\s+activated|sudo\s+mode|###\s+\S.*\b(instructions?|directives?|task|prompt)\b|\b(highest|top|maximum|absolute)\s+priority\b|(print|show|output|reveal|return|list|display|dump)\s+(all\s+)?(env(ironment)?\s+var(iable)?s?|connection\s+string|credentials?|secrets?|api[\s_]?keys?|passwords?)/im,
    detail:
      "Contains patterns that attempt to override or hijack the AI system instructions.",
  },
];

// ---------------------------------------------------------------------------
// Content-type check — only runs when no log evidence is present
// ---------------------------------------------------------------------------

/** Signals that the text contains genuine incident / log content. */
const LOG_EVIDENCE =
  /error|exception|traceback|timeout|timed\s*out|denied|failed|refused|503|500|502|504|4\d{2}|panic|oom|throttl|segfault|crash|killed|out\s+of\s+memory|connection\s+reset/i;

/**
 * Timestamp or log-level prefixes typical of structured logs.
 * Aligned with the backend validate_log_format() heuristics.
 */
const LOG_LINE_PATTERN = new RegExp(
  // ISO 8601 / RFC 3339
  "\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}" +
  // Syslog (Apr 24 08:12:44)
  "|\\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2}\\b" +
  // Bracketed / slash-delimited
  "|\\[\\d{2}[/\\-]\\w+[/\\-]\\d{2,4}[:\\s]|\\[\\d{4}[/\\-]\\d{2}[/\\-]\\d{2}" +
  // Log level keywords
  "|\\b(DEBUG|INFO|WARN(?:ING)?|ERROR|CRITICAL|FATAL|NOTICE|TRACE|SEVERE)\\b" +
  // Python / Java stack trace
  "|Traceback \\(most recent call last\\)|^\\s+at\\s+[\\w.$<>]+\\s*\\(|File \"[^\"]+\", line \\d+" +
  // HTTP access log
  "|\\b(?:GET|POST|PUT|DELETE|PATCH|HEAD)\\s+/\\S*\\s+HTTP/\\d" +
  // NDJSON log line (line starts with { and has log-semantic keys)
  '|^\\s*\\{.*"(?:level|severity|msg|message|timestamp|time|ts)"',
  "mi"
);

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

/**
 * True when text is a JSON array of structured-log objects (pretty-printed).
 * Line-based fraction checks fail on this format because most lines are only
 * `{`, `}`, commas, or property names — not timestamps or log levels alone.
 */
function jsonArrayLooksLikeLogExport(text) {
  const raw = text.trim();
  if (!raw.startsWith("[")) return false;
  try {
    const data = JSON.parse(raw);
    if (!Array.isArray(data) || data.length === 0) return false;
    const timeKeys = new Set(["timestamp", "time", "ts", "@timestamp", "datetime", "date"]);
    const levelMsgKeys = new Set(["level", "severity", "log_level", "message", "msg", "text", "log"]);
    let ok = 0;
    for (const item of data) {
      if (!item || typeof item !== "object" || Array.isArray(item)) continue;
      const kl = new Set(Object.keys(item).map((k) => k.toLowerCase()));
      const hasTime = [...timeKeys].some((k) => kl.has(k));
      const hasLevelOrMsg = [...levelMsgKeys].some((k) => kl.has(k));
      if (hasTime && hasLevelOrMsg) ok += 1;
    }
    if (ok === 0) return false;
    return ok / data.length >= 0.75;
  } catch {
    return false;
  }
}

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
  { id: "jailbreak", pattern: /\bjailbreak\b|\bDAN\s*:|do\s+anything\s+now|\b(AIM|STAN|DUDE|KEVIN|DAVE)\s*:/i, label: "Jailbreak attempt" },
  { id: "reveal_prompt", pattern: /(print|reveal|show|output|repeat)\s+(your\s+)?(system\s+)?(prompt|instructions|rules|guidelines)/i, label: "System prompt extraction" },
  { id: "bypass", pattern: /bypass\s+(safety|filter|restriction|guardrail|policy)|override\s+(instructions|rules|system|policy)/i, label: "Safety bypass" },
  { id: "token_smuggling", pattern: /<\s*[/]?(human|user|assistant|system|context|instructions)\s*>|\[\s*INST\s*\]|<[|]im_start[|]>|<\|system\||<\|user\||<\|assistant\||###\s*(Human|Assistant)/i, label: "Token boundary injection" },
  { id: "dev_mode", pattern: /developer\s+mode\s+enabled|maintenance\s+mode\s+activated|sudo\s+mode/i, label: "Privileged mode activation" },
  { id: "script_tag", pattern: /<script[\s\S]*?>/i, label: "Script injection" },
  { id: "event_handler", pattern: /\bon\w{2,}\s*=\s*["'][^"']*["']/i, label: "Inline event handler" },
  { id: "heading_injection", pattern: /###\s+\S.*\b(instructions?|directives?|task|prompt)\b/i, label: "Markdown heading injection" },
  { id: "priority_escalation", pattern: /\b(highest|top|maximum|absolute)\s+priority\b/i, label: "Priority escalation" },
  { id: "exfiltration", pattern: /(print|show|output|reveal|return|list|display|dump)\s+(all\s+)?(env(ironment)?\s+var(iable)?s?|connection\s+string|credentials?|secrets?|api[\s_]?keys?|passwords?)/i, label: "Credential exfiltration attempt" },
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

  // 2. Content-type check — fraction-based, mirroring backend _MIN_LOG_LINE_FRACTION = 0.5
  // At least half of the non-empty lines must carry a log signal.
  const MIN_LOG_LINE_FRACTION = 0.5;
  const nonEmptyLines = text.split("\n").filter((l) => l.trim().length > 0);
  const logLikeCount = nonEmptyLines.filter((l) => LOG_LINE_PATTERN.test(l)).length;
  const logFraction = nonEmptyLines.length > 0 ? logLikeCount / nonEmptyLines.length : 0;

  const hasLogEvidence =
    jsonArrayLooksLikeLogExport(text) ||
    logFraction >= MIN_LOG_LINE_FRACTION ||
    LOG_EVIDENCE.test(text);
  if (!hasLogEvidence) {
    const codeHits = CODE_SIGNALS.filter((p) => p.test(text)).length;
    // Treat non-log content as a hard error: the backend will also reject it.
    issues.push({
      id: "wrong_content_type",
      severity: "danger",
      label: "Not recognised as log data",
      detail:
        codeHits >= 3
          ? "Multiple source-code patterns detected. Paste log output, a stack trace, or a structured log file — not source code."
          : logLikeCount > 0
          ? `Too little log content — only ${logLikeCount} of ${nonEmptyLines.length} lines contain a timestamp, log level, or error keyword. Add more log output or remove non-log text.`
          : "No log data found. Paste raw log output, a stack trace, or a structured log file — not prose, configuration, or source code.",
    });
  }

  return issues.length > 0 ? { issues } : null;
}
