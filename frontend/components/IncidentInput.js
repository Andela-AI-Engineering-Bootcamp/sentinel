import { useEffect, useRef, useState } from "react";

import { detectContentIssue } from "../lib/contentGuard";

/** Icons shown beside each issue row. */
const SEVERITY_ICON = { danger: "🚫", warn: "⚠" };

export default function IncidentInput({ onAnalyze, loading, draft, onDraftChange, onClear, canClear }) {
  const fileRef = useRef(null);
  const warnRef = useRef(null);
  /**
   * contentWarning shape:
   *   { issues: [{id, severity, label, detail}], source: "text"|"upload", filename?: string }
   */
  const [contentWarning, setContentWarning] = useState(null);
  const [acknowledged, setAcknowledged] = useState(false);

  // Re-evaluate whenever the text body changes.
  useEffect(() => {
    const result = detectContentIssue(draft.text);
    if (!result) {
      setContentWarning(null);
      setAcknowledged(false);
      return;
    }
    setContentWarning((prev) => {
      if (prev && acknowledged) return prev;
      return { ...result, source: "text" };
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [draft.text]);

  async function onFileChange(event) {
    const file = event.target.files?.[0];
    if (!file) return;
    let value = await file.text();
    const name = file.name.toLowerCase();
    const looksJson = file.type === "application/json" || name.endsWith(".json");
    if (looksJson) {
      try {
        value = JSON.stringify(JSON.parse(value), null, 2);
      } catch {
        /* keep raw if not valid JSON */
      }
    }

    const result = detectContentIssue(value);
    setContentWarning(result ? { ...result, source: "upload", filename: file.name } : null);
    setAcknowledged(false);

    onDraftChange({ text: value, source: "upload" });
    event.target.value = "";
  }

  function submit(event) {
    event.preventDefault();
    if (contentWarning && !acknowledged) {
      warnRef.current?.scrollIntoView({ behavior: "smooth", block: "center" });
      warnRef.current?.classList.add("content-warn--shake");
      setTimeout(() => warnRef.current?.classList.remove("content-warn--shake"), 600);
      return;
    }
    onAnalyze({ title: draft.title.trim(), source: draft.source, text: draft.text.trim() });
  }

  function dismiss() {
    setAcknowledged(true);
    setContentWarning(null);
  }

  function proceedAnyway() {
    setAcknowledged(true);
    setContentWarning(null);
    onAnalyze({ title: draft.title.trim(), source: draft.source, text: draft.text.trim() });
  }

  const showWarning = contentWarning && !acknowledged;

  // Overall severity is the worst level across all issues.
  const hasDanger = showWarning && contentWarning.issues.some((i) => i.severity === "danger");
  const overallSeverity = hasDanger ? "danger" : "warn";

  const sourceLabel =
    showWarning && contentWarning.source === "upload"
      ? `in uploaded file "${contentWarning.filename}"`
      : "in the pasted content";

  return (
    <form className="card stack" onSubmit={submit}>
      <h2>Incident Input</h2>
      <label>
        Incident Title
        <input
          className="input"
          value={draft.title}
          onChange={(e) => onDraftChange({ title: e.target.value })}
          maxLength={200}
        />
      </label>
      <label>
        Incident Source
        <select
          className="input"
          value={draft.source}
          onChange={(e) => onDraftChange({ source: e.target.value })}
        >
          <option value="manual">Manual paste</option>
          <option value="upload">File upload</option>
          <option value="monitoring">Monitoring export</option>
        </select>
      </label>
      <label>
        Paste Logs / Incident Text
        <textarea
          className="input textarea"
          value={draft.text}
          onChange={(e) => onDraftChange({ text: e.target.value })}
          placeholder={"2024-04-23T08:12:44Z ERROR database connection refused...\n2024-04-23T08:12:50Z CRITICAL panic: runtime error...\n\nPaste logs, stack trace, and context here..."}
          required
        />
      </label>

      {showWarning && (
        <div
          className={`content-warn content-warn--${overallSeverity}`}
          ref={warnRef}
          role="alert"
          aria-live="assertive"
        >
          <div className="content-warn__header">
            <span className="content-warn__title-icon">
              {hasDanger ? "🚫" : "⚠"}
            </span>
            <strong className="content-warn__title">
              {hasDanger
                ? `${contentWarning.issues.length} security issue${contentWarning.issues.length > 1 ? "s" : ""} detected ${sourceLabel}`
                : `Content warning ${sourceLabel}`}
            </strong>
            <button
              type="button"
              className="btn btn-muted content-warn__dismiss"
              onClick={dismiss}
              aria-label="Dismiss warning"
            >
              ✕
            </button>
          </div>

          <ul className="content-warn__issue-list">
            {contentWarning.issues.map((issue) => (
              <li key={issue.id} className={`content-warn__issue content-warn__issue--${issue.severity}`}>
                <span className="content-warn__issue-icon" aria-hidden="true">
                  {SEVERITY_ICON[issue.severity]}
                </span>
                <div className="content-warn__issue-body">
                  <span className="content-warn__issue-label">{issue.label}</span>
                  <span className="content-warn__issue-detail">{issue.detail}</span>
                </div>
              </li>
            ))}
          </ul>

          <div className="content-warn__footer">
            <button type="button" className="btn btn-warn-proceed" onClick={proceedAnyway}>
              Analyze anyway
            </button>
            <span className="content-warn__footer-note muted small">
              {hasDanger
                ? "Script tags, HTML injection, and prompt-injection phrases will be stripped by the backend before analysis."
                : "Results may be less accurate for non-log content."}
            </span>
          </div>
        </div>
      )}

      <div className="row gap wrap-actions">
        <button
          type="button"
          className="btn btn-muted"
          onClick={() => fileRef.current?.click()}
        >
          Upload .txt or .json
        </button>
        <input
          ref={fileRef}
          type="file"
          accept="text/plain,application/json,.txt,.text,.log,.md,.json"
          hidden
          onChange={onFileChange}
        />
        <button type="submit" className="btn" disabled={loading || !draft.text.trim()}>
          {loading ? "Analyzing..." : showWarning ? "Analyze anyway?" : "Analyze Incident"}
        </button>
        <button
          type="button"
          className="btn btn-secondary"
          onClick={() => { onClear(); setContentWarning(null); setAcknowledged(false); }}
          disabled={!canClear}
          title="Clear form and analysis state"
        >
          Clear
        </button>
      </div>
    </form>
  );
}
