import { useState } from "react";

import { streamInvestigation } from "../lib/api";

export default function InvestigationStreamPanel({ job, getToken, disabled }) {
  const [streaming, setStreaming] = useState(false);
  const [text, setText] = useState("");
  const [error, setError] = useState("");

  async function runStream() {
    if (!job?.analysis?.summary || !job?.normalized_text) return;
    setStreaming(true);
    setText("");
    setError("");
    try {
      const token = getToken ? await getToken() : null;
      await streamInvestigation(
        {
          summary: job.analysis.summary.summary,
          normalized_text: job.normalized_text,
          evidence_snippets: job.analysis.root_cause?.supporting_evidence || [],
        },
        token,
        {
          onChunk: (_c, acc) => setText(acc),
          onDone: () => {},
        }
      );
    } catch (e) {
      setError(e.message || "Stream failed");
    } finally {
      setStreaming(false);
    }
  }

  if (!job?.analysis) return null;

  return (
    <section className="stream-panel card-elevated">
      <div className="stream-head">
        <h2>Live model stream</h2>
        <p className="muted small">
          Re-runs the investigator with Bedrock streaming when enabled; otherwise streams a heuristic JSON replay.
        </p>
        <button type="button" className="btn btn-secondary" disabled={disabled || streaming} onClick={runStream}>
          {streaming ? "Streaming…" : "Stream investigator output"}
        </button>
      </div>
      {error ? <p className="error compact">{error}</p> : null}
      {text ? (
        <pre className="stream-pre mono" aria-live="polite">
          {text}
        </pre>
      ) : null}
    </section>
  );
}
