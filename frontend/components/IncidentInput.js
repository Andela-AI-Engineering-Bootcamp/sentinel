import { useRef } from "react";

export default function IncidentInput({ onAnalyze, loading, draft, onDraftChange, onClear, canClear }) {
  const fileRef = useRef(null);

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
    onDraftChange({ text: value, source: "upload" });
    event.target.value = "";
  }

  function submit(event) {
    event.preventDefault();
    onAnalyze({ title: draft.title.trim(), source: draft.source, text: draft.text.trim() });
  }

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
          {loading ? "Analyzing..." : "Analyze Incident"}
        </button>
        <button
          type="button"
          className="btn btn-secondary"
          onClick={() => onClear()}
          disabled={!canClear}
          title="Clear form and analysis state"
        >
          Clear
        </button>
      </div>
    </form>
  );
}
