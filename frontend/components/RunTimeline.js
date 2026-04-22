const STAGES = [
  { id: "queued", label: "Queue" },
  { id: "normalize", label: "Normalize" },
  { id: "summarize", label: "Summarize" },
  { id: "root_cause", label: "Root cause" },
  { id: "remediate", label: "Remediate" },
  { id: "completed", label: "Done" },
];

function stageIndex(stageId) {
  if (!stageId) return -1;
  if (stageId === "failed") return STAGES.length;
  const i = STAGES.findIndex((s) => s.id === stageId);
  return i;
}

export default function RunTimeline({ job, pipelineEvents = [], running }) {
  const lastEv = pipelineEvents.length ? pipelineEvents[pipelineEvents.length - 1] : null;
  const current = job?.current_stage || lastEv?.stage || "";
  const idx = stageIndex(current);
  const failed = job?.status === "failed";

  return (
    <section className="run-panel card-elevated">
      <div className="run-panel-head">
        <h2 className="run-title">Live run</h2>
        {running ? (
          <span className="pulse-badge" role="status" aria-live="polite">
            In progress
          </span>
        ) : failed ? (
          <span className="pill-badge pill-badge-warn">Failed</span>
        ) : job?.status === "completed" ? (
          <span className="pill-badge pill-badge-ok">Complete</span>
        ) : (
          <span className="muted small">Idle</span>
        )}
      </div>

      <ol className="pipeline-rail" aria-label="Pipeline stages">
        {STAGES.map((step, i) => {
          let state = "pending";
          if (failed) {
            if (step.id === "completed") {
              state = "error";
            } else {
              state = "done";
            }
          } else if (idx >= i) {
            state = i === idx && running ? "current" : "done";
          }
          if (step.id === "completed" && job?.status === "completed") {
            state = "done";
          }
          return (
            <li key={step.id} className={`pipeline-step pipeline-step-${state}`}>
              <span className="pipeline-dot" aria-hidden />
              <span className="pipeline-label">{step.label}</span>
            </li>
          );
        })}
      </ol>

      <div className="run-log" role="log" aria-live="polite" aria-relevant="additions">
        {pipelineEvents.length === 0 && !running ? (
          <p className="muted small">Submit an incident to stream pipeline stages here.</p>
        ) : null}
        <ul className="run-log-list">
          {pipelineEvents.map((ev, i) => (
            <li key={`${ev.at}-${i}`} className="run-log-item">
              <span className="run-log-stage">{ev.stage}</span>
              {ev.detail ? <span className="run-log-detail muted">{ev.detail}</span> : null}
            </li>
          ))}
        </ul>
      </div>

      {failed && job?.error ? (
        <p className="error compact" role="alert">
          {job.error}
        </p>
      ) : null}
    </section>
  );
}
