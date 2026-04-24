import { RedirectToSignIn, SignedIn, SignedOut, useAuth, useUser } from "@clerk/nextjs";
import { useState } from "react";

import AnalysisReport from "../components/AnalysisReport";
import AppShell from "../components/AppShell";
import IncidentInput from "../components/IncidentInput";
import InvestigationStreamPanel from "../components/InvestigationStreamPanel";
import RunTimeline from "../components/RunTimeline";
import { useAnalyzeSession } from "../context/AnalyzeSessionContext";
import { createIncident, pollJobUntilDone, streamJobUntilTerminal } from "../lib/api";
import { isClerkEnabled } from "../lib/clerk";

const clerkEnabled = isClerkEnabled();

async function runIncidentAnalysis(payload, getToken, onLiveEvents) {
  const token = clerkEnabled && getToken ? await getToken() : null;
  const created = await createIncident(payload, token);

  const collected = [];
  let finalJob = null;
  try {
    finalJob = await streamJobUntilTerminal(created.job_id, token, {
      onEvent: (p) => {
        if (p.event) {
          collected.push(p.event);
          if (onLiveEvents) onLiveEvents([...collected]);
        }
        if (p.terminal && p.job) {
          finalJob = p.job;
        }
      },
    });
  } catch {
    /* fall back to polling */
  }

  if (!finalJob) {
    finalJob = await pollJobUntilDone(created.job_id, token);
  }

  const events =
    finalJob?.pipeline_events?.length >= collected.length ? finalJob.pipeline_events : collected.length ? collected : finalJob?.pipeline_events || [];

  return { job: finalJob, events };
}

function HomeContent({ getToken = null, userProfile = null }) {
  const [loading, setLoading] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const {
    result,
    setResult,
    pipelineEvents,
    setPipelineEvents,
    error,
    setError,
    draft,
    updateDraft,
    clearAnalysis,
  } = useAnalyzeSession();

  async function onAnalyze(payload) {
    setLoading(true);
    setError("");
    setSubmitError("");
    updateDraft({ title: payload.title, source: payload.source, text: payload.text });
    setResult(null);
    setPipelineEvents([]);
    try {
      const { job, events } = await runIncidentAnalysis(payload, getToken, (live) => setPipelineEvents(live));
      setResult(job);
      setPipelineEvents(events);
    } catch (err) {
      if (err.status === 422) {
        // Input validation failure — show inside the form, not as a general error.
        setSubmitError(err.message || "Input validation failed. Check your log data and try again.");
      } else {
        setError(err.message || "Failed to analyze incident");
      }
    } finally {
      setLoading(false);
    }
  }

  const canClear = !!(draft.text.trim() || result || pipelineEvents.length || error);

  return (
    <AppShell activeHref="/analyze">
      <header className="page-header">
        <div>
          <p className="eyebrow">Incident intelligence</p>
          <h1 className="page-title">Command center</h1>
          <p className="page-sub muted">Submit logs, then follow the live pipeline. Charts and file export are on the Dashboard.</p>
        </div>
        <div className="page-header-actions">
          {!clerkEnabled ? <p className="muted small">Local mode: auth disabled.</p> : null}
        </div>
      </header>

      <div className="analyze-grid">
        <div className="analyze-col analyze-col-input">
          <IncidentInput
            onAnalyze={onAnalyze}
            loading={loading}
            draft={draft}
            onDraftChange={(changes) => { setSubmitError(""); updateDraft(changes); }}
            onClear={() => { clearAnalysis(); setSubmitError(""); }}
            canClear={canClear}
            submitError={submitError}
          />
          {error ? <p className="error compact">{error}</p> : null}
        </div>
        <div className="analyze-col analyze-col-run">
          <RunTimeline job={result} pipelineEvents={pipelineEvents} running={loading} />
        </div>
      </div>

      <p className="muted small" style={{ marginTop: 8 }}>
        After a run finishes, open the{" "}
        <a className="link-subtle" href="/dashboard">
          Dashboard
        </a>{" "}
        for log charts and exports.
      </p>

      <AnalysisReport result={result} getToken={getToken} userProfile={userProfile} showExport={false} />

      <InvestigationStreamPanel
        job={result}
        getToken={getToken}
        disabled={loading || !result || result.status !== "completed"}
      />
    </AppShell>
  );
}

function UnauthenticatedHome() {
  return <HomeContent />;
}

function AuthenticatedHome() {
  const { getToken } = useAuth();
  const { user } = useUser();
  const userProfile = user ? {
    email: user.primaryEmailAddress?.emailAddress || "",
    name: user.fullName || user.username || "",
  } : null;
  return <HomeContent getToken={getToken} userProfile={userProfile} />;
}

export default function Home() {
  if (!clerkEnabled) {
    return <UnauthenticatedHome />;
  }

  return (
    <>
      <SignedIn>
        <AuthenticatedHome />
      </SignedIn>
      <SignedOut>
        <RedirectToSignIn />
      </SignedOut>
    </>
  );
}
