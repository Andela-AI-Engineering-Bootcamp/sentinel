import { RedirectToSignIn, SignedIn, SignedOut } from "@clerk/nextjs";

import AppShell from "../components/AppShell";
import FeatureLockedCard from "../components/FeatureLockedCard";
import { useEntitlements } from "../context/EntitlementContext";
import { isClerkEnabled } from "../lib/clerk";

const clerkEnabled = isClerkEnabled();

function LockedPreview() {
  return (
    <>
      <FeatureLockedCard
        title="Live Incident Board"
        description="Monitor CloudWatch logs in real time and auto-open live incidents when threshold and pattern rules are crossed."
      />

      <section className="live-preview-grid">
        <div className="card-elevated live-preview-card">
          <p className="eyebrow">What You Unlock</p>
          <h3 style={{ marginTop: 0 }}>Continuous incident watch</h3>
          <p className="muted small" style={{ marginBottom: 0 }}>
            Sentinel watches selected CloudWatch log groups, detects spikes in errors, exceptions, timeouts, and auth
            failures, then opens a live incident with evidence-backed RCA and next actions.
          </p>
        </div>
        <div className="card-elevated live-preview-card">
          <p className="eyebrow">Board Preview</p>
          <h3 style={{ marginTop: 0 }}>Live incident story</h3>
          <ul className="live-preview-list">
            <li>Active incident status and severity</li>
            <li>Top evidence snippets from the log stream</li>
            <li>Evolving likely root cause and confidence</li>
            <li>Raw CloudWatch tail as supporting context</li>
          </ul>
        </div>
      </section>
    </>
  );
}

function EnabledBoard() {
  return (
    <>
      <section className="card-elevated live-board-shell">
        <div className="live-board-head">
          <div>
            <p className="eyebrow">LiveOps</p>
            <h2 style={{ margin: "0 0 6px" }}>Live Incident Board</h2>
            <p className="page-sub muted" style={{ margin: 0 }}>
              This account is enabled for the premium live-incident surface. CloudWatch ingestion and threshold-based
              incident detection can now be built on top of this gated experience without affecting the rest of the app.
            </p>
          </div>
          <span className="feature-locked-badge">Enabled</span>
        </div>
      </section>

      <section className="live-preview-grid">
        <div className="card-elevated live-preview-card">
          <p className="eyebrow">Board Status</p>
          <h3 style={{ marginTop: 0 }}>Ready for CloudWatch</h3>
          <p className="muted small" style={{ marginBottom: 0 }}>
            The entitlement, navigation, and premium route are live. The next implementation step is wiring the
            CloudWatch watcher and threshold detector into this page.
          </p>
        </div>
        <div className="card-elevated live-preview-card">
          <p className="eyebrow">Planned Signals</p>
          <h3 style={{ marginTop: 0 }}>Threshold triggers</h3>
          <ul className="live-preview-list">
            <li>ERROR / Exception bursts</li>
            <li>Timeout and upstream failure spikes</li>
            <li>Auth and permission failure clusters</li>
            <li>Crash and restart patterns</li>
          </ul>
        </div>
      </section>
    </>
  );
}

function LiveContent() {
  const { hasFeature, loading } = useEntitlements();
  const enabled = hasFeature("live_incident_board");

  return (
    <AppShell activeHref="/live">
      <header className="page-header">
        <div>
          <p className="eyebrow">Premium Operations</p>
          <h1 className="page-title">Live Incident Board</h1>
          <p className="page-sub muted">
            CloudWatch-powered live incident detection for SRE and platform teams.
          </p>
        </div>
      </header>

      {loading ? (
        <div className="card-elevated live-loading-card">
          <p className="muted small" style={{ margin: 0 }}>Loading entitlements…</p>
        </div>
      ) : enabled ? (
        <EnabledBoard />
      ) : (
        <LockedPreview />
      )}
    </AppShell>
  );
}

export default function LivePage() {
  if (!clerkEnabled) {
    return <LiveContent />;
  }

  return (
    <>
      <SignedIn>
        <LiveContent />
      </SignedIn>
      <SignedOut>
        <RedirectToSignIn />
      </SignedOut>
    </>
  );
}
