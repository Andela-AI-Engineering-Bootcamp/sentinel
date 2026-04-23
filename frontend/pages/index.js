import { SignedIn, SignedOut, SignInButton } from "@clerk/nextjs";
import Link from "next/link";

import { isClerkEnabled } from "../lib/clerk";

const clerkEnabled = isClerkEnabled();

const highlights = [
  {
    title: "Multi-agent analysis",
    body: "Normalizer, summarizer, investigator, and remediator coordinate to produce grounded incident outcomes.",
  },
  {
    title: "Operator-ready workflow",
    body: "Track each run, review remediation actions, and export audit artifacts for post-incident reporting.",
  },
  {
    title: "Built for secure teams",
    body: "Clerk-backed identity, per-user run isolation, and Aurora-backed persistence for production workloads.",
  },
];

function CtaButton() {
  if (!clerkEnabled) {
    return (
      <Link href="/dashboard" className="landing-cta landing-cta-primary">
        Go to dashboard
      </Link>
    );
  }

  return (
    <>
      <SignedOut>
        <SignInButton mode="modal">
          <button type="button" className="landing-cta landing-cta-primary">
            Sign in
          </button>
        </SignInButton>
      </SignedOut>
      <SignedIn>
        <Link href="/dashboard" className="landing-cta landing-cta-primary">
          Go to dashboard
        </Link>
      </SignedIn>
    </>
  );
}

export default function LandingPage() {
  return (
    <main className="landing-page">
      <div className="landing-glow landing-glow-left" aria-hidden />
      <div className="landing-glow landing-glow-right" aria-hidden />
      <div className="landing-grid" aria-hidden />

      <header className="landing-header">
        <Link href="/" className="landing-brand">
          <span className="landing-brand-mark" aria-hidden>
            <svg width="30" height="30" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path
                d="M16 4L26 10V22L16 28L6 22V10L16 4Z"
                stroke="currentColor"
                strokeWidth="1.5"
                fill="none"
              />
              <circle cx="16" cy="16" r="3" fill="currentColor" />
            </svg>
          </span>
          <span>
            <strong>Odyssey Sentinel</strong>
            <small>AI Incident Command</small>
          </span>
        </Link>

        <div className="landing-header-actions">
          <CtaButton />
        </div>
      </header>

      <section className="landing-hero">
        <p className="landing-kicker">Production Incident Intelligence</p>
        <h1>
          <span className="landing-title-line">Resolve high-impact incidents</span>
          <span className="landing-title-line">with calm, speed, and reliable AI support.</span>
        </h1>
        <p className="landing-subhead">
          Sentinel helps your team triage logs, identify root cause, and execute remediation with an auditable,
          structured workflow from first signal to post-incident report.
        </p>

        <div className="landing-hero-actions">
          <CtaButton />
          <Link href="/analyze" className="landing-cta landing-cta-secondary">
            Open analyze workspace
          </Link>
        </div>

        <div className="landing-stat-row" aria-label="Platform highlights">
          <article>
            <strong>5</strong>
            <span>Specialized agents</span>
          </article>
          <article>
            <strong>Aurora</strong>
            <span>Production-grade data layer</span>
          </article>
          <article>
            <strong>Live</strong>
            <span>Pipeline progress timeline</span>
          </article>
        </div>
      </section>

      <section className="landing-highlights" aria-label="Core capabilities">
        {highlights.map((item) => (
          <article key={item.title} className="landing-card">
            <h2>{item.title}</h2>
            <p>{item.body}</p>
          </article>
        ))}
      </section>

      <footer className="landing-footer">
        <p>Designed for modern reliability teams handling complex, high-stakes operations.</p>
      </footer>
    </main>
  );
}
