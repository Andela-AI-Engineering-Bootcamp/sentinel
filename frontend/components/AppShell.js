import { SignedIn, SignedOut, SignInButton, UserButton, useUser } from "@clerk/nextjs";
import Link from "next/link";

import { useEntitlements } from "../context/EntitlementContext";
import { isClerkEnabled } from "../lib/clerk";

const clerkEnabled = isClerkEnabled();

const nav = [
  { href: "/", label: "Home" },
  { href: "/analyze", label: "Analyze" },
  { href: "/dashboard", label: "Dashboard" },
  {
    href: "/live",
    label: "Live Incident Board",
    feature: "live_incident_board",
    badge: "Pro",
    lockedHint: "Real-time CloudWatch incident detection for paid plans",
  },
  { href: "/audit", label: "Audit" },
  { href: "/settings", label: "Settings" },
];

function UserSection() {
  const { user } = useUser();
  const name =
    [user?.firstName, user?.lastName].filter(Boolean).join(" ") ||
    user?.emailAddresses?.[0]?.emailAddress ||
    "Account";
  const email = user?.emailAddresses?.[0]?.emailAddress || "";

  return (
    <div className="shell-user">
      <UserButton
        afterSignOutUrl="/"
        appearance={{
          elements: {
            avatarBox: { width: 32, height: 32 },
          },
        }}
      />
      <div className="shell-user-info">
        <span className="shell-user-name">{name}</span>
        {email ? <span className="shell-user-email">{email}</span> : null}
      </div>
    </div>
  );
}

export default function AppShell({ children, activeHref = "/" }) {
  const { hasFeature, loading } = useEntitlements();

  return (
    <div className="app-root">
      <div className="app-bg" aria-hidden />
      <aside className="shell-aside">
        <Link href="/" className="shell-brand">
          <span className="shell-logo" aria-hidden>
            <svg width="32" height="32" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
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
            <strong>Odyssey</strong>
            <span className="shell-brand-sub">Sentinel</span>
          </span>
        </Link>

        <nav className="shell-nav" aria-label="Primary">
          {nav.map((item) => {
            const locked = item.feature && !loading && !hasFeature(item.feature);
            return (
              <Link
                key={item.href}
                href={item.href}
                className={`shell-nav-link${activeHref === item.href ? " is-active" : ""}${locked ? " is-locked" : ""}`}
                aria-disabled={locked ? "true" : undefined}
                title={locked ? item.lockedHint : undefined}
              >
                <span className="shell-nav-label">{item.label}</span>
                {item.badge ? <span className={`shell-nav-badge${locked ? " is-locked" : ""}`}>{item.badge}</span> : null}
              </Link>
            );
          })}
        </nav>

        <div className="shell-foot">
          <p className="muted small" style={{ marginBottom: clerkEnabled ? 16 : 0 }}>
            AI-powered incident management.
          </p>

          {clerkEnabled ? (
            <>
              <SignedIn>
                <UserSection />
              </SignedIn>
              <SignedOut>
                <SignInButton mode="redirect" redirectUrl="/">
                  <button type="button" className="shell-signin-btn">
                    Sign in
                  </button>
                </SignInButton>
              </SignedOut>
            </>
          ) : (
            <p className="muted small" style={{ fontSize: 11, marginTop: 4 }}>Local mode — auth disabled.</p>
          )}
        </div>
      </aside>

      <div className="shell-content">{children}</div>
    </div>
  );
}
