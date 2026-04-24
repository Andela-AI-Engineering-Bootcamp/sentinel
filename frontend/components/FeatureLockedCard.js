export default function FeatureLockedCard({
  title,
  badge = "Pro",
  description,
  ctaLabel = "Upgrade to unlock",
}) {
  return (
    <section className="card-elevated feature-locked-card">
      <div className="feature-locked-head">
        <div>
          <p className="eyebrow">Premium Feature</p>
          <h2 className="feature-locked-title">{title}</h2>
        </div>
        <span className="feature-locked-badge">{badge}</span>
      </div>

      <p className="page-sub muted" style={{ marginTop: 0 }}>
        {description}
      </p>

      <div className="feature-locked-panel">
        <p style={{ margin: 0, fontWeight: 600 }}>Available on paid plans</p>
        <p className="muted small" style={{ margin: "6px 0 0" }}>
          Unlock always-on CloudWatch monitoring, threshold-based incident detection, and a live board that turns
          raw production logs into an evolving incident narrative for your SRE team.
        </p>
      </div>

      <div className="feature-locked-actions">
        <button type="button" className="btn" disabled>
          {ctaLabel}
        </button>
        <p className="muted small" style={{ margin: 0 }}>
          The upgrade flow can be wired to billing later. The entitlement gate is already enforced in the product
          surface.
        </p>
      </div>
    </section>
  );
}
