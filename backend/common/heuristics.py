"""Deterministic heuristics used when Bedrock is disabled or unavailable."""

from __future__ import annotations

import re

from common.models import ClarificationQuestion, Confidence, IncidentSummary, RemediationPlan, RootCauseAnalysis, Severity


def classify_severity(text: str) -> tuple[Severity, str]:
    """Rule-based severity from incident text."""

    t = text.lower()
    if re.search(r"(outage|sev1|critical|panic|fatal|service down|database down)", t):
        return "critical", "Detected outage/fatal indicators in the incident log."
    if re.search(r"(error|exception|timeout|timed out|503|500|failed|refused)", t):
        return "high", "Detected repeated runtime failures or availability issues."
    if re.search(r"(warn|degraded|retry|slow)", t):
        return "medium", "Detected warning/degradation patterns that can escalate."
    return "low", "No strong failure indicators detected in the supplied input."


def summarize_incident(text: str) -> IncidentSummary:
    """Generate concise incident summary and severity."""

    sev, reason = classify_severity(text)
    lines = [line.strip() for line in text.split("\n") if line.strip()][:4]
    headline = lines[0] if lines else "Incident input received with limited details."
    summary = (
        f"Incident indicates {sev} impact. Primary signal: {headline}. "
        f"Additional context lines reviewed: {max(len(lines) - 1, 0)}."
    )
    return IncidentSummary(summary=summary, severity=sev, severity_reason=reason)


def infer_root_cause(text: str, evidence: list[str]) -> RootCauseAnalysis:
    """Infer likely root cause from known patterns."""

    t = text.lower()
    if re.search(r"(access denied|unauthorized|forbidden|permission denied)", t):
        return RootCauseAnalysis(
            likely_root_cause="Authentication/authorization misconfiguration",
            confidence="high",
            reasoning="Logs show explicit auth or permission failure responses.",
            supporting_evidence=evidence[:3],
        )
    if re.search(r"(connection refused|could not connect|database unavailable|db timeout)", t):
        return RootCauseAnalysis(
            likely_root_cause="Database connectivity instability",
            confidence="high",
            reasoning="Connection errors indicate DB endpoint/network instability or resource exhaustion.",
            supporting_evidence=evidence[:3],
        )
    if re.search(r"(timeout|timed out|deadline exceeded)", t):
        return RootCauseAnalysis(
            likely_root_cause="Downstream service timeout/latency spike",
            confidence="medium",
            reasoning="Request timeout signatures suggest dependency latency or saturation.",
            supporting_evidence=evidence[:3],
        )
    if re.search(r"(oom|out of memory|killed process)", t):
        return RootCauseAnalysis(
            likely_root_cause="Memory pressure causing process instability",
            confidence="high",
            reasoning="OOM markers indicate memory limit breaches under workload.",
            supporting_evidence=evidence[:3],
        )
    if re.search(r"(throttl|rate limit|too many requests)", t):
        return RootCauseAnalysis(
            likely_root_cause="Rate limiting or quota exhaustion",
            confidence="medium",
            reasoning="Throttling signals indicate traffic beyond configured quotas.",
            supporting_evidence=evidence[:3],
        )
    return RootCauseAnalysis(
        likely_root_cause="Insufficient evidence to isolate one definitive root cause",
        confidence="low",
        reasoning="No dominant pattern was detected in the provided logs.",
        supporting_evidence=evidence[:3],
    )


def recommend_actions(root: RootCauseAnalysis, severity: Severity) -> RemediationPlan:
    """Generate pragmatic remediation based on root-cause classification."""

    cause = root.likely_root_cause.lower()
    actions: list[str] = []
    checks: list[str] = [
        "Correlate timestamps across application logs, infrastructure metrics, and recent deploy events.",
        "Confirm blast radius by checking error-rate and latency by service/endpoint.",
    ]

    if "auth" in cause or "permission" in cause:
        actions = [
            "Validate IAM/API auth configuration and rotate invalid credentials if needed.",
            "Re-run failed request with least-privilege test credentials.",
            "Add explicit auth failure alerts for early detection.",
        ]
    elif "database" in cause:
        actions = [
            "Validate database endpoint health, connection limits, and security-group rules.",
            "Temporarily increase connection pool timeout/backoff and retry jitter.",
            "Review recent DB changes and rollback if correlated to incident start.",
        ]
    elif "timeout" in cause or "latency" in cause:
        actions = [
            "Increase downstream timeout and circuit-breaker protection conservatively.",
            "Profile slow dependency path and cache high-latency calls.",
            "Scale impacted service tier and monitor p95/p99 latency recovery.",
        ]
    elif "memory" in cause:
        actions = [
            "Increase memory allocation for affected workload and restart unhealthy pods/functions.",
            "Inspect heap/object growth to identify leak candidates.",
            "Set memory saturation alerts before OOM thresholds.",
        ]
    elif "rate limiting" in cause or "quota" in cause:
        actions = [
            "Implement client backoff and request coalescing to reduce burst pressure.",
            "Request quota increase or shard traffic across regions/accounts as applicable.",
            "Introduce token-bucket controls at ingress to smooth spikes.",
        ]
    else:
        actions = [
            "Capture additional structured logs around the failing transaction path.",
            "Compare behavior against last known good release and config snapshot.",
            "Run focused replay in a staging environment to reproduce the failure deterministically.",
        ]

    risk = (
        "Critical customer impact likely to continue without immediate remediation."
        if severity == "critical"
        else "Service quality and reliability may degrade further if unresolved."
    )
    return RemediationPlan(recommended_actions=actions, next_checks=checks, risk_if_unresolved=risk)


def confidence_to_score(conf: Confidence) -> int:
    """Map confidence levels to integer for UI sorting/filters."""

    return {"low": 1, "medium": 2, "high": 3}[conf]


def generate_questions(root: RootCauseAnalysis, evidence: list[str]) -> list[ClarificationQuestion]:
    """Generate targeted clarification questions based on root-cause and confidence level."""

    questions: list[ClarificationQuestion] = []
    cause = root.likely_root_cause.lower()

    if root.confidence == "low":
        questions.append(ClarificationQuestion(
            id="recent_changes",
            question="Were there any deployments, config changes, or infrastructure updates in the 24 hours before this incident?",
            rationale="Recent changes are the most common trigger for incidents and enable specific rollback steps.",
            kind="yes_no",
        ))
        questions.append(ClarificationQuestion(
            id="additional_context",
            question="Can you share any related metrics (CPU, memory, request rate) or additional log context around the time of the incident?",
            rationale="Extra context significantly improves root-cause confidence and produces more specific remediation steps.",
            kind="text",
        ))

    if "auth" in cause or "permission" in cause:
        questions.append(ClarificationQuestion(
            id="credential_rotation",
            question="Were any IAM roles, API keys, or service account credentials recently rotated or revoked?",
            rationale="Knowing whether credential rotation occurred narrows the fix to reverting a specific secret vs. diagnosing misconfiguration.",
            kind="yes_no",
        ))
        questions.append(ClarificationQuestion(
            id="affected_service",
            question="Which specific service or API endpoint is generating the auth failures?",
            rationale="Pinpointing the failing endpoint allows targeted IAM policy validation and a precise fix.",
            kind="text",
        ))

    elif "database" in cause:
        questions.append(ClarificationQuestion(
            id="db_change",
            question="Was there a recent database migration, schema change, connection pool config update, or infrastructure change?",
            rationale="DB-level changes are a primary cause of connectivity failures and knowing this guides the rollback target.",
            kind="yes_no",
        ))
        questions.append(ClarificationQuestion(
            id="db_type",
            question="What type of database is affected?",
            rationale="Fix approaches differ significantly between RDS, Aurora, DynamoDB, Redis, and self-managed databases.",
            kind="choice",
            options=["PostgreSQL / RDS", "MySQL / RDS", "Aurora", "DynamoDB", "Redis / ElastiCache", "MongoDB", "Other"],
        ))

    elif "timeout" in cause or "latency" in cause:
        questions.append(ClarificationQuestion(
            id="downstream_service",
            question="Which downstream service or dependency was slow or timing out (if known)?",
            rationale="Naming the dependency enables targeted circuit-breaker or scaling steps instead of generic advice.",
            kind="text",
        ))
        questions.append(ClarificationQuestion(
            id="traffic_spike",
            question="Was this incident associated with a traffic spike or a scheduled batch job?",
            rationale="Traffic-driven timeouts require scaling and rate-limiting fixes; dependency-driven timeouts call for circuit breakers.",
            kind="yes_no",
        ))

    elif "memory" in cause:
        questions.append(ClarificationQuestion(
            id="recent_deploy",
            question="Has this service had a new deployment or significant traffic growth in the last 48 hours?",
            rationale="New deployments may introduce memory leaks; traffic growth may require resource limit increases.",
            kind="yes_no",
        ))
        questions.append(ClarificationQuestion(
            id="workload_type",
            question="What type of workload is the affected process?",
            rationale="Memory management strategies differ significantly by workload type.",
            kind="choice",
            options=["Web server / API", "Batch / ETL job", "ML inference", "Stream processor", "Background worker", "Other"],
        ))

    elif "rate" in cause or "quota" in cause or "throttl" in cause:
        questions.append(ClarificationQuestion(
            id="traffic_source",
            question="Is the rate limiting coming from a single client/consumer or broadly across all traffic?",
            rationale="Single-consumer spikes need client-side backoff; broad limits need quota increases or traffic sharding.",
            kind="choice",
            options=["Single client / consumer", "Multiple clients", "All traffic broadly", "Unknown"],
        ))
        questions.append(ClarificationQuestion(
            id="quota_owner",
            question="Is this an external API quota (e.g. AWS, third-party) or an internal rate limit you control?",
            rationale="External quotas require a quota increase request; internal limits can be tuned immediately.",
            kind="choice",
            options=["External API quota (AWS, third-party)", "Internal rate limit we control", "Unsure"],
        ))

    # Deduplicate by id while preserving order
    seen: set[str] = set()
    unique: list[ClarificationQuestion] = []
    for q in questions:
        if q.id not in seen:
            seen.add(q.id)
            unique.append(q)
    return unique
