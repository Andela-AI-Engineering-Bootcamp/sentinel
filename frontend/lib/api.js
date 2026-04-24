const RAW_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
const BASE_URL = RAW_BASE_URL.replace(/\/+$/, "");

async function request(path, options = {}) {
  const token = options.token;
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };

  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const res = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers,
  });

  if (!res.ok) {
    const text = await res.text();
    // FastAPI validation errors (422) return JSON with a `detail` array.
    // Extract a readable message so callers can surface it without dumping raw JSON.
    if (res.status === 422) {
      try {
        const json = JSON.parse(text);
        const msgs = (json.detail || [])
          .map((d) => {
            const raw = d.msg || d.message || JSON.stringify(d);
            // FastAPI prepends "Value error, " to every Pydantic ValueError — strip it.
            return raw.replace(/^value\s+error,\s*/i, "");
          })
          .join(" ");
        const err = new Error(msgs || "Input validation failed.");
        err.status = 422;
        throw err;
      } catch (parseErr) {
        if (parseErr.status === 422) throw parseErr;
      }
    }
    const err = new Error(text || `Request failed: ${res.status}`);
    err.status = res.status;
    throw err;
  }

  return res.json();
}

export async function createIncident(payload, token) {
  return request("/api/incidents", {
    method: "POST",
    body: JSON.stringify(payload),
    token,
  });
}

export async function fetchRemediationActions(jobId, token) {
  return request(`/api/jobs/${jobId}/actions`, { token });
}

export async function fetchClarificationQuestions(jobId, token) {
  return request(`/api/jobs/${jobId}/clarification-questions`, { token });
}

export async function submitClarifications(jobId, answers, token) {
  return request(`/api/jobs/${jobId}/clarify`, {
    method: "POST",
    body: JSON.stringify({ answers }),
    token,
  });
}

export async function updateRemediationAction(jobId, actionId, updates, token) {
  return request(`/api/jobs/${jobId}/actions/${actionId}`, {
    method: "PATCH",
    body: JSON.stringify(updates),
    token,
  });
}

export async function fetchIntegrations(token) {
  return request("/api/integrations", { token });
}

export async function saveIntegration(payload, token) {
  return request("/api/integrations", {
    method: "POST",
    body: JSON.stringify(payload),
    token,
  });
}

export async function deleteIntegration(integrationId, token) {
  return request(`/api/integrations/${integrationId}`, {
    method: "DELETE",
    token,
  });
}

export async function generateDigest(days, token) {
  return request("/api/reports/digest", {
    method: "POST",
    body: JSON.stringify({ days }),
    token,
  });
}

export async function downloadDigestPdf(days, token) {
  const headers = { Accept: "application/pdf" };
  if (token) headers.Authorization = `Bearer ${token}`;
  const res = await fetch(`${BASE_URL}/api/reports/digest/export?days=${days}`, { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Export failed: ${res.status}`);
  }
  const buf = await res.arrayBuffer();
  const blob = new Blob([buf], { type: "application/pdf" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `sentinel-digest-${days}d.pdf`;
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 1000);
}

export async function fetchJob(jobId, token) {
  return request(`/api/jobs/${jobId}`, { token });
}

/** GET /api/jobs/{id}/workflow — full readonly workflow snapshot (audit / review in UI). */
export async function fetchJobWorkflow(jobId, token) {
  return request(`/api/jobs/${encodeURIComponent(jobId)}/workflow`, { token });
}

/**
 * Download GET /api/jobs/{id}/audit/pdf — Classic (traditional) audit report as PDF.
 */
export async function downloadAuditPdf(jobId, token) {
  const enc = encodeURIComponent(jobId);
  const headers = { Accept: "application/pdf" };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const res = await fetch(`${BASE_URL}/api/jobs/${enc}/audit/pdf`, { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Audit PDF export failed: ${res.status}`);
  }
  const buf = await res.arrayBuffer();
  const head = new Uint8Array(buf.slice(0, 5));
  const sig = String.fromCharCode(...head.slice(0, 4));
  if (sig !== "%PDF") {
    const text = new TextDecoder().decode(buf);
    throw new Error(text || "Server did not return a valid PDF.");
  }
  const blob = new Blob([buf], { type: "application/pdf" });
  const name = `sentinel-audit-${String(jobId).slice(0, 8)}.pdf`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  setTimeout(() => {
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, 0);
}

export async function fetchJobs(limit = 25, token) {
  return request(`/api/jobs?limit=${encodeURIComponent(String(limit))}`, { token });
}

export async function fetchCurrentUser(token) {
  return request("/api/me", { token });
}

/**
 * Download GET /api/jobs/{id}/export?format=json|pdf as a file.
 * Pass a pre-fetched `token` when possible (e.g. from a hook) so the save dialog still opens after async work
 * in browsers that require a current user-activation.
 */
export async function downloadJobExport(jobId, format, token) {
  const enc = encodeURIComponent(jobId);
  const fmt = format === "pdf" ? "pdf" : "json";
  const accept = fmt === "pdf" ? "application/pdf" : "application/json";
  const headers = { Accept: accept };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const res = await fetch(`${BASE_URL}/api/jobs/${enc}/export?format=${fmt}`, { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Export failed: ${res.status}`);
  }
  const buf = await res.arrayBuffer();
  if (fmt === "pdf") {
    const head = new Uint8Array(buf.slice(0, 5));
    const sig = String.fromCharCode(...head.slice(0, 4));
    if (sig !== "%PDF") {
      const text = new TextDecoder().decode(buf);
      throw new Error(text || "Server did not return a valid PDF. Sign in and pick a completed run.");
    }
  }
  const mime = fmt === "pdf" ? "application/pdf" : "application/json";
  const blob = new Blob([buf], { type: mime });
  const ext = fmt === "pdf" ? "pdf" : "json";
  const name = `${fmt === "json" ? "sentinel-workflow" : "sentinel-export"}-${String(jobId).slice(0, 8)}.${ext}`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  setTimeout(() => {
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, 0);
}

/** Poll job until completed or failed (fallback if SSE unavailable). */
export async function pollJobUntilDone(jobId, token, { intervalMs = 480, maxWaitMs = 180000 } = {}) {
  const started = Date.now();
  while (Date.now() - started < maxWaitMs) {
    const job = await fetchJob(jobId, token);
    if (job.status === "completed" || job.status === "failed") {
      return job;
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error("Analysis timed out. Try again or look up the job on the dashboard.");
}

/**
 * SSE over fetch (supports Authorization). Invokes onEvent for each message; resolves with final job or null.
 */
export async function streamJobUntilTerminal(jobId, token, { onEvent } = {}) {
  const headers = {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const res = await fetch(`${BASE_URL}/api/jobs/${jobId}/stream`, { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Stream failed: ${res.status}`);
  }
  const reader = res.body?.getReader();
  if (!reader) {
    throw new Error("Streaming not supported in this environment.");
  }
  const decoder = new TextDecoder();
  let buffer = "";
  let finalJob = null;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let sep;
    while ((sep = buffer.indexOf("\n\n")) !== -1) {
      const block = buffer.slice(0, sep);
      buffer = buffer.slice(sep + 2);
      for (const line of block.split("\n")) {
        if (!line.startsWith("data: ")) continue;
        const raw = line.slice(6);
        let payload;
        try {
          payload = JSON.parse(raw);
        } catch {
          continue;
        }
        if (onEvent) onEvent(payload);
        if (payload.terminal && payload.job) {
          finalJob = payload.job;
        }
      }
    }
  }
  return finalJob;
}

// ── Follow-up reminders ────────────────────────────────────────────────────────

export async function fetchFollowUps(jobId, token) {
  return request(`/api/jobs/${encodeURIComponent(jobId)}/follow-ups`, { token });
}

export async function createFollowUp(jobId, payload, token) {
  return request(`/api/jobs/${encodeURIComponent(jobId)}/follow-ups`, {
    method: "POST",
    body: JSON.stringify(payload),
    token,
  });
}

export async function deleteFollowUp(jobId, followUpId, token) {
  const res = await fetch(
    `${BASE_URL}/api/jobs/${encodeURIComponent(jobId)}/follow-ups/${encodeURIComponent(followUpId)}`,
    {
      method: "DELETE",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    },
  );
  if (!res.ok && res.status !== 204) {
    const text = await res.text();
    throw new Error(text || `Delete failed: ${res.status}`);
  }
}

/** GET /api/jobs/{jobId}/actions/{actionId}/chat — fetch saved chat history. */
export async function fetchActionChatHistory(jobId, actionId, token) {
  const enc = encodeURIComponent(jobId);
  const aid = encodeURIComponent(actionId);
  return request(`/api/jobs/${enc}/actions/${aid}/chat`, { token });
}

/**
 * POST /api/jobs/{jobId}/actions/{actionId}/chat — stream a remediation chat reply.
 * history: array of { role: "user"|"assistant", content: string }
 */
export async function streamActionChat(jobId, actionId, message, history, token, { onChunk, onDone } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const enc = encodeURIComponent(jobId);
  const aid = encodeURIComponent(actionId);
  const res = await fetch(`${BASE_URL}/api/jobs/${enc}/actions/${aid}/chat`, {
    method: "POST",
    headers,
    body: JSON.stringify({ message, history }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Chat stream failed: ${res.status}`);
  }
  const reader = res.body?.getReader();
  if (!reader) throw new Error("No stream body");
  const decoder = new TextDecoder();
  let buffer = "";
  let accumulated = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let sep;
    while ((sep = buffer.indexOf("\n\n")) !== -1) {
      const block = buffer.slice(0, sep);
      buffer = buffer.slice(sep + 2);
      for (const line of block.split("\n")) {
        if (!line.startsWith("data: ")) continue;
        let payload;
        try {
          payload = JSON.parse(line.slice(6));
        } catch {
          continue;
        }
        if (payload.chunk != null) {
          accumulated += payload.chunk;
          if (onChunk) onChunk(payload.chunk, accumulated);
        }
        if (payload.done) {
          if (onDone) onDone(accumulated);
        }
      }
    }
  }
}

// ── Action findings evaluation ────────────────────────────────────────────────

export async function evaluateActionFindings(jobId, actionId, findings, token) {
  return request(
    `/api/jobs/${encodeURIComponent(jobId)}/actions/${encodeURIComponent(actionId)}/evaluate`,
    { method: "POST", body: JSON.stringify({ findings }), token },
  );
}

// ── Remediation follow-up ─────────────────────────────────────────────────────

export async function submitRemediationFollowup(jobId, additional_context, token, anchor_action_id = null) {
  return request(`/api/jobs/${encodeURIComponent(jobId)}/remediation-followup`, {
    method: "POST",
    body: JSON.stringify({ additional_context, ...(anchor_action_id ? { anchor_action_id } : {}) }),
    token,
  });
}

// ── Incident resolution & Post-Incident Review ────────────────────────────────

export async function resolveIncident(incidentId, { status = "resolved", resolution_notes = "" } = {}, token) {
  return request(`/api/incidents/${encodeURIComponent(incidentId)}/status`, {
    method: "PATCH",
    body: JSON.stringify({ status, resolution_notes }),
    token,
  });
}

export async function generatePIR(jobId, token) {
  return request(`/api/jobs/${encodeURIComponent(jobId)}/pir`, {
    method: "POST",
    token,
  });
}

export async function fetchPIR(jobId, token) {
  return request(`/api/jobs/${encodeURIComponent(jobId)}/pir`, { token });
}

/** POST /api/stream/investigate — SSE chunks + final parse. */
export async function streamInvestigation(body, token, { onChunk, onDone } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const res = await fetch(`${BASE_URL}/api/stream/investigate`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Stream failed: ${res.status}`);
  }
  const reader = res.body?.getReader();
  if (!reader) throw new Error("No stream body");
  const decoder = new TextDecoder();
  let buffer = "";
  let accumulated = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let sep;
    while ((sep = buffer.indexOf("\n\n")) !== -1) {
      const block = buffer.slice(0, sep);
      buffer = buffer.slice(sep + 2);
      for (const line of block.split("\n")) {
        if (!line.startsWith("data: ")) continue;
        let payload;
        try {
          payload = JSON.parse(line.slice(6));
        } catch {
          continue;
        }
        if (payload.chunk != null) {
          accumulated += payload.chunk;
          if (onChunk) onChunk(payload.chunk, accumulated);
        }
        if (payload.done) {
          if (onDone) onDone(payload, accumulated);
        }
      }
    }
  }
}
