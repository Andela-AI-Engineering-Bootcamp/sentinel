import { useEffect, useRef, useState } from "react";

import {
  createFollowUp,
  deleteFollowUp,
  downloadJobExport,
  fetchActionChatHistory,
  fetchFollowUps,
  fetchRemediationActions,
  streamActionChat,
  updateRemediationAction,
} from "../lib/api";

// ── Chat drawer

function ChatMessage({ msg }) {
  const isUser = msg.role === "user";
  return (
    <div
      style={{
        display: "flex",
        justifyContent: isUser ? "flex-end" : "flex-start",
        marginBottom: 10,
      }}
    >
      <div
        style={{
          maxWidth: "85%",
          background: isUser ? "var(--accent)" : "var(--surface-2)",
          color: isUser ? "#fff" : "var(--text)",
          borderRadius: isUser ? "12px 12px 2px 12px" : "12px 12px 12px 2px",
          padding: "9px 13px",
          fontSize: 13,
          lineHeight: 1.55,
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
        }}
      >
        {msg.content}
        {msg.streaming && (
          <span
            style={{
              display: "inline-block",
              width: 7,
              height: 13,
              background: "var(--muted)",
              marginLeft: 3,
              borderRadius: 1,
              animation: "sentinel-blink 1s steps(2) infinite",
              verticalAlign: "text-bottom",
            }}
          />
        )}
      </div>
    </div>
  );
}

function ChatDrawer({ action, jobId, getToken, onClose }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [loadingHistory, setLoadingHistory] = useState(true);
  const [error, setError] = useState("");
  const bottomRef = useRef(null);
  const inputRef = useRef(null);

  // Load persisted history on open
  useEffect(() => {
    let cancel = false;
    (async () => {
      try {
        const token = getToken ? await getToken() : null;
        const history = await fetchActionChatHistory(jobId, action.id, token);
        if (!cancel) {
          setMessages(history.map((m) => ({ role: m.role, content: m.content })));
        }
      } catch {
        /* best-effort — start with empty chat if history fetch fails */
      } finally {
        if (!cancel) setLoadingHistory(false);
      }
    })();
    return () => { cancel = true; };
  }, [jobId, action.id, getToken]);

  // Auto-scroll to bottom whenever messages change
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Focus input once history is loaded
  useEffect(() => {
    if (!loadingHistory) inputRef.current?.focus();
  }, [loadingHistory]);

  async function sendMessage(text, history) {
    const userMsg = { role: "user", content: text };
    const nextHistory = [...(history ?? messages), userMsg];
    setMessages(nextHistory);
    setStreaming(true);
    setError("");

    const assistantIdx = nextHistory.length;
    setMessages((prev) => [...prev, { role: "assistant", content: "", streaming: true }]);

    try {
      const token = getToken ? await getToken() : null;
      const chatHistory = nextHistory.slice(0, -1).map((m) => ({ role: m.role, content: m.content }));
      await streamActionChat(jobId, action.id, text, chatHistory, token, {
        onChunk: (_c, acc) => {
          setMessages((prev) =>
            prev.map((m, i) => (i === assistantIdx ? { ...m, content: acc } : m)),
          );
        },
        onDone: (acc) => {
          setMessages((prev) =>
            prev.map((m, i) => (i === assistantIdx ? { role: "assistant", content: acc, streaming: false } : m)),
          );
        },
      });
    } catch (e) {
      setError(e.message || "Chat failed");
      setMessages((prev) => prev.filter((_, i) => i !== assistantIdx));
    } finally {
      setStreaming(false);
    }
  }

  function handleSubmit(e) {
    e.preventDefault();
    const text = input.trim();
    if (!text || streaming) return;
    setInput("");
    sendMessage(text, messages);
  }

  function handleKeyDown(e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  }

  return (
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{
          position: "fixed",
          inset: 0,
          background: "rgba(0,0,0,0.35)",
          zIndex: 200,
        }}
      />

      {/* Drawer panel */}
      <div
        style={{
          position: "fixed",
          top: 0,
          right: 0,
          bottom: 0,
          width: "min(480px, 95vw)",
          background: "var(--bg-panel, var(--surface))",
          borderLeft: "1px solid var(--border-strong)",
          boxShadow: "-4px 0 32px rgba(0,0,0,0.18)",
          zIndex: 201,
          display: "flex",
          flexDirection: "column",
        }}
      >
        {/* Header */}
        <div
          style={{
            padding: "16px 18px 14px",
            borderBottom: "1px solid var(--border)",
            display: "flex",
            alignItems: "flex-start",
            gap: 10,
          }}
        >
          <div style={{ flex: 1, minWidth: 0 }}>
            <p style={{ margin: 0, fontSize: 11, fontWeight: 600, color: "var(--accent)", textTransform: "uppercase", letterSpacing: "0.07em" }}>
              Remediation Chat
            </p>
            <p
              style={{
                margin: "3px 0 0",
                fontSize: 13,
                fontWeight: 500,
                color: "var(--text)",
                lineHeight: 1.4,
                overflow: "hidden",
                display: "-webkit-box",
                WebkitLineClamp: 2,
                WebkitBoxOrient: "vertical",
              }}
            >
              {action.action_text}
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            style={{
              flexShrink: 0,
              background: "none",
              border: "none",
              cursor: "pointer",
              color: "var(--muted)",
              fontSize: 20,
              lineHeight: 1,
              padding: "2px 4px",
              borderRadius: 4,
            }}
            aria-label="Close chat"
          >
            ×
          </button>
        </div>

        {/* Messages */}
        <div
          style={{
            flex: 1,
            overflowY: "auto",
            padding: "14px 16px",
          }}
        >
          {loadingHistory && (
            <p className="muted small" style={{ textAlign: "center", marginTop: 24 }}>
              Loading conversation…
            </p>
          )}
          {!loadingHistory && messages.length === 0 && !streaming && (
            <div style={{ textAlign: "center", marginTop: 32, padding: "0 16px" }}>
              <p style={{ fontSize: 13, fontWeight: 500, color: "var(--text)", margin: "0 0 6px" }}>
                Ask anything about this step
              </p>
              <p className="muted small" style={{ margin: 0, lineHeight: 1.5 }}>
                e.g. "What does this step involve?" or "Which tool should I use for this?"
              </p>
            </div>
          )}
          {messages.map((msg, i) => (
            <ChatMessage key={i} msg={msg} />
          ))}
          {error && (
            <p style={{ color: "var(--error, #e05)", fontSize: 12, marginTop: 8 }}>{error}</p>
          )}
          <div ref={bottomRef} />
        </div>

        {/* Input */}
        <form
          onSubmit={handleSubmit}
          style={{
            padding: "12px 14px",
            borderTop: "1px solid var(--border)",
            display: "flex",
            gap: 8,
            alignItems: "flex-end",
            background: "var(--surface)",
          }}
        >
          <textarea
            ref={inputRef}
            className="input"
            rows={2}
            placeholder="Ask a follow-up question…"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            disabled={streaming || loadingHistory}
            style={{
              flex: 1,
              resize: "none",
              fontSize: 13,
              marginTop: 0,
              minHeight: 44,
            }}
          />
          <button
            type="submit"
            className="btn"
            disabled={streaming || loadingHistory || !input.trim()}
            style={{ flexShrink: 0, alignSelf: "flex-end" }}
          >
            {streaming ? "…" : "Send"}
          </button>
        </form>
      </div>

      <style>{`
        @keyframes sentinel-blink {
          0% { opacity: 1; }
          50% { opacity: 0; }
        }
      `}</style>
    </>
  );
}


function EvidenceList({ items, title }) {
  if (!items || items.length === 0) {
    return (
      <div className="report-block">
        <h3>{title}</h3>
        <p className="muted small">None listed.</p>
      </div>
    );
  }
  const body = (
    <ul className="evidence-list">
      {items.map((item, index) => (
        <li key={`${index}-${item.slice(0, 12)}`}>{item}</li>
      ))}
    </ul>
  );
  if (items.length > 2) {
    return (
      <div className="report-block">
        <details className="report-drilldown" open>
          <summary className="report-drilldown-sum">{title}</summary>
          {body}
        </details>
      </div>
    );
  }
  return (
    <div className="report-block">
      <h3>{title}</h3>
      {body}
    </div>
  );
}

// ── Status + Severity metadata ────────────────────────────────────────────────

const STATUS_META = {
  pending:     { label: "Pending",     color: "var(--muted)",  hint: "Not started yet" },
  in_progress: { label: "In Progress", color: "var(--warn)",   hint: "Actively being worked on" },
  done:        { label: "Done",        color: "var(--ok)",     hint: "Completed" },
  skipped:     { label: "Skipped",     color: "var(--muted)",  hint: "Not applicable or intentionally bypassed" },
};

const SEVERITY_META = {
  critical: { label: "Critical", bg: "rgba(220,38,38,0.12)",  border: "rgba(220,38,38,0.4)",  text: "#dc2626" },
  high:     { label: "High",     bg: "rgba(234,88,12,0.12)",  border: "rgba(234,88,12,0.4)",  text: "#ea580c" },
  medium:   { label: "Medium",   bg: "rgba(202,138,4,0.12)",  border: "rgba(202,138,4,0.4)",  text: "#ca8a04" },
  low:      { label: "Low",      bg: "rgba(22,163,74,0.12)",  border: "rgba(22,163,74,0.4)",  text: "#16a34a" },
};

function SeverityBadge({ severity }) {
  const meta = SEVERITY_META[severity] || SEVERITY_META.medium;
  return (
    <span
      title={`Severity: ${meta.label} (assigned by AI)`}
      style={{
        background: meta.bg,
        border: `1px solid ${meta.border}`,
        borderRadius: 5,
        color: meta.text,
        fontSize: 10,
        fontWeight: 700,
        padding: "2px 6px",
        flexShrink: 0,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        userSelect: "none",
      }}
    >
      {meta.label}
    </span>
  );
}

// ── Remind Me modal ────────────────────────────────────────────────────────────

function RemindMeModal({ action, jobId, getToken, userProfile, onClose, followUps, onFollowUpsChange }) {
  const existingForAction = followUps.filter((f) => f.action_id === action.id);
  const [email, setEmail] = useState(userProfile?.email || "");
  const [name, setName] = useState(userProfile?.name || "");
  const [remindAt, setRemindAt] = useState("");
  const [message, setMessage] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  async function handleCreate(e) {
    e.preventDefault();
    if (!email || !remindAt) return;
    setSaving(true);
    setError("");
    try {
      const token = getToken ? await getToken() : null;
      await createFollowUp(jobId, {
        action_id: action.id,
        user_email: email,
        user_name: name || undefined,
        remind_at: new Date(remindAt).toISOString(),
        message: message || undefined,
      }, token);
      const updated = await fetchFollowUps(jobId, token);
      onFollowUpsChange(updated);
      setRemindAt("");
      setMessage("");
    } catch (err) {
      setError(err.message || "Failed to schedule reminder");
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete(fuId) {
    try {
      const token = getToken ? await getToken() : null;
      await deleteFollowUp(jobId, fuId, token);
      const updated = await fetchFollowUps(jobId, token);
      onFollowUpsChange(updated);
    } catch { /* best-effort */ }
  }

  return (
    <>
      <div onClick={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", zIndex: 300 }} />
      <div style={{
        position: "fixed",
        top: "50%",
        left: "50%",
        transform: "translate(-50%, -50%)",
        width: "min(440px, 94vw)",
        background: "var(--bg-panel, var(--surface))",
        border: "1px solid var(--border-strong)",
        borderRadius: "var(--radius, 12px)",
        boxShadow: "0 8px 40px rgba(0,0,0,0.22)",
        zIndex: 301,
        padding: "20px 22px",
        display: "flex",
        flexDirection: "column",
        gap: 14,
      }}>
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between" }}>
          <div>
            <p style={{ margin: 0, fontSize: 11, fontWeight: 700, color: "var(--accent)", textTransform: "uppercase", letterSpacing: "0.07em" }}>
              Follow-up Reminder
            </p>
            <p style={{ margin: "4px 0 0", fontSize: 13, fontWeight: 500, color: "var(--text)", lineHeight: 1.4, maxWidth: 340 }}>
              {action.action_text}
            </p>
          </div>
          <button type="button" onClick={onClose} style={{ background: "none", border: "none", cursor: "pointer", color: "var(--muted)", fontSize: 20, padding: "2px 4px", flexShrink: 0 }}>×</button>
        </div>

        {/* Existing reminders */}
        {existingForAction.length > 0 && (
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <p style={{ margin: 0, fontSize: 11, fontWeight: 600, color: "var(--muted)", textTransform: "uppercase" }}>Scheduled</p>
            {existingForAction.map((fu) => (
              <div key={fu.id} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", background: "var(--surface-2)", borderRadius: 6, padding: "7px 10px", fontSize: 12 }}>
                <div>
                  <span style={{ fontWeight: 500 }}>{new Date(fu.remind_at).toLocaleString()}</span>
                  {fu.sent_at && <span style={{ marginLeft: 8, color: "var(--ok)", fontSize: 11 }}>✓ Sent</span>}
                  {fu.message && <p style={{ margin: "2px 0 0", color: "var(--muted)", fontSize: 11 }}>{fu.message}</p>}
                </div>
                {!fu.sent_at && (
                  <button type="button" onClick={() => handleDelete(fu.id)} style={{ background: "none", border: "none", cursor: "pointer", color: "var(--muted)", fontSize: 16, padding: 0 }}>×</button>
                )}
              </div>
            ))}
          </div>
        )}

        <form onSubmit={handleCreate} style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <p style={{ margin: 0, fontSize: 11, fontWeight: 600, color: "var(--muted)", textTransform: "uppercase" }}>New Reminder</p>
          <div style={{ display: "flex", gap: 8 }}>
            <div style={{ flex: 1 }}>
              <label style={{ fontSize: 11, color: "var(--muted)", display: "block", marginBottom: 3 }}>Email *</label>
              <input className="input" type="email" required value={email} onChange={(e) => setEmail(e.target.value)} style={{ fontSize: 12, marginTop: 0 }} />
            </div>
            <div style={{ flex: 1 }}>
              <label style={{ fontSize: 11, color: "var(--muted)", display: "block", marginBottom: 3 }}>Name</label>
              <input className="input" type="text" value={name} onChange={(e) => setName(e.target.value)} style={{ fontSize: 12, marginTop: 0 }} />
            </div>
          </div>
          <div>
            <label style={{ fontSize: 11, color: "var(--muted)", display: "block", marginBottom: 3 }}>Remind at *</label>
            <input className="input" type="datetime-local" required value={remindAt} onChange={(e) => setRemindAt(e.target.value)} style={{ fontSize: 12, marginTop: 0 }} />
          </div>
          <div>
            <label style={{ fontSize: 11, color: "var(--muted)", display: "block", marginBottom: 3 }}>Message (optional)</label>
            <textarea className="input" rows={2} placeholder="Add context for this reminder…" value={message} onChange={(e) => setMessage(e.target.value)} style={{ fontSize: 12, resize: "vertical", marginTop: 0 }} />
          </div>
          {error && <p style={{ color: "var(--error, #e05)", fontSize: 12, margin: 0 }}>{error}</p>}
          <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
            <button type="button" className="btn btn-muted" onClick={onClose} style={{ fontSize: 12 }}>Cancel</button>
            <button type="submit" className="btn" disabled={saving || !email || !remindAt} style={{ fontSize: 12 }}>
              {saving ? "Saving…" : "Schedule Reminder"}
            </button>
          </div>
        </form>
      </div>
    </>
  );
}

// ── Action item (TODO card) ────────────────────────────────────────────────────

function ActionItem({ action, onStatusChange, onNotesSave, onDueDateChange, onChatOpen, onRemindOpen }) {
  const [notesOpen, setNotesOpen] = useState(false);
  const [noteDraft, setNoteDraft] = useState(action.notes || "");
  const [editingDue, setEditingDue] = useState(false);
  const [dueDraft, setDueDraft] = useState(action.due_date || "");
  const isDone = action.status === "done";
  const isSkipped = action.status === "skipped";
  const meta = STATUS_META[action.status] || STATUS_META.pending;

  const isOverdue = action.due_date && !isDone && new Date(action.due_date) < new Date();

  function handleCheckbox() {
    onStatusChange(action.id, isDone ? "pending" : "done");
  }

  function handleNotesBlur() {
    if (noteDraft !== (action.notes || "")) {
      onNotesSave(action.id, noteDraft);
    }
  }

  function handleDueBlur() {
    setEditingDue(false);
    if (dueDraft !== (action.due_date || "")) {
      onDueDateChange(action.id, dueDraft || null);
    }
  }

  return (
    <li
      style={{
        background: "var(--surface-2)",
        borderRadius: "var(--radius-sm)",
        padding: "10px 12px",
        opacity: isSkipped ? 0.5 : 1,
        display: "flex",
        flexDirection: "column",
        gap: 6,
      }}
    >
      {/* Top row: checkbox + text + severity + status */}
      <div style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
        <button
          type="button"
          onClick={handleCheckbox}
          title={isDone ? "Mark as pending" : "Mark as done"}
          style={{
            flexShrink: 0,
            marginTop: 2,
            width: 18,
            height: 18,
            borderRadius: 4,
            border: `2px solid ${isDone ? "var(--ok)" : "var(--border)"}`,
            background: isDone ? "var(--ok)" : "transparent",
            cursor: "pointer",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: 0,
          }}
        >
          {isDone && (
            <svg width="10" height="8" viewBox="0 0 10 8" fill="none">
              <path d="M1 4l3 3 5-6" stroke="#fff" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          )}
        </button>

        <span
          style={{
            flex: 1,
            fontSize: 13,
            lineHeight: 1.5,
            textDecoration: isDone ? "line-through" : "none",
            color: isDone || isSkipped ? "var(--muted)" : "var(--text)",
          }}
        >
          {action.action_text}
        </span>

        {/* Severity badge (AI-assigned, read-only) */}
        <SeverityBadge severity={action.severity || "medium"} />

        {/* Status dropdown */}
        <select
          value={action.status}
          onChange={(e) => onStatusChange(action.id, e.target.value)}
          title={meta.hint}
          style={{
            background: "var(--surface)",
            border: "1px solid var(--border)",
            borderRadius: 6,
            color: meta.color,
            fontSize: 11,
            fontWeight: 600,
            padding: "2px 6px",
            cursor: "pointer",
            flexShrink: 0,
          }}
        >
          {Object.entries(STATUS_META).map(([val, m]) => (
            <option key={val} value={val} title={m.hint}>{m.label}</option>
          ))}
        </select>
      </div>

      {/* Due date row */}
      <div style={{ paddingLeft: 28, display: "flex", alignItems: "center", gap: 6 }}>
        {!editingDue ? (
          <button
            type="button"
            onClick={() => setEditingDue(true)}
            style={{
              background: "none",
              border: "none",
              cursor: "pointer",
              fontSize: 11,
              color: isOverdue ? "var(--error, #dc2626)" : action.due_date ? "var(--warn)" : "var(--muted)",
              padding: 0,
              display: "flex",
              alignItems: "center",
              gap: 4,
            }}
          >
            <svg width="11" height="11" viewBox="0 0 16 16" fill="currentColor">
              <path d="M5 1v2H3a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h10a1 1 0 0 0 1-1V4a1 1 0 0 0-1-1h-2V1h-2v2H7V1H5zm-2 4h10v8H3V5zm4 2v2H5V7h2zm4 0v2H9V7h2z" />
            </svg>
            {action.due_date
              ? `Due ${new Date(action.due_date).toLocaleDateString()}${isOverdue ? " · overdue" : ""}`
              : "Set due date"}
          </button>
        ) : (
          <input
            type="date"
            className="input"
            value={dueDraft ? dueDraft.slice(0, 10) : ""}
            onChange={(e) => setDueDraft(e.target.value)}
            onBlur={handleDueBlur}
            autoFocus
            style={{ fontSize: 11, padding: "2px 6px", marginTop: 0, width: "auto" }}
          />
        )}
      </div>

      {/* Notes + optional Chat row */}
      <div style={{ paddingLeft: 28, display: "flex", alignItems: "flex-start", gap: 8 }}>
        <div style={{ flex: 1 }}>
          {!notesOpen && (
            <button
              type="button"
              onClick={() => setNotesOpen(true)}
              style={{
                background: "none",
                border: "none",
                cursor: "pointer",
                fontSize: 11,
                color: action.notes ? "var(--accent)" : "var(--muted)",
                padding: 0,
              }}
            >
              {action.notes ? `Note: ${action.notes}` : "+ Add note"}
            </button>
          )}
          {notesOpen && (
            <div style={{ display: "flex", gap: 6, alignItems: "flex-start" }}>
              <textarea
                className="input"
                rows={2}
                placeholder="Note why this was done, skipped, or what you found…"
                value={noteDraft}
                onChange={(e) => setNoteDraft(e.target.value)}
                style={{ fontSize: 12, resize: "vertical", flex: 1, marginTop: 0 }}
                autoFocus
              />
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <button
                  type="button"
                  onClick={() => { handleNotesBlur(); setNotesOpen(false); }}
                  className="btn btn-muted"
                  style={{ fontSize: 11, padding: "3px 8px", flexShrink: 0 }}
                >
                  Save
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setNoteDraft(action.notes || "");
                    setNotesOpen(false);
                  }}
                  className="btn btn-muted"
                  style={{
                    fontSize: 14,
                    padding: "0 8px",
                    flexShrink: 0,
                    color: "var(--error, #dc2626)",
                    fontWeight: "bold",
                  }}
                  title="Cancel"
                >
                  ×
                </button>
              </div>
            </div>
          )}
        </div>

        <div style={{ display: "flex", gap: 6, flexShrink: 0 }}>
          {/* Remind me button */}
          {onRemindOpen && (
            <button
              type="button"
              onClick={() => onRemindOpen(action)}
              title="Schedule a follow-up reminder"
              style={{
                display: "flex",
                alignItems: "center",
                gap: 4,
                background: "var(--surface)",
                border: "1px solid var(--border)",
                borderRadius: 6,
                color: "var(--muted)",
                fontSize: 11,
                fontWeight: 600,
                padding: "3px 10px",
                cursor: "pointer",
                whiteSpace: "nowrap",
              }}
            >
              <svg width="11" height="11" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 1a5 5 0 1 0 0 10A5 5 0 0 0 8 1zM0 8a8 8 0 1 1 16 0A8 8 0 0 1 0 8zm8-3v3.5l2.5 1.5-.75 1.25L7 9.5V5h1z" />
              </svg>
              Remind
            </button>
          )}

          {/* Chat button — only shown for recommended actions */}
          {onChatOpen && (
            <button
              type="button"
              onClick={() => onChatOpen(action)}
              title="Chat with AI about this step"
              style={{
                display: "flex",
                alignItems: "center",
                gap: 5,
                background: "var(--accent-dim, rgba(99,102,241,0.1))",
                border: "1px solid var(--accent)",
                borderRadius: 6,
                color: "var(--accent)",
                fontSize: 11,
                fontWeight: 600,
                padding: "3px 10px",
                cursor: "pointer",
                whiteSpace: "nowrap",
              }}
            >
              <svg width="12" height="12" viewBox="0 0 16 16" fill="currentColor">
                <path d="M2 2h12a1 1 0 0 1 1 1v8a1 1 0 0 1-1 1H5l-3 3V3a1 1 0 0 1 1-1z" />
              </svg>
              Chat
            </button>
          )}
        </div>
      </div>
    </li>
  );
}

// ── Remediation Checklist ──────────────────────────────────────────────────────

function RemediationChecklist({ jobId, getToken, userProfile }) {
  const [actions, setActions] = useState([]);
  const [loaded, setLoaded] = useState(false);
  const [chatAction, setChatAction] = useState(null);
  const [remindAction, setRemindAction] = useState(null);
  const [followUps, setFollowUps] = useState([]);

  useEffect(() => {
    if (!jobId) return;
    let cancel = false;
    (async () => {
      try {
        const token = getToken ? await getToken() : null;
        const [actionsData, fuData] = await Promise.all([
          fetchRemediationActions(jobId, token),
          fetchFollowUps(jobId, token).catch(() => []),
        ]);
        if (!cancel) {
          setActions(actionsData);
          setFollowUps(fuData);
        }
      } catch {
        /* best-effort */
      } finally {
        if (!cancel) setLoaded(true);
      }
    })();
    return () => { cancel = true; };
  }, [jobId, getToken]);

  async function changeStatus(actionId, newStatus) {
    try {
      const token = getToken ? await getToken() : null;
      await updateRemediationAction(jobId, actionId, { status: newStatus }, token);
      setActions((prev) => prev.map((a) => a.id === actionId ? { ...a, status: newStatus } : a));
    } catch { /* best-effort */ }
  }

  async function saveNotes(actionId, notes) {
    try {
      const token = getToken ? await getToken() : null;
      await updateRemediationAction(jobId, actionId, { notes }, token);
      setActions((prev) => prev.map((a) => a.id === actionId ? { ...a, notes } : a));
    } catch { /* best-effort */ }
  }

  async function changeDueDate(actionId, due_date) {
    try {
      const token = getToken ? await getToken() : null;
      await updateRemediationAction(jobId, actionId, { due_date: due_date || "" }, token);
      setActions((prev) => prev.map((a) => a.id === actionId ? { ...a, due_date } : a));
    } catch { /* best-effort */ }
  }

  if (!loaded || actions.length === 0) return null;

  const recommended = actions.filter((a) => a.action_type === "recommended");
  // Sort by severity: critical → high → medium → low
  const SORDER = { critical: 0, high: 1, medium: 2, low: 3 };
  recommended.sort((a, b) => (SORDER[a.severity] ?? 2) - (SORDER[b.severity] ?? 2));

  const doneCount = recommended.filter((a) => a.status === "done").length;
  const total = recommended.length;
  const pct = total > 0 ? Math.round((doneCount / total) * 100) : 0;

  if (!recommended.length) return null;

  return (
    <>
      <article className="card-elevated report-card" id="report-remediation-checklist">
        {/* Header + progress */}
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12, marginBottom: 10 }}>
          <div>
            <h2 style={{ margin: 0 }}>Remediation TODO</h2>
            <p className="muted small" style={{ margin: "3px 0 0" }}>
              {doneCount} of {total} step{total !== 1 ? "s" : ""} completed — click{" "}
              <strong style={{ color: "var(--accent)" }}>Chat</strong> on any step for AI-guided instructions
            </p>
          </div>
          <span
            style={{
              fontSize: 12,
              fontWeight: 700,
              color: pct === 100 ? "var(--ok)" : "var(--muted)",
              flexShrink: 0,
              alignSelf: "center",
            }}
          >
            {pct}%
          </span>
        </div>

        {/* Progress bar */}
        <div
          style={{
            height: 4,
            background: "var(--surface-2)",
            borderRadius: 2,
            marginBottom: 16,
            overflow: "hidden",
          }}
        >
          <div
            style={{
              height: "100%",
              width: `${pct}%`,
              background: pct === 100 ? "var(--ok)" : "var(--accent)",
              borderRadius: 2,
              transition: "width 0.3s ease",
            }}
          />
        </div>

        {/* Status legend */}
        <div
          style={{
            display: "flex",
            gap: 14,
            flexWrap: "wrap",
            marginBottom: 14,
            paddingBottom: 12,
            borderBottom: "1px solid var(--border)",
          }}
        >
          {Object.entries(STATUS_META).map(([val, m]) => (
            <span key={val} style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11 }}>
              <span
                style={{
                  display: "inline-block",
                  width: 8,
                  height: 8,
                  borderRadius: "50%",
                  background: m.color,
                  flexShrink: 0,
                }}
              />
              <span style={{ color: "var(--muted)" }}>
                <strong style={{ color: m.color }}>{m.label}</strong> — {m.hint}
              </span>
            </span>
          ))}
        </div>

        <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 6 }}>
          {recommended.map((action) => (
            <ActionItem
              key={action.id}
              action={action}
              onStatusChange={changeStatus}
              onNotesSave={saveNotes}
              onDueDateChange={changeDueDate}
              onChatOpen={setChatAction}
              onRemindOpen={setRemindAction}
            />
          ))}
        </ul>
      </article>

      {chatAction && (
        <ChatDrawer action={chatAction} jobId={jobId} getToken={getToken} onClose={() => setChatAction(null)} />
      )}
      {remindAction && (
        <RemindMeModal
          action={remindAction}
          jobId={jobId}
          getToken={getToken}
          userProfile={userProfile}
          followUps={followUps}
          onFollowUpsChange={setFollowUps}
          onClose={() => setRemindAction(null)}
        />
      )}
    </>
  );
}

// ── Immediate Checks card (no chat, sits above Guardrails) ─────────────────────

function ImmediateChecksCard({ jobId, getToken, userProfile }) {
  const [actions, setActions] = useState([]);
  const [loaded, setLoaded] = useState(false);
  const [remindAction, setRemindAction] = useState(null);
  const [followUps, setFollowUps] = useState([]);

  useEffect(() => {
    if (!jobId) return;
    let cancel = false;
    (async () => {
      try {
        const token = getToken ? await getToken() : null;
        const [data, fuData] = await Promise.all([
          fetchRemediationActions(jobId, token),
          fetchFollowUps(jobId, token).catch(() => []),
        ]);
        if (!cancel) {
          setActions(data.filter((a) => a.action_type === "check"));
          setFollowUps(fuData);
        }
      } catch {
        /* best-effort */
      } finally {
        if (!cancel) setLoaded(true);
      }
    })();
    return () => { cancel = true; };
  }, [jobId, getToken]);

  async function changeStatus(actionId, newStatus) {
    try {
      const token = getToken ? await getToken() : null;
      await updateRemediationAction(jobId, actionId, { status: newStatus }, token);
      setActions((prev) => prev.map((a) => a.id === actionId ? { ...a, status: newStatus } : a));
    } catch { /* best-effort */ }
  }

  async function saveNotes(actionId, notes) {
    try {
      const token = getToken ? await getToken() : null;
      await updateRemediationAction(jobId, actionId, { notes }, token);
      setActions((prev) => prev.map((a) => a.id === actionId ? { ...a, notes } : a));
    } catch { /* best-effort */ }
  }

  async function changeDueDate(actionId, due_date) {
    try {
      const token = getToken ? await getToken() : null;
      await updateRemediationAction(jobId, actionId, { due_date: due_date || "" }, token);
      setActions((prev) => prev.map((a) => a.id === actionId ? { ...a, due_date } : a));
    } catch { /* best-effort */ }
  }

  if (!loaded || actions.length === 0) return null;

  return (
    <>
      <article className="card-elevated report-card" id="report-immediate-checks">
        <h2>Immediate Checks</h2>
        <p className="muted small" style={{ margin: "-6px 0 12px" }}>
          Quick verifications to run now. Mark each as done when confirmed.
        </p>
        <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 6 }}>
          {actions.map((action) => (
            <ActionItem
              key={action.id}
              action={action}
              onStatusChange={changeStatus}
              onNotesSave={saveNotes}
              onDueDateChange={changeDueDate}
              onChatOpen={null}
              onRemindOpen={setRemindAction}
            />
          ))}
        </ul>
      </article>
      {remindAction && (
        <RemindMeModal
          action={remindAction}
          jobId={jobId}
          getToken={getToken}
          userProfile={userProfile}
          followUps={followUps}
          onFollowUpsChange={setFollowUps}
          onClose={() => setRemindAction(null)}
        />
      )}
    </>
  );
}

// ── Main export ────────────────────────────────────────────────────────────────

export default function AnalysisReport({
  result,
  getToken,
  userProfile = null,
  showExport = true,
  showRemediation = true,
  showGuardrails = true,
}) {
  const [exportToken, setExportToken] = useState(null);

  useEffect(() => {
    let cancel = false;
    if (!getToken) {
      setExportToken(null);
      return undefined;
    }
    (async () => {
      try {
        const t = await getToken();
        if (!cancel) setExportToken(t);
      } catch {
        if (!cancel) setExportToken(null);
      }
    })();
    return () => {
      cancel = true;
    };
  }, [getToken, result?.job_id]);

  async function exportAs(format) {
    const token = getToken ? exportToken ?? (await getToken()) : null;
    await downloadJobExport(result.job_id, format, token);
  }

  if (!result?.job_id) {
    return null;
  }

  if (!result?.analysis) {
    if (showExport) {
      return (
        <div className="report-grid" id="analysis-report">
          <div className="report-export card-elevated">
            <div>
              <h2 className="run-title" style={{ margin: 0 }}>
                Export
              </h2>
              <p className="muted small" style={{ margin: "4px 0 0" }}>
                Log stats and any partial analysis. JSON is always available; PDF follows the same payload.
              </p>
            </div>
            <div className="row gap wrap-actions">
              <button type="button" className="btn btn-secondary" onClick={() => exportAs("json")}>
                Download JSON
              </button>
              <button type="button" className="btn" onClick={() => exportAs("pdf")}>
                Download PDF
              </button>
            </div>
          </div>
        </div>
      );
    }
    return (
      <section className="report-empty card-elevated">
        <h2>Analysis report</h2>
        <p className="muted">Run an incident to generate remediation and guardrail details.</p>
      </section>
    );
  }

  const { analysis } = result;

  return (
    <div className="report-grid" id="analysis-report">
      {showExport ? (
        <div className="report-export card-elevated">
          <div>
            <h2 className="run-title" style={{ margin: 0 }}>
              Export
            </h2>
            <p className="muted small" style={{ margin: "4px 0 0" }}>
              Structured JSON for integrations; PDF for tickets and handoffs.
            </p>
          </div>
          <div className="row gap wrap-actions">
            <button type="button" className="btn btn-secondary" onClick={() => exportAs("json")}>
              Download JSON
            </button>
            <button type="button" className="btn" onClick={() => exportAs("pdf")}>
              Download PDF
            </button>
          </div>
        </div>
      ) : null}

      {/* Left column: Remediation TODO (recommended actions with Chat) */}
      {showRemediation ? (
        <RemediationChecklist
          jobId={result?.job_id}
          getToken={getToken}
          userProfile={userProfile}
        />
      ) : null}

      {/* Right column: Immediate Checks above Guardrails */}
      <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
        {showRemediation ? (
          <ImmediateChecksCard
            jobId={result?.job_id}
            getToken={getToken}
            userProfile={userProfile}
          />
        ) : null}

        {showGuardrails ? (
          <article className="card-elevated report-card report-guard">
            <h2>Guardrails</h2>
            <ul className="guard-stats">
              <li>
                Prompt injection: <strong>{analysis.guardrails.prompt_injection_detected ? "Yes" : "No"}</strong>
              </li>
              <li>
                Unsafe content removed: <strong>{analysis.guardrails.unsafe_content_removed ? "Yes" : "No"}</strong>
              </li>
              <li>
                Input truncated: <strong>{analysis.guardrails.input_truncated ? "Yes" : "No"}</strong>
              </li>
            </ul>
            {analysis.guardrails.notes?.length ? <EvidenceList items={analysis.guardrails.notes} title="Notes" /> : null}
          </article>
        ) : null}
      </div>

    </div>
  );
}
