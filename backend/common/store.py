"""SQLite persistence layer for Sentinel MVP local and lambda testing."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

from common.models import IncidentAnalysis


class Database:
    """Lightweight database wrapper for incidents and analysis jobs."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS incidents (
                  id TEXT PRIMARY KEY,
                  clerk_user_id TEXT NOT NULL DEFAULT 'anonymous',
                  title TEXT,
                  source TEXT NOT NULL,
                  raw_text TEXT NOT NULL,
                  sanitized_text TEXT,
                  guardrail_json TEXT,
                  created_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY,
                  incident_id TEXT NOT NULL,
                  clerk_user_id TEXT NOT NULL DEFAULT 'anonymous',
                  status TEXT NOT NULL,
                  error_message TEXT,
                  analysis_json TEXT,
                  created_at TEXT NOT NULL,
                  completed_at TEXT,
                  FOREIGN KEY (incident_id) REFERENCES incidents(id)
                )
                """
            )
            self._ensure_column("incidents", "clerk_user_id", "TEXT NOT NULL DEFAULT 'anonymous'")
            self._ensure_column("jobs", "clerk_user_id", "TEXT NOT NULL DEFAULT 'anonymous'")
            self._ensure_column("jobs", "current_stage", "TEXT")
            self._ensure_column("jobs", "pipeline_events", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("jobs", "similar_incidents_json", "TEXT")
            self._ensure_column("incidents", "status", "TEXT NOT NULL DEFAULT 'open'")
            self._ensure_column("incidents", "assigned_to", "TEXT")
            self._ensure_column("incidents", "resolved_at", "TEXT")
            self._ensure_column("incidents", "resolution_notes", "TEXT")
            self._ensure_column("jobs", "clarification_answers_json", "TEXT")
            self._create_remediation_actions_table()
            self._create_integrations_table()
            self._create_chat_messages_table()
            self._create_follow_ups_table()
            self._ensure_column("remediation_actions", "severity", "TEXT NOT NULL DEFAULT 'medium'")
            self._ensure_column("remediation_actions", "due_date", "TEXT")

    def _create_remediation_actions_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS remediation_actions (
              id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL,
              action_text TEXT NOT NULL,
              action_type TEXT NOT NULL DEFAULT 'recommended',
              status TEXT NOT NULL DEFAULT 'pending',
              assigned_to TEXT,
              completed_at TEXT,
              notes TEXT,
              created_at TEXT NOT NULL,
              FOREIGN KEY (job_id) REFERENCES jobs(id)
            )
            """
        )

    def _create_follow_ups_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS follow_ups (
              id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL,
              action_id TEXT,
              clerk_user_id TEXT NOT NULL,
              user_email TEXT NOT NULL,
              user_name TEXT,
              message TEXT,
              remind_at TEXT NOT NULL,
              sent_at TEXT,
              created_at TEXT NOT NULL,
              FOREIGN KEY (job_id) REFERENCES jobs(id)
            )
            """
        )

    def _create_chat_messages_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_messages (
              id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL,
              action_id TEXT NOT NULL,
              role TEXT NOT NULL,
              content TEXT NOT NULL,
              created_at TEXT NOT NULL,
              FOREIGN KEY (job_id) REFERENCES jobs(id)
            )
            """
        )

    def _create_integrations_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS integrations (
              id TEXT PRIMARY KEY,
              clerk_user_id TEXT NOT NULL DEFAULT 'anonymous',
              type TEXT NOT NULL,
              config_json TEXT NOT NULL DEFAULT '{}',
              enabled INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL
            )
            """
        )

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        cols = [row["name"] for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if column not in cols:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def create_incident(
        self,
        text: str,
        title: str | None,
        source: str,
        clerk_user_id: str,
        sanitized_text: str | None = None,
        guardrail_json: dict | None = None,
    ) -> str:
        incident_id = str(uuid.uuid4())
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO incidents (id, clerk_user_id, title, source, raw_text, sanitized_text, guardrail_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    incident_id,
                    clerk_user_id,
                    title,
                    source,
                    text,
                    sanitized_text,
                    json.dumps(guardrail_json or {}),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        return incident_id

    def update_incident_sanitization(self, incident_id: str, sanitized_text: str, guardrail_json: dict) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE incidents SET sanitized_text=?, guardrail_json=? WHERE id=?",
                (sanitized_text, json.dumps(guardrail_json), incident_id),
            )

    def create_job(self, incident_id: str, clerk_user_id: str, status: str = "pending") -> str:
        job_id = str(uuid.uuid4())
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO jobs (id, incident_id, clerk_user_id, status, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, incident_id, clerk_user_id, status, datetime.now(timezone.utc).isoformat()),
            )
        return job_id

    def update_job_status(self, job_id: str, status: str, error_message: str | None = None) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE jobs SET status=?, error_message=? WHERE id=?",
                (status, error_message, job_id),
            )

    def set_job_stage(self, job_id: str, stage: str, detail: str | None = None) -> None:
        payload = detail or ""
        with self._conn:
            self._conn.execute(
                "UPDATE jobs SET current_stage=? WHERE id=?",
                (stage, job_id),
            )
            row = self._conn.execute(
                "SELECT pipeline_events FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            events: list[dict] = []
            if row and row["pipeline_events"]:
                try:
                    parsed = json.loads(row["pipeline_events"])
                    if isinstance(parsed, list):
                        events = parsed
                except json.JSONDecodeError:
                    events = []
            events.append(
                {
                    "stage": stage,
                    "detail": payload,
                    "at": datetime.now(timezone.utc).isoformat(),
                }
            )
            self._conn.execute(
                "UPDATE jobs SET pipeline_events=? WHERE id=?",
                (json.dumps(events), job_id),
            )

    def set_similar_incidents(self, job_id: str, similar: list[dict]) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE jobs SET similar_incidents_json=? WHERE id=?",
                (json.dumps(similar), job_id),
            )

    def save_analysis(self, job_id: str, analysis: IncidentAnalysis) -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE jobs
                SET status='completed', analysis_json=?, completed_at=?
                WHERE id=?
                """,
                (
                    analysis.model_dump_json(),
                    datetime.now(timezone.utc).isoformat(),
                    job_id,
                ),
            )

    def get_incident(self, incident_id: str, clerk_user_id: str | None = None) -> dict | None:
        if clerk_user_id:
            row = self._conn.execute(
                "SELECT * FROM incidents WHERE id=? AND clerk_user_id=?",
                (incident_id, clerk_user_id),
            ).fetchone()
        else:
            row = self._conn.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        return dict(row) if row else None

    def list_incidents(self, limit: int = 50, clerk_user_id: str | None = None) -> list[dict]:
        if clerk_user_id:
            rows = self._conn.execute(
                "SELECT * FROM incidents WHERE clerk_user_id=? ORDER BY created_at DESC LIMIT ?",
                (clerk_user_id, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM incidents ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_jobs(self, limit: int = 25, clerk_user_id: str | None = None) -> list[dict[str, Any]]:
        """Recent analysis jobs with incident title for dashboard listing."""

        if clerk_user_id:
            rows = self._conn.execute(
                """
                SELECT j.id AS job_id, j.incident_id, j.status, j.created_at, j.completed_at, i.title, i.source
                FROM jobs j
                JOIN incidents i ON i.id = j.incident_id
                WHERE j.clerk_user_id = ?
                ORDER BY j.created_at DESC
                LIMIT ?
                """,
                (clerk_user_id, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT j.id AS job_id, j.incident_id, j.status, j.created_at, j.completed_at, i.title, i.source
                FROM jobs j
                JOIN incidents i ON i.id = j.incident_id
                ORDER BY j.created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_job(self, job_id: str, clerk_user_id: str | None = None) -> dict | None:
        if clerk_user_id:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id=? AND clerk_user_id=?",
                (job_id, clerk_user_id),
            ).fetchone()
        else:
            row = self._conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def get_job_with_incident(self, job_id: str, clerk_user_id: str | None = None) -> dict | None:
        if clerk_user_id:
            row = self._conn.execute(
                """
                SELECT j.*, i.raw_text, i.title, i.source, i.sanitized_text, i.guardrail_json
                FROM jobs j
                JOIN incidents i ON i.id = j.incident_id
                WHERE j.id = ? AND j.clerk_user_id = ?
                """,
                (job_id, clerk_user_id),
            ).fetchone()
        else:
            row = self._conn.execute(
                """
                SELECT j.*, i.raw_text, i.title, i.source, i.sanitized_text, i.guardrail_json
                FROM jobs j
                JOIN incidents i ON i.id = j.incident_id
                WHERE j.id = ?
                """,
                (job_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_latest_job_for_incident(self, incident_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE incident_id=? ORDER BY created_at DESC LIMIT 1",
            (incident_id,),
        ).fetchone()
        return dict(row) if row else None

    def update_incident_status(
        self,
        incident_id: str,
        status: str,
        clerk_user_id: str | None = None,
    ) -> bool:
        resolved_at = datetime.now(timezone.utc).isoformat() if status == "resolved" else None
        with self._conn:
            if clerk_user_id:
                cur = self._conn.execute(
                    "UPDATE incidents SET status=?, resolved_at=COALESCE(?, resolved_at) WHERE id=? AND clerk_user_id=?",
                    (status, resolved_at, incident_id, clerk_user_id),
                )
            else:
                cur = self._conn.execute(
                    "UPDATE incidents SET status=?, resolved_at=COALESCE(?, resolved_at) WHERE id=?",
                    (status, resolved_at, incident_id),
                )
        return cur.rowcount > 0

    def update_incident_assign(
        self,
        incident_id: str,
        assigned_to: str | None,
        clerk_user_id: str | None = None,
    ) -> bool:
        with self._conn:
            if clerk_user_id:
                cur = self._conn.execute(
                    "UPDATE incidents SET assigned_to=? WHERE id=? AND clerk_user_id=?",
                    (assigned_to, incident_id, clerk_user_id),
                )
            else:
                cur = self._conn.execute(
                    "UPDATE incidents SET assigned_to=? WHERE id=?",
                    (assigned_to, incident_id),
                )
        return cur.rowcount > 0

    def seed_remediation_actions(
        self,
        job_id: str,
        actions: list[str],
        action_type: str = "recommended",
        severity: str = "medium",
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            for text in actions:
                self._conn.execute(
                    """
                    INSERT INTO remediation_actions (id, job_id, action_text, action_type, status, severity, created_at)
                    VALUES (?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (str(uuid.uuid4()), job_id, text, action_type, severity, now),
                )

    def list_remediation_actions(self, job_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM remediation_actions WHERE job_id=? ORDER BY created_at",
            (job_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def update_remediation_action(
        self,
        action_id: str,
        status: str | None = None,
        assigned_to: str | None = None,
        notes: str | None = None,
        severity: str | None = None,
        due_date: str | None = None,
    ) -> bool:
        parts: list[str] = []
        vals: list[Any] = []
        if status is not None:
            parts.append("status=?")
            vals.append(status)
            if status == "done":
                parts.append("completed_at=?")
                vals.append(datetime.now(timezone.utc).isoformat())
        if assigned_to is not None:
            parts.append("assigned_to=?")
            vals.append(assigned_to)
        if notes is not None:
            parts.append("notes=?")
            vals.append(notes)
        if severity is not None:
            parts.append("severity=?")
            vals.append(severity)
        if due_date is not None:
            parts.append("due_date=?")
            vals.append(due_date)
        if not parts:
            return False
        vals.append(action_id)
        with self._conn:
            cur = self._conn.execute(
                f"UPDATE remediation_actions SET {', '.join(parts)} WHERE id=?",  # noqa: S608
                vals,
            )
        return cur.rowcount > 0

    def create_integration(self, clerk_user_id: str, int_type: str, config: dict, enabled: bool = True) -> str:
        int_id = str(uuid.uuid4())
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO integrations (id, clerk_user_id, type, config_json, enabled, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int_id,
                    clerk_user_id,
                    int_type,
                    json.dumps(config),
                    1 if enabled else 0,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        return int_id

    def list_integrations(self, clerk_user_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM integrations WHERE clerk_user_id=? ORDER BY created_at DESC",
            (clerk_user_id,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["config"] = json.loads(d.pop("config_json", "{}"))
            except json.JSONDecodeError:
                d["config"] = {}
            d["enabled"] = bool(d.get("enabled", 1))
            out.append(d)
        return out

    def delete_integration(self, integration_id: str, clerk_user_id: str) -> bool:
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM integrations WHERE id=? AND clerk_user_id=?",
                (integration_id, clerk_user_id),
            )
        return cur.rowcount > 0

    def save_clarification_answers(self, job_id: str, answers: dict) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE jobs SET clarification_answers_json=? WHERE id=?",
                (json.dumps(answers), job_id),
            )

    def get_clarification_answers(self, job_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT clarification_answers_json FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        if not row or not row["clarification_answers_json"]:
            return None
        try:
            return json.loads(row["clarification_answers_json"])
        except json.JSONDecodeError:
            return None

    def delete_remediation_actions(self, job_id: str) -> None:
        with self._conn:
            self._conn.execute(
                "DELETE FROM remediation_actions WHERE job_id=?",
                (job_id,),
            )

    def update_analysis_remediation(self, job_id: str, remediation_json: str) -> None:
        """Patch the remediation field inside analysis_json for a completed job."""
        row = self._conn.execute(
            "SELECT analysis_json FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        if not row or not row["analysis_json"]:
            return
        try:
            analysis = json.loads(row["analysis_json"])
        except json.JSONDecodeError:
            return
        try:
            analysis["remediation"] = json.loads(remediation_json)
        except json.JSONDecodeError:
            return
        with self._conn:
            self._conn.execute(
                "UPDATE jobs SET analysis_json=? WHERE id=?",
                (json.dumps(analysis), job_id),
            )

    def create_follow_up(
        self,
        job_id: str,
        clerk_user_id: str,
        user_email: str,
        remind_at: str,
        action_id: str | None = None,
        user_name: str | None = None,
        message: str | None = None,
    ) -> str:
        fu_id = str(uuid.uuid4())
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO follow_ups
                  (id, job_id, action_id, clerk_user_id, user_email, user_name, message, remind_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fu_id, job_id, action_id, clerk_user_id,
                    user_email, user_name, message, remind_at,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        return fu_id

    def list_follow_ups(self, job_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM follow_ups WHERE job_id=? ORDER BY remind_at",
            (job_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def delete_follow_up(self, follow_up_id: str, clerk_user_id: str) -> bool:
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM follow_ups WHERE id=? AND clerk_user_id=?",
                (follow_up_id, clerk_user_id),
            )
        return cur.rowcount > 0

    def get_pending_follow_ups(self, before_iso: str) -> list[dict]:
        """Return unsent follow-ups whose remind_at is <= before_iso."""
        rows = self._conn.execute(
            "SELECT * FROM follow_ups WHERE sent_at IS NULL AND remind_at <= ? ORDER BY remind_at",
            (before_iso,),
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_follow_up_sent(self, follow_up_id: str) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE follow_ups SET sent_at=? WHERE id=?",
                (datetime.now(timezone.utc).isoformat(), follow_up_id),
            )

    def save_chat_message(self, job_id: str, action_id: str, role: str, content: str) -> str:
        msg_id = str(uuid.uuid4())
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO chat_messages (id, job_id, action_id, role, content, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (msg_id, job_id, action_id, role, content, datetime.now(timezone.utc).isoformat()),
            )
        return msg_id

    def list_chat_messages(self, job_id: str, action_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM chat_messages WHERE job_id=? AND action_id=? ORDER BY created_at",
            (job_id, action_id),
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        self._conn.close()
