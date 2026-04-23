"""Adaptive background scheduler for processing follow-up reminders."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

from common.config import get_db_path, reminder_interval_seconds
from common.email import send_follow_up_reminder
from common.store import Database

logger = logging.getLogger(__name__)


class ReminderScheduler:
    """Manages a background thread that processes pending reminders.

    The thread runs only when there are pending or failed reminders to process.
    It goes dormant (exits) when the queue is clear.
    """

    _instance = None
    _lock = threading.Lock()
    _thread: threading.Thread | None = None

    @classmethod
    def get_instance(cls) -> ReminderScheduler:
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def ensure_running(self) -> None:
        """Wake up the scheduler if it's not already running."""
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run_loop, daemon=True)
                self._thread.start()
                logger.info("Background reminder scheduler woke up.")

    def _run_loop(self) -> None:
        """Main loop that keeps running as long as there is work to do."""
        interval = reminder_interval_seconds()

        while True:
            # Small delay before start to let DB transactions settle
            time.sleep(2)

            sent, failed = self.process_all_pending()

            # If no reminders were sent and none failed (meaning none were due), go dormant
            if sent == 0 and failed == 0:
                with self._lock:
                    self._thread = None
                    logger.info("No pending or failed reminders. Scheduler going dormant.")
                break

            # Wait for the configured interval before checking again
            logger.debug("Scheduler waiting %ds for next check...", interval)
            time.sleep(interval)

    def process_all_pending(self) -> tuple[int, int]:
        """Query the database and attempt to send all due follow-ups."""
        now_iso = datetime.now(timezone.utc).isoformat()
        db = Database(get_db_path())
        sent = 0
        failed = 0

        try:
            pending = db.get_pending_follow_ups(now_iso)
            if not pending:
                return 0, 0

            logger.info("Scheduler found %d pending reminders.", len(pending))

            for fu in pending:
                action_text = "Remediation action"
                if fu.get("action_id"):
                    actions = db.list_remediation_actions(fu["job_id"])
                    match = next((a for a in actions if a["id"] == fu["action_id"]), None)
                    if match:
                        action_text = match["action_text"]

                ok = send_follow_up_reminder(
                    to_email=fu["user_email"],
                    to_name=fu.get("user_name"),
                    action_text=action_text,
                    message=fu.get("message"),
                    remind_at=fu["remind_at"],
                )

                if ok:
                    db.mark_follow_up_sent(fu["id"])
                    sent += 1
                else:
                    failed += 1

            logger.info("Batch processed: %d sent, %d failed.", sent, failed)
            return sent, failed

        except Exception as e:
            logger.error("Error in background reminder loop: %s", str(e))
            return 0, 1  # Return failed=1 to keep the loop alive for a retry
        finally:
            db.close()
