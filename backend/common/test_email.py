"""Test suite for email notification utilities."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

# Load dotenv if running directly
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from common.email import send_follow_up_reminder

# --- CONFIGURATION FOR TESTING ---
# Set SHOULD_MOCK to False to send a real email using your RESEND_API_KEY
SHOULD_MOCK = False

# Change this to your actual email address when SHOULD_MOCK is False
TEST_RECIPIENT = "[EMAIL_ADDRESS]"
# ----------------------------------


def _run_mock_tests() -> None:
    """Verify logic using mocks (no network calls)."""
    # Setup mock environment
    os.environ["RESEND_API_KEY"] = "re_test_key"

    with patch("httpx.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        success = send_follow_up_reminder(
            to_email="test@example.com",
            action_text="Fix the broken database connection",
            remind_at="2024-04-23T12:00:00Z",
            to_name="Test Engineer",
            message="Don't forget to check the logs first!",
        )

        assert success is True
        assert mock_post.called

        # Verify headers and payload
        _, kwargs = mock_post.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer re_test_key"
        assert kwargs["json"]["to"] == "test@example.com"
        assert "database connection" in kwargs["json"]["subject"]
        assert "Don't forget" in kwargs["json"]["html"]

    # Test failure case (missing API key)
    with patch.dict(os.environ, {"RESEND_API_KEY": ""}):
        success = send_follow_up_reminder(
            to_email="test@example.com", action_text="Test", remind_at="now"
        )
        assert success is False

    print("[SUCCESS] Mock tests passed!")


def _run_live_test() -> None:
    """Perform a real API call to Resend."""
    import logging

    logging.basicConfig(level=logging.DEBUG)

    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        print("[ERROR] RESEND_API_KEY not found in environment. Cannot run live test.")
        return

    print(f"Attempting to send real email to {TEST_RECIPIENT}...")
    success = send_follow_up_reminder(
        to_email=TEST_RECIPIENT,
        action_text="Live Test: Verify Sentinel Email Delivery",
        remind_at="Just now (Live Test)",
        to_name="Sentinel Operator",
        message="This is a real test email from the Sentinel test suite to verify Resend integration.",
    )

    if success:
        print(f"[SUCCESS] Live email sent successfully to {TEST_RECIPIENT}!")
        print("Check your inbox (and spam folder).")
    else:
        print("[FAILURE] Live email delivery failed. Check the logs above for the error.")


def main() -> None:
    """Entry point for the test script."""
    if SHOULD_MOCK:
        _run_mock_tests()
    else:
        _run_live_test()


if __name__ == "__main__":
    main()
