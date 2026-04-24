"""Manually enable or disable Sentinel product entitlements for a Clerk user."""

from __future__ import annotations

import argparse

from src.pathing import ensure_backend_root_on_path

ensure_backend_root_on_path()

from src.db import get_database


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set Sentinel subscription tier and premium feature entitlements for a user."
    )
    parser.add_argument(
        "--user-id",
        required=True,
        help="Clerk user id to update (the same value returned by /api/me as user_id).",
    )
    parser.add_argument(
        "--email",
        default=None,
        help="Optional email to persist alongside the user record.",
    )
    parser.add_argument(
        "--tier",
        default="pro",
        choices=["free", "pro", "enterprise"],
        help="Subscription tier to assign (default: pro).",
    )
    parser.add_argument(
        "--live-board",
        dest="live_board",
        action="store_true",
        help="Enable the Live Incident Board feature.",
    )
    parser.add_argument(
        "--no-live-board",
        dest="live_board",
        action="store_false",
        help="Disable the Live Incident Board feature.",
    )
    parser.set_defaults(live_board=True)
    args = parser.parse_args()

    db = get_database()
    try:
        db.upsert_user_entitlements(
            args.user_id,
            subscription_tier=args.tier,
            live_incident_board_enabled=args.live_board,
            email=args.email,
        )
        entitlements = db.get_user_entitlements(args.user_id)
    finally:
        db.close()

    print("---")
    print("ENTITLEMENT UPDATED")
    print("---")
    print(f"user_id: {args.user_id}")
    print(f"subscription_tier: {entitlements['subscription_tier']}")
    print(f"live_incident_board: {entitlements['features']['live_incident_board']}")
    print("---")
    print("You can now sign in with this user and open /live to verify the premium surface.")


if __name__ == "__main__":
    main()
