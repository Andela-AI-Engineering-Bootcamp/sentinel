"""Minimal schema models for Aurora-backed database utilities."""

from __future__ import annotations

from pydantic import BaseModel, Field


class IncidentRecord(BaseModel):
    incident_id: str
    title: str | None = Field(default=None)
    source: str
    created_at: str


class ClerkUserRecord(BaseModel):
    clerk_user_id: str
    email: str | None = None
    created_at: str
