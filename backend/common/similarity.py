"""Lightweight lexical similarity for institutional memory (no extra ML deps)."""

from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any


_TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall((text or "").lower())


def bow_counter(tokens: list[str]) -> Counter[str]:
    return Counter(tokens)


def cosine_bow(a: Counter[str], b: Counter[str]) -> float:
    if not a or not b:
        return 0.0
    common = set(a) & set(b)
    dot = sum(a[t] * b[t] for t in common)
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def incident_text(row: dict) -> str:
    st = (row.get("sanitized_text") or "").strip()
    if st:
        return st
    return (row.get("raw_text") or "").strip()


def find_similar_incidents(
    reference_text: str,
    candidates: list[dict],
    exclude_id: str,
    *,
    limit: int = 5,
    min_score: float = 0.12,
) -> list[dict[str, Any]]:
    """Return top similar past incidents with score and naive token diff highlights."""

    ref_tokens = tokenize(reference_text)
    ref_bow = bow_counter(ref_tokens)
    ref_set = set(ref_tokens)
    scored: list[tuple[float, dict]] = []

    for row in candidates:
        iid = row.get("id")
        if not iid or iid == exclude_id:
            continue
        body = incident_text(row)
        if not body.strip():
            continue
        cand_bow = bow_counter(tokenize(body))
        score = cosine_bow(ref_bow, cand_bow)
        if score < min_score:
            continue
        scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)
    out: list[dict[str, Any]] = []
    for score, row in scored[:limit]:
        body = incident_text(row)
        cand_set = set(tokenize(body))
        added = sorted(ref_set - cand_set)[:12]
        missing = sorted(cand_set - ref_set)[:12]
        out.append(
            {
                "incident_id": row["id"],
                "title": row.get("title"),
                "source": row.get("source"),
                "created_at": row.get("created_at"),
                "similarity": round(score, 4),
                "diff": {
                    "tokens_new_vs_match": added,
                    "tokens_in_match_not_in_current": missing,
                },
            }
        )
    return out
