"""Shared utilities — run/batch IDs, date helpers."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta


def new_run_id() -> str:
    """One per pipeline invocation. Used to correlate quality_logs rows."""
    return f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"


def new_batch_id(tenant: str, layer: str) -> str:
    """One per (run, tenant, layer) — written to bronze rows for traceability."""
    return (
        f"batch_{tenant}_{layer}_"
        f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S%f')}_{uuid.uuid4().hex[:6]}"
    )


def date_range(start_date: str, end_date: str) -> list[str]:
    """Yield ``fecha_proceso`` keys (YYYYMMDD) between start and end inclusive.

    Inputs are ISO ``YYYY-MM-DD``.
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError(f"end_date {end_date} precedes start_date {start_date}")
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return out


def yyyymmdd_to_iso(yyyymmdd: str) -> str:
    """Convert YYYYMMDD → YYYY-MM-DD."""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
