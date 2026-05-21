"""Pure-Python utility tests (no Spark)."""

from __future__ import annotations

import pytest

from saas_pipeline.utils import date_range, new_batch_id, new_run_id, yyyymmdd_to_iso


def test_date_range_inclusive() -> None:
    out = date_range("2025-01-01", "2025-01-03")
    assert out == ["20250101", "20250102", "20250103"]


def test_date_range_single_day() -> None:
    assert date_range("2025-06-15", "2025-06-15") == ["20250615"]


def test_date_range_reverse_raises() -> None:
    with pytest.raises(ValueError):
        date_range("2025-02-01", "2025-01-01")


def test_yyyymmdd_to_iso() -> None:
    assert yyyymmdd_to_iso("20250314") == "2025-03-14"


def test_ids_unique() -> None:
    a = new_run_id()
    b = new_run_id()
    assert a != b
    c = new_batch_id("sv", "bronze")
    d = new_batch_id("sv", "bronze")
    assert c != d
