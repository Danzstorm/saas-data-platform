"""Validates quality_logs schema and the check_passed flag computation."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import Row

from saas_pipeline.quality import (
    _check_cantidad_positive,
    _check_dim_one_current_per_material,
    _check_no_orphan_in_fact,
    _check_revenue_non_negative,
)
from tests.conftest import requires_spark


@requires_spark
def test_no_orphan_check_detects_nulls(spark) -> None:
    df = spark.createDataFrame(
        [
            Row(descripcion="ok"),
            Row(descripcion=None),
            Row(descripcion="ok"),
        ]
    )
    checked, failed = _check_no_orphan_in_fact(df)
    assert checked == 3
    assert failed == 1


@requires_spark
def test_cantidad_positive_check(spark) -> None:
    df = spark.createDataFrame(
        [Row(cantidad_st=Decimal("1")), Row(cantidad_st=Decimal("0")), Row(cantidad_st=Decimal("-1"))]
    )
    checked, failed = _check_cantidad_positive(df)
    assert checked == 3
    assert failed == 2


@requires_spark
def test_revenue_non_negative(spark) -> None:
    df = spark.createDataFrame(
        [
            Row(cantidad_st=Decimal("1"), precio=Decimal("10")),
            Row(cantidad_st=Decimal("-1"), precio=Decimal("10")),
        ]
    )
    checked, failed = _check_revenue_non_negative(df)
    assert checked == 2
    assert failed == 1


@requires_spark
def test_dim_one_current_per_material(spark) -> None:
    df = spark.createDataFrame(
        [
            Row(material="A", is_current=True),
            Row(material="A", is_current=False),
            Row(material="B", is_current=True),
            Row(material="B", is_current=True),  # double-current → fail
        ]
    )
    checked, failed = _check_dim_one_current_per_material(df)
    assert checked == 2  # 2 distinct materials
    assert failed == 1   # only B fails
