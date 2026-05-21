"""Spark-based tests for Silver transforms — unit conversion, anomaly classification, SCD2 join."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import Row

from saas_pipeline.silver import (
    _add_flags,
    _classify_anomalies,
    _dedup_exact,
    _flag_orphans,
    _normalize_units,
)
from tests.conftest import requires_spark


VALID_TYPES = ["ZPRE", "ZVE1", "Z04", "Z05"]


@requires_spark
def test_normalize_cs_to_st(spark) -> None:
    df = spark.createDataFrame(
        [
            Row(unidad="CS", cantidad=Decimal("3")),
            Row(unidad="ST", cantidad=Decimal("5")),
        ]
    )
    out = _normalize_units(df, 20).collect()
    cs_row = next(r for r in out if r.unidad == "CS")
    st_row = next(r for r in out if r.unidad == "ST")
    assert cs_row.cantidad_st == Decimal("60")
    assert st_row.cantidad_st == Decimal("5")


@requires_spark
def test_classify_anomalies_assigns_correct_reasons(spark) -> None:
    rows = [
        Row(pais="SV", fecha_proceso=None, transporte=1, ruta=1, tipo_entrega="ZPRE",
            material="A", precio=Decimal("10"), cantidad=Decimal("1"), unidad="ST"),
        Row(pais="SV", fecha_proceso="20250101", transporte=1, ruta=1, tipo_entrega="ZPRE",
            material="A", precio=Decimal("10"), cantidad=Decimal("0"), unidad="ST"),
        Row(pais="SV", fecha_proceso="20250101", transporte=1, ruta=1, tipo_entrega="ZPRE",
            material="A", precio=None, cantidad=Decimal("1"), unidad="ST"),
        Row(pais="SV", fecha_proceso="20250101", transporte=1, ruta=1, tipo_entrega="COBR",
            material="A", precio=Decimal("10"), cantidad=Decimal("1"), unidad="ST"),
        Row(pais="SV", fecha_proceso="20250101", transporte=1, ruta=1, tipo_entrega="ZPRE",
            material="A", precio=Decimal("10"), cantidad=Decimal("1"), unidad="ST"),
    ]
    df = spark.createDataFrame(rows)
    out = {r.tipo_entrega + "_" + str(r.cantidad) + "_" + str(r.precio) + "_" + str(r.fecha_proceso): r._quarantine_reason
           for r in _classify_anomalies(df, VALID_TYPES).collect()}
    # Note: keys collide; rebuild differently by enumerating
    out_list = _classify_anomalies(df, VALID_TYPES).collect()
    reasons = [r._quarantine_reason for r in out_list]
    assert "invalid_fecha_proceso" in reasons
    assert "invalid_cantidad" in reasons
    assert "null_precio" in reasons
    assert "__discard__" in reasons
    assert None in reasons  # the clean row


@requires_spark
def test_dedup_exact(spark) -> None:
    rows = [
        Row(pais="SV", fecha_proceso="20250101", transporte=1, ruta=1, tipo_entrega="ZPRE",
            material="A", precio=Decimal("10"), cantidad=Decimal("1"), unidad="ST"),
    ] * 3
    df = spark.createDataFrame(rows)
    assert _dedup_exact(df).count() == 1


@requires_spark
def test_add_flags(spark) -> None:
    rows = [
        Row(tipo_entrega="ZPRE"),
        Row(tipo_entrega="Z04"),
        Row(tipo_entrega="Z05"),
        Row(tipo_entrega="ZVE1"),
    ]
    df = spark.createDataFrame(rows)
    out = {(r.tipo_entrega, r.is_routine_delivery, r.is_bonus_delivery) for r in _add_flags(df, ["ZPRE", "ZVE1"], ["Z04", "Z05"]).collect()}
    assert ("ZPRE", True, False) in out
    assert ("Z04", False, True) in out
    assert ("ZVE1", True, False) in out
    assert ("Z05", False, True) in out


@requires_spark
def test_flag_orphans_marks_only_clean_rows(spark) -> None:
    rows = [
        # clean + no dim match → orphan
        Row(_quarantine_reason=None, descripcion=None),
        # already invalid_cantidad → stays
        Row(_quarantine_reason="invalid_cantidad", descripcion=None),
        # clean + dim match → stays clean
        Row(_quarantine_reason=None, descripcion="ok"),
    ]
    df = spark.createDataFrame(rows)
    out = [r._quarantine_reason for r in _flag_orphans(df).collect()]
    assert "orphan_material" in out
    assert "invalid_cantidad" in out
    assert None in out
