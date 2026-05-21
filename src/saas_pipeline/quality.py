"""Data quality checks + quality_logs Delta table.

Schema (sec 5.9):
  _run_id, _batch_id, tenant_id, layer, table_name, check_name, check_severity,
  records_checked, records_failed, check_passed, executed_at

Checks implemented over Silver:
  1. ``silver_no_orphan_in_fact`` (critical): rows in fact_deliveries must all have a non-null
     ``descripcion`` from the temporal join.
  2. ``silver_cantidad_positive`` (critical): cantidad_st > 0 for all persisted rows.
  3. ``silver_revenue_non_negative`` (warning): cantidad_st * precio >= 0.
  4. ``silver_dim_one_current_per_material`` (warning): exactly one ``is_current=true`` per material.

Persistence: append to ``data/shared/quality_logs/`` (Delta).
"""

from __future__ import annotations

from omegaconf import DictConfig
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from saas_pipeline.paths import layer_table_path, quality_logs_path
from saas_pipeline.utils import new_batch_id

QUALITY_SCHEMA = StructType(
    [
        StructField("_run_id", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("tenant_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_severity", StringType(), False),
        StructField("records_checked", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("check_passed", BooleanType(), False),
        StructField("executed_at", TimestampType(), False),
    ]
)

CRITICAL = "critical"
WARNING = "warning"
INFO = "info"


# ──────────────────────────────── check definitions ───────────────────────────


def _check_no_orphan_in_fact(fact: DataFrame) -> tuple[int, int]:
    checked = fact.count()
    failed = fact.filter(F.col("descripcion").isNull()).count()
    return checked, failed


def _check_cantidad_positive(fact: DataFrame) -> tuple[int, int]:
    checked = fact.count()
    failed = fact.filter((F.col("cantidad_st").isNull()) | (F.col("cantidad_st") <= 0)).count()
    return checked, failed


def _check_revenue_non_negative(fact: DataFrame) -> tuple[int, int]:
    checked = fact.count()
    failed = fact.filter((F.col("cantidad_st") * F.col("precio")) < 0).count()
    return checked, failed


def _check_dim_one_current_per_material(dim: DataFrame) -> tuple[int, int]:
    grouped = dim.groupBy("material").agg(F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("n_current"))
    checked = grouped.count()
    failed = grouped.filter(F.col("n_current") != 1).count()
    return checked, failed


# ──────────────────────────────── orchestration ───────────────────────────────


def _emit(
    rows: list[Row],
    spark: SparkSession,
    cfg: DictConfig,
) -> None:
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=QUALITY_SCHEMA)
    (df.write.format("delta").mode("append").save(quality_logs_path(cfg)))


def run_silver_checks(spark: SparkSession, cfg: DictConfig, tenant: str, run_id: str) -> list[dict]:
    """Execute all silver checks for a tenant, persist results, return as dicts."""
    batch_id = new_batch_id(tenant, "silver_quality")
    fact_path = layer_table_path(cfg, layer="silver", tenant=tenant, table="fact_deliveries")
    dim_path = layer_table_path(cfg, layer="silver", tenant=tenant, table="dim_materials")

    fact = spark.read.format("delta").load(fact_path)
    dim = spark.read.format("delta").load(dim_path)

    now = F.current_timestamp()  # noqa: F841 — placeholder if we need driver-side later
    import datetime as _dt

    executed_at = _dt.datetime.utcnow()

    specs = [
        ("silver_no_orphan_in_fact", CRITICAL, "fact_deliveries", *_check_no_orphan_in_fact(fact)),
        ("silver_cantidad_positive", CRITICAL, "fact_deliveries", *_check_cantidad_positive(fact)),
        ("silver_revenue_non_negative", WARNING, "fact_deliveries", *_check_revenue_non_negative(fact)),
        ("silver_dim_one_current_per_material", WARNING, "dim_materials", *_check_dim_one_current_per_material(dim)),
    ]

    rows: list[Row] = []
    results: list[dict] = []
    for name, sev, table, checked, failed in specs:
        passed = failed == 0
        row = Row(
            _run_id=run_id,
            _batch_id=batch_id,
            tenant_id=tenant,
            layer="silver",
            table_name=table,
            check_name=name,
            check_severity=sev,
            records_checked=int(checked),
            records_failed=int(failed),
            check_passed=passed,
            executed_at=executed_at,
        )
        rows.append(row)
        results.append(dict(row.asDict()))

    _emit(rows, spark, cfg)
    return results


def critical_failed(spark: SparkSession, cfg: DictConfig, tenant: str, run_id: str) -> bool:
    """Return True if any critical Silver check for this run/tenant failed."""
    path = quality_logs_path(cfg)
    try:
        logs = spark.read.format("delta").load(path)
    except Exception:
        return False
    return (
        logs.filter(
            (F.col("_run_id") == run_id)
            & (F.col("tenant_id") == tenant)
            & (F.col("check_severity") == CRITICAL)
            & (~F.col("check_passed"))
        ).count()
        > 0
    )
