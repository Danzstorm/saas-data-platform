"""Bronze layer — ingest raw CSV deliveries to Delta with overwrite-by-partition.

Per architecture (sec 5.2/5.4/5.5):
- Path: data/bronze/<tenant>/deliveries/fecha_proceso=YYYYMMDD/
- Partitioned by (fecha_proceso, tenant_id), although tenant_id is a single value per write.
- Schema preserved from source CSV plus technical columns prefixed with underscore.
- Idempotent via Delta ``replaceWhere`` over the (tenant_id, fecha_proceso) range.

Notes:
- ``pais`` from CSV (uppercase) is normalized to lowercase as ``_tenant_id``.
- Rows whose ``pais`` does not match the current tenant are filtered before write.
"""

from __future__ import annotations

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline.paths import layer_table_path
from saas_pipeline.schemas import RAW_DELIVERIES_SCHEMA
from saas_pipeline.utils import new_batch_id

TABLE = "deliveries"


def read_raw_deliveries(spark: SparkSession, cfg: DictConfig) -> DataFrame:
    """Read the raw deliveries CSV with an explicit schema."""
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(RAW_DELIVERIES_SCHEMA)
        .csv(cfg.paths.raw_deliveries)
    )


def _add_technical_columns(df: DataFrame, tenant: str, source_file: str, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_tenant_id", F.lit(tenant))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def _filter_to_tenant_and_range(
    df: DataFrame, tenant: str, start_date_iso: str, end_date_iso: str
) -> DataFrame:
    """Filter raw rows for this tenant and the requested fecha_proceso range.

    Rows whose ``fecha_proceso`` is null or fails ``YYYYMMDD`` parsing are kept here
    (so they can still flow to Bronze and be quarantined in Silver according to 5.6).
    But: a Bronze partition is keyed by ``fecha_proceso``, so we can only store
    rows that *have* a valid fecha_proceso. Invalid-date rows are routed to a
    dedicated ``fecha_proceso=__invalid__`` synthetic partition so Silver can pick
    them up later and quarantine them. Range filtering applies only to valid dates.
    """
    parsed = F.to_date(F.col("fecha_proceso"), "yyyyMMdd")
    is_invalid_date = parsed.isNull()
    in_range = (parsed >= F.lit(start_date_iso)) & (parsed <= F.lit(end_date_iso))

    return df.filter(F.upper(F.col("pais")) == F.lit(tenant.upper())).filter(
        is_invalid_date | in_range
    ).withColumn(
        "fecha_proceso",
        F.when(is_invalid_date, F.lit("__invalid__")).otherwise(F.col("fecha_proceso")),
    )


def run(spark: SparkSession, cfg: DictConfig, tenant: str, run_id: str) -> None:
    """Ingest deliveries for one tenant for the configured fecha_proceso range."""
    start_iso = cfg.execution.start_date
    end_iso = cfg.execution.end_date
    out_path = layer_table_path(cfg, layer="bronze", tenant=tenant, table=TABLE)
    batch_id = new_batch_id(tenant=tenant, layer="bronze")
    source_file = cfg.paths.raw_deliveries

    raw = read_raw_deliveries(spark, cfg)
    filtered = _filter_to_tenant_and_range(raw, tenant, start_iso, end_iso)
    enriched = _add_technical_columns(filtered, tenant, source_file, batch_id)

    if enriched.rdd.isEmpty():
        print(f"[bronze:{tenant}] no rows in range — skipping write")
        return

    # replaceWhere over the (tenant, range) slice → idempotent reprocesses.
    replace_where = (
        f"_tenant_id = '{tenant}' AND "
        f"(fecha_proceso BETWEEN '{start_iso.replace('-', '')}' "
        f"AND '{end_iso.replace('-', '')}' OR fecha_proceso = '__invalid__')"
    )

    (
        enriched.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", replace_where)
        .partitionBy("fecha_proceso", "_tenant_id")
        .save(out_path)
    )
    print(f"[bronze:{tenant}] wrote {out_path} batch_id={batch_id} run_id={run_id}")
