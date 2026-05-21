"""Silver layer — clean / normalize / enrich.

Builds two tables per tenant:

- ``dim_materials``  : SCD Type 2 materials catalog, MERGE on (material, valid_from).
- ``fact_deliveries``: cleaned deliveries with CS→ST normalization, delivery-type
  flags, anomaly handling (quarantine + dedup + discard), and a temporal join
  against ``dim_materials`` (NOT ``is_current=true``).

Anomaly handling (sec 5.6):
- ``fecha_proceso`` null/invalid    → quarantine (reason='invalid_fecha_proceso')
- ``cantidad`` null / <= 0          → quarantine ('invalid_cantidad')
- ``precio`` null                   → quarantine ('null_precio')
- ``material`` not in catalog       → quarantine ('orphan_material')
- ``tipo_entrega`` not in valid set → discard (count only, no persist)
- Duplicado exacto                  → deduplicate (keep one)
"""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline.paths import layer_table_path, quarantine_path
from saas_pipeline.schemas import RAW_MATERIALS_SCHEMA
from saas_pipeline.utils import new_batch_id

DIM_TABLE = "dim_materials"
FACT_TABLE = "fact_deliveries"
BRONZE_TABLE = "deliveries"

# Discard sentinel used by _classify_anomalies. Rows with tipo_entrega outside the
# valid set are counted but NOT persisted (sec 5.6).
DISCARD_SENTINEL = "__discard__"


# ─────────────────────────────────────── dim_materials ────────────────────────


def run_dim_materials(spark: SparkSession, cfg: DictConfig, tenant: str, run_id: str) -> None:
    """Upsert the materials catalog as SCD Type 2 per tenant.

    Note: the source catalog is global; each tenant gets the same dimension table.
    A real platform would source from a tenant-specific feed.
    """
    src = (
        spark.read.option("header", True)
        .schema(RAW_MATERIALS_SCHEMA)
        .csv(cfg.paths.raw_materials)
        .withColumn("_tenant_id", F.lit(tenant))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_batch_id", F.lit(new_batch_id(tenant, "silver_dim")))
    )

    out_path = layer_table_path(cfg, layer="silver", tenant=tenant, table=DIM_TABLE)

    if not DeltaTable.isDeltaTable(spark, out_path):
        src.write.format("delta").mode("overwrite").save(out_path)
        print(f"[silver:dim_materials:{tenant}] initial write -> {out_path}")
        return

    tgt = DeltaTable.forPath(spark, out_path)
    (
        tgt.alias("t")
        .merge(
            src.alias("s"),
            "t.material = s.material AND t.valid_from = s.valid_from",
        )
        .whenMatchedUpdate(
            set={
                "descripcion": "s.descripcion",
                "categoria": "s.categoria",
                "precio_base": "s.precio_base",
                "valid_to": "s.valid_to",
                "is_current": "s.is_current",
                "_ingestion_timestamp": "s._ingestion_timestamp",
                "_batch_id": "s._batch_id",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[silver:dim_materials:{tenant}] merged -> {out_path}")


# ────────────────────────────────────── fact_deliveries ───────────────────────


def _read_bronze(spark: SparkSession, cfg: DictConfig, tenant: str) -> DataFrame:
    path = layer_table_path(cfg, layer="bronze", tenant=tenant, table=BRONZE_TABLE)
    df = spark.read.format("delta").load(path)
    # Restrict to current run's date range (range filter is informative; replaceWhere on
    # Bronze ensures the same set of partitions are present).
    start_compact = cfg.execution.start_date.replace("-", "")
    end_compact = cfg.execution.end_date.replace("-", "")
    return df.filter(
        (F.col("fecha_proceso") == "__invalid__")
        | ((F.col("fecha_proceso") >= start_compact) & (F.col("fecha_proceso") <= end_compact))
    )


def _classify_anomalies(df: DataFrame, valid_types: list[str]) -> DataFrame:
    """Tag each row with the first applicable anomaly reason (or null = clean)."""
    # See bronze.py: try_to_date for Databricks ANSI-mode compatibility.
    parsed_date = F.to_date(F.col("fecha_proceso"), "yyyyMMdd")
    reason = (
        F.when(
            F.col("fecha_proceso").isNull() | (F.col("fecha_proceso") == "__invalid__") | parsed_date.isNull(),
            F.lit("invalid_fecha_proceso"),
        )
        .when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), F.lit("invalid_cantidad"))
        .when(F.col("precio").isNull(), F.lit("null_precio"))
        # tipo_entrega outside valid set is a *discard*, not a quarantine.
        .when(~F.col("tipo_entrega").isin(valid_types), F.lit("__discard__"))
        .otherwise(F.lit(None).cast("string"))
    )
    return df.withColumn("_quarantine_reason", reason)


def _dedup_exact(df: DataFrame) -> DataFrame:
    """Drop full-row duplicates on the original business columns (ignore tech cols)."""
    cols = ["pais", "fecha_proceso", "transporte", "ruta", "tipo_entrega", "material", "precio", "cantidad", "unidad"]
    return df.dropDuplicates(cols)


def _normalize_units(df: DataFrame, cs_to_st: int) -> DataFrame:
    """Add ``cantidad_st`` regardless of unit. CS rows × factor; ST rows unchanged."""
    return df.withColumn(
        "cantidad_st",
        F.when(F.col("unidad") == "CS", F.col("cantidad") * F.lit(cs_to_st)).otherwise(F.col("cantidad")),
    )


def _add_flags(df: DataFrame, routine: list[str], bonus: list[str]) -> DataFrame:
    return df.withColumn("is_routine_delivery", F.col("tipo_entrega").isin(routine)).withColumn(
        "is_bonus_delivery", F.col("tipo_entrega").isin(bonus)
    )


def _temporal_join_materials(spark: SparkSession, cfg: DictConfig, tenant: str, fact: DataFrame) -> DataFrame:
    """Enrich fact rows with the catalog version valid at ``fecha_proceso``."""
    dim_path = layer_table_path(cfg, layer="silver", tenant=tenant, table=DIM_TABLE)
    dim = spark.read.format("delta").load(dim_path).select(
        "material",
        "descripcion",
        "categoria",
        F.col("precio_base").alias("precio_base_catalogo"),
        "valid_from",
        "valid_to",
    )
    fecha_date = F.to_date(F.col("fecha_proceso"), "yyyyMMdd")
    joined = (
        fact.alias("f")
        .join(
            dim.alias("d"),
            (F.col("f.material") == F.col("d.material"))
            & (fecha_date >= F.col("d.valid_from"))
            & (fecha_date <= F.col("d.valid_to")),
            "left",
        )
        .drop(F.col("d.material"))
    )
    return joined


def _flag_orphans(df: DataFrame) -> DataFrame:
    """After the temporal join, rows with no dim match (no version covers the date)
    get reason='orphan_material' (overrides existing reason only if currently clean).
    """
    return df.withColumn(
        "_quarantine_reason",
        F.when(
            F.col("_quarantine_reason").isNull() & F.col("descripcion").isNull(),
            F.lit("orphan_material"),
        ).otherwise(F.col("_quarantine_reason")),
    )


def _write_quarantine(df: DataFrame, cfg: DictConfig, tenant: str, run_id: str, batch_id: str) -> int:
    """Persist quarantined rows to the parallel quarantine table. Idempotent per tenant:
    each run overwrites the tenant's quarantine entirely, because quarantine is fully
    reconstructable from the source on every run (same as Gold).
    """
    q = (
        df.filter(F.col("_quarantine_reason").isNotNull() & (F.col("_quarantine_reason") != "__discard__"))
        .withColumn("_silver_run_id", F.lit(run_id))
        .withColumn("_silver_batch_id", F.lit(batch_id))
        .withColumn("_silver_ingestion_timestamp", F.current_timestamp())
    )
    cnt = q.count()
    path = quarantine_path(cfg, layer="silver", tenant=tenant, table=FACT_TABLE)
    if cnt == 0:
        # Still need to clear any prior quarantine state from previous runs (idempotent empty result).
        try:
            from delta.tables import DeltaTable

            if DeltaTable.isDeltaTable(df.sparkSession, path):
                df.sparkSession.sql(f"DELETE FROM delta.`{path}` WHERE _tenant_id = '{tenant}'")
        except Exception:
            pass
        return 0
    (
        q.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", f"_tenant_id = '{tenant}'")
        .save(path)
    )
    return cnt


def _write_fact(spark: SparkSession, df: DataFrame, cfg: DictConfig, tenant: str) -> None:
    """MERGE clean rows into Silver fact_deliveries on the business key."""
    out_path = layer_table_path(cfg, layer="silver", tenant=tenant, table=FACT_TABLE)
    if not DeltaTable.isDeltaTable(spark, out_path):
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("fecha_proceso")
            .save(out_path)
        )
        return

    tgt = DeltaTable.forPath(spark, out_path)
    key = (
        "t._tenant_id = s._tenant_id AND t.fecha_proceso = s.fecha_proceso "
        "AND t.transporte = s.transporte AND t.ruta = s.ruta "
        "AND t.material = s.material AND t.tipo_entrega = s.tipo_entrega"
    )
    (
        tgt.alias("t")
        .merge(df.alias("s"), key)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_fact_deliveries(spark: SparkSession, cfg: DictConfig, tenant: str, run_id: str) -> dict:
    """Build fact_deliveries for one tenant. Returns counters for quality_logs."""
    batch_id = new_batch_id(tenant, "silver_fact")
    bronze = _read_bronze(spark, cfg, tenant)

    classified = _classify_anomalies(bronze, list(cfg.business.valid_delivery_types))
    dedup = _dedup_exact(classified)

    total = dedup.count()
    discards = dedup.filter(F.col("_quarantine_reason") == "__discard__").count()

    # Enrich first so orphans become visible (temporal join also relies on dim table existing).
    enriched = _temporal_join_materials(spark, cfg, tenant, dedup)
    enriched = _flag_orphans(enriched)

    quarantined = _write_quarantine(enriched, cfg, tenant, run_id=run_id, batch_id=batch_id)

    clean = enriched.filter(F.col("_quarantine_reason").isNull())
    clean = _normalize_units(clean, int(cfg.business.cs_to_st_factor))
    clean = _add_flags(clean, list(cfg.business.routine_types), list(cfg.business.bonus_types))
    clean = (
        clean.withColumn("_silver_ingestion_timestamp", F.current_timestamp())
        .withColumn("_silver_batch_id", F.lit(batch_id))
        .withColumn("_silver_run_id", F.lit(run_id))
        .drop("_quarantine_reason")
    )

    _write_fact(spark, clean, cfg, tenant)

    counters = {
        "total_input": total,
        "discarded_invalid_type": discards,
        "quarantined": quarantined,
        "persisted": clean.count(),
    }
    print(f"[silver:fact_deliveries:{tenant}] {counters}")
    return counters


def now_utc_iso() -> str:
    """Return current UTC timestamp in ISO format — used by tests for determinism hooks."""
    return datetime.utcnow().isoformat()
