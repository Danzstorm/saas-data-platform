"""Gold layer — business aggregates.

``daily_metrics_by_delivery_type`` per tenant. Granularity: (tenant_id, fecha_proceso, tipo_entrega).
Metrics:
  - total_units       : sum(cantidad_st)
  - total_revenue     : sum(cantidad_st * precio)     ← precio of the transaction, NOT precio_base.
  - active_routes     : count distinct(ruta)
  - active_transports : count distinct(transporte)

Strategy: full recompute by date partition (idempotent — Gold is derivative, not authoritative).
"""

from __future__ import annotations

from omegaconf import DictConfig
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.paths import layer_table_path

SILVER_TABLE = "fact_deliveries"
GOLD_TABLE = "daily_metrics_by_delivery_type"
GOLD_TOP_MATERIALS = "top_materials_by_month"


def run_daily_metrics(spark: SparkSession, cfg: DictConfig, tenant: str, run_id: str) -> None:
    src_path = layer_table_path(cfg, layer="silver", tenant=tenant, table=SILVER_TABLE)
    fact = spark.read.format("delta").load(src_path)

    start_compact = cfg.execution.start_date.replace("-", "")
    end_compact = cfg.execution.end_date.replace("-", "")
    fact = fact.filter(
        (F.col("fecha_proceso") >= start_compact) & (F.col("fecha_proceso") <= end_compact)
    )

    agg = (
        fact.groupBy("_tenant_id", "fecha_proceso", "tipo_entrega")
        .agg(
            F.sum("cantidad_st").alias("total_units"),
            F.sum(F.col("cantidad_st") * F.col("precio")).alias("total_revenue"),
            F.countDistinct("ruta").alias("active_routes"),
            F.countDistinct("transporte").alias("active_transports"),
        )
        .withColumn("_gold_run_id", F.lit(run_id))
        .withColumn("_gold_ingestion_timestamp", F.current_timestamp())
    )

    out_path = layer_table_path(cfg, layer="gold", tenant=tenant, table=GOLD_TABLE)

    # Replace only the date range we recomputed → idempotent reprocesses.
    replace_where = f"fecha_proceso BETWEEN '{start_compact}' AND '{end_compact}'"
    (
        agg.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", replace_where)
        .partitionBy("fecha_proceso")
        .save(out_path)
    )
    print(f"[gold:daily_metrics:{tenant}] wrote {out_path}")


def run_top_materials_by_month(spark: SparkSession, cfg: DictConfig, tenant: str, run_id: str) -> None:
    """Bonus Gold #2: top 10 materiales por revenue, por (tenant, año-mes).

    Granularidad: una fila por (tenant_id, year_month, rank). Útil para dashboards
    que muestran "qué se vende más" mes a mes.

    Uso típico (downstream):
        SELECT * FROM gold_<tenant>.top_materials_by_month
        WHERE year_month = '2025-03' ORDER BY rank LIMIT 10;
    """
    from pyspark.sql.window import Window

    src_path = layer_table_path(cfg, layer="silver", tenant=tenant, table=SILVER_TABLE)
    fact = spark.read.format("delta").load(src_path)

    start_compact = cfg.execution.start_date.replace("-", "")
    end_compact = cfg.execution.end_date.replace("-", "")
    fact = fact.filter(
        (F.col("fecha_proceso") >= start_compact) & (F.col("fecha_proceso") <= end_compact)
    ).withColumn("year_month", F.substring("fecha_proceso", 1, 6))

    agg = fact.groupBy("_tenant_id", "year_month", "material", "descripcion", "categoria").agg(
        F.sum("cantidad_st").alias("total_units"),
        F.sum(F.col("cantidad_st") * F.col("precio")).alias("total_revenue"),
    )

    w = Window.partitionBy("_tenant_id", "year_month").orderBy(F.col("total_revenue").desc())
    ranked = (
        agg.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 10)
        .withColumn("_gold_run_id", F.lit(run_id))
        .withColumn("_gold_ingestion_timestamp", F.current_timestamp())
    )

    out_path = layer_table_path(cfg, layer="gold", tenant=tenant, table=GOLD_TOP_MATERIALS)
    # Replace only the months we recomputed → idempotent for the requested window.
    months = (
        fact.select("year_month").distinct().collect()
    )
    if not months:
        print(f"[gold:top_materials:{tenant}] no data in range — skipping")
        return
    quoted = ", ".join(f"'{r.year_month}'" for r in months)
    replace_where = f"year_month IN ({quoted})"

    (
        ranked.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", replace_where)
        .partitionBy("year_month")
        .save(out_path)
    )
    print(f"[gold:top_materials:{tenant}] wrote {out_path}")
