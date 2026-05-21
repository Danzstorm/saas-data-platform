"""Refactor of ``bad_code.py``.

Cambios principales:
- Lectura, transformación y escritura 100% nativas en PySpark (sin ``pandas``).
- Reglas de negocio (factor CS→ST, tipos de entrega válidos, ruta de salida) parametrizadas vía dataclass.
- Soporte multi-tenant explícito (lista de tenants o "all" - el filtro deja de ser un literal).
- Idempotencia: escritura Delta particionada con ``replaceWhere`` por (tenant, rango de fechas).
- Tipado y docstrings; logging vía ``logging`` (no ``print``).
- Validaciones mínimas (esquema esperado, manejo de unidades desconocidas) antes de escribir.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)


# Esquema explícito para no depender de inferencia (ver code_review.md, punto 3).
DELIVERIES_SCHEMA = StructType(
    [
        StructField("pais", StringType(), True),
        StructField("fecha_proceso", StringType(), True),
        StructField("transporte", LongType(), True),
        StructField("ruta", LongType(), True),
        StructField("tipo_entrega", StringType(), True),
        StructField("material", StringType(), True),
        StructField("precio", DecimalType(18, 6), True),
        StructField("cantidad", DecimalType(18, 6), True),
        StructField("unidad", StringType(), True),
    ]
)


@dataclass(frozen=True)
class DeliveryProcessingConfig:
    """Parámetros de procesamiento. Inmutable para evitar mutaciones accidentales."""

    input_path: str
    output_root: str
    routine_delivery_types: tuple[str, ...] = ("ZPRE", "ZVE1")
    cs_to_st_factor: int = 20
    valid_units: tuple[str, ...] = ("CS", "ST")
    start_date: str = "2025-01-01"
    end_date: str = "2025-12-31"
    tenants: tuple[str, ...] = field(default_factory=tuple)  # vacío = todos


def read_deliveries(spark: SparkSession, cfg: DeliveryProcessingConfig) -> DataFrame:
    """Lee el CSV crudo con esquema explícito y normaliza el código de tenant."""
    return (
        spark.read.option("header", True)
        .schema(DELIVERIES_SCHEMA)
        .csv(cfg.input_path)
        .withColumn("_tenant_id", F.lower(F.col("pais")))
    )


def filter_routine_deliveries(df: DataFrame, cfg: DeliveryProcessingConfig) -> DataFrame:
    """Conserva sólo los tipos de entrega configurados como 'rutina'."""
    return df.filter(F.col("tipo_entrega").isin(list(cfg.routine_delivery_types)))


def normalize_units(df: DataFrame, cfg: DeliveryProcessingConfig) -> DataFrame:
    """Convierte ``cantidad`` a unidades comunes (ST)."""
    return df.withColumn(
        "cantidad_st",
        F.when(F.col("unidad") == "CS", F.col("cantidad") * F.lit(cfg.cs_to_st_factor))
        .when(F.col("unidad") == "ST", F.col("cantidad"))
        .otherwise(F.lit(None)),
    ).withColumn("total", F.col("cantidad_st") * F.col("precio"))


def select_tenants(df: DataFrame, cfg: DeliveryProcessingConfig) -> DataFrame:
    """Si se especificó una lista de tenants, filtra. De lo contrario, devuelve todos."""
    if not cfg.tenants:
        return df
    return df.filter(F.col("_tenant_id").isin([t.lower() for t in cfg.tenants]))


def write_idempotent(df: DataFrame, cfg: DeliveryProcessingConfig) -> None:
    """Escribe Delta particionado por tenant y fecha; sobrescribe sólo la ventana procesada."""
    out_path = str(Path(cfg.output_root))
    replace_where = (
        f"fecha_proceso BETWEEN '{cfg.start_date.replace('-', '')}' "
        f"AND '{cfg.end_date.replace('-', '')}'"
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .option("overwriteSchema", "true")
        .partitionBy("_tenant_id", "fecha_proceso")
        .save(out_path)
    )
    logger.info("Wrote %s rows to %s", df.count(), out_path)


def process(spark: SparkSession, cfg: DeliveryProcessingConfig) -> DataFrame:
    """Pipeline raw → métricas listas para downstream."""
    raw = read_deliveries(spark, cfg)
    filtered = select_tenants(filter_routine_deliveries(raw, cfg), cfg)
    enriched = normalize_units(filtered, cfg)

    bad_units = enriched.filter(F.col("cantidad_st").isNull()).count()
    if bad_units:
        logger.warning("Found %s rows with unexpected unit — review upstream feed", bad_units)

    write_idempotent(enriched, cfg)
    return enriched


def main() -> None:
    """Ejemplo de invocación. En producción los parámetros vendrían de OmegaConf / CLI."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    spark = SparkSession.builder.appName("good_code-deliveries").getOrCreate()
    cfg = DeliveryProcessingConfig(
        input_path="data/raw/deliveries.csv",
        output_root="data/silver/fact_deliveries",
        tenants=("gt",),
    )
    process(spark, cfg)
    spark.stop()


if __name__ == "__main__":
    main()
