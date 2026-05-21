"""Explicit schemas for raw inputs — avoids schema inference surprises."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# global_mobility_data_entrega_productos.csv
RAW_DELIVERIES_SCHEMA = StructType(
    [
        StructField("pais", StringType(), True),
        StructField("fecha_proceso", StringType(), True),  # YYYYMMDD; some rows nullable/invalid
        StructField("transporte", LongType(), True),
        StructField("ruta", LongType(), True),
        StructField("tipo_entrega", StringType(), True),
        StructField("material", StringType(), True),
        StructField("precio", DecimalType(18, 6), True),
        StructField("cantidad", DecimalType(18, 6), True),
        StructField("unidad", StringType(), True),
    ]
)

# materials_catalog.csv
RAW_MATERIALS_SCHEMA = StructType(
    [
        StructField("material", StringType(), True),
        StructField("descripcion", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("precio_base", DecimalType(18, 6), True),
        StructField("valid_from", DateType(), True),
        StructField("valid_to", DateType(), True),
        StructField("is_current", BooleanType(), True),
    ]
)
