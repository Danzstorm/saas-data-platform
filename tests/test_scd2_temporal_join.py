"""Validates SCD Type 2 temporal join — fact rows enriched with the catalog version
valid at fecha_proceso, NOT the current one. This is the eval criterion that explicitly
flags `is_current=true`-only joins as a bug.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import functions as F

from tests.conftest import requires_spark


@requires_spark
def test_temporal_join_picks_correct_version(spark) -> None:
    """Material AA004003 has two versions:
       v1: precio 31.95 valid 2024-01-01 → 2025-03-31 (is_current=false)
       v2: precio 33.80 valid 2025-04-01 → 9999-12-31 (is_current=true)

    A fact row at fecha_proceso=20250301 must match v1 (price 31.95), even though
    v2 is the current version.
    """
    dim = spark.createDataFrame(
        [
            Row(
                material="AA004003",
                precio_base=Decimal("31.95"),
                valid_from=dt.date(2024, 1, 1),
                valid_to=dt.date(2025, 3, 31),
                is_current=False,
            ),
            Row(
                material="AA004003",
                precio_base=Decimal("33.80"),
                valid_from=dt.date(2025, 4, 1),
                valid_to=dt.date(9999, 12, 31),
                is_current=True,
            ),
        ]
    )
    fact = spark.createDataFrame(
        [
            Row(material="AA004003", fecha_proceso="20250301"),
            Row(material="AA004003", fecha_proceso="20250501"),
        ]
    )

    fecha_date = F.to_date(F.col("f.fecha_proceso"), "yyyyMMdd")
    joined = (
        fact.alias("f")
        .join(
            dim.alias("d"),
            (F.col("f.material") == F.col("d.material"))
            & (fecha_date >= F.col("d.valid_from"))
            & (fecha_date <= F.col("d.valid_to")),
            "left",
        )
        .select("f.fecha_proceso", "d.precio_base")
        .collect()
    )
    by_fecha = {r.fecha_proceso: r.precio_base for r in joined}
    assert by_fecha["20250301"] == Decimal("31.95"), "Should match v1 (historical version)"
    assert by_fecha["20250501"] == Decimal("33.80"), "Should match v2 (current version)"


@requires_spark
def test_orphan_material_left_join_yields_null(spark) -> None:
    """A material absent from the catalog must produce a null match (caught downstream
    by the orphan-material quarantine rule).
    """
    dim = spark.createDataFrame(
        [
            Row(
                material="EXISTS",
                precio_base=Decimal("10.00"),
                valid_from=dt.date(2024, 1, 1),
                valid_to=dt.date(9999, 12, 31),
                is_current=True,
            )
        ]
    )
    fact = spark.createDataFrame([Row(material="GHOST", fecha_proceso="20250101")])

    fecha_date = F.to_date(F.col("f.fecha_proceso"), "yyyyMMdd")
    joined = (
        fact.alias("f")
        .join(
            dim.alias("d"),
            (F.col("f.material") == F.col("d.material"))
            & (fecha_date >= F.col("d.valid_from"))
            & (fecha_date <= F.col("d.valid_to")),
            "left",
        )
        .collect()
    )
    assert len(joined) == 1
    assert joined[0]["precio_base"] is None
