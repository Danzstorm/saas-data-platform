"""End-to-end pipeline test on a reduced 12-row fixture.

This is *not* a unit test — it actually runs Bronze → Silver → Gold against a
small CSV and asserts row counts and anomaly classification. It catches the
class of bugs that the granular tests miss (e.g. schema drift between layers,
broken replaceWhere expressions, wrong join key types).

Skipped when Java/Spark isn't available.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from omegaconf import OmegaConf

from saas_pipeline import bronze, gold, quality, silver
from tests.conftest import requires_spark

FIXTURES = Path(__file__).parent / "fixtures"


def _make_cfg(tmp: Path) -> OmegaConf:
    return OmegaConf.create(
        {
            "paths": {
                "bronze": str(tmp / "bronze"),
                "silver": str(tmp / "silver"),
                "gold": str(tmp / "gold"),
                "quarantine_root": str(tmp),
                "quality_logs": str(tmp / "shared" / "quality_logs"),
                "raw_deliveries": str(FIXTURES / "deliveries_sample.csv"),
                "raw_materials": str(FIXTURES / "materials_sample.csv"),
            },
            "execution": {"start_date": "2025-01-01", "end_date": "2025-12-31"},
            "business": {
                "cs_to_st_factor": 20,
                "valid_delivery_types": ["ZPRE", "ZVE1", "Z04", "Z05"],
                "routine_types": ["ZPRE", "ZVE1"],
                "bonus_types": ["Z04", "Z05"],
            },
            "quality": {"fail_on_critical": False},
            "spark": {
                "app_name": "saas-e2e",
                "shuffle_partitions": 2,
                "driver_memory": "1g",
                "log_level": "ERROR",
                "delta_extensions": True,
            },
        }
    )


@requires_spark
def test_pipeline_end_to_end_on_fixture(spark, tmp_path):
    """Corre el pipeline completo y valida conteos contra el fixture conocido."""
    cfg = _make_cfg(tmp_path)
    run_id = "test_run_001"

    # Bronze
    bronze.run(spark, cfg, tenant="pe", run_id=run_id)

    # Silver
    silver.run_dim_materials(spark, cfg, tenant="pe", run_id=run_id)
    silver.run_fact_deliveries(spark, cfg, tenant="pe", run_id=run_id)
    quality.run_silver_checks(spark, cfg, tenant="pe", run_id=run_id)

    # Gold
    gold.run_daily_metrics(spark, cfg, tenant="pe", run_id=run_id)

    # ─── Verificaciones ───────────────────────────────────────────────────────
    fact = spark.read.format("delta").load(f"{cfg.paths.silver}/pe/fact_deliveries")
    quar = spark.read.format("delta").load(f"{tmp_path}/silver_quarantine/pe/fact_deliveries")
    gold_t = spark.read.format("delta").load(f"{cfg.paths.gold}/pe/daily_metrics_by_delivery_type")
    qlogs = spark.read.format("delta").load(cfg.paths.quality_logs)

    # 12 filas raw → 1 Z99 descarte → 1 duplicado fusionado → 5 cuarentena (sin_precio, sin_cantidad, orphan, fecha_null, neg_cantidad) → 5 limpias
    # Nota: el duplicado exacto se contabiliza dentro del raw count pero dedup lo deja en 1.
    persisted = fact.count()
    quarantined = quar.count()
    assert persisted == 5, f"esperaba 5 filas persistidas, encontradas {persisted}"
    assert quarantined == 5, f"esperaba 5 cuarentena, encontradas {quarantined}"

    # SCD2: la fila de AA004003 del 20250301 debe matchear precio_base 31.95
    # y la del 20250501 debe matchear 33.80
    aa = (
        fact.filter(fact.material == "AA004003")
        .select("fecha_proceso", "precio_base_catalogo")
        .collect()
    )
    by_date = {r.fecha_proceso: float(r.precio_base_catalogo) for r in aa}
    assert by_date["20250301"] == 31.95
    assert by_date["20250501"] == 33.80

    # Gold tiene al menos una fila por (fecha, tipo) presente en fact
    assert gold_t.count() > 0

    # quality_logs tiene 4 filas (4 checks × 1 tenant × 1 run)
    assert qlogs.count() == 4
    # Crítico: cantidad_positive debe pasar (sin filas con qty<=0 en fact)
    critical_pass = qlogs.filter(
        (qlogs.check_name == "silver_cantidad_positive") & qlogs.check_passed
    ).count()
    assert critical_pass == 1


@requires_spark
def test_pipeline_idempotent_double_run(spark, tmp_path):
    """Re-correr el mismo rango no debe duplicar filas."""
    cfg = _make_cfg(tmp_path)
    run_id = "test_run_002"

    for _ in range(2):
        bronze.run(spark, cfg, tenant="pe", run_id=run_id)
        silver.run_dim_materials(spark, cfg, tenant="pe", run_id=run_id)
        silver.run_fact_deliveries(spark, cfg, tenant="pe", run_id=run_id)
        gold.run_daily_metrics(spark, cfg, tenant="pe", run_id=run_id)

    fact = spark.read.format("delta").load(f"{cfg.paths.silver}/pe/fact_deliveries")
    quar = spark.read.format("delta").load(f"{tmp_path}/silver_quarantine/pe/fact_deliveries")

    # Después de 2 runs, los conteos siguen iguales (MERGE / replaceWhere).
    assert fact.count() == 5
    assert quar.count() == 5

    # cleanup
    shutil.rmtree(tmp_path, ignore_errors=True)
