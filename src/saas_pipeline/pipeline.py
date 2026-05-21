"""Orchestration loop shared by the local CLI and the Databricks entrypoint.

The two entrypoints differ only in how they bootstrap Spark + paths. The
business-level orchestration (for each tenant, run Bronze → Silver → quality
gate → Gold) lives here so there is a single source of truth.
"""

from __future__ import annotations

from collections.abc import Callable

from omegaconf import DictConfig
from pyspark.sql import SparkSession

from saas_pipeline import bronze, gold, quality, silver
from saas_pipeline.config import list_known_tenants

Logger = Callable[[str], None]


def _default_logger(msg: str) -> None:
    print(msg)


def resolve_tenants(cfg: DictConfig, tenant: str) -> list[str]:
    """Expand ``'all'`` to the configured tenant list, or return ``[tenant]``."""
    if tenant == "all":
        return list_known_tenants(cfg)
    return [tenant.lower()]


def run_tenant(
    spark: SparkSession,
    cfg: DictConfig,
    *,
    layer: str,
    tenant: str,
    run_id: str,
    log: Logger = _default_logger,
) -> None:
    """Execute the requested layer(s) for a single tenant.

    Raises any exception thrown by an inner step — caller decides whether to
    abort or move on to the next tenant.
    """
    log(f"=== tenant: {tenant} ===")
    if layer in {"bronze", "all"}:
        bronze.run(spark, cfg, tenant=tenant, run_id=run_id)
    if layer in {"silver", "all"}:
        silver.run_dim_materials(spark, cfg, tenant=tenant, run_id=run_id)
        silver.run_fact_deliveries(spark, cfg, tenant=tenant, run_id=run_id)
        quality.run_silver_checks(spark, cfg, tenant=tenant, run_id=run_id)
    if layer in {"gold", "all"}:
        if quality.critical_failed(spark, cfg, tenant=tenant, run_id=run_id):
            msg = f"Critical DQ check failed for tenant={tenant}; skipping Gold."
            if cfg.quality.fail_on_critical:
                raise RuntimeError(msg)
            log(msg)
        else:
            gold.run_daily_metrics(spark, cfg, tenant=tenant, run_id=run_id)
            gold.run_top_materials_by_month(spark, cfg, tenant=tenant, run_id=run_id)


def run_all(
    spark: SparkSession,
    cfg: DictConfig,
    *,
    layer: str,
    tenant: str,
    run_id: str,
    log: Logger = _default_logger,
) -> list[tuple[str, str]]:
    """Run ``layer`` for one or many tenants.

    Returns a list of ``(tenant, error_message)`` for failed tenants. When
    ``cfg.execution.fail_fast`` is set, returns after the first failure.
    """
    tenants = resolve_tenants(cfg, tenant)
    failures: list[tuple[str, str]] = []
    for t in tenants:
        try:
            run_tenant(spark, cfg, layer=layer, tenant=t, run_id=run_id, log=log)
        except Exception as e:  # noqa: BLE001 — top-level orchestration catches everything
            log(f"[error] tenant {t}: {e}")
            failures.append((t, str(e)))
            if cfg.execution.fail_fast:
                break
    return failures
