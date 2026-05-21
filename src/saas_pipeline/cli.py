"""Pipeline CLI entrypoint (Typer)."""

from __future__ import annotations

import sys

import typer
from rich.console import Console

from saas_pipeline import bronze, gold, quality, silver
from saas_pipeline.config import list_known_tenants, load_config
from saas_pipeline.spark import get_spark
from saas_pipeline.utils import new_run_id

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()


@app.command()
def version() -> None:
    """Print the package version."""
    from saas_pipeline import __version__

    console.print(f"saas-pipeline {__version__}")


def _resolve_tenants(cfg, tenant: str) -> list[str]:
    if tenant == "all":
        return list_known_tenants(cfg)
    return [tenant.lower()]


@app.command()
def run(
    layer: str = typer.Option(
        "all", help="Which layer(s) to run: bronze | silver | gold | all"
    ),
    tenant: str = typer.Option("all", help="Tenant code (e.g. sv) or 'all'"),
    env: str = typer.Option("dev", help="Environment: dev | qa | main"),
    start_date: str = typer.Option(..., "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., "--end-date", help="YYYY-MM-DD"),
    fail_fast: bool = typer.Option(False, help="Abort on first tenant failure (when --tenant all)"),
    fail_on_critical: bool = typer.Option(
        False, help="Abort before Gold if any critical DQ check fails for the tenant"
    ),
) -> None:
    """Run one or more pipeline layers for a date range and tenant scope."""

    if layer not in {"bronze", "silver", "gold", "all"}:
        raise typer.BadParameter(f"Invalid --layer: {layer}")

    overrides = {
        "execution": {
            "start_date": start_date,
            "end_date": end_date,
            "tenant": tenant,
            "fail_fast": fail_fast,
        },
        "quality": {"fail_on_critical": fail_on_critical},
    }

    cfg = load_config(env=env, tenant=None, overrides=overrides)
    spark = get_spark(cfg)
    run_id = new_run_id()
    console.log(f"[bold cyan]run_id={run_id}[/] env={env} layer={layer} tenant={tenant}")

    tenants = _resolve_tenants(cfg, tenant)
    failures: list[tuple[str, str]] = []

    for t in tenants:
        try:
            console.rule(f"[bold]Tenant: {t}[/]")
            if layer in {"bronze", "all"}:
                bronze.run(spark, cfg, tenant=t, run_id=run_id)
            if layer in {"silver", "all"}:
                silver.run_dim_materials(spark, cfg, tenant=t, run_id=run_id)
                silver.run_fact_deliveries(spark, cfg, tenant=t, run_id=run_id)
                quality.run_silver_checks(spark, cfg, tenant=t, run_id=run_id)
            if layer in {"gold", "all"}:
                if quality.critical_failed(spark, cfg, tenant=t, run_id=run_id):
                    msg = f"Critical DQ check failed for tenant={t}; skipping Gold."
                    if cfg.quality.fail_on_critical:
                        raise RuntimeError(msg)
                    console.log(f"[yellow]{msg}[/]")
                else:
                    gold.run_daily_metrics(spark, cfg, tenant=t, run_id=run_id)
                    gold.run_top_materials_by_month(spark, cfg, tenant=t, run_id=run_id)
        except Exception as e:  # noqa: BLE001 — top-level CLI catches everything
            console.log(f"[red]Tenant {t} failed:[/] {e}")
            failures.append((t, str(e)))
            if cfg.execution.fail_fast:
                break

    spark.stop()

    if failures:
        console.log(f"[red]Failures: {failures}[/]")
        sys.exit(1)
    console.log("[bold green]Pipeline finished successfully[/]")


if __name__ == "__main__":
    app()
