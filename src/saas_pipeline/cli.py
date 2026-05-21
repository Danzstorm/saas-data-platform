"""Pipeline CLI entrypoint (Typer) for local execution.

This is the *local* entrypoint. The Databricks job uses ``databricks_entrypoint.py``,
which shares the orchestration in ``pipeline.run_all`` but does its own Spark
bootstrap (no Delta extensions to wire, ANSI mode off, etc.).
"""

from __future__ import annotations

import sys

import typer
from rich.console import Console

from saas_pipeline import pipeline
from saas_pipeline.config import load_config
from saas_pipeline.spark import get_spark
from saas_pipeline.utils import new_run_id

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()


@app.command()
def version() -> None:
    """Print the package version."""
    from saas_pipeline import __version__

    console.print(f"saas-pipeline {__version__}")


@app.command()
def run(
    layer: str = typer.Option("all", help="bronze | silver | gold | all"),
    tenant: str = typer.Option("all", help="Tenant code (e.g. sv) or 'all'"),
    env: str = typer.Option("dev", help="Environment: dev | qa | main"),
    start_date: str = typer.Option(..., "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., "--end-date", help="YYYY-MM-DD"),
    fail_fast: bool = typer.Option(False, help="Abort on first tenant failure"),
    fail_on_critical: bool = typer.Option(False, help="Abort before Gold on critical DQ"),
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

    failures = pipeline.run_all(
        spark, cfg,
        layer=layer, tenant=tenant, run_id=run_id,
        log=lambda m: console.log(m),
    )

    spark.stop()

    if failures:
        console.log(f"[red]Failures: {failures}[/]")
        sys.exit(1)
    console.log("[bold green]Pipeline finished successfully[/]")


if __name__ == "__main__":
    app()
