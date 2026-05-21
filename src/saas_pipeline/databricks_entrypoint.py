"""Databricks job entrypoint.

Mirrors what ``cli.py`` does but uses the existing Spark session injected by the
Databricks runtime (no local SparkSession builder, no Delta-pip install). The
runtime already provides PySpark 3.5 and Delta Lake.

Invoked by the DAB job as a ``spark_python_task``.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _bootstrap_paths() -> Path:
    """Return the source directory containing the ``saas_pipeline`` package.

    Databricks serverless runs ``spark_python_task`` files through an ipykernel
    wrapper where ``__file__`` is not defined. We fall back to ``sys.argv[0]``
    (the script's invoked path) and walk up one directory.
    """
    if "__file__" in globals():
        here = Path(globals()["__file__"]).resolve()
    else:
        here = Path(sys.argv[0]).resolve()
    src_dir = here.parent.parent
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    # Config directory lives next to src/ in our deployed layout.
    if "SAAS_CONFIG_DIR" not in os.environ:
        candidate = src_dir.parent / "config"
        if candidate.exists():
            os.environ["SAAS_CONFIG_DIR"] = str(candidate)
    return src_dir


_bootstrap_paths()

from pyspark.sql import SparkSession  # noqa: E402

from saas_pipeline import bronze, gold, quality, silver  # noqa: E402
from saas_pipeline.config import list_known_tenants, load_config  # noqa: E402
from saas_pipeline.utils import new_run_id  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", default="all")
    parser.add_argument("--tenant", default="all")
    parser.add_argument("--env", default="databricks")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--fail-on-critical", action="store_true")
    args = parser.parse_args()

    overrides = {
        "execution": {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "tenant": args.tenant,
            "fail_fast": args.fail_fast,
        },
        "quality": {"fail_on_critical": args.fail_on_critical},
    }
    cfg = load_config(env=args.env, tenant=None, overrides=overrides)

    # Reuse the runtime's session (do NOT pip-install Delta or wire extensions).
    spark = SparkSession.builder.getOrCreate()
    run_id = new_run_id()
    print(f"[run] {run_id} env={args.env} layer={args.layer} tenant={args.tenant}")

    tenants = list_known_tenants(cfg) if args.tenant == "all" else [args.tenant.lower()]
    failures: list[tuple[str, str]] = []

    for t in tenants:
        try:
            print(f"=== tenant: {t} ===")
            if args.layer in {"bronze", "all"}:
                bronze.run(spark, cfg, tenant=t, run_id=run_id)
            if args.layer in {"silver", "all"}:
                silver.run_dim_materials(spark, cfg, tenant=t, run_id=run_id)
                silver.run_fact_deliveries(spark, cfg, tenant=t, run_id=run_id)
                quality.run_silver_checks(spark, cfg, tenant=t, run_id=run_id)
            if args.layer in {"gold", "all"}:
                if quality.critical_failed(spark, cfg, tenant=t, run_id=run_id):
                    msg = f"Critical DQ failed for tenant={t}; skipping Gold."
                    if cfg.quality.fail_on_critical:
                        raise RuntimeError(msg)
                    print(msg)
                else:
                    gold.run_daily_metrics(spark, cfg, tenant=t, run_id=run_id)
                    gold.run_top_materials_by_month(spark, cfg, tenant=t, run_id=run_id)
        except Exception as e:  # noqa: BLE001
            print(f"[error] tenant {t}: {e}")
            failures.append((t, str(e)))
            if cfg.execution.fail_fast:
                break

    if failures:
        print(f"[failures] {failures}")
        sys.exit(1)
    print("[done] pipeline finished")


if __name__ == "__main__":
    main()
