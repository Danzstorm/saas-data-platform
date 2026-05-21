"""Databricks job entrypoint.

The orchestration loop lives in ``saas_pipeline.pipeline.run_all`` — this file
only handles what's specific to Databricks:

1. ``__file__`` doesn't exist under ipykernel (serverless wraps the script);
   fall back to ``sys.argv[0]`` and add the package's parent to ``sys.path``.
2. The runtime already ships PySpark + Delta — don't try to ``pip-install``
   Delta extensions on top.
3. Databricks runs ANSI=on by default; turn it off so ``to_date`` returns NULL
   on invalid strings instead of raising. Single source of truth with local.
4. Config files live next to ``src/`` in the deployed layout — point
   ``SAAS_CONFIG_DIR`` at them.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _bootstrap_paths() -> Path:
    """Locate the source directory and wire it into ``sys.path``.

    Under Databricks serverless ``__file__`` is undefined; we fall back to
    ``sys.argv[0]``.
    """
    if "__file__" in globals():
        here = Path(globals()["__file__"]).resolve()
    else:
        here = Path(sys.argv[0]).resolve()
    src_dir = here.parent.parent
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    if "SAAS_CONFIG_DIR" not in os.environ:
        candidate = src_dir.parent / "config"
        if candidate.exists():
            os.environ["SAAS_CONFIG_DIR"] = str(candidate)
    return src_dir


_bootstrap_paths()

from pyspark.sql import SparkSession  # noqa: E402

from saas_pipeline import pipeline  # noqa: E402
from saas_pipeline.config import load_config  # noqa: E402
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

    spark = SparkSession.builder.getOrCreate()
    # Match local Spark semantics: ``to_date`` returns NULL on invalid strings.
    spark.conf.set("spark.sql.ansi.enabled", "false")

    run_id = new_run_id()
    print(f"[run] {run_id} env={args.env} layer={args.layer} tenant={args.tenant}")

    failures = pipeline.run_all(
        spark, cfg,
        layer=args.layer, tenant=args.tenant, run_id=run_id,
    )

    if failures:
        print(f"[failures] {failures}")
        sys.exit(1)
    print("[done] pipeline finished")


if __name__ == "__main__":
    main()
