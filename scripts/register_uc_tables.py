"""Register Delta files in Unity Catalog as managed tables.

Free Edition writes via paths to a UC volume; this script materializes those
Delta files as managed UC tables so they appear in Catalog Explorer and are
queryable as ``<catalog>.<schema>.<table>`` from any SQL warehouse / notebook.

This is intentionally a separate, opt-in step. The pipeline itself is path-
based (so it runs identically on local and Databricks). Registering UC tables
is a deploy concern, not a pipeline concern — keeping them separate avoids
adding Databricks-specific code paths into ``bronze.py`` / ``silver.py`` /
``gold.py``.

Usage:
    uv run python scripts/register_uc_tables.py \\
        --catalog saas_dev \\
        --tenant pe \\
        --volume-root /Volumes/saas_dev/shared/data \\
        --warehouse-id 25e00cb35ca0fd42

Prereqs:
    - ``databricks`` CLI authenticated against the workspace.
    - The catalog and schemas already exist (see docs/databricks-free-edition.md).
    - The pipeline has run at least once (so the Delta files exist).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

TABLES = [
    # (schema_suffix, table_name, volume_subpath)
    ("bronze_{tenant}", "deliveries", "bronze/{tenant}/deliveries"),
    ("silver_{tenant}", "fact_deliveries", "silver/{tenant}/fact_deliveries"),
    ("silver_{tenant}", "dim_materials", "silver/{tenant}/dim_materials"),
    ("silver_{tenant}", "fact_deliveries_quarantine", "silver_quarantine/{tenant}/fact_deliveries"),
    ("gold_{tenant}", "daily_metrics_by_delivery_type", "gold/{tenant}/daily_metrics_by_delivery_type"),
    ("gold_{tenant}", "top_materials_by_month", "gold/{tenant}/top_materials_by_month"),
    ("shared", "quality_logs", "shared/quality_logs"),
]


def run_sql(sql: str, warehouse_id: str) -> dict:
    body = {"warehouse_id": warehouse_id, "statement": sql, "wait_timeout": "50s"}
    tmp = "_q.json"
    with open(tmp, "w") as f:
        json.dump(body, f)
    try:
        p = subprocess.run(
            ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", f"@{tmp}"],
            capture_output=True, text=True
        )
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
    try:
        return json.loads(p.stdout)
    except json.JSONDecodeError:
        return {"status": {"state": "FAILED", "error": {"message": p.stdout + p.stderr}}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--catalog", required=True, help="UC catalog name (e.g. saas_dev)")
    parser.add_argument("--tenant", required=True, help="Tenant code in lowercase (e.g. pe)")
    parser.add_argument(
        "--volume-root", required=True,
        help="Root volume path where the pipeline wrote outputs (e.g. /Volumes/saas_dev/shared/data)"
    )
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse ID to execute against")
    parser.add_argument(
        "--drop-existing", action="store_true",
        help="Use CREATE OR REPLACE TABLE (default). Disable with --no-drop-existing to use CREATE TABLE IF NOT EXISTS."
    )
    args = parser.parse_args()

    create_verb = "CREATE OR REPLACE TABLE" if args.drop_existing else "CREATE TABLE IF NOT EXISTS"

    failed = 0
    for schema_tmpl, table, subpath_tmpl in TABLES:
        schema = schema_tmpl.format(tenant=args.tenant)
        full_name = f"{args.catalog}.{schema}.{table}"
        location = f"{args.volume_root.rstrip('/')}/{subpath_tmpl.format(tenant=args.tenant)}"
        sql = f"{create_verb} {full_name} AS SELECT * FROM delta.`{location}`"
        result = run_sql(sql, args.warehouse_id)
        state = result.get("status", {}).get("state", "?")
        if state == "SUCCEEDED":
            print(f"[OK] {full_name}")
        else:
            failed += 1
            err = result.get("status", {}).get("error", {}).get("message", "(no message)")
            print(f"[FAIL] {full_name}: {err}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
