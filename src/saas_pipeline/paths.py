"""Path composition helpers — mirrors the Unity Catalog naming convention.

UC mapping (Databricks):  saas_<env>.<layer>_<tenant>.<table>
Local filesystem:         <paths.<layer>>/<tenant>/<table>/[fecha_proceso=YYYYMMDD/]
Quarantine:               <quarantine_root>/<layer>_quarantine/<tenant>/<table>/
"""

from __future__ import annotations

from pathlib import Path

from omegaconf import DictConfig


def layer_table_path(cfg: DictConfig, layer: str, tenant: str, table: str) -> str:
    """Return the base table path for a given layer + tenant + table."""
    base = getattr(cfg.paths, layer)
    return str(Path(base) / tenant / table)


def partition_path(cfg: DictConfig, layer: str, tenant: str, table: str, fecha_proceso: str) -> str:
    """Return the partition path for a single fecha_proceso (YYYYMMDD)."""
    return str(Path(layer_table_path(cfg, layer, tenant, table)) / f"fecha_proceso={fecha_proceso}")


def quarantine_path(cfg: DictConfig, layer: str, tenant: str, table: str) -> str:
    """Return the quarantine table path for a given layer + tenant + table."""
    return str(Path(cfg.paths.quarantine_root) / f"{layer}_quarantine" / tenant / table)


def quality_logs_path(cfg: DictConfig) -> str:
    """Return the shared quality logs Delta table path."""
    return str(Path(cfg.paths.quality_logs))
