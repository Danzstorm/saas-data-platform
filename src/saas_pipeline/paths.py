"""Path composition helpers — mirrors the Unity Catalog naming convention.

UC mapping (Databricks):  saas_<env>.<layer>_<tenant>.<table>
Local filesystem:         <paths.<layer>>/<tenant>/<table>/[fecha_proceso=YYYYMMDD/]
Quarantine:               <quarantine_root>/<layer>_quarantine/<tenant>/<table>/

When ``cfg.paths.bronze`` (etc.) starts with ``dbfs:/`` or ``abfss:/``, we keep
the URI prefix and join the rest with forward slashes (Path() would mangle the
scheme on Windows).
"""

from __future__ import annotations

from pathlib import Path

from omegaconf import DictConfig

_REMOTE_PREFIXES = ("dbfs:/", "abfss:/", "s3:/", "gs:/")


def _is_posix_absolute(p: str) -> bool:
    """``True`` for paths like ``/Volumes/...`` that must keep forward slashes
    even on Windows (otherwise pathlib normalises to backslashes and Spark
    won't read them as Unity Catalog volume URIs).
    """
    return p.startswith("/") and not p.startswith("//")


def _join(base: str, *parts: str) -> str:
    """Path-style join that preserves remote URI prefixes and POSIX volume paths."""
    if base.startswith(_REMOTE_PREFIXES) or _is_posix_absolute(base):
        return "/".join([base.rstrip("/"), *(p.strip("/") for p in parts)])
    return str(Path(base, *parts))


def layer_table_path(cfg: DictConfig, layer: str, tenant: str, table: str) -> str:
    """Return the base table path for a given layer + tenant + table."""
    base = getattr(cfg.paths, layer)
    return _join(base, tenant, table)


def partition_path(cfg: DictConfig, layer: str, tenant: str, table: str, fecha_proceso: str) -> str:
    """Return the partition path for a single fecha_proceso (YYYYMMDD)."""
    return _join(layer_table_path(cfg, layer, tenant, table), f"fecha_proceso={fecha_proceso}")


def quarantine_path(cfg: DictConfig, layer: str, tenant: str, table: str) -> str:
    """Return the quarantine table path for a given layer + tenant + table."""
    return _join(cfg.paths.quarantine_root, f"{layer}_quarantine", tenant, table)


def quality_logs_path(cfg: DictConfig) -> str:
    """Return the shared quality logs Delta table path."""
    return str(cfg.paths.quality_logs)
