"""Hierarchical configuration loader (OmegaConf).

Merge precedence (lowest → highest): base → env/<env> → tenants/<tenant> → CLI overrides.
"""

from __future__ import annotations

from pathlib import Path

from omegaconf import DictConfig, OmegaConf

CONFIG_DIR_DEFAULT = Path(__file__).resolve().parents[2] / "config"


def load_config(
    env: str,
    tenant: str | None = None,
    overrides: dict | None = None,
    config_dir: Path | None = None,
) -> DictConfig:
    """Load and merge configuration files for an environment + optional tenant.

    Args:
        env: Environment code (dev / qa / main).
        tenant: Tenant code (sv, hn, ...). ``None`` or ``"all"`` skips tenant merge.
        overrides: Dict of additional overrides (e.g. CLI args).
        config_dir: Override path to the config root (used in tests).
    """
    root = Path(config_dir) if config_dir else CONFIG_DIR_DEFAULT

    base_path = root / "base.yaml"
    env_path = root / "env" / f"{env}.yaml"
    if not base_path.exists():
        raise FileNotFoundError(f"Base config not found: {base_path}")
    if not env_path.exists():
        raise FileNotFoundError(f"Environment config not found: {env_path}")

    cfg = OmegaConf.merge(OmegaConf.load(base_path), OmegaConf.load(env_path))

    if tenant and tenant != "all":
        tenant_path = root / "tenants" / f"{tenant}.yaml"
        if not tenant_path.exists():
            raise FileNotFoundError(f"Tenant config not found: {tenant_path}")
        cfg = OmegaConf.merge(cfg, OmegaConf.load(tenant_path))

    if overrides:
        cfg = OmegaConf.merge(cfg, OmegaConf.create(overrides))

    OmegaConf.resolve(cfg)
    return cfg  # type: ignore[return-value]


def list_known_tenants(cfg: DictConfig) -> list[str]:
    """Return list of tenant codes the platform knows about."""
    return list(cfg.tenants.known)
