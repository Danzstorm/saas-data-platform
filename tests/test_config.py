"""Validates that all YAML config files load correctly + merge precedence works."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from saas_pipeline.config import CONFIG_DIR_DEFAULT, load_config

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "rel_path",
    [
        "config/base.yaml",
        "config/env/dev.yaml",
        "config/env/qa.yaml",
        "config/env/main.yaml",
        "config/tenants/sv.yaml",
        "config/tenants/hn.yaml",
        "config/tenants/jm.yaml",
        "config/tenants/ec.yaml",
        "config/tenants/pe.yaml",
        "config/tenants/gt.yaml",
    ],
)
def test_yaml_loads(rel_path: str) -> None:
    with (REPO_ROOT / rel_path).open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert isinstance(data, dict)
    assert len(data) > 0


def test_config_merge_precedence() -> None:
    """env override wins over base; tenant override wins over env."""
    cfg = load_config(env="qa", tenant="sv", config_dir=CONFIG_DIR_DEFAULT)
    # env override
    assert cfg.spark.shuffle_partitions == 32
    assert cfg.quality.fail_on_critical is True
    # tenant present
    assert cfg.tenant.id == "sv"


def test_config_all_tenants_skipped() -> None:
    cfg = load_config(env="dev", tenant="all", config_dir=CONFIG_DIR_DEFAULT)
    assert "tenant" not in cfg or cfg.get("tenant") is None or not cfg.get("tenant", {}).get("id")


def test_config_unknown_env_raises() -> None:
    with pytest.raises(FileNotFoundError):
        load_config(env="does-not-exist", tenant=None, config_dir=CONFIG_DIR_DEFAULT)
