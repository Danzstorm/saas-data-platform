"""Pytest fixtures — shared SparkSession with Delta and lightweight config."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from omegaconf import OmegaConf


def _spark_available() -> bool:
    """True if a JVM/Java is reachable so PySpark can actually start."""
    import shutil as _sh

    return _sh.which("java") is not None


requires_spark = pytest.mark.skipif(not _spark_available(), reason="Java/Spark not available")


@pytest.fixture(scope="session")
def spark():
    if not _spark_available():
        pytest.skip("Java/Spark not available")
    import os
    import sys

    # Force PySpark workers to use the project's interpreter (Windows defaults to
    # the Microsoft Store Python alias, which breaks).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("saas-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    s = configure_spark_with_delta_pip(builder).getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def tmp_data_root(tmp_path: Path) -> Path:
    """Per-test isolated data root."""
    yield tmp_path
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.fixture
def cfg(tmp_data_root: Path):
    return OmegaConf.create(
        {
            "paths": {
                "raw": str(tmp_data_root / "raw"),
                "bronze": str(tmp_data_root / "bronze"),
                "silver": str(tmp_data_root / "silver"),
                "gold": str(tmp_data_root / "gold"),
                "quarantine_root": str(tmp_data_root),
                "quality_logs": str(tmp_data_root / "shared" / "quality_logs"),
                "raw_deliveries": str(tmp_data_root / "raw" / "deliveries.csv"),
                "raw_materials": str(tmp_data_root / "raw" / "materials.csv"),
            },
            "execution": {"start_date": "2025-01-01", "end_date": "2025-12-31", "tenant": "sv"},
            "business": {
                "cs_to_st_factor": 20,
                "valid_delivery_types": ["ZPRE", "ZVE1", "Z04", "Z05"],
                "routine_types": ["ZPRE", "ZVE1"],
                "bonus_types": ["Z04", "Z05"],
            },
            "quality": {"fail_on_critical": False},
            "tenants": {"known": ["sv"]},
            "spark": {
                "app_name": "saas-tests",
                "shuffle_partitions": 2,
                "driver_memory": "1g",
                "log_level": "ERROR",
                "delta_extensions": True,
            },
        }
    )
