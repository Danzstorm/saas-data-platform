"""SparkSession factory with Delta Lake extensions."""

from __future__ import annotations

from omegaconf import DictConfig
from pyspark.sql import SparkSession


def get_spark(cfg: DictConfig) -> SparkSession:
    """Build a SparkSession configured for Delta Lake.

    For local execution we wire up Delta via the ``configure_spark_with_delta_pip``
    helper from ``delta-spark`` so the matching JARs are pulled at session start.
    On Databricks the runtime already provides Delta — these settings are no-ops.
    """
    import os
    import sys

    # On Windows, PYSPARK_PYTHON defaults to "python" which resolves to the
    # Microsoft Store stub and breaks worker creation. Force the active interpreter.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(cfg.spark.app_name)
        .config("spark.sql.shuffle.partitions", str(cfg.spark.shuffle_partitions))
        .config("spark.driver.memory", cfg.spark.driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )

    if cfg.spark.get("delta_extensions", True):
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            pass  # Databricks runtime — Delta JARs already on classpath

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.spark.log_level)
    return spark
