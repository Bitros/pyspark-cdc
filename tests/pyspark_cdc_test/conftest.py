from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from collections.abc import Generator


def create_spark_delta_enabled_session() -> SparkSession:
    return configure_spark_with_delta_pip(
        SparkSession.builder.appName("Pyspark_CDC_Test_App")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.warehouse.dir",
            Path(__file__).parent.resolve() / "spark-warehouse",
        )
        .config("spark.local.dir", Path(__file__).parent.resolve() / "temp"),
        extra_packages=["org.postgresql:postgresql:42.7.7"],
    ).getOrCreate()


def databricks_runtime() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@pytest.fixture(scope="session")
def clean_up() -> bool:
    if not databricks_runtime():
        warehouse = Path(__file__).parent.resolve() / "spark-warehouse"
        shutil.rmtree(warehouse) if warehouse.exists() else ...
    return True


@pytest.fixture(scope="session")
def mock_spark() -> Generator[SparkSession, None, None]:
    if databricks_runtime():
        yield SparkSession.builder.getOrCreate()
    else:
        spark = create_spark_delta_enabled_session()
        yield spark
        spark.stop()
        temp_dir = Path(__file__).parent.resolve() / "temp"
        shutil.rmtree(temp_dir) if temp_dir.exists() else ...
