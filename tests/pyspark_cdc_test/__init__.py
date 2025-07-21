from __future__ import annotations

import os
from pathlib import Path

catalog_schema = os.environ.get(
    "PYSPARK_CDC_TEST_CATALOG_SCHEMA", "spark_catalog.default"
)

external_location = os.environ.get(
    "PYSPARK_CDC_TEST_EXTERNAL_LOCATION",
    Path(__file__).parent.resolve() / "spark-warehouse" / "external",
)

__all__ = [
    "catalog_schema",
    "external_location",
]
