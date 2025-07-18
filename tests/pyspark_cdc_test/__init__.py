import os
from pathlib import Path

catalog_schema = os.environ.get(
    "pyspark_cdc_test_catalog_schema", "spark_catalog.default"
)

external_location = os.environ.get(
    "pyspark_cdc_test_external_location",
    Path(__file__).parent.resolve() / "spark-warehouse" / "external",
)

__all__ = [
    "catalog_schema",
    "external_location",
]
