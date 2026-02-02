from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.functions import col

from pyspark_cdc import capture
from pyspark_cdc_test import catalog_schema
from pyspark_cdc_test.utils.dataframe_operations import (
    insert,
    update,
)
from pyspark_cdc_test.utils.employee_generator import EmployeeGenerator

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

generator = EmployeeGenerator()


def test_null_pks(mock_spark: SparkSession, clean_up: bool) -> None:
    df = mock_spark.createDataFrame(
        *generator.generate(count=20, watermark_start="-30d", watermark_end="-29d")
    )
    # make some records status NULL
    df = update(
        df,
        {"STATUS": None},
        condition=col("AGE") > 40,
    )
    dt = (
        capture(df, mock_spark)
        .table(f"{catalog_schema}.extra_test1")
        .mode("incremental")
        .format("delta")
        .primary_keys(["ID", "STATUS"])
        .watermark_column("UPDATED_AT")
        .enable_deletion_detect()
        .start()
    )
    assert df.count() == dt.toDF().count(), "DataFrame count mismatch after capture."
    # Insert
    new_data = mock_spark.createDataFrame(
        *generator.generate(count=20, watermark_start="-28d", watermark_end="-27d")
    )
    df = insert(new_data, df)

    dt = (
        capture(df, mock_spark)
        .table(f"{catalog_schema}.extra_test1")
        .mode("incremental")
        .format("delta")
        .primary_keys(["ID", "STATUS"])
        .watermark_column("UPDATED_AT")
        .enable_deletion_detect()
        .start()
    )
    assert df.count() == dt.toDF().count(), "DataFrame count mismatch after capture."
