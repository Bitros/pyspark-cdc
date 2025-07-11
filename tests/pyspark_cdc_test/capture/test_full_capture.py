from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.functions import col, concat, lit
from pyspark_cdc_test import catalog_schema, external_location
from pyspark_cdc_test.test_utils.dataframe_operations import (
    add_column,
    delete,
    insert,
    update,
)
from pyspark_cdc_test.test_utils.employee_generator import EmployeeGenerator

from pyspark_cdc import capture

if TYPE_CHECKING:
    from typing import Callable

    from delta import DeltaTable
    from pyspark.sql import DataFrame, SparkSession


def test_start(clean_up: bool) -> None:
    assert clean_up


def _capture_assert(
    df: DataFrame,
    spark: SparkSession,
    capture_func: Callable[[DataFrame, SparkSession], DeltaTable],
) -> DeltaTable:
    dt = capture_func(df, spark)
    assert (
        df.count() == dt.toDF().count()
    ), "DataFrame count mismatch after full capture."
    return dt


def _test_steps(
    spark: SparkSession, capture_func: Callable[[DataFrame, SparkSession], DeltaTable]
) -> None:
    generator = EmployeeGenerator()

    # Initial load
    df = spark.createDataFrame(
        *generator.generate(count=100, watermark_start="-30d", watermark_end="-29d")
    )
    dt = _capture_assert(df, spark, capture_func)

    # Insert
    new_data = spark.createDataFrame(
        *generator.generate(count=100, watermark_start="-28d", watermark_end="-27d")
    )
    df = insert(new_data, df)
    dt = _capture_assert(df, spark, capture_func)

    # Update
    df = update(df, {"STATUS": "inactive"}, condition=col("AGE") > 40)
    dt = _capture_assert(df, spark, capture_func)

    # Delete
    df = delete(df, condition=col("AGE") > 40)
    dt = _capture_assert(df, spark, capture_func)

    # Add column
    df = add_column(
        df, "FULL_NAME", concat(col("FIRST_NAME"), lit(" "), col("SURNAME"))
    )
    dt = _capture_assert(df, spark, capture_func)

    # dt.history().show(truncate=False)
    # dt.detail().show(truncate=False)


# ---------- Capture Config Generators ----------


def managed_default(df: DataFrame, spark: SparkSession) -> DeltaTable:
    return (
        capture(df, spark)
        .tableName(f"{catalog_schema}.employee")
        .mode("full")
        .logLevel("DEBUG")
        .format("delta")
        .start()
    )


def managed_with_partition_zorder(df: DataFrame, spark: SparkSession) -> DeltaTable:
    return (
        capture(df, spark)
        .tableName(f"{catalog_schema}.employee")
        .mode("full")
        # .partitionedBy(["COUNTRY", "GENDER"])
        .scheduleZOrder("*", ["FIRST_NAME", "SURNAME"])
        .format("delta")
        .logLevel("DEBUG")
        .tableProperties(
            {
                "delta.deletedFileRetentionDuration": "interval 1 day",
                "delta.logRetentionDuration": "interval 1 day",
                "delta.appendOnly": "false",
            }
        )
        .options(
            {
                "overwriteSchema": True,
                "maxRecordsPerFile": 1000,
                "userMetadata": "Full capture test",
            }
        )
        .start()
    )


def external_default(df: DataFrame, spark: SparkSession) -> DeltaTable:
    return (
        capture(df, spark)
        .location(f"{external_location}/employee")
        .mode("full")
        .logLevel("DEBUG")
        .format("delta")
        .start()
    )


def external_with_partition_zorder(df: DataFrame, spark: SparkSession) -> DeltaTable:
    return (
        capture(df, spark)
        .location(f"{external_location}/employee")
        .mode("full")
        .partitionedBy(["COUNTRY", "GENDER"])
        .scheduleZOrder("1-31", ["FIRST_NAME", "SURNAME"])
        .format("delta")
        .logLevel("DEBUG")
        .tableProperties(
            {
                "delta.deletedFileRetentionDuration": "interval 1 day",
                "delta.logRetentionDuration": "interval 1 day",
                "delta.appendOnly": "false",
                "delta.enableDeletionVectors": "true",
                "delta.autoOptimize.autoCompact": "true",
            }
        )
        .options(
            {
                "overwriteSchema": True,
                "maxRecordsPerFile": 1000,
                "userMetadata": "Full capture test",
            }
        )
        .start()
    )


# ---------- Tests ----------


def test_managed_with_default_configs(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, managed_default)


def test_managed_with_partition_zorder(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, managed_with_partition_zorder)


def test_external_with_default_configs(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, external_default)


def test_external_with_partition_zorder(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, external_with_partition_zorder)
