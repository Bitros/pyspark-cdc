from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql.functions import col, concat, lit

from pyspark_cdc import capture
from pyspark_cdc_test import catalog_schema, external_location
from pyspark_cdc_test.utils.dataframe_operations import (
    add_column,
    delete,
    insert,
    update,
)
from pyspark_cdc_test.utils.employee_generator import EmployeeGenerator

if TYPE_CHECKING:
    from collections.abc import Callable

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
    assert df.count() == dt.toDF().count(), "DataFrame count mismatch after capture."
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
    df = update(
        df,
        {"STATUS": "inactive", "UPDATED_AT": datetime.now()},
        condition=col("AGE") > 40,
    )
    dt = _capture_assert(df, spark, capture_func)

    # Delete
    df = delete(df, condition=col("AGE") > 40)
    dt = _capture_assert(df, spark, capture_func)

    # Add column
    df = add_column(
        df, "FULL_NAME", concat(col("FIRST_NAME"), lit(" "), col("SURNAME"))
    )
    df = update(df, {"UPDATED_AT": datetime.now()}, condition=lit(True))
    dt = _capture_assert(df, spark, capture_func)  # noqa

    # dt.history().show(truncate=False)
    # dt.detail().show(truncate=False)


# ---------- Capture Config Generators ----------


def managed_default_single_pk_datetime_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    """
    single pk
    datetime watermark
    """
    return (
        capture(df, spark)
        .table(f"{catalog_schema}.employee_spk_dw")
        .mode("incremental")
        .log_level("DEBUG")
        .format("delta")
        .primary_keys(["ID"])
        .watermark_column("UPDATED_AT")
        .enable_deletion_detect()
        .start()
    )


def managed_default_multiple_pk_datetime_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    """
    multiple pks
    datetime watermark
    """
    return (
        capture(df, spark)
        .table(f"{catalog_schema}.employee_mpk_dw")
        .mode("incremental")
        .log_level("DEBUG")
        .format("delta")
        .primary_keys(["ID", "FIRST_NAME"])
        .watermark_column("UPDATED_AT")
        .enable_deletion_detect()
        .start()
    )


def managed_default_single_pk_int_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    """
    single pk
    int watermark
    """
    return (
        capture(df, spark)
        .table(f"{catalog_schema}.employee_spk_iw")
        .mode("incremental")
        .log_level("DEBUG")
        .format("delta")
        .primary_keys(["ID"])
        .watermark_column("ID")
        .enable_deletion_detect()
        .start()
    )


def managed_default_multiple_pk_int_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    """
    multiple pks
    int watermark
    """
    return (
        capture(df, spark)
        .table(f"{catalog_schema}.employee_mpk_iw")
        .mode("incremental")
        .format("delta")
        .primary_keys(["ID", "FIRST_NAME"])
        .watermark_column("ID")
        .enable_deletion_detect()
        .start()
    )


def managed_with_partition_zorder_single_pk_datetime_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    return (
        capture(df, spark)
        .table(f"{catalog_schema}.employee_spk_dw_pz")
        .mode("incremental")
        .format("delta")
        .primary_keys(["ID"])
        .watermark_column("UPDATED_AT")
        .partition_by(["COUNTRY", "GENDER"])
        .schedule_zorder("*", ["FIRST_NAME", "SURNAME"])
        .enable_deletion_detect()
        .table_properties(
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
                "maxRecordsPerFile": 1000,
            }
        )
        .start()
    )


def external_default_single_pk_datetime_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    """
    single pk
    datetime watermark
    """
    return (
        capture(df, spark)
        .location(f"{external_location}/employee_spk_dw")
        .mode("incremental")
        .log_level("DEBUG")
        .format("delta")
        .primary_keys(["ID"])
        .watermark_column("UPDATED_AT")
        .enable_deletion_detect()
        .start()
    )


def external_default_multiple_pk_datetime_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    """
    multiple pks
    datetime watermark
    """
    return (
        capture(df, spark)
        .location(f"{external_location}/employee_mpk_dw")
        .mode("incremental")
        .log_level("DEBUG")
        .format("delta")
        .primary_keys(["ID", "FIRST_NAME"])
        .watermark_column("UPDATED_AT")
        .enable_deletion_detect()
        .start()
    )


def external_default_single_pk_int_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    """
    single pk
    int watermark
    """
    return (
        capture(df, spark)
        .location(f"{external_location}/employee_spk_iw")
        .mode("incremental")
        .log_level("DEBUG")
        .format("delta")
        .primary_keys(["ID"])
        .watermark_column("ID")
        .enable_deletion_detect()
        .start()
    )


def external_default_multiple_pk_int_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    """
    multiple pks
    int watermark
    """
    return (
        capture(df, spark)
        .location(f"{external_location}/employee_mpk_iw")
        .mode("incremental")
        .log_level("DEBUG")
        .format("delta")
        .primary_keys(["ID", "FIRST_NAME"])
        .watermark_column("ID")
        .enable_deletion_detect()
        .start()
    )


def external_with_partition_zorder_single_pk_datetime_watermark(
    df: DataFrame, spark: SparkSession
) -> DeltaTable:
    return (
        capture(df, spark)
        .location(f"{external_location}/employee_spk_dw_pz")
        .mode("incremental")
        .log_level("DEBUG")
        .format("delta")
        .primary_keys(["ID"])
        .watermark_column("UPDATED_AT")
        .partition_by(["COUNTRY", "GENDER"])
        .schedule_zorder("1-31", ["FIRST_NAME", "SURNAME"])
        .enable_deletion_detect()
        .table_properties(
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
                "maxRecordsPerFile": 1000,
            }
        )
        .start()
    )


# ---------- Tests ----------


def test_managed_with_default_configs_spk_datetime(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, managed_default_single_pk_datetime_watermark)


def test_managed_with_default_configs_mpk_datetime(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, managed_default_multiple_pk_datetime_watermark)


def test_managed_with_default_configs_spk_int(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, managed_default_single_pk_int_watermark)


def test_managed_with_default_configs_mpk_int(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, managed_default_multiple_pk_int_watermark)


def test_managed_with_partition_zorder_spk_datetime(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, managed_with_partition_zorder_single_pk_datetime_watermark)


def test_external_with_default_configs_spk_datetime(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, external_default_single_pk_datetime_watermark)


def test_external_with_default_configs_mpk_datetime(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, external_default_multiple_pk_datetime_watermark)


def test_external_with_default_configs_spk_int(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, external_default_single_pk_int_watermark)


def test_external_with_default_configs_mpk_int(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, external_default_multiple_pk_int_watermark)


def test_external_with_partition_zorder_spk_datetime(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, external_with_partition_zorder_single_pk_datetime_watermark)
