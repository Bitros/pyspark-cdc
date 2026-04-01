from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pyspark_cdc import capture
from pyspark_cdc_test import catalog_schema, external_location
from pyspark_cdc_test.utils import generate_table_name
from pyspark_cdc_test.utils.employee_generator import EmployeeGenerator
from pyspark_cdc_test.utils.postgres_operations import (
    add_column,
    create_with_ddl_file,
    delete,
    insert,
    update,
)

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


def _load_pg_df(spark: SparkSession, table: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .options(
            url="jdbc:postgresql://postgres:5432/postgres",
            dbtable=table,
            user="postgres",
            password="postgres",
            driver="org.postgresql.Driver",
        )
        .load()
    )


def _test_steps(
    spark: SparkSession, capture_func: Callable[[DataFrame, SparkSession], DeltaTable]
) -> None:
    table = "public.employee"

    employee_ddl_file = Path(__file__).resolve().parent / "employee_ddl.sql"

    create_with_ddl_file(table, employee_ddl_file, recreate=True)

    generator = EmployeeGenerator()

    df = _load_pg_df(spark, table)

    # Insert
    insert(
        table,
        *generator.generate(count=100, watermark_start="-28d", watermark_end="-27d"),
    )
    dt = _capture_assert(df, spark, capture_func)

    # update
    update(table, {"status": "inactive"}, "AGE > 40")
    dt = _capture_assert(df, spark, capture_func)

    # delete
    delete(table, "status = 'inactive'")
    dt = _capture_assert(df, spark, capture_func)

    # add column
    add_column(table, "full_name", "VARCHAR(50)")
    update(table, {"full_name": "first_name || ' ' || surname"})
    # reload to get latest schema
    df = _load_pg_df(spark, table)
    dt = _capture_assert(df, spark, capture_func)

    # Insert again
    insert(
        table,
        *generator.generate(count=100, watermark_start="-25d", watermark_end="-24d"),
    )
    dt = _capture_assert(df, spark, capture_func)  # noqa

    # dt.history().show(truncate=False)
    # dt.detail().show(truncate=False)


# ---------- Capture Config Generators ----------


def managed_default(df: DataFrame, spark: SparkSession) -> DeltaTable:
    return (
        capture(df, spark)
        .table(f"{catalog_schema}.{generate_table_name()}")
        .mode("full")
        .format("delta")
        .start()
    )


def managed_with_partition_zorder(df: DataFrame, spark: SparkSession) -> DeltaTable:
    return (
        capture(df, spark)
        .table(f"{catalog_schema}.{generate_table_name()}")
        .mode("full")
        # .partition_by(["COUNTRY", "GENDER"])
        .schedule_zorder("*", ["FIRST_NAME", "SURNAME"])
        .format("delta")
        .table_properties(
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
            }
        )
        .start()
    )


def external_default(df: DataFrame, spark: SparkSession) -> DeltaTable:
    test_table_name = generate_table_name()
    return (
        capture(df, spark)
        .table(f"{catalog_schema}.{test_table_name}")
        .options(
            {
                "path": f"{external_location}/{test_table_name}",
            }
        )
        .mode("full")
        .format("delta")
        .start()
    )


def external_with_partition_zorder(df: DataFrame, spark: SparkSession) -> DeltaTable:
    test_table_name = generate_table_name()
    return (
        capture(df, spark)
        .table(f"{catalog_schema}.{test_table_name}")
        .mode("full")
        .partition_by(["COUNTRY", "GENDER"])
        .schedule_zorder("1-31", ["FIRST_NAME", "SURNAME"])
        .format("delta")
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
                "path": f"{external_location}/{test_table_name}",
                "overwriteSchema": True,
                "maxRecordsPerFile": 1000,
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


@pytest.mark.xfail(reason="Known bug in https://github.com/delta-io/delta/issues/4855")
def test_external_with_partition_zorder(mock_spark: SparkSession) -> None:
    _test_steps(mock_spark, external_with_partition_zorder)
