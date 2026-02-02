from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pyspark_cdc_test import catalog_schema
from pyspark_cdc_test.utils.employee_generator import EmployeeGenerator

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="module")
def mock_df(mock_spark: SparkSession) -> DataFrame:
    generator = EmployeeGenerator()
    return mock_spark.createDataFrame(*generator.generate(count=20))


# Found in local test, works in DBR 17 LTS.
@pytest.mark.xfail(reason="Known bug in https://github.com/delta-io/delta/issues/4823")
def test_bug_01(mock_spark: SparkSession, mock_df: DataFrame) -> None:
    mock_df.writeTo(f"{catalog_schema}.bug01_employee").using("delta").clusterBy(
        "country", "gender"
    ).create()


# Found in local test, works in DBR 17 LTS.
@pytest.mark.xfail(reason="Known bug in https://github.com/delta-io/delta/issues/4855")
def test_bug_02(mock_spark: SparkSession, mock_df: DataFrame) -> None:
    mock_df.writeTo(f"{catalog_schema}.bug02_employee").using("delta").partitionedBy(
        "COUNTRY", "GENDER"
    ).option("overwriteSchema", True).createOrReplace()

    mock_df.writeTo(f"{catalog_schema}.bug02_employee").using("delta").partitionedBy(
        "COUNTRY", "GENDER"
    ).option("overwriteSchema", True).createOrReplace()

    mock_df.writeTo(f"{catalog_schema}.bug02_employee").using("delta").partitionedBy(
        "COUNTRY", "GENDER"
    ).option("overwriteSchema", True).createOrReplace()
