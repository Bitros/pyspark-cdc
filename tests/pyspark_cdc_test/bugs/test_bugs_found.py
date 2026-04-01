from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pyspark_cdc_test import catalog_schema
from pyspark_cdc_test.utils import generate_table_name
from pyspark_cdc_test.utils.employee_generator import EmployeeGenerator

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="module")
def mock_df(mock_spark: SparkSession) -> DataFrame:
    generator = EmployeeGenerator()
    return mock_spark.createDataFrame(*generator.generate(count=20))


# Found in local test, works in DBR 17 LTS.
@pytest.mark.xfail(reason="Known bug in https://github.com/delta-io/delta/issues/4823")
def test_bug_01(mock_df: DataFrame) -> None:
    mock_df.writeTo(f"{catalog_schema}.{generate_table_name()}").using(
        "delta"
    ).clusterBy("country", "gender").create()


# Found in local test, works in DBR 17 LTS.
@pytest.mark.xfail(reason="Known bug in https://github.com/delta-io/delta/issues/4855")
def test_bug_02(mock_df: DataFrame) -> None:
    test_table_name = generate_table_name()
    mock_df.writeTo(f"{catalog_schema}.{test_table_name}").using("delta").partitionedBy(
        "COUNTRY", "GENDER"
    ).option("overwriteSchema", True).createOrReplace()

    mock_df.writeTo(f"{catalog_schema}.{test_table_name}").using("delta").partitionedBy(
        "COUNTRY", "GENDER"
    ).option("overwriteSchema", True).createOrReplace()

    mock_df.writeTo(f"{catalog_schema}.{test_table_name}").using("delta").partitionedBy(
        "COUNTRY", "GENDER"
    ).option("overwriteSchema", True).createOrReplace()
