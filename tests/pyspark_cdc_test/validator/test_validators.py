from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from pyspark_cdc.validator.validators import columns_exist, null_watermarks_check

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="module")
def mock_df(mock_spark: SparkSession) -> DataFrame:
    return mock_spark.createDataFrame(
        [
            ("Alice", 25, None),
            ("Bob", 30, datetime.now()),
            ("Charlie", 35, None),
            ("David", 44, datetime.now()),
        ],
        ["name", "age", "updated_at"],
    )


@pytest.fixture(scope="module")
def mock_perfect_df(mock_spark: SparkSession) -> DataFrame:
    return mock_spark.createDataFrame(
        [
            ("Alice", 25, datetime.now()),
            ("Bob", 30, datetime.now()),
            ("Charlie", 35, datetime.now()),
            ("David", 44, datetime.now()),
        ],
        ["name", "age", "updated_at"],
    )


def test_columns_exist_all_present(mock_df: DataFrame) -> None:
    assert columns_exist(mock_df, "name", "age")


def test_columns_exist_some_missing(mock_df: DataFrame) -> None:
    assert not columns_exist(mock_df, "name", "missing_column")


def test_columns_exist_case_insensitive(mock_df: DataFrame) -> None:
    assert columns_exist(mock_df, "Name", "AGE")


def test_null_watermarks_check(mock_df: DataFrame) -> None:
    assert null_watermarks_check(mock_df, "updated_at") == 2


def test_null_watermarks_check_all_present(mock_perfect_df: DataFrame) -> None:
    assert null_watermarks_check(mock_perfect_df, "updated_at") == 0
