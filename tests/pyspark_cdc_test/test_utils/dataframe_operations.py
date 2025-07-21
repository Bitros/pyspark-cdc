from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.functions import col, when

if TYPE_CHECKING:
    from typing import Any

    from pyspark.sql import Column, DataFrame


def insert(df: DataFrame, target_df: DataFrame) -> DataFrame:
    return df.unionByName(target_df, allowMissingColumns=True)


def update(df: DataFrame, updates: dict[str, Any], condition: Column) -> DataFrame:
    for column_name, new_value in updates.items():
        df = df.withColumn(
            column_name, when(condition, new_value).otherwise(col(column_name))
        )
    return df


def delete(df: DataFrame, condition: Column) -> DataFrame:
    return df.filter(~condition)


def add_column(df: DataFrame, column_name: str, expression: Column) -> DataFrame:
    return df.withColumn(column_name, expression)
