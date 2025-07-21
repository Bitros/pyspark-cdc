from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def columns_exist(df: DataFrame, *columns: str) -> bool:
    """
    Check if the specified columns exist in the DataFrame.
    """
    existing_columns = {col.upper() for col in df.columns}
    return all(column.upper() in existing_columns for column in columns)


def null_watermarks_check(df: DataFrame, watermark_column: str) -> int:
    null_watermark_count = (
        df.select(watermark_column).where(f"{watermark_column} is null").count()
    )
    return int(null_watermark_count)
