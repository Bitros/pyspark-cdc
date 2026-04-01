from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from delta import DeltaTable

from pyspark_cdc.capture.logger import logger
from pyspark_cdc.optimizer import delta_optimize
from pyspark_cdc.validator import columns_exist, null_watermarks_check
from pyspark_cdc.watermark import WATERMARK_TYPES, Watermark

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from pyspark.sql import DataFrame, DataFrameWriterV2, SparkSession

    from pyspark_cdc.capture.builder import CapturerConfiguration


def _clear_watermark(spark: SparkSession) -> None:
    if "spark.databricks.delta.commitInfo.userMetadata" in spark.conf.getAll:
        spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")


def _write_watermark(spark: SparkSession, watermark: Watermark) -> None:
    meta_json = json.dumps({"__watermark": watermark.dict()})
    spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", meta_json)


def _read_watermark(dt: DeltaTable) -> Watermark:
    user_metadata_row_list = dt.history(20).select("userMetadata").collect()
    for user_metadata_row in user_metadata_row_list:
        if user_metadata_row and "userMetadata" in user_metadata_row:
            meta_json_str = user_metadata_row["userMetadata"]
            if meta_json_str:
                meta_json = json.loads(meta_json_str)
                if "__watermark" in meta_json:
                    # Convert the watermark data to a Watermark object
                    return Watermark(**meta_json["__watermark"])

    raise ValueError(
        "No watermark found in the last 20 history metadata records. Maybe this delta table was corrupted?"
    )


def _incremental_capture_precheck(df: DataFrame, config: CapturerConfiguration) -> None:
    if "primary_keys" not in config:
        raise ValueError("Primary keys must be specified for incremental capture mode.")

    if "watermark_column" not in config:
        raise ValueError(
            "Watermark column must be specified for incremental capture mode."
        )

    primary_keys = config["primary_keys"]
    if not columns_exist(df, *primary_keys):
        raise ValueError(f"Primary keys {primary_keys} do not exist in the DataFrame.")

    watermark_column = config["watermark_column"]
    if not columns_exist(df, watermark_column):
        raise ValueError(
            f"Watermark column {watermark_column} does not exist in the DataFrame."
        )

    _, watermark_type = df.select(watermark_column).dtypes[0]
    if watermark_type not in WATERMARK_TYPES:
        raise ValueError(f"Unsupported watermark type: {watermark_type}")

    if "enable_null_watermark_check" in config:
        null_watermarks_count = null_watermarks_check(df, watermark_column)

        if null_watermarks_count != 0:
            logger.warning(
                f"⚠️ Found {null_watermarks_count} records with null watermark values. "
                f"Capturer will capture and update these records every time."
            )


def _overwrite_to_table(
    df: DataFrame,
    spark: SparkSession,
    config: CapturerConfiguration,
    replace: bool = True,
) -> DeltaTable:
    table_identifier = config["table_identifier"]

    dfwv2: DataFrameWriterV2 = df.writeTo(table_identifier).using("delta")

    if "writer_options" in config:
        dfwv2 = dfwv2.options(**config["writer_options"])

    if "partition_columns" in config:
        dfwv2 = dfwv2.partitionedBy(*config["partition_columns"])

    if "cluster_columns" in config:
        dfwv2 = dfwv2.clusterBy(*config["cluster_columns"])

    if "table_properties" in config:
        for key, value in config["table_properties"].items():
            dfwv2 = dfwv2.tableProperty(key, value)

    dfwv2.createOrReplace() if replace else dfwv2.create()

    return DeltaTable.forName(spark, table_identifier)


def _max_watermark(
    df: DataFrame, table: str, watermark_column: str, tz: ZoneInfo
) -> Watermark:
    _, watermark_type = df.select(watermark_column).dtypes[0]
    max_value = df.agg({f"{watermark_column}": "max"}).collect()[0][0]
    logger.info(f"Raw max watermark value: {max_value}")

    if not max_value:
        raise ValueError(
            f"No valid watermark values found in column '{watermark_column}'."
        )

    if isinstance(max_value, datetime):
        formatted_max_value: str | int = max_value.astimezone(tz).isoformat()
    elif isinstance(max_value, int):
        formatted_max_value = max_value
    else:
        raise ValueError(
            f"Unsupported watermark type '{watermark_type}'. Expected datetime or int."
        )

    return Watermark(table, watermark_column, formatted_max_value, watermark_type)


def _generate_watermark_condition(
    max_watermark: Watermark, min_watermark: Watermark | None = None
) -> str:
    col = max_watermark.column_name

    if min_watermark:
        if isinstance(min_watermark.value, str):
            condition = f"({col} > '{min_watermark.value}' AND {col} <= '{max_watermark.value}') OR ({col} IS NULL)"
        else:
            condition = f"({col} > {min_watermark.value} AND {col} <= {max_watermark.value}) OR ({col} IS NULL)"
    else:
        if isinstance(max_watermark.value, str):
            condition = f"({col} <= '{max_watermark.value}') OR ({col} IS NULL)"
        else:
            condition = f"({col} <= {max_watermark.value}) OR ({col} IS NULL)"

    return condition


def _merge_to_delta_table(df: DataFrame, dt: DeltaTable, join_condition: str) -> None:
    (
        dt.alias("target")
        .merge(df.alias("source"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _replicate_deletion_to_delta_table(
    df: DataFrame, dt: DeltaTable, join_condition: str
) -> None:
    (
        dt.alias("target")
        .merge(df.alias("source"), join_condition)
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def _incremental_capture(
    df: DataFrame, spark: SparkSession, config: CapturerConfiguration
) -> DeltaTable:
    _incremental_capture_precheck(df, config)

    table_identifier = config["table_identifier"]
    watermark_column = config["watermark_column"]
    primary_keys = config["primary_keys"]
    deletion_detect = config.get("enable_deletion_detect", False)
    join_condition = " AND ".join(
        [f"target.{pk} <=> source.{pk}" for pk in primary_keys]
    )
    logger.info(f"Join condition: {join_condition}")

    max_watermark = _max_watermark(
        df, table_identifier, watermark_column, config["timezone"]
    )

    table_exists = spark.catalog.tableExists(table_identifier)

    _write_watermark(spark, max_watermark)

    try:
        if not table_exists:
            logger.info(
                f"No delta table found for: {table_identifier}. Creating a new one."
            )

            condition = _generate_watermark_condition(max_watermark)
            logger.info(f"Capture condition for new table: {condition}")

            filtered_df = df.where(condition)
            dt = _overwrite_to_table(filtered_df, spark, config, replace=False)
        else:
            logger.info(f"Delta table found for: {table_identifier}. Updating it.")

            dt = DeltaTable.forName(spark, table_identifier)

            min_watermark = _read_watermark(dt)

            if max_watermark == min_watermark:
                logger.info(
                    f"Max watermark {max_watermark.value} is equal to the min watermark {min_watermark.value}. No new data to capture."
                )
            elif max_watermark < min_watermark:
                logger.info(
                    f"Max watermark {max_watermark.value} is less than the min watermark {min_watermark.value}. "
                    "Maybe delete actions happened in source side. Make sure you used 'enable_deletion_detect' to capture."
                )
            else:
                condition = _generate_watermark_condition(max_watermark, min_watermark)

                logger.info(f"Capture condition: {condition}")

                _merge_to_delta_table(df.where(condition), dt, join_condition)

            if deletion_detect:
                full_condition_df = df.where(
                    _generate_watermark_condition(max_watermark)
                )
                _replicate_deletion_to_delta_table(
                    full_condition_df.select(primary_keys), dt, join_condition
                )
    except Exception as e:
        raise e
    finally:
        _clear_watermark(spark)

    return dt


def delta_capture(
    df: DataFrame, spark: SparkSession, config: CapturerConfiguration
) -> DeltaTable:
    mode = config["mode"]
    scheduler_switch = config["scheduler_switch"]
    match mode:
        case "full":
            dt = _overwrite_to_table(df, spark, config)
        case "incremental":
            dt = _incremental_capture(df, spark, config)
        case _:
            raise ValueError(
                "Unsupported capture mode 'mode'. Supported modes are 'full' and 'incremental'."
            )
    logger.info("✅ Delta table capture completed successfully.")
    if scheduler_switch == "ON":
        logger.info("🚀 Running delta optimization...")
        delta_optimize(dt, config)
    return dt
