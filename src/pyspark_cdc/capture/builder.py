from __future__ import annotations

from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from pyspark_cdc.capture.logger import logger

if TYPE_CHECKING:
    from delta import DeltaTable
    from pyspark.sql import DataFrame, SparkSession


class CapturerConfiguration(dict[Any, Any]):
    def __contains__(self, key: object) -> bool:
        """
        Check if the key exists in the configuration and its value is not empty.

        Args:
            key (object): The key to check in the configuration.

        Returns:
            bool: True if the key exists and its value is not empty, False otherwise.
        """
        return super().__contains__(key) and bool(self[key])


class CapturerBuilder:
    """Builder class for creating data capture configurations."""

    def __init__(self, df: DataFrame, spark: SparkSession):
        """
        Initialize the CapturerBuilder with a DataFrame and SparkSession.

        Args:
            df (DataFrame): The input DataFrame to capture.
            spark (SparkSession): The Spark session object.
        """
        self.df = df
        self.spark = spark
        self.config = CapturerConfiguration()
        # set defaults
        tz = self.spark.conf.get("spark.sql.session.timeZone")
        if tz:
            self.config["timezone"] = ZoneInfo(tz)
        else:
            self.config["timezone"] = ZoneInfo("Etc/UTC")
        self.config["format"] = "delta"
        self.config["scheduler_switch"] = "ON"

    def table(self, table: str) -> CapturerBuilder:
        """
        Set the target table name for managed tables.

        Args:
            table (str): The name of the managed table.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        if "table_identifier" in self.config:
            raise ValueError(
                "Cannot set both table name and location. Use either 'tableName' or 'location'."
            )
        self.config["table_identifier"] = table
        self.config["managed"] = True
        return self

    def mode(self, mode: str) -> CapturerBuilder:
        """
        Set the capture mode.

        Args:
            mode (str): The capture mode, either 'incremental' or 'full'.

        Returns:
            CapturerBuilder: The builder instance for chaining.

        Raises:
            ValueError: If the mode is not 'incremental' or 'full'.
        """
        if mode not in ["incremental", "full"]:
            raise ValueError(
                f"Invalid capture mode '{mode}'. Use 'incremental' or 'full'."
            )
        self.config["mode"] = mode
        return self

    def location(self, location: str) -> CapturerBuilder:
        """
        Set the target location path for external tables.

        Args:
            location (str): The file system path for the external table.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        if "table_identifier" in self.config:
            raise ValueError(
                "Cannot set both table name and location. Use either 'tableName' or 'location'."
            )
        self.config["table_identifier"] = location
        self.config["managed"] = False
        return self

    def partition_by(self, cols: list[str]) -> CapturerBuilder:
        """
        Set partition columns for the target table.

        Args:
            cols (list[str]): List of column names to partition by.

        Returns:
            CapturerBuilder: The builder instance for chaining.

        Raises:
            ValueError: If clustering columns are already defined.
        """
        if "cluster_columns" in self.config:
            raise ValueError(
                "Cannot defined both partition columns and clustering columns."
            )
        self.config["partition_columns"] = cols
        return self

    def cluster_by(self, cols: list[str]) -> CapturerBuilder:
        """
        Set cluster columns for the target table.

        Args:
            cols (list[str]): List of column names to cluster by.

        Returns:
            CapturerBuilder: The builder instance for chaining.

        Raises:
            ValueError: If partition columns or Z-Order columns are already defined.
        """
        if "partition_columns" in self.config:
            raise ValueError(
                "Cannot defined both partition columns and clustering columns."
            )

        if "z_order_columns" in self.config:
            raise ValueError(
                "Cannot defined both Z-Order columns and clustering columns."
            )
        self.config["cluster_columns"] = cols
        return self

    def table_properties(self, props: dict[str, str]) -> CapturerBuilder:
        """
        Set table properties.

        Args:
            props (dict[str, str]): Dictionary of table properties.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["table_properties"] = props
        return self

    def options(self, options: dict[str, bool | float | int | str]) -> CapturerBuilder:
        """
        Set writer options.

        Args:
            options (dict[str, bool | float | int | str]): Dictionary of writer options.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["writer_options"] = options
        return self

    def format(self, format: str) -> CapturerBuilder:
        """
        Set the table format.

        Args:
            format (str): The table format (e.g., 'delta').

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["format"] = format
        return self

    def primary_keys(self, primary_keys: list[str]) -> CapturerBuilder:
        """
        Set primary keys for incremental capture mode.

        Args:
            primary_keys (list[str]): List of primary key column names.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["primary_keys"] = primary_keys
        return self

    def watermark_column(self, watermark_column: str) -> CapturerBuilder:
        """
        Set the watermark column for incremental capture mode.

        Args:
            watermark_column (str): The name of the watermark column.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["watermark_column"] = watermark_column
        return self

    def enable_deletion_detect(self) -> CapturerBuilder:
        """
        Enable deletion detection for incremental capture mode.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["enable_deletion_detect"] = True
        return self

    def enable_null_watermark_check(self) -> CapturerBuilder:
        """
        Enable null watermark check.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["enable_null_watermark_check"] = True
        return self

    def __str__(self) -> str:
        """
        String representation of the builder configuration.

        Returns:
            str: A string representation of the configuration.
        """
        lines = []
        for key, value in self.config.items():
            lines.append(f"  {key}: {value}")
        return "\n".join(lines)

    def schedule_vacuum(self, cron: str) -> CapturerBuilder:
        """
        Schedule vacuum operation using a cron expression.

        Args:
            cron (str): The standard day-of-the-month cron expression for vacuum scheduling.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["vacuum_cron"] = cron
        return self

    def schedule_zorder(self, cron: str, cols: list[str]) -> CapturerBuilder:
        """
        Schedule Z-Order operation using a cron expression and columns.

        Args:
            cron (str): The standard day-of-the-month cron expression for Z-Order scheduling.
            cols (list[str]): List of columns to Z-Order by.

        Returns:
            CapturerBuilder: The builder instance for chaining.

        Raises:
            ValueError: If clustering columns are already defined.
        """
        if "cluster_columns" in self.config:
            raise ValueError(
                "Cannot z-order by columns when clustering is already defined."
            )
        self.config["z_order_columns"] = cols
        self.config["z_order_cron"] = cron
        return self

    def schedule_compaction(self, cron: str) -> CapturerBuilder:
        """
        Schedule compaction operation using a cron expression.

        Args:
            cron (str): The standard day-of-the-month cron expression for compaction scheduling.

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["compaction_cron"] = cron
        return self

    def log_level(self, level: str) -> CapturerBuilder:
        """
        Set the log level for the capture process.

        Args:
            level (str): The log level (e.g., 'INFO', 'DEBUG').

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        logger.setLevel(level.upper())
        return self

    def timezone(self, tz: str) -> CapturerBuilder:
        """
        Set the timezone for the capture process.

        Args:
            tz (str): The timezone string (e.g., 'UTC', 'America/New_York').

        Returns:
            CapturerBuilder: The builder instance for chaining.
        """
        self.config["timezone"] = ZoneInfo(tz)
        self.spark.conf.set("spark.sql.session.timeZone", tz)
        return self

    def scheduler_switch(self, switch: str) -> CapturerBuilder:
        """
        Set the scheduler switch for the capture process.

        Args:
            switch (str): The scheduler switch, either 'ON' or 'OFF'.

        Returns:
            CapturerBuilder: The builder instance for chaining.

        Raises:
            ValueError: If the switch is not 'ON' or 'OFF'.
        """
        switch = switch.upper()
        if switch not in ["ON", "OFF"]:
            raise ValueError(f"Invalid scheduler switch '{switch}'. Use 'ON' or 'OFF'.")
        self.config["scheduler_switch"] = switch
        return self

    def start(self) -> DeltaTable:
        """
        Start the capture process based on the configured properties.

        Returns:
            DeltaTable: The resulting DeltaTable after capture.

        Raises:
            ValueError: If required configuration is missing or format is unsupported.
        """
        if "mode" not in self.config:
            raise ValueError(
                "Capture mode is not specified. Use the 'mode' method to specify the capture mode."
            )

        logger.info(f"CapturerBuilder Configuration: \n{self}")

        logger.info(f"DataFrame Schema: \n{self.df.schema.treeString()}")

        match self.config["format"]:
            case "delta":
                from pyspark_cdc.capture.delta import delta_capture

                return delta_capture(self.df, self.spark, self.config)
            case _:
                raise ValueError(
                    f"Unsupported table format '{self.config['format']}'. Supported formats are 'delta' and 'iceberg'."
                )
