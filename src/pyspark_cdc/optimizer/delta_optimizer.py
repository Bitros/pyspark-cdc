from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pyspark_cdc.capture.logger import logger
from pyspark_cdc.optimizer.cron_parser import parse_day_of_month

if TYPE_CHECKING:
    from delta import DeltaTable

    from pyspark_cdc.capture.builder import CapturerConfiguration


def delta_optimize(dt: DeltaTable, config: CapturerConfiguration) -> None:
    today = datetime.now().day
    if (
        "z_order_columns" in config
        and "z_order_cron" in config
        and today in parse_day_of_month(config["z_order_cron"])
    ):
        logger.info(
            f"🚀 Optimizing DeltaTable with Z-Order by {config['z_order_columns']}"
        )
        dt.optimize().executeZOrderBy(config["z_order_columns"])
    if "compaction_cron" in config and today in parse_day_of_month(
        config["compaction_cron"]
    ):
        logger.info("🚀 Optimizing DeltaTable with compaction")
        dt.optimize().executeCompaction()
    if "vacuum_cron" in config and today in parse_day_of_month(config["vacuum_cron"]):
        logger.info("🚀 Vacuuming DeltaTable")
        dt.vacuum()
