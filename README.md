[![PyPI version](https://img.shields.io/pypi/v/pyspark-cdc.svg)](https://pypi.org/project/pyspark-cdc/)
[![Test](https://github.com/Bitros/pyspark-cdc/actions/workflows/test.yaml/badge.svg)](https://github.com/Bitros/pyspark-cdc/actions/workflows/test.yaml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)


# pyspark-cdc

A Python library for Change Data Capture (CDC) workflows using PySpark. This project provides tools to capture, optimize, validate, and manage data changes efficiently in distributed environments.

## Features
- Full and incremental data capture.
- Cron-based scheduling utilities for Delta optimizations.
- No extra dependencies.
- Use internal [`commitInfo.userMetadata`](https://docs.databricks.com/aws/en/delta/custom-metadata) to store watermark.

## Installation

You can install the package using **pip** :


```bash
pip install pyspark-cdc
```

## Usage

Let's assume that there is a table in a PostgreSQL database. Use this module to capture it as a managed delta table.

```python
from pyspark_cdc import capture

... # necessary variables

df = (
    spark.read.format("jdbc")
    .options(
        url=f"{postgresql_jdbc_url}",
        dbtable=f"{postgresql_schema}.{postgresql_table}",
        user=f"{postgresql_user}",
        password=f"{postgresql_password}",
        driver="org.postgresql.Driver",                # Ensure JDBC Driver JAR is in Spark.
    )
    .load()
)
# quick start
(
    capture(df, spark)
    .table(f"{catalog}.{database}.{table_name}")       # managed table name
    .mode("incremental")
    .format("delta")
    .primary_keys(["ID"])                              # PK
    .watermark_column("UPDATED_AT")                    # Watermark column
    .enable_deletion_detect()                          # detect DELETE operations
    .start()
)
# With more
(
    capture(df, spark)
    .table(f"{catalog}.{database}.{table_name}")
    .mode("incremental")
    .format("delta")
    .primary_keys(["ID"])
    .watermark_column("UPDATED_AT")
    .partition_by(["COUNTRY", "GENDER"])               # partitioning
    .schedule_zorder("*/3", ["FIRST_NAME", "SURNAME"]) # run z-order every 3 days
    .schedule_vacuum("5,20")                           # run vacuum on 5th and 20th every month
    .schedule_compaction("10-25")                      # run compaction every day between 5th and 25th every month
    .enable_deletion_detect()                          # detect hard delete operations in source side
    .table_properties(                                 # extra delta table properties
        {
            "delta.deletedFileRetentionDuration": "interval 3 day",
            "delta.logRetentionDuration": "interval 3 day",
            "delta.appendOnly": "false",
            "delta.enableDeletionVectors": "true",
        }
    )
    .options(                                         # extra DataFrame writer options
        {
            "maxRecordsPerFile": 1000,
        }
    )
    .start()
)
```

See the `samples/` directory and the `tests/` folder for more usage examples and test cases.

## Delta Optimize
> **Note:** Schedulers use standard day-of-the-month crontab expressions:
> - **\***: any value
> - **,**: value list separator
> - **-**: range of values
> - **/**: step values
> - **1-31**: allowed values

> ⚠️ If you run capture multiple times in a day, the optimize schedulers are triggered during each run. You can turn it off by using `.scheduler_switch("OFF")`

## Typical Scenarios
The following table summarizes common use cases:

| Mode        | Primary Key         | Watermark Column | Example Usage                                                                                                                    | Comment                                               |
|-------------|---------------------|------------------|----------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| Full        | No need             | No need          | ...<br>.mode("full")<br>.format("delta")<br>...                                                                                  | No auto incremental PK or watermark, it's better to add watermark column for big tables.                   |
| Incremental | Single              | Yes (datetime)   | ...<br>.mode("incremental")<br>.primary_keys(["ID"])<br>.watermark_column("UPDATED_AT")<br>.format("delta")<br>...               | Common case.                                           |
| Incremental | Auto incremental PK | No               | ...<br>.mode("incremental")<br>.primary_keys(["ID"])<br>.watermark_column("ID")<br>.format("delta")<br>...                       | Use auto incremental PK as watermark, **but cannot capture `UPDATE` operations.**                |
| Incremental | Multi               | Yes (datetime)   | ...<br>.mode("incremental")<br>.primary_keys(["ID", "FIRST_NAME"])<br>.watermark_column("UPDATED_AT")<br>.format("delta")<br>... | Common case.                                           |
| Incremental | Multi               | Yes (int)        | ...<br>.mode("incremental")<br>.primary_keys(["ID", "FIRST_NAME"])<br>.watermark_column("ID")<br>.format("delta")<br>...         | Multi-column PK, Use auto incremental PK as watermark, **but cannot capture `UPDATE` operations.** |

To capture `DELETE` operations, use `enable_deletion_detect()`, it will compare records at two sides based on the PK(s).

## Library Version Matrix
The version of this module follows [`Databricks Runtime LTS`](https://docs.databricks.com/aws/en/release-notes/runtime/)
| Version|JDK|Python|Databricks Runtime|Spark| Delta Lake|
|--------|---|------|------------------|-----|-----------|
|1.0.0|17|3.12|17 LTS|4.0.0|4.0.0|

## License

This project is licensed under the MIT License. See [`LICENSE`](LICENSE) for details.

## Contact

For questions or support, open an issue on GitHub.
