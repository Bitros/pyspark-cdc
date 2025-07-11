[![PyPI version](https://img.shields.io/pypi/v/pyspark-cdc.svg)](https://pypi.org/project/pyspark-cdc/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![code style](https://img.shields.io/badge/code_style-black-black)](https://github.com/psf/black)


# pyspark-cdc

A Python library for Change Data Capture (CDC) workflows using PySpark. This project provides tools to capture, optimize, validate, and manage data changes efficiently in distributed environments.

## Features
- Full and incremental data capture.
- Delta optimization for efficient processing.
- Cron-based scheduling utilities.
- No extra configurations.
- Use inerntal [`commitInfo.userMetadata`](https://docs.databricks.com/aws/en/delta/custom-metadata) to store watermark.

## Installation

You can install the package using **pip** :


```bash
pip install pyspark-cdc
```

## Usage

Let's assume that there is a table in PostgreSQL Database. Use this module to capture as a managed delta table.

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
        driver="org.postgresql.Driver", # Ensure the required JDBC driver file is available to Spark.
    )
    .load()
)
# quick start
(
    capture(df, spark)
    .tableName(f"{catalog}.{database}.{table_name}")    # managed table name
    .mode("incremental")
    .format("delta")
    .primaryKeys(["ID"])                                # PK
    .watermarkColumn("UPDATED_AT")                      # Watermark column
    .enableDeletionDetect()                             # detect DELETE operations
    .start()
)
# With more
(
    capture(df, spark)
    .tableName(f"{catalog}.{database}.{table_name}")
    .mode("incremental")
    .format("delta")
    .primaryKeys(["ID"])
    .watermarkColumn("UPDATED_AT")
    .partitionedBy(["COUNTRY", "GENDER"])             # partitoning
    .scheduleZOrder("*/3", ["FIRST_NAME", "SURNAME"]) # run z-order every 3 days
    .scheduleVacuum("5,20")                           # run vaccum on 5th and 20th every month
    .scheduleCompaction("10-25")                      # run compaction every day between 5th and 25th every month
    .enableDeletionDetect()                           # detect hard delete operations in source side
    .tableProperties(                                 # extra delta table properties
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

> ⚠️ If you run capture multiple times in a day, the optimize schedulers are triggered during each run.

## Typical Scenarios
The following table summarizes common use cases:

| Mode        | Primary Key         | Watermark Column | Example Usage                                                                                                                  | Comment                                               |
|-------------|---------------------|------------------|--------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| Full        | No                  | No               | ...<br>.mode("full")<br>.format("delta")<br>...                                                                                | No auto incremental PK or watermark, it better to add watermark column for big tables.                   |
| Incremental | Single              | Yes (datetime)   | ...<br>.mode("incremental")<br>.primaryKeys(["ID"])<br>.watermarkColumn("UPDATED_AT")<br>.format("delta")<br>...               | Common case.                                           |
| Incremental | Auto Incremental PK | No               | ...<br>.mode("incremental")<br>.primaryKeys(["ID"])<br>.watermarkColumn("ID")<br>.format("delta")<br>...                       | Use auto incremental PK as watermark, **but cannot capture `UPDATE` operations.**                |
| Incremental | Multi               | Yes (datetime)   | ...<br>.mode("incremental")<br>.primaryKeys(["ID", "FIRST_NAME"])<br>.watermarkColumn("UPDATED_AT")<br>.format("delta")<br>... | Common case.                                           |
| Incremental | Multi               | Yes (int)        | ...<br>.mode("incremental")<br>.primaryKeys(["ID", "FIRST_NAME"])<br>.watermarkColumn("ID")<br>.format("delta")<br>...         | Multi-column PK, Use auto incremental PK as watermark, **but cannot capture `UPDATE` operations.** |

To capture `DELETE` operations, use `enableDeletionDetect()`, it will compare records at two sides based on the PK(s).
## License

This project is licensed under the MIT License. See [`LICENSE`](LICENSE) for details.

## Contact

For questions or support, open an issue on GitHub.
