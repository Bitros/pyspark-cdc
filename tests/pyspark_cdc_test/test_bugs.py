# type: ignore


def test_start(clean_up) -> bool:
    assert clean_up


def bug_01(mock_spark) -> None:
    """
    https://github.com/delta-io/delta/issues/4855
    """
    data = [
        (
            1,
            "USA",
            "John",
            "Doe",
            "M",
            30,
            "john.doe@example.com",
            "2024-01-01",
            "2024-01-02",
            "ACTIVE",
        ),
        (
            2,
            "UK",
            "Jane",
            "Smith",
            "F",
            25,
            "jane.smith@example.co.uk",
            "2024-01-03",
            "2024-01-04",
            "INACTIVE",
        ),
    ]
    schema = (
        "ID",
        "COUNTRY",
        "FIRST_NAME",
        "SURNAME",
        "GENDER",
        "AGE",
        "EMAIL",
        "CREATED_AT",
        "UPDATED_AT",
        "STATUS",
    )
    df = mock_spark.createDataFrame(data, schema=schema)
    df.writeTo("default.employee").using("delta").partitionedBy(
        "COUNTRY", "GENDER"
    ).option("overwriteSchema", True).tableProperty(
        "delta.appendOnly", "false"
    ).createOrReplace()

    df.writeTo("default.employee").using("delta").partitionedBy(
        "COUNTRY", "GENDER"
    ).option("overwriteSchema", True).tableProperty(
        "delta.appendOnly", "false"
    ).createOrReplace()

    df.writeTo("default.employee").using("delta").partitionedBy(
        "COUNTRY", "GENDER"
    ).option("overwriteSchema", True).tableProperty(
        "delta.appendOnly", "false"
    ).createOrReplace()


def bug_02(mock_spark) -> None:
    """
    https://github.com/delta-io/delta/issues/4823
    """
    data = [
        (
            1,
            "USA",
            "John",
            "Doe",
            "M",
            30,
            "john.doe@example.com",
            "2024-01-01",
            "2024-01-02",
            "ACTIVE",
        ),
        (
            2,
            "UK",
            "Jane",
            "Smith",
            "F",
            25,
            "jane.smith@example.co.uk",
            "2024-01-03",
            "2024-01-04",
            "INACTIVE",
        ),
    ]
    schema = (
        "ID",
        "COUNTRY",
        "FIRST_NAME",
        "SURNAME",
        "GENDER",
        "AGE",
        "EMAIL",
        "CREATED_AT",
        "UPDATED_AT",
        "STATUS",
    )
    df = mock_spark.createDataFrame(data, schema=schema)

    (df.writeTo("employee2").using("delta").clusterBy("COUNTRY", "GENDER").create())
