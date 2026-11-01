
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from pyspark.sql import SparkSession
from src.validation import validate_orders


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("AutomatedValidationTests")
        .master("local[2]")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.fixture
def schema():
    from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        StringType,
        DoubleType
    )

    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("order_timestamp", StringType(), True)
    ])


def test_valid_orders(spark, schema):
    data = [
        (1, "C001", "P001", 500.0, "2026-10-01"),
        (2, "C002", "P002", 300.0, "2026-10-02")
    ]

    orders = spark.createDataFrame(data, schema)

    valid, invalid, duplicates = validate_orders(orders)

    assert valid.count() == 2
    assert invalid.count() == 0
    assert duplicates.count() == 0


def test_null_and_malformed_amount(spark, schema):
    data = [
        (1, "C001", "P001", None, "2026-10-01"),
        (2, None, "P002", 300.0, "2026-10-02")
    ]

    orders = spark.createDataFrame(data, schema)

    valid, invalid, duplicates = validate_orders(orders)

    assert valid.count() == 0
    assert invalid.count() == 2


def test_duplicate_order_ids(spark, schema):
    data = [
        (10, "C001", "P001", 500.0, "2026-10-01"),
        (10, "C001", "P002", 600.0, "2026-10-02")
    ]

    orders = spark.createDataFrame(data, schema)

    valid, invalid, duplicates = validate_orders(orders)

    assert valid.count() == 0
    assert duplicates.count() == 2


def test_empty_dataset(spark, schema):
    orders = spark.createDataFrame([], schema)

    valid, invalid, duplicates = validate_orders(orders)

    assert valid.count() == 0
    assert invalid.count() == 0
    assert duplicates.count() == 0


def test_negative_and_zero_amounts(spark, schema):
    data = [
        (1, "C001", "P001", -100.0, "2026-10-01"),
        (2, "C002", "P002", 0.0, "2026-10-02"),
        (3, "C003", "P003", 250.0, "2026-10-03")
    ]

    orders = spark.createDataFrame(data, schema)

    valid, invalid, duplicates = validate_orders(orders)

    assert valid.count() == 1
    assert invalid.count() == 2
    assert duplicates.count() == 0


def test_blank_customer_id(spark, schema):
    data = [
        (1, "   ", "P001", 500.0, "2026-10-01")
    ]

    orders = spark.createDataFrame(data, schema)

    valid, invalid, duplicates = validate_orders(orders)

    assert valid.count() == 0
    assert invalid.count() == 1
