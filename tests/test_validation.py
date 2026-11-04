import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from src.validation import validate_orders
spark = (
    SparkSession.builder
    .appName("ValidationTest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = [
    "order_id",
    "customer_id",
    "product_id",
    "amount",
    "order_timestamp"
]

test_data = [
    (1, "C001", "P001", 500.0, "2026-10-01"),
    (2, None, "P002", 300.0, "2026-10-01"),
    (3, "C003", "P003", -100.0, "2026-10-01"),
    (4, "C004", "P004", 700.0, "2026-10-01"),
    (4, "C004", "P005", 900.0, "2026-10-02"),
]

orders = spark.createDataFrame(test_data, schema)

valid, invalid, duplicates = validate_orders(orders)

print("\n========== VALIDATION TEST RESULTS ==========")

print("Valid records:", valid.count())
print("Invalid records:", invalid.count())
print("Duplicate records:", duplicates.count())

print("\nValid data:")
valid.show()

print("\nInvalid data:")
invalid.show()

print("\nDuplicate data:")
duplicates.show()

# Test an empty dataset
empty_orders = orders.limit(0)
empty_valid, empty_invalid, empty_duplicates = validate_orders(
    empty_orders
)

print("\n========== EMPTY DATASET TEST ==========")
print("Empty input:", empty_orders.count())
print("Empty output:", empty_valid.count())

spark.stop()
