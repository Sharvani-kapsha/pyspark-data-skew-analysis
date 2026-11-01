
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    when,
    floor,
    rand,
    count
)

spark = (
    SparkSession.builder
    .appName("DataSkewBenchmark")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/input/orders.csv")
)

# Remove invalid and duplicate records for a fair benchmark
orders = orders.filter(
    col("order_id").isNotNull()
    & col("customer_id").isNotNull()
    & (col("customer_id") != "")
    & col("amount").isNotNull()
    & (col("amount") > 0)
)

duplicate_ids = (
    orders.groupBy("order_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .select("order_id")
)

orders = orders.join(
    duplicate_ids,
    on="order_id",
    how="left_anti"
)

orders.cache()
print("Benchmark input records:", orders.count())

TRIALS = 3

normal_times = []
salted_times = []

for trial in range(1, TRIALS + 1):

    print(f"\n========== TRIAL {trial} ==========")

    # Normal aggregation
    start = time.perf_counter()

    normal_result = (
        orders.groupBy("customer_id")
        .agg(spark_sum("amount").alias("total_amount"))
    )

    normal_result.cache()
    normal_result.count()

    normal_time = time.perf_counter() - start
    normal_times.append(normal_time)

    print(f"Normal aggregation: {normal_time:.3f} seconds")

    # Salted aggregation
    start = time.perf_counter()

    salted_orders = orders.withColumn(
        "salt",
        when(
            col("customer_id") == "CUST_HOT",
            floor(rand(seed=42) * 8)
        ).otherwise(0)
    )

    partial_totals = (
        salted_orders
        .groupBy("customer_id", "salt")
        .agg(spark_sum("amount").alias("partial_amount"))
    )

    salted_result = (
        partial_totals
        .groupBy("customer_id")
        .agg(spark_sum("partial_amount").alias("total_amount"))
    )

    salted_result.cache()
    salted_result.count()

    salted_time = time.perf_counter() - start
    salted_times.append(salted_time)

    print(f"Salted aggregation: {salted_time:.3f} seconds")

    # Verify correctness
    difference = (
        normal_result.alias("normal")
        .join(
            salted_result.alias("salted"),
            "customer_id",
            "full_outer"
        )
        .select(
            col("normal.total_amount").alias("normal_total"),
            col("salted.total_amount").alias("salted_total")
        )
        .filter(
            col("normal_total").isNull()
            | col("salted_total").isNull()
            | (col("normal_total") - col("salted_total") > 0.01)
            | (col("normal_total") - col("salted_total") < -0.01)
        )
    )

    mismatch_count = difference.count()

    print("Mismatched customer totals:", mismatch_count)
    print("Results match:", mismatch_count == 0)

    normal_result.unpersist()
    salted_result.unpersist()

# Calculate averages
average_normal = sum(normal_times) / TRIALS
average_salted = sum(salted_times) / TRIALS

print("\n========== FINAL PERFORMANCE COMPARISON ==========")

print("Normal trial times:", [round(t, 3) for t in normal_times])
print("Salted trial times:", [round(t, 3) for t in salted_times])

print(f"Average normal time: {average_normal:.3f} seconds")
print(f"Average salted time: {average_salted:.3f} seconds")

if average_salted < average_normal:
    improvement = (
        (average_normal - average_salted) / average_normal
    ) * 100
    print(f"Salting was {improvement:.2f}% faster on average.")
else:
    difference = (
        (average_salted - average_normal) / average_normal
    ) * 100
    print(f"Salting was {difference:.2f}% slower on average.")

orders.unpersist()
spark.stop()
