
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    floor,
    rand,
    spark_partition_id,
    count
)

spark = (
    SparkSession.builder
    .appName("DataSkewOptimization")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read the original orders
orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/input/orders.csv")
)

# Add a salt value from 0 to 7 for the hot customer.
# Normal customers receive salt 0.
salted_orders = orders.withColumn(
    "salt",
    when(
        col("customer_id") == "CUST_HOT",
        floor(rand(seed=42) * 8)
    ).otherwise(0)
)

# Distribute records using customer_id + salt
distributed_orders = salted_orders.repartition(
    8,
    col("customer_id"),
    col("salt")
)

# Display the partition distribution
print("\n========== SALTED PARTITION DISTRIBUTION ==========")

partition_counts = (
    distributed_orders
    .withColumn("partition_id", spark_partition_id())
    .groupBy("partition_id")
    .agg(count("*").alias("orders"))
    .orderBy("partition_id")
)

partition_counts.show()

# Verify that salting did not lose or add records
original_count = orders.count()
salted_count = distributed_orders.count()

print("\n========== CORRECTNESS CHECK ==========")
print("Original records:", original_count)
print("Salted records:", salted_count)
print("Record counts match:", original_count == salted_count)

spark.stop()
