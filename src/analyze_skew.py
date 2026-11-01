
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, spark_partition_id

# Start Spark
spark = (
    SparkSession.builder
    .appName("DataSkewAnalysis")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read our generated CSV file
orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/input/orders.csv")
)

print("\n========== DATASET OVERVIEW ==========")

total_orders = orders.count()
print("Total orders:", total_orders)

print("\n========== CUSTOMER ORDER DISTRIBUTION ==========")

customer_counts = (
    orders.groupBy("customer_id")
    .count()
    .orderBy(desc("count"))
)

customer_counts.show(10, truncate=False)

# Find orders belonging to the hot customer
hot_customer_orders = (
    orders.filter(col("customer_id") == "CUST_HOT").count()
)

hot_percentage = (hot_customer_orders / total_orders) * 100

print("\n========== DATA SKEW RESULT ==========")
print("Hot customer orders:", hot_customer_orders)
print(f"Hot customer percentage: {hot_percentage:.2f}%")

print("\n========== SPARK PARTITION DISTRIBUTION ==========")

partition_counts = (
    orders.repartition(8, col("customer_id"))
    .withColumn("partition_id", spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

partition_counts.show()
spark.stop()