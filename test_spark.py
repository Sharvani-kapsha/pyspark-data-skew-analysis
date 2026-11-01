from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkStartupCheck")
    .master("local[2]")
    .getOrCreate()
)

print("SUCCESS: Spark started!")

data = [(1, "Python"), (2, "Spark")]
df = spark.createDataFrame(data, ["id", "technology"])

df.show()

spark.stop()