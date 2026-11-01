
import argparse
import json
import sys
import logging
import time
import shutil
from pathlib import Path

# Make project root available for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    floor,
    rand,
    when
)

from src.validation import validate_orders


def create_spark_session():
    return (
        SparkSession.builder
        .appName("EcommerceDataSkewPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def save_dataframe(dataframe, folder_path):
    """Save a Spark DataFrame as a local CSV file."""
    folder = Path(folder_path)
    folder.mkdir(parents=True, exist_ok=True)

    pandas_df = dataframe.toPandas()
    pandas_df.to_csv(folder / "part-00000.csv", index=False)


def run_pipeline(input_path, output_dir):
    start_time = time.perf_counter()

    output_dir = Path(output_dir)

    # Clean previous outputs before starting
    if output_dir.exists():
        for item in output_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            elif item.name != "pipeline.log":
                item.unlink()

    output_dir.mkdir(parents=True, exist_ok=True)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(
                output_dir / "pipeline.log",
                mode="a"
            ),
            logging.StreamHandler()
        ],
        force=True
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        logging.info("Pipeline started")

        # 1. Read input data
        orders = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(input_path))
        )

        total_records = orders.count()
        logging.info("Input records: %s", total_records)

        # 2. Partition analysis
        original_partitions = orders.rdd.getNumPartitions()

        repartitioned_orders = orders.repartition(8)
        repartitioned_partitions = (
            repartitioned_orders.rdd.getNumPartitions()
        )

        coalesced_orders = repartitioned_orders.coalesce(2)
        coalesced_partitions = (
            coalesced_orders.rdd.getNumPartitions()
        )

        logging.info(
            "Original partitions: %s",
            original_partitions
        )
        logging.info(
            "After repartition(8): %s",
            repartitioned_partitions
        )
        logging.info(
            "After coalesce(2): %s",
            coalesced_partitions
        )

        print("\nPARTITION ANALYSIS")
        print("Original partitions:", original_partitions)
        print("After repartition(8):", repartitioned_partitions)
        print("After coalesce(2):", coalesced_partitions)

        # 3. Validate records
        valid_orders, invalid_orders, duplicate_orders = (
            validate_orders(orders)
        )

        valid_count = valid_orders.count()
        invalid_count = invalid_orders.count()
        duplicate_count = duplicate_orders.count()

        logging.info("Valid records: %s", valid_count)
        logging.info("Invalid records: %s", invalid_count)
        logging.info("Duplicate records: %s", duplicate_count)

        # 4. Save validated and quarantined records
        save_dataframe(
            valid_orders,
            output_dir / "valid_orders"
        )

        save_dataframe(
            invalid_orders,
            output_dir / "invalid_orders"
        )

        save_dataframe(
            duplicate_orders,
            output_dir / "duplicate_orders"
        )

        # 5. Normal aggregation
        normal_start = time.perf_counter()

        normal_result = (
            valid_orders
            .groupBy("customer_id")
            .agg(
                spark_sum("amount").alias("total_amount")
            )
        )

        normal_count = normal_result.count()
        normal_seconds = time.perf_counter() - normal_start

        # 6. Salted aggregation
        salted_start = time.perf_counter()

        salted = valid_orders.withColumn(
            "salt",
            when(
                col("customer_id") == "CUST_HOT",
                floor(rand(seed=42) * 8)
            ).otherwise(0)
        )

        partial = (
            salted
            .groupBy("customer_id", "salt")
            .agg(
                spark_sum("amount").alias("partial_amount")
            )
        )

        optimized_result = (
            partial
            .groupBy("customer_id")
            .agg(
                spark_sum("partial_amount").alias("total_amount")
            )
        )

        optimized_count = optimized_result.count()
        salted_seconds = time.perf_counter() - salted_start

        # 7. Verify aggregation correctness
        differences = (
            normal_result.alias("normal")
            .join(
                optimized_result.alias("optimized"),
                on="customer_id",
                how="full_outer"
            )
            .filter(
                col("normal.total_amount").isNull()
                | col("optimized.total_amount").isNull()
                | (
                    col("normal.total_amount")
                    - col("optimized.total_amount") > 0.01
                )
                | (
                    col("normal.total_amount")
                    - col("optimized.total_amount") < -0.01
                )
            )
            .count()
        )

        # 8. Save optimized aggregation
        save_dataframe(
            optimized_result,
            output_dir / "customer_totals"
        )

        total_seconds = time.perf_counter() - start_time

        # 9. Save processing metrics
        metrics = {
            "input_records": total_records,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "duplicate_records": duplicate_count,
            "original_partitions": original_partitions,
            "repartitioned_partitions": repartitioned_partitions,
            "coalesced_partitions": coalesced_partitions,
            "normal_customer_groups": normal_count,
            "optimized_customer_groups": optimized_count,
            "normal_aggregation_seconds": round(
                normal_seconds, 3
            ),
            "salted_aggregation_seconds": round(
                salted_seconds, 3
            ),
            "mismatched_customer_totals": differences,
            "aggregation_results_match": differences == 0,
            "total_pipeline_seconds": round(total_seconds, 3)
        }

        with open(
            output_dir / "metrics.json",
            "w",
            encoding="utf-8"
        ) as file:
            json.dump(metrics, file, indent=4)

        logging.info("Pipeline completed")

        print("\nPIPELINE RESULTS")
        print(json.dumps(metrics, indent=4))

    except Exception:
        logging.exception("Pipeline failed")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the Spark data skew pipeline"
    )

    parser.add_argument(
        "--input",
        default="data/input/orders.csv",
        help="Input orders CSV path"
    )

    parser.add_argument(
        "--output",
        default="data/output",
        help="Output directory"
    )

    args = parser.parse_args()

    run_pipeline(args.input, args.output)
