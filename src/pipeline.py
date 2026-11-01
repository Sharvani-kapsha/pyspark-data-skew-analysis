
import argparse
import csv
import json
import sys
import logging
import time
import shutil
import uuid

from datetime import datetime, timezone
from pathlib import Path

# Make project root available for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    floor,
    rand,
    when
)

from src.validation import validate_orders


# Explicit source schema
ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_timestamp", StringType(), True)
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("EcommerceDataSkewPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def save_dataframe(dataframe, folder_path):
    """Stream Spark DataFrame rows into a local CSV file."""

    folder = Path(folder_path)
    folder.mkdir(parents=True, exist_ok=True)

    output_file = folder / "part-00000.csv"

    with output_file.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)
        writer.writerow(dataframe.columns)

        for row in dataframe.toLocalIterator():
            writer.writerow([
                value if value is not None else ""
                for value in row
            ])


def save_metrics(metrics, output_dir):
    """Save execution metrics as JSON."""

    with open(
        output_dir / "metrics.json",
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(metrics, file, indent=4)


def close_logging_handlers():
    """Close log files before moving output directories."""

    root_logger = logging.getLogger()

    for handler in root_logger.handlers[:]:
        handler.flush()
        handler.close()
        root_logger.removeHandler(handler)


def run_pipeline(input_path, output_dir):

    start_time = time.perf_counter()

    # Generate a unique ID for every execution
    run_id = str(uuid.uuid4())

    started_at = datetime.now(timezone.utc).isoformat()

    output_dir = Path(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)

    # Temporary and failed-run directories
    staging_dir = output_dir.parent / (
        f"{output_dir.name}_staging_{run_id}"
    )

    failed_dir = output_dir.parent / (
        f"{output_dir.name}_failed_{run_id}"
    )

    backup_dir = output_dir.parent / (
        f"{output_dir.name}_backup_{run_id}"
    )

    staging_dir.mkdir(parents=True, exist_ok=True)

    # Initial metrics
    metrics = {
        "run_id": run_id,
        "status": "RUNNING",
        "started_at_utc": started_at,
        "ended_at_utc": None,
        "input_path": str(input_path),
        "input_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "duplicate_records": 0,
        "original_partitions": 0,
        "repartitioned_partitions": 0,
        "coalesced_partitions": 0,
        "normal_customer_groups": 0,
        "optimized_customer_groups": 0,
        "normal_aggregation_seconds": 0,
        "salted_aggregation_seconds": 0,
        "mismatched_customer_totals": None,
        "aggregation_results_match": None,
        "total_pipeline_seconds": None,
        "failure_reason": None
    }

    # Configure logging in the temporary directory
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(
                staging_dir / "pipeline.log",
                mode="a"
            ),
            logging.StreamHandler()
        ],
        force=True
    )

    spark = None
    successful = False

    try:

        logging.info("Pipeline started. Run ID: %s", run_id)

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("ERROR")

        # 1. Read input data
        orders = (
            spark.read
            .option("header", True)
            .schema(ORDERS_SCHEMA)
            .csv(str(input_path))
        )

        total_records = orders.count()
        metrics["input_records"] = total_records

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

        metrics.update({
            "original_partitions": original_partitions,
            "repartitioned_partitions": repartitioned_partitions,
            "coalesced_partitions": coalesced_partitions
        })

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

        metrics.update({
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "duplicate_records": duplicate_count
        })

        logging.info("Valid records: %s", valid_count)
        logging.info("Invalid records: %s", invalid_count)
        logging.info("Duplicate records: %s", duplicate_count)

        # 4. Save validated and quarantined records
        save_dataframe(
            valid_orders,
            staging_dir / "valid_orders"
        )

        save_dataframe(
            invalid_orders,
            staging_dir / "invalid_orders"
        )

        save_dataframe(
            duplicate_orders,
            staging_dir / "duplicate_orders"
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

        normal_seconds = (
            time.perf_counter() - normal_start
        )

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

        salted_seconds = (
            time.perf_counter() - salted_start
        )

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

        metrics.update({
            "normal_customer_groups": normal_count,
            "optimized_customer_groups": optimized_count,
            "normal_aggregation_seconds": round(normal_seconds, 3),
            "salted_aggregation_seconds": round(salted_seconds, 3),
            "mismatched_customer_totals": differences,
            "aggregation_results_match": differences == 0
        })

        # 8. Save optimized aggregation
        save_dataframe(
            optimized_result,
            staging_dir / "customer_totals"
        )

        # Stop Spark before marking the run successful
        spark.stop()
        spark = None

        # 9. Finalize successful metrics
        metrics["status"] = "SUCCESS"
        metrics["ended_at_utc"] = (
            datetime.now(timezone.utc).isoformat()
        )

        metrics["total_pipeline_seconds"] = round(
            time.perf_counter() - start_time, 3
        )

        save_metrics(metrics, staging_dir)

        logging.info("Pipeline completed successfully")
        logging.info("Run ID: %s", run_id)

        successful = True

        print("\nPIPELINE RESULTS")
        print(json.dumps(metrics, indent=4))

    except Exception as error:

        logging.exception("Pipeline failed. Run ID: %s", run_id)

        metrics["status"] = "FAILED"
        metrics["failure_reason"] = str(error)

        metrics["ended_at_utc"] = (
            datetime.now(timezone.utc).isoformat()
        )

        metrics["total_pipeline_seconds"] = round(
            time.perf_counter() - start_time, 3
        )

        try:
            save_metrics(metrics, staging_dir)
        except Exception:
            logging.exception("Could not save failure metrics")

        raise

    finally:

        if spark is not None:
            spark.stop()

        # Close file handlers before moving directories
        close_logging_handlers()

        if successful:

            # Preserve the previous successful output temporarily
            if output_dir.exists():
                output_dir.replace(backup_dir)

            try:
                # Publish new output
                staging_dir.replace(output_dir)

                # Remove old output only after successful publishing
                if backup_dir.exists():
                    shutil.rmtree(backup_dir)

            except Exception:

                # Restore previous output if publishing fails
                if backup_dir.exists() and not output_dir.exists():
                    backup_dir.replace(output_dir)

                raise

        else:

            # Preserve failed run for debugging
            if staging_dir.exists():
                staging_dir.replace(failed_dir)

            print(
                "\nPipeline failed. Previous successful output "
                "has been preserved."
            )
            print("Failed run details:", failed_dir)


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
