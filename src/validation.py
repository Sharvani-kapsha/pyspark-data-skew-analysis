
from pyspark.sql.functions import (
    col,
    count,
    trim,
    coalesce,
    try_to_timestamp,
    lit
)


def validate_orders(orders):

    # Accept both ISO date and ISO datetime formats safely
    parsed_timestamp = coalesce(
        try_to_timestamp(
            col("order_timestamp"),
            lit("yyyy-MM-dd'T'HH:mm:ss")
        ),
        try_to_timestamp(
            col("order_timestamp"),
            lit("yyyy-MM-dd")
        )
    )

    valid_timestamp = parsed_timestamp.isNotNull()

    # Rules for identifying invalid records
    invalid_condition = (
        col("order_id").isNull()
        | col("customer_id").isNull()
        | (trim(col("customer_id")) == "")
        | col("product_id").isNull()
        | col("amount").isNull()
        | (col("amount") <= 0)
        | col("order_timestamp").isNull()
        | ~valid_timestamp
    )

    # Separate invalid records
    invalid_orders = orders.filter(invalid_condition)

    # Continue validation only with clean records
    clean_orders = orders.filter(~invalid_condition)

    # Identify duplicate order IDs
    duplicate_ids = (
        clean_orders.groupBy("order_id")
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .select("order_id")
    )

    # Quarantine every record with a duplicate order ID
    duplicate_orders = clean_orders.join(
        duplicate_ids,
        on="order_id",
        how="inner"
    )

    # Keep only valid, unique records
    valid_orders = clean_orders.join(
        duplicate_ids,
        on="order_id",
        how="left_anti"
    )

    return valid_orders, invalid_orders, duplicate_orders
