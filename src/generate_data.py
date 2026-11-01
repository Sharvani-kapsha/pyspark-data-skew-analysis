
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Fixed seed: generates reproducible data
random.seed(42)

# Project input folder
OUTPUT_FILE = Path("data/input/orders.csv")

# Create 100,000 orders
TOTAL_ORDERS = 100_000

# One customer receives most of the orders
HOT_CUSTOMER = "CUST_HOT"

normal_customers = [
    f"CUST_{i:04d}" for i in range(1, 1001)
]

start_date = datetime(2026, 1, 1)

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    writer.writerow([
        "order_id",
        "customer_id",
        "product_id",
        "amount",
        "order_timestamp"
    ])

    for i in range(1, TOTAL_ORDERS + 1):

        # 70% of orders belong to one customer
        if random.random() < 0.70:
            customer_id = HOT_CUSTOMER
        else:
            customer_id = random.choice(normal_customers)

        product_id = f"PROD_{random.randint(1, 100)}"
        amount = round(random.uniform(100, 5000), 2)

        # Generate timestamps across 30 days
        order_time = start_date + timedelta(
            minutes=random.randint(0, 30 * 24 * 60)
        )

        writer.writerow([
            i,
            customer_id,
            product_id,
            amount,
            order_time.isoformat()
        ])
# Add extra records to test data-quality validation
extra_records = [
    [100001, None, "PROD_10", 500.00, "2026-01-15T10:00:00"],
    [100002, "CUST_0001", "PROD_20", None, "2026-01-16T11:00:00"],
    [100003, "CUST_0002", "PROD_30", -100.00, "2026-01-17T12:00:00"],
    [100004, "CUST_0003", "PROD_40", 0.00, "2026-01-18T13:00:00"],
    [100005, "CUST_0004", "PROD_50", 250.00, "2026-01-19T14:00:00"],
    [100005, "CUST_0004", "PROD_60", 350.00, "2026-01-19T15:00:00"],
    [100007, "   ", "PROD_70", 400.00, "2026-01-20T16:00:00"],
    [100008, "CUST_0005", "PROD_80", 600.00, "2025-12-20T09:00:00"]
]

with OUTPUT_FILE.open("a", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerows(extra_records)

print(f"Extra data-quality test records added: {len(extra_records)}")
print("Data generation completed!")
print(f"Total orders generated: {TOTAL_ORDERS}")
print(f"Output file: {OUTPUT_FILE}")
print("Expected hot customer share: approximately 70%")
