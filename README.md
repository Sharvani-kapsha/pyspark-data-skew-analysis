# Spark Data Skew Analysis and Optimization

## 1. Project Overview

This project demonstrates how data skew affects distributed data processing in Apache Spark and how **salting** can help distribute heavily concentrated keys across partitions.

A reproducible synthetic e-commerce orders dataset is generated using Python. One customer (`CUST_HOT`) receives approximately 70% of all orders, creating a data-skew scenario.

The project analyzes partition distribution, applies a salting technique, compares normal and optimized aggregations, validates data quality, and measures performance.

## 2. Objectives

- Generate reproducible synthetic data with a fixed random seed.
- Simulate data skew using a highly concentrated customer key.
- Identify uneven partition distribution.
- Implement salting to distribute hot-key records.
- Compare normal and salted aggregation performance.
- Validate business rules and quarantine invalid and duplicate records.
- Verify that both aggregation approaches produce matching results.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.13 | Data generation and pipeline logic |
| Apache Spark 4.0.3 | Distributed data processing |
| PySpark | Spark DataFrame operations |
| Pandas | Local CSV output writing |
| Pytest | Automated testing |
| Java 21 | Spark runtime |

## 4. Project Structure

```text
spark-data-skew-project/
│
├── data/
│   ├── input/
│   │   └── orders.csv
│   └── output/
│       ├── valid_orders/
│       ├── invalid_orders/
│       ├── duplicate_orders/
│       ├── customer_totals/
│       ├── metrics.json
│       └── pipeline.log
│
├── src/
│   ├── generate_data.py
│   ├── analyze_skew.py
│   ├── optimize_skew.py
│   ├── benchmark.py
│   ├── validation.py
│   └── pipeline.py
│
├── tests/
│   ├── test_validation.py
│   └── test_pipeline.py
│
├── reports/
├── README.md
└── test_spark.py
```

## 5. Dataset Description

The generator creates 100,000 base orders and appends 8 controlled data-quality test records, resulting in 100,008 input records.

A fixed random seed (`42`) ensures reproducibility.

| Column | Description |
|---|---|
| order_id | Unique order identifier, except deliberate duplicate test records |
| customer_id | Customer identifier |
| product_id | Product identifier |
| amount | Order amount |
| order_timestamp | Order creation timestamp |

### Data distribution

- Approximately 70% of base orders belong to `CUST_HOT`.
- Remaining orders are distributed across 1,000 normal customers.
- Additional records test missing values, invalid amounts, duplicate IDs, blank customer IDs, and late-arriving timestamps.

## 6. Architecture and Processing Flow

```text
Synthetic Data Generator
          |
          v
      orders.csv
          |
          v
     Spark Pipeline
          |
          v
   Data Validation
          |
     +----+----+
     |    |    |
     v    v    v
   Valid Invalid Duplicate
   Data   Data   Data
     |
     v
  Skew Analysis
     |
     v
 Normal Aggregation
     |
     v
 Salted Aggregation
     |
     v
 Correctness and
 Performance Comparison
     |
     v
 Metrics and Logs
```

## 7. Data Skew Analysis

The initial dataset is partitioned by `customer_id`.

The hot customer creates an uneven partition distribution.

### Observed initial partition distribution

| Partition | Records |
|---|---:|
| 0 | 3,163 |
| 1 | 3,759 |
| 2 | 73,992 |
| 3 | 4,070 |
| 4 | 3,152 |
| 5 | 3,964 |
| 6 | 3,806 |
| 7 | 4,094 |

Partition 2 contains most of the records, demonstrating the impact of key skew.

## 8. Optimization: Salting

Salting adds an additional key to distribute records belonging to the hot customer.

For `CUST_HOT`, a salt value between 0 and 7 is generated. Other customers receive salt value 0.

Instead of grouping only by `customer_id`, the first aggregation groups by:

`customer_id, salt`

A second aggregation combines partial totals by `customer_id`.

### Observed partition distribution after salting

| Partition | Records |
|---|---:|
| 0 | 12,730 |
| 1 | 12,152 |
| 2 | 12,991 |
| 3 | 13,073 |
| 4 | 12,363 |
| 5 | 21,041 |
| 6 | 3,685 |
| 7 | 11,965 |

The busiest partition decreased from 73,992 to 21,041 records, approximately a 71.6% reduction.

## 9. Benchmark Results

Three trials were conducted using the same cached input data.

| Trial | Normal Aggregation (s) | Salted Aggregation (s) |
|---|---:|---:|
| 1 | 0.519 | 0.845 |
| 2 | 0.249 | 0.415 |
| 3 | 0.214 | 0.311 |
| **Average** | **0.327** | **0.523** |

### Interpretation

Salting was approximately 60.1% slower on average in this local benchmark.

Although salting improved partition distribution, the additional aggregation stage introduced overhead. For this workload and environment, the normal aggregation was faster.

This demonstrates that reducing data skew does not automatically guarantee lower end-to-end runtime. Optimization decisions should consider workload size, partition distribution, and processing overhead.

## 10. Data Quality and Validation

The pipeline applies business validation rules.

- Reject records with missing order IDs.
- Reject records with missing or blank customer IDs.
- Reject records with missing product IDs.
- Reject records with missing or non-positive amounts.
- Reject records with missing timestamps.
- Quarantine all records associated with duplicate order IDs.
- Retain older timestamps when the other validation rules pass.

### Pipeline execution results

| Metric | Result |
|---|---:|
| Input records | 100,008 |
| Valid records | 100,001 |
| Invalid records | 5 |
| Duplicate records | 2 |
| Normal customer groups | 1,001 |
| Optimized customer groups | 1,001 |
| Mismatched customer totals | 0 |
| Aggregation results match | True |

The records reconcile:

`100008 = 100001 + 5 + 2`

## 11. Running the Project

### Prerequisites

- Python 3.13
- Java 21
- Windows, macOS, or Linux with a compatible Spark environment

### Install dependencies

```bash
python -m pip install pyspark==4.0.3 pytest pandas
```

### Generate synthetic data

```bash
python src/generate_data.py
```

### Analyze skew

```bash
python src/analyze_skew.py
```

### Run salting optimization

```bash
python src/optimize_skew.py
```

### Run benchmark

```bash
python src/benchmark.py
```

### Run the complete pipeline

```bash
python src/pipeline.py
```

### Run automated tests

```bash
python -m pytest tests/test_pipeline.py -v
```

## 12. Output Files

The pipeline produces:

- `valid_orders/` — validated orders.
- `invalid_orders/` — rejected records.
- `duplicate_orders/` — duplicate records.
- `customer_totals/` — aggregated customer totals.
- `metrics.json` — record counts, runtime measurements, and correctness results.
- `pipeline.log` — execution logs.


## 13. Testing

The project includes six automated tests using Pytest.

| Test Case | Result |
|---|---|
| Valid orders | PASSED |
| Null and malformed amounts | PASSED |
| Duplicate order IDs | PASSED |
| Empty datasets | PASSED |
| Negative and zero amounts | PASSED |
| Blank customer IDs | PASSED |

**Test execution summary:**
- Total tests: 6
- Passed: 6
- Failed: 0
- Execution time: 139.80 seconds

Run the tests using:

```bash
python -m pytest tests/ -v

14. Limitations and Trade-offs

- The benchmark uses local Spark execution, not a multi-node cluster.
- Runtime measurements can vary between runs.
- The benchmark uses three trials in one Spark session; it is not a fully isolated performance study.
- The salting implementation uses eight salt buckets.
- Pandas is used to write local CSV outputs because of the current Windows Hadoop configuration. This approach is suitable for this project's local dataset, but collecting large distributed datasets into Pandas is not scalable.
- The current validation does not implement a complete late-arriving event or malformed timestamp policy.
- Pipeline restart, backfill, and idempotency behaviour require further operational hardening.
### Idempotency and Repeatability Test

The pipeline was executed twice using the same input dataset.

Both executions produced identical record counts:

| Metric | Result |
|---|---:|
| Input records | 100,008 |
| Valid records | 100,001 |
| Invalid records | 5 |
| Duplicate records | 2 |
| Customer groups | 1,001 |
| Aggregation mismatches | 0 |

Before each run, previous generated outputs are cleaned from the output directory, while the pipeline log is retained and appended to.

This confirms repeatability for successful runs with the same input dataset. It does not yet guarantee transactional recovery after a failed or interrupted write.

## 15. Conclusion

This project demonstrates the impact of data skew in Apache Spark, identifies a heavily concentrated customer key, and applies salting to improve partition distribution.

The benchmark shows that salting can reduce the busiest partition while introducing additional runtime overhead. Correctness checks confirm that normal and salted aggregation produce matching customer totals.

The project also demonstrates data-quality validation, duplicate quarantine, automated testing, and execution metrics in a reproducible data-processing workflow.
