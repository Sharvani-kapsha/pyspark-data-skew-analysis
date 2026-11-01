
# Spark Data Skew Analysis and Optimization

## 1. Project Overview

This project demonstrates how data skew affects distributed data processing in Apache Spark and how salting can help distribute records associated with heavily concentrated keys across partitions.

A reproducible synthetic e-commerce orders dataset is generated using Python. One customer (`CUST_HOT`) receives approximately 70% of the base orders, creating a data-skew scenario.

The project includes:

- Synthetic data generation
- Data ingestion and schema definition
- Data quality validation
- Duplicate detection and quarantine
- Partition analysis
- Normal and salted aggregations
- Correctness and performance comparison
- Execution metrics and logging
- Repeatable pipeline execution and failure handling
- Automated tests

## 2. Objectives

- Generate reproducible synthetic data using a fixed random seed.
- Simulate data skew using a highly concentrated customer key.
- Analyze partition distribution.
- Apply salting to distribute hot-key records.
- Compare normal and optimized aggregation results.
- Measure aggregation execution time.
- Validate business rules and quarantine invalid records.
- Verify that normal and salted aggregations produce matching results.
- Support repeatable execution and preserve previous successful output when a new run fails.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.13 | Data generation and pipeline logic |
| Apache Spark 4.0.3 | Distributed data processing |
| PySpark | Spark DataFrame transformations |
| Java 21 | Spark runtime |
| Pandas / NumPy | Supporting data analysis |
| Matplotlib | Data visualization |
| Pytest | Automated testing |
| CSV / JSON | Input, output and execution metrics |

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
│   └── test_pipeline.py
│
├── reports/
├── requirements.txt
├── README.md
├── .gitignore
└── test_spark.py
```

Generated output and temporary failure folders are excluded from Git.

## 5. Dataset Description

The synthetic dataset contains:

- 100,000 base orders.
- 8 additional controlled data-quality test records.
- 100,008 total input records.

A fixed random seed (`42`) ensures reproducibility.

### Source schema

| Column | Data type | Description |
|---|---|---|
| order_id | Integer | Order identifier |
| customer_id | String | Customer identifier |
| product_id | String | Product identifier |
| amount | Double | Order amount |
| order_timestamp | String | Order creation date/time |

### Data distribution

- Approximately 70% of base orders belong to `CUST_HOT`.
- Remaining orders are distributed across 1,000 normal customers.
- Additional records cover null values, invalid amounts, duplicate IDs, blank customer IDs and a late/out-of-range timestamp.

## 6. Architecture and Processing Flow

```text
Synthetic Data Generator
          |
          v
      orders.csv
          |
          v
     Spark Ingestion
          |
          v
    Schema Definition
          |
          v
    Data Validation
          |
     +----+----+
     |    |    |
     v    v    v
   Valid Invalid Duplicate
   Data  Data   Data
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
Correctness Comparison
     |
     v
Performance Measurement
     |
     v
Metrics, Logs and Output
```

## 7. Data Skew Analysis

Data skew occurs when a small number of keys receive a disproportionately large number of records.

In this project, `CUST_HOT` receives approximately 70% of the base orders.

This creates an uneven workload during customer-level aggregation.

The project analyzes partition distribution and compares the normal aggregation with a salted aggregation approach.

## 8. Optimization: Salting

Salting introduces an additional key to distribute records belonging to a heavily concentrated customer.

For `CUST_HOT`, a salt value between 0 and 7 is generated using a fixed random seed. Other customers receive salt value 0.

### Normal aggregation

The normal approach groups records using:

```python
groupBy("customer_id")
```

### Salted aggregation

The optimized approach performs two aggregation stages:

1. Group by `customer_id` and `salt` to calculate partial totals.
2. Group by `customer_id` again to combine the partial totals.

This helps distribute the hot customer's workload across multiple groups before calculating the final total.

**Important:** Salting can improve workload distribution, but it does not guarantee faster execution. Additional aggregation stages introduce overhead.

## 9. Pipeline Execution Results

The latest successful pipeline execution produced the following results.

| Metric | Result |
|---|---:|
| Input records | 100,008 |
| Valid records | 100,001 |
| Invalid records | 5 |
| Duplicate records | 2 |
| Original partitions | 2 |
| Repartitioned partitions | 8 |
| Coalesced partitions | 2 |
| Normal customer groups | 1,001 |
| Optimized customer groups | 1,001 |
| Mismatched customer totals | 0 |
| Aggregation results match | True |

### Record reconciliation

```text
100,008 = 100,001 + 5 + 2
```

All input records are accounted for across the valid, invalid and duplicate outputs.

### Aggregation performance

Latest recorded pipeline run:

| Metric | Execution time |
|---|---:|
| Normal aggregation | 1.337 seconds |
| Salted aggregation | 1.028 seconds |
| Total pipeline runtime | 31.218 seconds |

These are measurements from one local execution, not a guaranteed performance improvement. Results may vary with the environment, data volume and Spark execution conditions.

## 10. Data Quality and Validation

The validation module applies the following rules:

- Reject records with missing order IDs.
- Reject records with missing or blank customer IDs.
- Reject records with missing product IDs.
- Reject records with missing amounts.
- Reject records with zero or negative amounts.
- Reject records with missing timestamps.
- Reject records with malformed timestamps.
- Quarantine all records associated with duplicate order IDs.

The timestamp validation accepts ISO date (`yyyy-MM-dd`) and ISO datetime (`yyyy-MM-dd'T'HH:mm:ss`) formats.

Older timestamps are not automatically rejected. A separate late-arriving event policy is outside the current implementation.

### Duplicate handling

If an order ID appears more than once, all records associated with that duplicate ID are quarantined rather than arbitrarily retaining one record.

## 11. Running the Project

### Prerequisites

- Python 3.13
- Java 21
- Git
- A compatible local Spark environment

### Clone the repository

```bash
git clone https://github.com/Sharvani-kapsha/pyspark-data-skew-analysis.git

cd pyspark-data-skew-analysis
```

### Create a virtual environment

Windows PowerShell:

```powershell
python -m venv venv

.\venv\Scripts\Activate.ps1
```

### Install dependencies

```bash
python -m pip install -r requirements.txt
```

### Generate synthetic data

```bash
python src/generate_data.py
```

### Run the complete pipeline

```bash
python -m src.pipeline --input data/input/orders.csv --output data/output
```

### Run individual analysis modules

```bash
python -m src.analyze_skew
```

```bash
python -m src.optimize_skew
```

```bash
python -m src.benchmark
```

### Run automated tests

```bash
python -m pytest tests/ -v
```

## 12. Output and Observability

The pipeline generates the following outputs under `data/output/`:

| Output | Description |
|---|---|
| valid_orders | Records that pass validation and uniqueness checks |
| invalid_orders | Records that fail business validation |
| duplicate_orders | All records associated with duplicate order IDs |
| customer_totals | Aggregated customer-level totals |
| metrics.json | Execution status, record counts, timings and correctness metrics |
| pipeline.log | Pipeline execution logs |

The metrics include a unique run ID, execution timestamps, input path, record counts, partition information, aggregation timings, comparison results and failure details.

## 13. Repeatability and Failure Recovery

The pipeline uses a staging output directory before publishing successful results.

### Successful execution

- Processing takes place in a run-specific staging directory.
- Results and metrics are written before publishing.
- On success, the staging output replaces the previous output.
- A previous successful output is backed up during publishing and removed after successful replacement.

### Failed execution

- The pipeline records a failed status and failure reason.
- Failed staging output is retained in a run-specific failure directory.
- The previous successful output is preserved when processing fails before publishing.

### Repeatability verification

The pipeline was executed more than once with the same input dataset.

The successful executions produced consistent record counts and matching aggregation results.

This demonstrates repeatability for the tested local filesystem workflow. It is not a guarantee of transactional recovery for distributed storage systems or every possible interruption during publishing.

## 14. Automated Testing

The project contains seven automated tests using Pytest.

| Test case | Result |
|---|---|
| Valid orders | PASSED |
| Null and malformed amounts | PASSED |
| Duplicate order IDs | PASSED |
| Empty dataset | PASSED |
| Negative and zero amounts | PASSED |
| Blank customer IDs | PASSED |
| Malformed timestamps | PASSED |

### Latest test execution

```text
Collected: 7 tests
Passed: 7
Failed: 0
Execution time: 252.90 seconds
```

Command:

```bash
python -m pytest tests/ -v
```

## 15. Limitations and Trade-offs

- The project uses local Spark execution rather than a multi-node cluster.
- Runtime measurements vary across executions and hardware environments.
- Salting introduces an additional aggregation stage and may not improve runtime for smaller datasets.
- The current pipeline uses eight salt buckets for the hot customer.
- Local CSV publishing uses Python's CSV writer and Spark's `toLocalIterator()` to stream records through the driver. This avoids collecting the complete dataset into Pandas, but it is not a distributed output-writing strategy.
- A product master/reference dataset and product-level referential integrity checks are not implemented.
- Run IDs are recorded in execution metrics; they are not added as columns to every output record.
- Late-arriving event handling and timezone-offset timestamp formats are not fully implemented.
- The current publishing mechanism is designed for a local filesystem and is not a transactional distributed data sink.

## 16. AI Assistance and Development

AI assistance was used during development for:

- Understanding Spark data skew and salting.
- Debugging PySpark transformations.
- Improving data validation logic.
- Reviewing pipeline reliability and failure handling.
- Preparing test cases and project documentation.

The implementation was executed and tested locally.

## 17. Conclusion

This project demonstrates a reproducible Apache Spark workflow for analyzing data skew in an e-commerce orders dataset.

It identifies a heavily concentrated customer key, applies salting, validates records, quarantines duplicates, compares aggregation correctness and measures execution performance.

The final pipeline execution produced matching normal and salted aggregation results with zero mismatches. Seven automated tests passed, and repeat execution and failure handling were verified in the local environment.

The project highlights an important optimization principle: improving partition distribution does not automatically guarantee lower execution time. Performance decisions should be based on measured results and workload characteristics.
