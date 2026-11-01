# Spark Data Skew Analysis and Optimization

## 1. Project Overview

This project demonstrates how data skew affects distributed data processing in Apache Spark and how **salting** can help distribute records associated with heavily concentrated keys.

A reproducible synthetic e-commerce orders dataset is generated using Python. One customer (`CUST_HOT`) receives approximately 70% of the base orders, creating a data-skew scenario.

The project includes:

- Synthetic data generation
- Data quality validation and duplicate quarantine
- Partition and skew analysis
- Normal and salted aggregations
- Aggregation correctness comparison
- Runtime measurement
- Pipeline logging and execution metrics
- Failure recovery and repeat-run verification
- Automated tests

## 2. Objectives

- Generate reproducible synthetic data using a fixed random seed.
- Simulate data skew using a highly concentrated customer key.
- Analyze partition distribution.
- Apply salting to distribute hot-key records.
- Compare normal and salted aggregation performance.
- Validate business rules and quarantine invalid records.
- Verify aggregation correctness.
- Support safe repeat execution and preserve previous successful output when a pipeline run fails.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.13 | Data generation and pipeline logic |
| Apache Spark 4.0.3 | Distributed data processing |
| PySpark | Spark DataFrame operations |
| Python CSV module | Local CSV output writing |
| Pytest | Automated testing |
| Java 21 | Spark runtime |

## 4. Project Structure

```text
spark-data-skew-project/
│
├── ai-documentation/
│   └── prompt-history.md
│
├── data/
│   ├── input/
│   │   └── orders.csv
│   └── output/                  # Generated locally
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
│   ├── metrics.json
│   ├── customer_totals.csv
│   ├── benchmark_results.csv
│   ├── data_skew_analysis.png
│   └── create_skew_chart.py
│
├── README.md
├── requirements.txt
├── .gitignore
└── test_spark.py
```

Generated pipeline output and temporary failed-run directories are excluded from Git.

## 5. AI Usage and Prompt History

This project was developed with assistance from ChatGPT.

AI assistance was used for:

- Learning Apache Spark and PySpark concepts.
- Understanding data skew and optimization techniques.
- Implementation guidance.
- Debugging and troubleshooting.
- Interpreting benchmark results.
- Preparing project documentation.

The developer ran the project locally, reviewed the implementation, verified outputs, and studied the design decisions.

[View AI Prompt History](ai-documentation/prompt-history.md)

**AI tool used:** ChatGPT.

The prompt-history document contains reconstructed prompt summaries rather than claiming to be a complete verbatim conversation transcript.

## 6. Dataset Description

The generator creates 100,000 base orders and appends eight controlled data-quality test records, resulting in **100,008 input records**.

A fixed random seed (`42`) ensures reproducibility.

### Source schema

| Column | Data type | Description |
|---|---|---|
| order_id | Integer | Order identifier |
| customer_id | String | Customer identifier |
| product_id | String | Product identifier |
| amount | Double | Order amount |
| order_timestamp | String | Order creation date or timestamp |

### Data distribution

- Approximately 70% of base orders belong to `CUST_HOT`.
- Remaining orders are distributed across 1,000 normal customers.
- Additional records test missing values, invalid amounts, duplicate IDs, blank customer IDs, and an older timestamp.

## 7. Architecture and Processing Flow

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
     +----+-----+
     |    |     |
     v    v     v
   Valid Invalid Duplicate
   Data  Data    Data
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
Output Publishing
     |
     v
Metrics and Logs
```

## 8. Data Skew Analysis

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

### Hot-key skew analysis

| Metric | Result |
|---|---:|
| Hot customer | CUST_HOT |
| Hot customer orders | 70,153 |
| Normal customers | 1,000 |
| Average orders per normal customer | 29.85 |
| Hot-key skew ratio | 2,350.34:1 |

**Key finding:** `CUST_HOT` has approximately 2,350 times more orders than the average normal customer.

### Business insights

| Metric | Result |
|---|---:|
| Total order value | 255,000,230.02 |
| CUST_HOT order value | 178,625,584.97 |
| CUST_HOT contribution to total order value | 70.05% |
| CUST_HOT order count | 70,153 |
| Total customer groups | 1,001 |

Amounts are expressed in dataset units, not a specified real-world currency.

## 9. Optimization: Salting

Salting adds an additional key to distribute records belonging to the hot customer.

For `CUST_HOT`, a salt value between 0 and 7 is generated. Other customers receive salt value 0.

The first aggregation groups by:

```python
customer_id, salt
```

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

The busiest partition decreased from 73,992 to 21,041 records, approximately a **71.6% reduction**.

## 10. Benchmark Results

Three trials were conducted using the same cached input data.

| Trial | Normal Aggregation (s) | Salted Aggregation (s) |
|---|---:|---:|
| 1 | 0.519 | 0.845 |
| 2 | 0.249 | 0.415 |
| 3 | 0.214 | 0.311 |
| **Average** | **0.327** | **0.523** |

### Interpretation

Salting was approximately 60.1% slower on average in this local benchmark.

Although salting improved partition distribution, the additional aggregation stage introduced overhead. For this workload and environment, normal aggregation was faster.

A later pipeline execution also measured normal aggregation at 1.337 seconds and salted aggregation at 1.028 seconds.

These measurements vary between executions and are not directly comparable to the three-trial benchmark. They demonstrate why performance conclusions should be based on repeated measurements under controlled conditions.

Reducing data skew does not automatically guarantee lower runtime.

## 11. Data Quality and Validation

The pipeline applies the following business validation rules:

- Reject records with missing order IDs.
- Reject records with missing or blank customer IDs.
- Reject records with missing product IDs.
- Reject records with missing or non-positive amounts.
- Reject records with missing or unparseable timestamps.
- Quarantine every record associated with a duplicate order ID.
- Retain older timestamps when the other validation rules pass.

Both records associated with a duplicate order ID are quarantined.

### Latest successful pipeline execution

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

```text
100008 = 100001 + 5 + 2
```

## 12. Running the Project

### Prerequisites

- Python 3.13
- Java 21
- A compatible local Spark environment

### Create a virtual environment (recommended)

```bash
python -m venv venv
```

Activate it on Windows PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

### Install dependencies

```bash
python -m pip install -r requirements.txt
```

### Generate synthetic data

```bash
python -m src.generate_data
```

### Analyze skew

```bash
python -m src.analyze_skew
```

### Run salting optimization

```bash
python -m src.optimize_skew
```

### Run benchmark

```bash
python -m src.benchmark
```

### Run the complete pipeline

```bash
python -m src.pipeline
```

The pipeline also supports custom input and output paths:

```bash
python -m src.pipeline --input data/input/orders.csv --output data/output
```

### Run automated tests

```bash
python -m pytest tests/ -v
```

## 13. Pipeline Outputs and Observability

The pipeline produces:

| Output | Description |
|---|---|
| `valid_orders/` | Validated orders |
| `invalid_orders/` | Records rejected by validation |
| `duplicate_orders/` | Records associated with duplicate order IDs |
| `customer_totals/` | Aggregated customer order values |
| `metrics.json` | Run status, timestamps, counts, timings, and correctness results |
| `pipeline.log` | Execution logs |

The metrics include:

- Unique run ID
- Run status
- Start and end timestamps in UTC
- Input path and record count
- Valid, invalid, and duplicate counts
- Partition counts
- Normal and salted aggregation timings
- Aggregation comparison result
- Total pipeline runtime
- Failure reason, when applicable

## 14. Failure Recovery and Idempotency

The pipeline uses a staging directory for each execution.

On successful processing:

1. Outputs are written to a new staging directory.
2. Metrics are recorded.
3. The previous successful output is backed up when present.
4. The staging directory replaces the previous output.
5. The backup is removed after successful publishing.

On processing failure:

- The failed run's metrics and logs are retained in a separate failed-run directory.
- The previous successful output is preserved.
- The failure reason is recorded in `metrics.json`.

### Failure recovery verification

A run was deliberately executed with a missing input file.

The pipeline reported `FAILED`, recorded the `PATH_NOT_FOUND` error, and preserved the previous successful output. A subsequent valid execution completed successfully.

### Repeatability verification

The pipeline was executed twice with the same input dataset.

Both executions produced identical output counts:

| Metric | Result |
|---|---:|
| Input records | 100,008 |
| Valid records | 100,001 |
| Invalid records | 5 |
| Duplicate records | 2 |
| Customer groups | 1,001 |
| Aggregation mismatches | 0 |

The output directory is replaced on a successful rerun rather than appended to, preventing duplicate output accumulation for this local CSV target.

This demonstrates repeatability and recovery for the tested local execution workflow. It is not a general transactional guarantee for distributed or database sinks.

## 15. Testing

The project includes seven automated tests using Pytest.

| Test case | Result |
|---|---|
| Valid orders | PASSED |
| Null and malformed amounts | PASSED |
| Duplicate order IDs | PASSED |
| Empty datasets | PASSED |
| Negative and zero amounts | PASSED |
| Blank customer IDs | PASSED |
| Malformed timestamps | PASSED |

### Recorded test execution

- Total tests: 7
- Passed: 7
- Failed: 0

Run the tests using:

```bash
python -m pytest tests/ -v
```

## 16. Limitations and Trade-offs

- The benchmark uses local Spark execution, not a multi-node cluster.
- Runtime measurements can vary between executions.
- The benchmark uses three trials in one Spark session and is not a fully isolated performance study.
- The salting implementation uses eight salt buckets.
- The local output writer uses Python's CSV module and Spark's `toLocalIterator()` to stream rows to a single local CSV file. This avoids collecting the entire DataFrame into Pandas, but the output writer is still driver-side and is not a distributed production sink.
- The pipeline currently validates order fields but does not implement a product master lookup or full referential-integrity validation.
- The pipeline's local directory replacement strategy is not a substitute for transactional storage guarantees in a production data platform.

## 17. Conclusion

This project demonstrates the impact of data skew in Apache Spark, identifies a heavily concentrated customer key, and applies salting to improve partition distribution.

The benchmark shows that salting can reduce the busiest partition while introducing additional runtime overhead. Correctness checks confirm that normal and salted aggregations produce matching customer totals.

The project also demonstrates data-quality validation, duplicate quarantine, automated testing, execution metrics, failure recovery, and repeatable output publishing in a reproducible data-processing workflow.