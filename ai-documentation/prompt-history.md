# AI Prompt History and Usage Disclosure

## Project
**Spark Data Skew Analysis and Optimization**

GitHub repository: https://github.com/Sharvani-kapsha/pyspark-data-skew-analysis

## 1. AI Tool Used

ChatGPT (OpenAI)

No other AI tools were used during this project.

## 2. Purpose of AI Assistance

ChatGPT was used as a learning and implementation assistant during project development.

The assistance included:
- Understanding Apache Spark and PySpark concepts.
- Learning data skew and its effects on distributed processing.
- Understanding data generation and data validation logic.
- Getting guidance on implementing salting for hot-key distribution.
- Understanding aggregation and partition behaviour.
- Debugging Python and PySpark errors.
- Reviewing benchmark results and interpreting performance differences.
- Understanding automated tests and project execution.
- Preparing project documentation and visual reports.

## 3. Project Development Prompt History

**Disclosure:** The following entries are reconstructed summaries of prompts from the project-development conversations. They are not claimed to be exact verbatim transcripts of every original message.

### Prompt 1 — Project understanding and setup
Help me understand and build a beginner-friendly PySpark project focused on analysing and optimizing data skew. Explain the project structure, required dependencies, and implementation steps.

### Prompt 2 — Synthetic data generation
Help me create a reproducible synthetic orders dataset using Python. Include a hot customer who receives a large proportion of the orders so that I can demonstrate data skew.

### Prompt 3 — Data validation
Help me implement PySpark data validation for orders. Identify missing values, blank customer IDs, invalid amounts, and duplicate order records. Explain the logic.

### Prompt 4 — Data skew analysis
Help me analyse the distribution of customer records across Spark partitions. Explain how to identify a hot key and calculate the skew ratio.

### Prompt 5 — Data skew optimization
Guide me through implementing the salting technique in PySpark to distribute records belonging to a hot customer across multiple salt buckets.

### Prompt 6 — Aggregation comparison
Help me compare normal aggregation with salted aggregation and verify that both approaches produce matching customer totals.

### Prompt 7 — Benchmarking
Help me benchmark normal and salted aggregation, record execution times, and interpret the results without making unsupported performance claims.

### Prompt 8 — Pipeline development
Help me connect data generation, validation, skew analysis, optimization, aggregation, and metrics into a single PySpark pipeline.

### Prompt 9 — Testing and debugging
Help me create and run automated tests for the validation and pipeline logic. Explain errors and guide me through fixing them.

### Prompt 10 — Reports and visualizations
Help me create project reports, export relevant metrics and aggregation results, and generate a chart comparing partition distributions before and after salting.

### Prompt 11 — Documentation
Help me document the project architecture, setup instructions, execution steps, benchmark results, findings, limitations, and visualizations in the README.

### Prompt 12 — GitHub publishing
Guide me through preparing the repository, committing project files, and publishing the project to GitHub.

## 4. Human Review and Verification

I used ChatGPT as an assistant, but I ran the project in my local development environment and reviewed its outputs.

I checked:
- Dataset generation.
- Data validation results.
- Valid, invalid, and duplicate record counts.
- Partition distribution before and after salting.
- Aggregation result consistency.
- Benchmark execution times.
- Automated test results.
- Generated reports and visualizations.

## 5. AI Usage and Ownership Disclosure

ChatGPT provided explanations, implementation guidance, debugging assistance, and documentation support.

I am responsible for reviewing the implementation, understanding the design decisions, verifying the results, and explaining the project's functionality and limitations.

I do not claim that the project was developed without AI assistance.

## 6. Limitations of This Prompt History

This document is a reconstructed summary of the project-related prompts based on the available ChatGPT conversation context. It is not a complete verbatim export of the original conversation history.

The AI responses have not been reproduced in this document.

---

**AI tool used:** ChatGPT only  
**Documentation type:** Reconstructed prompt history and AI usage disclosure