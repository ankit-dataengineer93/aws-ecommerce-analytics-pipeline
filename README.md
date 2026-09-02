# E-commerce Analytics Pipeline (AWS Batch + Streaming)

A production-style data pipeline that ingests e-commerce order and clickstream events through both **batch** and **streaming** paths, cleans and catalogs them with AWS Glue, loads them into a **Redshift** star schema, and orchestrates the whole thing with **Step Functions** — with automatic email alerts on failure.

Built as a hands-on project to learn real AWS data engineering patterns: schema design, IAM permissions, service orchestration, and debugging actual production-style failures (not just a tutorial happy path).

---

## Architecture

![Architecture Diagram](docs/screenshots/architecture-diagram.png)
*(See "Screenshots" section below for how to export this — or view it live in the project write-up.)*

**Data flows in two ways:**

1. **Streaming path:** A Python event generator pushes clickstream events (views, cart adds, purchases) to **Kinesis Data Streams**, which **Firehose** buffers and delivers to S3, partitioned by date/hour.
2. **Batch path:** Historical order data is generated as CSV and uploaded directly to S3.

Both paths land in an **S3 raw zone**. From there:

- **AWS Glue** cleans, deduplicates, and converts the data to Parquet in an **S3 curated zone**.
- A **Glue Crawler** catalogs the curated data, making it queryable via **Athena**.
- A **Lambda function** triggers a Redshift **COPY** command to load curated data into a **Redshift Serverless** star schema (`fact_orders`, `dim_customer`, `dim_product`, `dim_date`).
- **Step Functions** orchestrates the Glue job → Lambda/Redshift load sequence daily, and **SNS** sends an email alert if any step fails.

---

## Repo Structure

```
aws-ecommerce-pipeline/
├── README.md
├── data-generator/
│   └── generate_events.py          # Simulates order/clickstream events (batch CSV or live Kinesis stream)
├── glue-jobs/
│   └── batch_clean_orders.py       # Glue PySpark job: cleans raw CSV, writes Parquet to curated zone
├── lambda/
│   └── redshift_copy_lambda.py     # Triggers the Redshift COPY command via the Data API
├── orchestration/
│   └── step_function_definition.json  # Step Functions state machine: Glue -> Lambda -> SNS on failure
├── warehouse/
│   ├── redshift_schema.sql         # Star schema DDL (fact_orders, dim_customer, dim_product, dim_date)
│   ├── load_data.sql               # Staging table + COPY + star schema population
│   └── analytical_queries.sql      # Revenue by day, top products, funnel analysis
└── docs/
    └── screenshots/                # AWS console screenshots (see below)
```

---

## Tech Stack

| Layer | Service |
|---|---|
| Streaming ingestion | Amazon Kinesis Data Streams |
| Streaming delivery | Amazon Kinesis Data Firehose |
| Storage | Amazon S3 (raw + curated zones) |
| Batch transformation | AWS Glue (PySpark) |
| Cataloging | AWS Glue Data Catalog + Crawler |
| Ad-hoc querying | Amazon Athena |
| Data warehouse | Amazon Redshift Serverless |
| Compute glue | AWS Lambda |
| Orchestration | AWS Step Functions |
| Alerting | Amazon SNS |

---

## How It Works — Step by Step

### 1. Generate data
```bash
# Batch mode — writes orders.csv locally
python3 generate_events.py batch

# Streaming mode — pushes live JSON events to Kinesis every 0.5-2s
python3 generate_events.py streaming
```

### 2. Batch cleaning (AWS Glue)
`batch_clean_orders.py` reads raw CSV from S3 with an explicit schema (avoiding fragile schema inference), drops duplicates and nulls, and writes cleaned Parquet to the curated zone. Runs with two job parameters: `--RAW_S3_PATH` and `--CURATED_S3_PATH`.

### 3. Cataloging
A Glue Crawler scans the curated S3 zone and registers a table in the Glue Data Catalog, making the data instantly queryable in Athena with standard SQL.

### 4. Warehouse load
`redshift_schema.sql` defines a star schema. `load_data.sql` stages the curated Parquet data and populates the fact/dimension tables. The `redshift_copy_lambda.py` function wraps this in a Lambda so it can be triggered as part of an automated workflow (rather than run manually in the Query Editor).

### 5. Orchestration
`step_function_definition.json` defines a state machine that:
- Runs the Glue job and waits for completion
- On success, invokes the Redshift-load Lambda
- On failure at **any** step, publishes a detailed error message to an SNS topic (which emails the on-call/developer)

### 6. Analysis
`analytical_queries.sql` includes:
- Revenue by day
- Top 5 products by revenue
- Orders by region and event type
- A view → cart → purchase conversion funnel

---

## Design Decisions & Trade-offs

- **Explicit schemas over `inferSchema`** — early iterations used Spark's automatic schema inference, which failed unpredictably on the raw CSV. Switching to an explicit `StructType` schema made the Glue job deterministic and easier to debug.
- **Lambda instead of direct Step Functions → Redshift Data API integration** — the native `aws-sdk:redshiftdata:executeStatement` Step Functions integration returned a `Redshift endpoint doesn't exist in this region` error that traced back to a workgroup-naming quirk in Redshift Serverless, not a real region issue. Wrapping the call in a small Lambda function sidestepped the issue and is arguably a more common real-world pattern anyway (easier to unit test, easier to extend with custom logic later).
- **Staging table pattern for the warehouse load** — rather than trying to `COPY` directly into the final star schema tables, curated data lands in a flat `staging_orders` table first, then `INSERT ... SELECT` statements populate the dimensions and fact table. This avoids partial/duplicate loads and makes the transformation logic auditable in plain SQL.
- **What I'd do differently at scale:** swap Kinesis for MSK (Kafka) if integrating with a broader event-driven architecture beyond AWS-native tooling; add data quality checks (e.g. Great Expectations or a custom row-count/null-check Lambda) as an explicit Step Functions state before the warehouse load; and move from Redshift Serverless to a provisioned cluster with reserved capacity once query volume becomes predictable (cost optimization).

---

## Real Problems Hit & Fixed (a running log)

This project involved genuine debugging, not just following steps. A few highlights, useful for interview talking points:

- **IAM trust policy propagation delay** — a freshly auto-created Firehose IAM role failed with "unable to assume role" on the first attempt; resolved by retrying after ~60 seconds once IAM propagated.
- **Parquet/Redshift type mismatch** — Redshift Spectrum rejected a `TIMESTAMP` column because the underlying Parquet file stored it as a string; fixed by staging it as `VARCHAR` and casting with `TO_TIMESTAMP()` during the star-schema load.
- **Redshift Serverless workgroup naming** — the console silently appended `-base-rpu` to the workgroup name I chose, causing every Redshift Data API call to fail with a misleading "endpoint doesn't exist in this region" error. Diagnosed via AWS CLI (`list-workgroups`) rather than trial and error.
- **Missing `redshift-serverless:GetCredentials` permission** — `AmazonRedshiftDataFullAccess` alone isn't sufficient for the Data API's temporary-credential flow; needed an additional scoped inline policy.

---

## Screenshots

Add these to `docs/screenshots/` and reference them here once captured:

- [ ] Glue job run history showing "Succeeded" status
- [ ] Glue Data Catalog table schema (auto-detected columns)
- [ ] Athena query results (`SELECT * FROM orders LIMIT 10`)
- [ ] Kinesis stream + Firehose delivery stream, both "Active"
- [ ] S3 bucket showing partitioned clickstream folders
- [ ] Redshift Query Editor v2 — star schema tables + row counts
- [ ] Analytical query results (revenue by day chart/table)
- [ ] Step Functions execution graph — full green run
- [ ] SNS email alert received on a simulated failure

---

## Future Improvements

- Add dbt for the warehouse transformation layer instead of raw SQL scripts
- Add data quality validation as an explicit pipeline step
- Schedule the pipeline daily via EventBridge instead of manual execution
- Add a QuickSight or Streamlit dashboard on top of the Redshift star schema
- Migrate the demo Lambda-based Redshift COPY into a more robust pattern (e.g. Redshift's native `COPY` scheduled via query, or a dbt run)
