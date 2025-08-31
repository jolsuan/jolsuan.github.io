---
title: End-to-End Workforce Data Pipeline
description: Streaming workforce roster data into a warehouse and dashboard
---

This project demonstrates an end-to-end data pipeline using only free and open-source tools. Kafka captures daily roster changes,
Spark processes the streaming data into Parquet files, and Airflow orchestrates the workflow inside Docker containers. dbt
models transform the data in a Postgres warehouse, which is then visualized in Superset. The entire stack runs on the AWS Free
Tier and is developed with VS Code.

## Workforce Daily Roster Dataset

The `workforce_daily_roster` dataset is unique by `employee_id` and `effective_date`. It records promotions, transfers, and
terminations so that an employee's final row shows an inactive status unless they are rehired. A sample of the data is shown
below:

| employee_id | effective_date | role            | department | status   |
|-------------|----------------|-----------------|------------|----------|
| 1001        | 2020-01-01     | Analyst         | Sales      | Active   |
| 1001        | 2021-06-15     | Senior Analyst  | Sales      | Active   |
| 1001        | 2022-10-01     | Manager         | Operations | Active   |
| 1001        | 2023-03-31     | Manager         | Operations | Inactive |

The full sample dataset is available [here](workforce_daily_roster.csv).

## Sample Python Pipeline

The [pipeline.py](pipeline.py) script outlines how Kafka, Spark, and Airflow work together to ingest roster events and load them into Postgres and Parquet outputs.
