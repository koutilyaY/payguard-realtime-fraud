🚀 PayGuard — Real-Time Fraud Detection Platform

A production-style, end-to-end real-time fraud detection system built using Kafka, Spark Structured Streaming, Delta Lake, Redis, PostgreSQL, Prometheus, and Grafana.

This project simulates how a modern fintech platform processes high-volume transaction streams, performs stateful risk scoring, serves low-latency decisions, manages fraud cases, and exposes operational + business metrics for monitoring.

📌 Problem Statement

Financial platforms require:

Real-time fraud detection (sub-second to seconds latency)

Stateful feature engineering on streaming data

Reliable dead-letter handling

Online serving of risk decisions

Case management for flagged users

Observability of system health and fraud KPIs

PayGuard implements all of the above in a modular, production-inspired architecture.

Transaction Producer
        │
        ▼
      Kafka
        │
        ▼
Spark Structured Streaming
        │
        ▼
     Delta Lake
   ┌────────────┬────────────┬────────────┐
   │   Bronze   │   Silver   │    Gold    │
   │    Raw     │   Cleaned  │  Business  │
   └────────────┴────────────┴────────────┘
        │
        ▼
      Redis  (Low-Latency Risk Serving)
        │
        ▼
  PostgreSQL (Fraud Case Management)
        │
        ▼
Prometheus → Pushgateway → Grafana

⚙️ Tech Stack
Streaming & Processing

Apache Kafka — event ingestion

Spark Structured Streaming — real-time processing

Delta Lake — ACID lakehouse storage (Bronze/Silver/Gold)

Serving Layer

Redis — latest user risk score lookup (low latency)

PostgreSQL — fraud case management system

Observability

Prometheus — metrics collection

Pushgateway — streaming metric push

Grafana — dashboards (business + engineering KPIs)

Infrastructure

Docker Compose — container orchestration

Makefile — reproducible local environment

🗂 Lakehouse Architecture
🟫 Bronze — Raw Validated Transactions

Ingested from Kafka

Schema validated

Rule-based filtering applied

Stored append-only

Source of truth for valid transactions

🟨 Silver — Clean & Deduplicated

Deduplicated by event_id

Event-time watermark applied

Standardized schema

Cleaned analytical layer

🟦 Gold — Business-Ready Outputs
1️⃣ Windowed Risk Feature Log

Per-user window aggregates

Transaction count (velocity)

Total amount

Average IP risk

Chargeback history signal

Used for:

Risk scoring

Model experimentation

Feature debugging

2️⃣ Latest Risk per User (Upsert Table)

One row per user_id

Latest window timestamp

Current risk score

Decision (ALLOW / REVIEW)

Reasons for flagging

Optimized for:

Serving layer

Business dashboards

Operational insights

3️⃣ Daily Fraud KPIs

Review volume

Review rate

Total evaluated windows

Updated incrementally per batch

4️⃣ Alerts Log

Append-only log of REVIEW decisions

Audit-friendly

Traceable by timestamp

🧠 Risk Scoring Logic

Window-based risk formula:
risk_score =
  0.55 * avg(ip_risk_score)
+ 0.25 * velocity_component
+ 0.20 * amount_component

Decision logic:
REVIEW if risk_score >= risk_threshold_alert
ALLOW otherwise

The pipeline exports real-time metrics to Prometheus.

Streaming Health

spark_input_rows_per_sec

spark_processed_rows_per_sec

spark_batch_duration_ms

spark_num_input_rows

Data Layer Growth

delta_bronze_rows

delta_silver_rows

delta_gold_rows

dlq_total_rows

Fraud KPIs

fraud_total_records

fraud_review_count

fraud_rate

Grafana dashboards visualize:

Fraud Rate (%)

Review Volume

DLQ Growth

Bronze/Silver/Gold Growth

Streaming Throughput

🚦 How to Run Locally
1️⃣ Start Infrastructure

make up-all

This launches:

Kafka

Zookeeper

Redis

PostgreSQL

Prometheus

Pushgateway

Grafana

2️⃣ Start Streaming Pipeline

make stream

3️⃣ Start Transaction Producer

make producer

4️⃣ Access Dashboards
Grafana
http://localhost:3000

Prometheus
http://localhost:9090

Pushgateway
http://localhost:9091

📈 Example Production Snapshot

Bronze rows: 300K+

Silver rows: 300K+

Gold latest users: 16K+

DLQ records: 26K+

Real-time fraud KPI tracking

🔐 Engineering Principles Applied

Event-time correctness with watermarking

Idempotent Delta writes

Upsert-based Gold layer for business stability

Separation of feature log vs serving table

Metrics-first engineering approach

Production-style observability

Failure isolation with DLQ

Streaming checkpointing for recovery

🧪 Failure Handling

Invalid records routed to DLQ (Delta + Kafka)

Kafka failOnDataLoss protection

Stateful operator safeguards respected

Metric sampling to reduce performance impact

🏆 What This Project Demonstrates

Real-time stream processing

Lakehouse architecture design

Stateful window aggregations

Online + offline serving patterns

Observability-driven engineering

Production-style container orchestration

Business KPI integration into streaming pipelines

👤 Author

Koutilya Yenumula
Real-Time Data Engineering | Streaming Systems | Lakehouse Architectures
