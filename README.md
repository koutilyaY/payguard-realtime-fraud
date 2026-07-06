# PayGuard

Fraud detection on **real** credit-card transactions, wired through a streaming
lakehouse: Kafka → Spark Structured Streaming → Delta Lake (bronze/silver/gold) →
LightGBM → FastAPI + Streamlit.

The data here is not made up. It's the ULB Credit-Card Fraud Detection dataset —
284,807 real European card transactions from September 2013, 492 of them
confirmed frauds (0.172%). The model is trained and evaluated on it. The one
synthetic piece is the "live stream": a producer that **replays** those real
transactions over Kafka so the streaming job has something to score. It's a
replay of real data, not live production traffic, and this README is careful to
say so wherever it matters.

## What's real and what isn't

| Piece | Status |
|---|---|
| Transaction data | **Real** — ULB dataset (284,807 txns, 492 frauds), pulled from OpenML |
| Model training + evaluation | **Real** — trained on the real transactions, honest held-out metrics |
| Train/test split | **Real, leak-free** — time-ordered (train earlier, test later) |
| Reported metrics | **Real** — the model's actual scores on the held-out set (below) |
| The "real-time" stream | **A replay** — the producer streams the real rows over Kafka; not live traffic |
| The Streamlit "Simulation" / "Historical" tabs | **Illustrative** — clearly labeled as fake in the app |

## Results (real, held-out test set)

Trained with LightGBM on the earlier ~70% of the transactions (by time) and
evaluated on the later ~30% — 85,443 transactions, 108 of them fraud. Numbers
from `make train`, also written to `mlruns/real_data_metrics.json`:

| Metric | Value |
|---|---|
| **PR-AUC (average precision)** | **0.733** |
| ROC-AUC | 0.976 |
| Brier score (calibration) | 0.0009 |
| Precision @ threshold 0.5 | 0.58 |
| Recall @ threshold 0.5 | 0.80 |
| F1 @ threshold 0.5 | 0.67 |
| Confusion @ 0.5 | TP 86 · FP 62 · FN 22 · TN 85,273 |

**Read this honestly.** PR-AUC is the metric that matters on a 0.17%-positive
problem; ROC-AUC looks flattering on any imbalanced set. At the 0.5 operating
threshold the model catches 86 of 108 held-out frauds (80% recall) but pays for
it with 62 false positives, so precision is only 0.58 — a real fraud team would
tune this threshold against their review-team capacity. Published results on this
dataset often quote higher numbers, but many of them use a **random** split,
which leaks fraud from the test period into training and inflates the score. This
uses a time-ordered split on purpose, so the numbers are lower and more like what
you'd actually see deploying forward in time. It's a solid model, not a magic one.

Why these modeling choices:

- **Time-ordered split.** The `Time` column is seconds from the first
  transaction and spans ~48 hours. Training on the earlier window and testing on
  the later one mimics real deployment and avoids look-ahead leakage.
- **Imbalance is handled explicitly**, but gently. The naïve `scale_pos_weight`
  (#neg/#pos ≈ 500) actually *wrecks* the model — it inflates every probability
  and tanks precision (PR-AUC drops to ~0.24). Using `sqrt` of that ratio (≈21)
  up-weights fraud enough while keeping the scores calibrated (Brier 0.0009).
  That single change is the difference between a useless and a usable model here.
- **Features** are the 28 PCA components (`V1..V28`) plus `Amount`. `Time` is used
  only to split, never as a feature.

---

## Run it — zero-infra path (no Docker)

This trains and evaluates the model on the real data locally. No Kafka, no Spark,
no containers.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install scikit-learn pandas lightgbm pyarrow mlflow pyyaml

python scripts/download_data.py      # downloads the real ULB CSV to data/ (~150 MB)
PYTHONPATH=. python -m src.ml.train_model
```

You'll see the real PR-AUC / ROC-AUC / confusion matrix printed, and the model +
metrics saved under `mlruns/`.

## Run it — full streaming stack (Docker)

```bash
# 0. Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
cp config.yaml.example config.yaml       # then set the Postgres password

# 1. Get the real data + train (the stream loads mlruns/fraud_model.pkl on startup)
make download
make train

# 2. Start infra (Kafka, Postgres, Redis, Prometheus/Grafana) and create topics
make up-all

# 3. Start the streaming pipeline (bronze → silver → gold + sinks)
make stream                              # leave running

# 4. In another shell: replay the real transactions into Kafka
make producer                            # or: REPLAY_LIMIT=20000 make producer

# 5. In another shell: the scoring/serving API on :8000
make api

# 6. Dashboard
cd streamlit_app && streamlit run app.py
```

> **Train before you stream.** `make stream` loads `mlruns/fraud_model.pkl` on
> startup and raises `FileNotFoundError` if it's missing. Run `make train` first.

Tear down with `make down-all`.

---

## The problem

Card fraud is a streaming problem with a hard latency budget: a decision that
arrives after the authorization is worthless. Batch scoring flags fraud hours
later, after the money is gone; static rule engines are easy to reason about but
over-flag and drift. PayGuard sketches the middle path — ingest continuously,
land everything in an auditable medallion lakehouse, score each transaction with
a gradient-boosted model, push results to an online store and an analyst case
queue, and feed confirmed/false-positive labels back into retraining.

Here that pipeline runs on the real ULB transactions, replayed as a stream.

## Architecture

```
  real ULB txns  →  ┌─────────────────┐  topics: txns, txns_dead_letter, txns_alerts
  (replayed by      │  Kafka  (:9092) │
   the producer)    └────────┬────────┘
                             │  readStream (failOnDataLoss=false)
                             ▼
              ┌──────────────────────────────────┐
              │   Spark Structured Streaming      │
              │   parse JSON · schema validation  │
              └───────┬───────────────────┬───────┘
                 valid│                    │invalid
                      ▼                    ▼
     BRONZE  delta/bronze/txns_v1     DLQ  delta + Kafka (txns_dead_letter)
                      │
                      │  watermark · dropDuplicates(event_id)
                      ▼
     SILVER  delta/silver/txns_clean_v1
                      │
                      ▼
              ┌────────────────────────┐
              │  LightGBM pandas_udf    │  P(fraud) per transaction from V1..V28 + Amount
              │  → REVIEW / ALLOW       │  (threshold from the trained model bundle)
              └─────────┬──────────────┘
                        ▼
     GOLD   txn_risk_scored_v1 (log) · txn_risk_latest_v1 (upsert)
            fraud_kpis_daily · alerts_v1 · txns_alerts (Kafka)
                        │
          ┌─────────────┼───────────────────────────┐
          ▼             ▼                            ▼
   Redis (:6379)   Postgres (:5433)            Prometheus Pushgateway (:9091)
   txn:{event_id}  cases table                 → Prometheus (:9090) → Grafana (:3000)
   risk snapshot   (analyst queue)
          │             │
          ▼             ▼
     FastAPI (:8000)  ── /decision/txn/{event_id}, /label/case/{id}, /cases/labeled, /health
          │
          ▼
     Streamlit dashboard
```

**Why these pieces:** Kafka decouples ingestion from processing and absorbs
bursts; Spark Structured Streaming gives stateful stream processing with
exactly-once checkpointing; the Delta medallion (bronze raw → silver deduped →
gold scored) keeps every layer queryable and auditable; LightGBM is fast to train
and score on tabular features; Redis serves the latest per-transaction risk for
low-latency lookups while Postgres holds the durable analyst case queue.

Note the model scores **per transaction** — the ULB dataset is per-transaction
and has no user identifier, so the online store and cases are keyed on
`event_id`, not a user id.

---

## Fraud signals & rules

The model consumes 29 features per transaction: the 28 PCA components
`V1..V28` plus `Amount` (see `src/ml/train_model.py` and
`stream_fraud_pipeline.py`). The PCA components are the ULB dataset's anonymized
features — the original raw fields were transformed for privacy before release,
so they aren't individually interpretable, which is itself an honest limitation.

Scoring is a single LightGBM `predict_proba`. A transaction is marked **REVIEW**
when `P(fraud) >= threshold` (0.5, baked into the trained model bundle), otherwise
**ALLOW**. Human-readable reason tags are attached alongside the score:

| Reason tag | Condition |
|---|---|
| `HIGH_MODEL_RISK` | `risk_score >= 0.90` |
| `HIGH_AMOUNT` | `amount >= 1000` |

Schema validation runs before scoring: rows missing the event wrapper, a
parseable timestamp, a sane non-negative amount, or any of the 28 PCA features
are routed to the dead-letter Delta table and the `txns_dead_letter` topic.

---

## Tech stack

| Layer | Technology |
|---|---|
| Streaming transport | Apache Kafka (Confluent images, Zookeeper) |
| Stream processing | PySpark 3.5 Structured Streaming |
| Storage / lakehouse | Delta Lake 3.2 (bronze/silver/gold + DLQ) |
| Model | LightGBM (binary classifier) |
| Experiment tracking | MLflow (local file store under `mlruns/`) |
| Online store | Redis 7 |
| Case store | PostgreSQL 16 |
| Serving API | FastAPI + Uvicorn |
| Dashboard | Streamlit (+ Plotly) |
| Observability | Prometheus, Pushgateway, Grafana, exporters |
| Dataset | ULB Credit-Card Fraud Detection (via OpenML) |

---

## Analyst feedback loop

REVIEW cases land in the Postgres `cases` table (keyed by `event_id`). Analysts
label them through the API:

```bash
curl -X POST localhost:8000/label/case/<case_id> \
     -H 'content-type: application/json' \
     -d '{"label":"FRAUD","analyst_id":"alice"}'   # FRAUD | LEGIT | UNKNOWN
```

`make retrain` then blends those up-weighted, analyst-confirmed real transactions
into the real base training set and retrains; restart `make stream` to pick up
the new model.

---

## Make targets, ports & endpoints

### Make targets

| Target | What it does |
|---|---|
| `make download` | Download the real ULB dataset to `data/creditcard.csv` (via OpenML) |
| `make train` | Train LightGBM on the real data (time-ordered split) → `mlruns/fraud_model.pkl` |
| `make up-all` | Start Kafka + Postgres + Redis + monitoring, then `kafka-init` |
| `make down-all` | Stop all of the above |
| `make kafka-init` | Create the `txns`, `txns_dead_letter`, `txns_alerts` topics |
| `make stream` | Run the Spark streaming pipeline (bronze→silver→gold + sinks) |
| `make producer` | Replay the real transactions into Kafka (`REPLAY_LIMIT=N` to cap) |
| `make api` | Serve the FastAPI app on `:8000` |
| `make dq_silver` | Run data-quality validation on the silver table |
| `make retrain` | Retrain using analyst labels + the real base data |

Dashboard: `cd streamlit_app && streamlit run app.py`.

### Service ports

| Service | Port |
|---|---|
| Kafka (host listener) | `9092` |
| Redis | `6379` |
| Postgres | `5433` (container `5432`) |
| FastAPI | `8000` |
| Prometheus `9090` · Pushgateway `9091` · Grafana `3000` (admin/admin) | |

### API endpoints (`src/serving/api.py`)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness + Redis connectivity |
| `GET` | `/decision/txn/{event_id}` | Latest risk snapshot for a scored transaction (from Redis) |
| `POST` | `/label/case/{case_id}` | Submit analyst label (FRAUD/LEGIT/UNKNOWN) |
| `GET` | `/cases/labeled` | List labeled cases for retraining review |

> There is no synchronous `/score` endpoint — scoring happens inside the Spark
> stream; the API reads the resulting snapshots and manages the feedback loop.

### Tests

```bash
pytest tests/
```

### Configuration

Runtime config lives in `config.yaml` (copy from `config.yaml.example`); a few
overrides come from `.env`. Set the Postgres password before running.

---

## Repository layout

```
payguard-realtime-fraud/
├── Makefile
├── config.yaml.example
├── requirements.txt
├── data/                            # real ULB CSV (git-ignored; see download script)
├── docker/
│   ├── docker-compose.yml           # Kafka, Zookeeper, Redis, Postgres, kafka-init
│   └── monitoring/                  # Prometheus, Grafana, exporters
├── scripts/
│   ├── download_data.py             # fetch the real ULB dataset from OpenML
│   └── kafka_init.sh                # idempotent topic creation
├── src/
│   ├── producer/produce_txns.py     # replays the real transactions into Kafka
│   ├── streaming/stream_fraud_pipeline.py  # bronze→silver→gold + DLQ + sinks
│   ├── ml/
│   │   ├── train_model.py           # real-data LightGBM training + MLflow
│   │   └── retrain.py               # retrain from analyst-labeled real cases
│   ├── serving/api.py               # FastAPI: decisions, labeling, health
│   ├── quality/                     # silver data-quality validation
│   └── utils/                       # config + logging helpers
├── streamlit_app/                   # Streamlit dashboard (shows the real metrics)
├── tests/test_unit.py
├── delta/                           # local Delta tables + checkpoints (generated)
└── mlruns/                          # MLflow store + fraud_model.pkl (generated)
```

---

## Data source & citation

ULB Credit-Card Fraud Detection dataset — Machine Learning Group, Université
Libre de Bruxelles. Transactions by European cardholders in September 2013;
features anonymized via PCA. Retrieved here through OpenML
(`fetch_openml(data_id=42175)`, which keeps the `Time` column needed for the
time-ordered split). Original dataset: <https://www.openml.org/d/42175> ·
<https://www.kaggle.com/mlg-ulb/creditcardfraud>.

Dua, D. and Graff, C. (2019); Dal Pozzolo et al., "Calibrating probability with
undersampling for unbalanced classification," IEEE SSCI, 2015.

## Limitations & honest caveats

- **The stream is a replay, not production traffic.** There's no live feed behind
  this; the producer replays a static 2-day dataset.
- **PCA features aren't interpretable.** The ULB features are anonymized, so you
  can't attach business meaning to individual signals — you get a score, not an
  explanation.
- **The model is decent, not stellar.** PR-AUC 0.73 on a leak-free split; 80%
  recall costs meaningful false positives at the default threshold. Tune it.
- **Local single-node Spark.** Tuned for a laptop, not a distributed deployment.
- **No auth/TLS on the API.** It's a demo server; add auth before exposing it.

## License

MIT. See `LICENSE`.
