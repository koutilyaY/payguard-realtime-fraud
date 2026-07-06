import json
import os
import pickle
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    lit,
    when,
    window,
    sum as fsum,
    count as fcount,
    avg as favg,
    expr,
    to_date,
    current_timestamp,
    row_number,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window as W
from delta.tables import DeltaTable

from src.utils.config import load_config
from src.utils.logging import get_logger

logger = get_logger("streaming")

MODEL_PKL_PATH = os.path.join("mlruns", "fraud_model.pkl")

# Feature contract must match src/ml/train_model.py: 28 PCA components + Amount.
FEATURE_COLS = [f"V{i}" for i in range(1, 29)] + ["Amount"]


def _load_model_bundle():
    """Load the real-data-trained model bundle (model + feature contract +
    threshold) from the local artifact path."""
    if not os.path.exists(MODEL_PKL_PATH):
        raise FileNotFoundError(
            f"Trained model not found at '{MODEL_PKL_PATH}'. "
            "Run 'make train' first to train the real-data model."
        )
    with open(MODEL_PKL_PATH, "rb") as f:
        bundle = pickle.load(f)
    # Older artifacts were a bare estimator; new ones are a dict bundle.
    if not isinstance(bundle, dict):
        return {"model": bundle, "feature_cols": FEATURE_COLS, "decision_threshold": 0.5}
    return bundle


def _make_fraud_udf(model, feature_cols):
    """
    Return a pandas_udf that scores a single real transaction (V1..V28 + Amount)
    with the LightGBM model. The model is captured in the closure so it is loaded
    once on the driver and serialized to each executor (local Spark mode).
    """
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import DoubleType as _DoubleType

    @pandas_udf(_DoubleType())
    def fraud_score(*cols: pd.Series) -> pd.Series:
        X = pd.DataFrame({name: cols[i].astype(float) for i, name in enumerate(feature_cols)})
        proba = model.predict_proba(X)[:, 1]
        return pd.Series(proba.clip(0.0, 1.0))

    return fraud_score


# ------------------------- REQUIRED ADD: Prometheus Pushgateway helper -------------------------
def push_metrics_to_prometheus(metrics: dict, job: str = "payguard_stream"):
    """
    Push metrics to Prometheus Pushgateway (good for batch/stream jobs).
    """
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    gateway = "localhost:9091"  # Pushgateway exposed locally
    registry = CollectorRegistry()

    for k, v in metrics.items():
        if v is None:
            continue
        g = Gauge(k, k, registry=registry)
        g.set(float(v))

    push_to_gateway(gateway, job=job, registry=registry)


# ------------------------- small config helpers -------------------------
def _pick(dct, *keys, default=None):
    """Return the first existing/non-empty key from dict."""
    for k in keys:
        if not isinstance(dct, dict):
            continue
        if k in dct and dct[k] not in (None, ""):
            return dct[k]
    return default


def _resolve_paths(cfg: dict) -> dict:
    """
    Supports either:
      cfg["paths"]["bronze"] style
    or flat keys:
      cfg["BRONZE"], cfg["SILVER"], cfg["GOLD"], cfg["DLQ_DELTA"], cfg["CHECKPOINTS"]
    """
    paths = cfg.get("paths") or {}

    out = {
        "bronze": _pick(paths, "bronze", default=_pick(cfg, "BRONZE", "bronze")),
        "silver": _pick(paths, "silver", default=_pick(cfg, "SILVER", "silver")),
        "gold_features": _pick(paths, "gold_features", "gold", default=_pick(cfg, "GOLD", "gold_features", "gold")),
        "dlq_delta": _pick(paths, "dlq_delta", "dlq", default=_pick(cfg, "DLQ_DELTA", "dlq_delta", "dlq")),
        "checkpoints": _pick(paths, "checkpoints", default=_pick(cfg, "CHECKPOINTS", "checkpoints")),
    }

    missing = [k for k, v in out.items() if not v]
    if missing:
        raise KeyError(f"Missing required paths in config: {missing}. Got: {out}")
    return out


def _resolve_kafka(cfg: dict) -> dict:
    """
    Supports either:
      cfg["kafka"]["topic_raw"] / ["topic_dlq"] / ["bootstrap_servers"]
    or flat:
      cfg["TOPIC_RAW"], cfg["TOPIC_DLQ"]
    """
    kafka = cfg.get("kafka") or {}
    out = {
        "bootstrap_servers": _pick(
            kafka, "bootstrap_servers", "bootstrap", default=_pick(cfg, "KAFKA_BOOTSTRAP", "bootstrap_servers")
        ),
        "topic_raw": _pick(kafka, "topic_raw", "topic", default=_pick(cfg, "TOPIC_RAW", "topic_raw")),
        "topic_dlq": _pick(kafka, "topic_dlq", default=_pick(cfg, "TOPIC_DLQ", "topic_dlq")),
        "topic_alerts": _pick(kafka, "topic_alerts", default=_pick(cfg, "TOPIC_ALERTS", "topic_alerts", default="txns_alerts")),
    }
    missing = [k for k, v in out.items() if not v and k != "topic_alerts"]
    if missing:
        raise KeyError(f"Missing required kafka config keys: {missing}. Got: {out}")
    return out


def _resolve_streaming(cfg: dict) -> dict:
    streaming = cfg.get("streaming") or {}
    # reasonable defaults if missing
    return {
        "watermark": _pick(streaming, "watermark", default="10 minutes"),
        "window": _pick(streaming, "window", default="5 minutes"),
        "trigger": _pick(streaming, "trigger", default="10 seconds"),
        "startingOffsets": _pick(streaming, "startingOffsets", default="latest"),
    }


# ---------- Helpers: Redis + Postgres ----------
def write_to_redis(rows, redis_cfg, ttl_seconds: int):
    import redis

    r = redis.Redis(host=redis_cfg["host"], port=int(redis_cfg["port"]), decode_responses=True)
    pipe = r.pipeline(transaction=False)
    for row in rows:
        event_id = row.get("event_id")
        if event_id is None:
            continue
        key = f"txn:{event_id}"
        payload = json.dumps(
            {
                "event_id": event_id,
                "amount": float(row.get("amount", 0.0)),
                "risk_score": float(row["risk_score"]),
                "decision": row["decision"],
                "reasons": row.get("reasons", ""),
                "updated_at": row["updated_at"],
            }
        )
        pipe.setex(key, ttl_seconds, payload)
    pipe.execute()


def _pg_dbname(pg: dict) -> str:
    return pg.get("db") or pg.get("dbname") or pg.get("database") or pg.get("POSTGRES_DB") or "postgres"


def ensure_cases_table(pg):
    import psycopg2

    conn = psycopg2.connect(
        host=pg["host"],
        port=int(pg["port"]),
        dbname=_pg_dbname(pg),
        user=pg["user"],
        password=pg["password"],
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS cases (
      case_id TEXT PRIMARY KEY,
      event_id TEXT,
      risk_score DOUBLE PRECISION,
      decision TEXT,
      reasons TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      status TEXT DEFAULT 'OPEN',
      analyst_label TEXT DEFAULT 'UNKNOWN'
    );
    """
    )
    cur.close()
    conn.close()


def insert_cases(rows, pg):
    import psycopg2

    conn = psycopg2.connect(
        host=pg["host"],
        port=int(pg["port"]),
        dbname=_pg_dbname(pg),
        user=pg["user"],
        password=pg["password"],
    )
    conn.autocommit = True
    cur = conn.cursor()

    for row in rows:
        cur.execute(
            """
            INSERT INTO cases (case_id, event_id, risk_score, decision, reasons)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (case_id) DO NOTHING
            """,
            (
                row["case_id"],
                row.get("event_id"),
                float(row["risk_score"]),
                row["decision"],
                row.get("reasons", ""),
            ),
        )

    cur.close()
    conn.close()


# ---------- Main ----------
if __name__ == "__main__":
    cfg = load_config()

    KAFKA = _resolve_kafka(cfg)
    PATHS = _resolve_paths(cfg)
    STREAM = _resolve_streaming(cfg)
    RULES = cfg["rules"]

    # Spark with Kafka + Delta
    spark = (
        SparkSession.builder.appName("PayGuard-Streaming")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                    "org.apache.kafka:kafka-clients:3.5.1",
                    "io.delta:delta-spark_2.12:3.2.0",
                ]
            ),
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load the real-data-trained LightGBM model and register as a Spark pandas UDF
    logger.info(f"Loading real-data fraud model from {MODEL_PKL_PATH} ...")
    _bundle = _load_model_bundle()
    _lgb_model = _bundle["model"]
    _feature_cols = _bundle.get("feature_cols", FEATURE_COLS)
    _model_threshold = float(_bundle.get("decision_threshold", RULES["risk_threshold_alert"]))
    fraud_score_udf = _make_fraud_udf(_lgb_model, _feature_cols)
    logger.info(f"Model loaded; scoring on {len(_feature_cols)} features, "
                f"threshold={_model_threshold}.")

    # Schema for incoming JSON — real ULB transaction fields replayed by the
    # producer: event_id/ts wrapper, the 28 PCA components, Amount, and the
    # ground-truth label (label_fraud) which the model never consumes.
    schema = StructType(
        [StructField("event_id", StringType(), True),
         StructField("ts", StringType(), True),
         StructField("time_offset", DoubleType(), True),
         StructField("amount", DoubleType(), True)]
        + [StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)]
        + [StructField("label_fraud", IntegerType(), True)]
    )

    # Kafka raw stream (IMPORTANT: failOnDataLoss=false prevents hard crash when partitions/offsets change)
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA["bootstrap_servers"])
        .option("subscribe", KAFKA["topic_raw"])
        .option("startingOffsets", STREAM["startingOffsets"])
        .option("failOnDataLoss", "false")
        # a few safety timeouts
        .option("kafka.request.timeout.ms", "120000")
        .option("kafka.session.timeout.ms", "60000")
        .option("kafka.metadata.max.age.ms", "30000")
        .load()
    )

    # Parse JSON
    parsed = raw.selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts").withColumn(
        "data", from_json(col("json_str"), schema)
    )

    # Flatten
    with_cols = (
        parsed.withColumn("event_id", col("data.event_id"))
        .withColumn("ts", col("data.ts"))
        .withColumn("event_time", to_timestamp(col("data.ts")))  # ISO timestamp
        .withColumn("time_offset", col("data.time_offset"))
        .withColumn("amount", col("data.amount"))
        .withColumn("label_fraud", col("data.label_fraud"))
    )
    for i in range(1, 29):
        with_cols = with_cols.withColumn(f"V{i}", col(f"data.V{i}"))
    with_cols = with_cols.drop("data")

    max_amount = float(RULES["max_amount"])

    # Validation for real ULB transactions: event wrapper present, a parseable
    # timestamp, a sane non-negative amount, and the 28 PCA features non-null.
    # (The V-features are PCA outputs and can legitimately be any real number,
    # so we only null-check them.)
    is_valid = (
        col("event_id").isNotNull()
        & col("event_time").isNotNull()
        & col("amount").isNotNull()
        & (col("amount") >= lit(0.0))
        & (col("amount") <= lit(max_amount))
    )
    for i in range(1, 29):
        is_valid = is_valid & col(f"V{i}").isNotNull()

    valid_df = with_cols.filter(is_valid)
    invalid_df = with_cols.filter(~is_valid).withColumn("dlq_reason", lit("schema_or_rule_violation"))

    # ---- BRONZE (Delta) ----
    bronze_q = (
        valid_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/bronze')
        .start(PATHS["bronze"])
    )

    # ---- DLQ (Delta) ----
    dlq_delta_q = (
        invalid_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/dlq_delta')
        .start(PATHS["dlq_delta"])
    )

    # ---- DLQ (Kafka) ----
    dlq_kafka_q = (
        invalid_df.selectExpr(
            "to_json(named_struct("
            "'event_id', event_id, "
            "'ts', ts, "
            "'reason', dlq_reason, "
            "'raw', json_str"
            ")) AS value"
        )
        .writeStream.format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", KAFKA["bootstrap_servers"])
        .option("topic", KAFKA["topic_dlq"])
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/dlq_kafka')
        .option("kafka.request.timeout.ms", "120000")
        .option("kafka.session.timeout.ms", "60000")
        .start()
    )

    # ---- SILVER ----
    base = valid_df.withWatermark("event_time", STREAM["watermark"])

    silver = base.dropDuplicates(["event_id"]).withColumn("event_date", to_date(col("event_time")))

    silver_q = (
        silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/silver')
        .start(PATHS["silver"])
    )

    # ---- GOLD: per-transaction scoring ----
    # The real-data model scores each transaction directly from its 28 PCA
    # components + Amount (the same contract it was trained on). No windowed
    # aggregation is needed for scoring; the model IS the decision function.
    feature_udf_cols = [col(c).cast(DoubleType()) for c in _feature_cols]

    scored = (
        silver
        .withColumn("risk_score", fraud_score_udf(*feature_udf_cols))
        .withColumn(
            "decision",
            when(col("risk_score") >= lit(_model_threshold), lit("REVIEW")).otherwise(lit("ALLOW")),
        )
        .withColumn(
            "reasons",
            expr(
                """
              concat_ws(
                ',',
                case when risk_score >= 0.90 then 'HIGH_MODEL_RISK' end,
                case when amount >= 1000 then 'HIGH_AMOUNT' end
              )
            """
            ),
        )
        # window_end drives the "latest per key" upsert and the daily KPI date.
        .withColumn("window_end", col("event_time"))
        .withColumn("window_start", col("event_time"))
    )

    # ---- GOLD (1) Feature log (append) ----
    # This keeps the full windowed feature history for debugging / backtests.
    gold_features_q = (
        scored.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/gold_features_log')
        .start(PATHS["gold_features"])
    )

    # ---- GOLD (2) Business-ready "latest risk per user" (UPSERT) ----
    # One row per event_id (fast lookups, stable table size).
    # Keyed on event_id — the ULB dataset has no user identifier; each real
    # transaction is its own scored entity.
    GOLD_LATEST_PATH = "delta/gold/txn_risk_latest_v1"

    def upsert_gold_latest(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        w = W.partitionBy("event_id").orderBy(col("window_end").desc())
        latest = (
            batch_df.select("event_id", "window_start", "window_end", "amount",
                            "risk_score", "decision", "reasons", "label_fraud")
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == lit(1))
            .drop("_rn")
            .withColumnRenamed("window_end", "last_window_end")
            .withColumn("updated_at", current_timestamp())
        )

        # Create table if it doesn't exist yet
        if not DeltaTable.isDeltaTable(spark, GOLD_LATEST_PATH):
            latest.write.format("delta").mode("overwrite").save(GOLD_LATEST_PATH)
            return

        tgt = DeltaTable.forPath(spark, GOLD_LATEST_PATH)

        (
            tgt.alias("t")
            .merge(latest.alias("s"), "t.event_id = s.event_id")
            .whenMatchedUpdate(
                set={
                    "last_window_end": "s.last_window_end",
                    "amount": "s.amount",
                    "risk_score": "s.risk_score",
                    "decision": "s.decision",
                    "reasons": "s.reasons",
                    "label_fraud": "s.label_fraud",
                    "updated_at": "s.updated_at",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "event_id": "s.event_id",
                    "last_window_end": "s.last_window_end",
                    "amount": "s.amount",
                    "risk_score": "s.risk_score",
                    "decision": "s.decision",
                    "reasons": "s.reasons",
                    "label_fraud": "s.label_fraud",
                    "updated_at": "s.updated_at",
                }
            )
            .execute()
        )

    gold_latest_q = (
        scored.writeStream.foreachBatch(upsert_gold_latest)
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/gold_latest')
        .start()
    )

    # ---- GOLD (3) Business KPIs (daily) via foreachBatch UPSERT ----
    # Avoids "global watermark correctness" error by doing KPI aggregation per micro-batch.
    GOLD_KPI_DAILY_PATH = "delta/gold/fraud_kpis_daily_v1"

    def upsert_kpis_daily(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        # Batch-level daily KPI increments
        daily_inc = (
            batch_df
            .withColumn("kpi_date", to_date(col("window_end")))
            .groupBy("kpi_date")
            .agg(
                fcount(lit(1)).alias("user_window_rows_inc"),
                fsum(when(col("decision") == lit("REVIEW"), lit(1)).otherwise(lit(0))).alias("review_rows_inc"),
            )
            .withColumn("updated_at", current_timestamp())
        )

        # Create table if missing
        if not DeltaTable.isDeltaTable(spark, GOLD_KPI_DAILY_PATH):
            daily_init = (
                daily_inc
                .withColumnRenamed("user_window_rows_inc", "user_window_rows")
                .withColumnRenamed("review_rows_inc", "review_rows")
                .withColumn(
                    "review_rate",
                    col("review_rows") / when(col("user_window_rows") > lit(0), col("user_window_rows")).otherwise(lit(1))
                )
                .select("kpi_date", "user_window_rows", "review_rows", "review_rate", "updated_at")
            )
            daily_init.write.format("delta").mode("overwrite").save(GOLD_KPI_DAILY_PATH)
            return

        tgt = DeltaTable.forPath(spark, GOLD_KPI_DAILY_PATH)

        # Merge: add increments into existing daily totals
        (
            tgt.alias("t")
            .merge(daily_inc.alias("s"), "t.kpi_date = s.kpi_date")
            .whenMatchedUpdate(set={
                "user_window_rows": "t.user_window_rows + s.user_window_rows_inc",
                "review_rows": "t.review_rows + s.review_rows_inc",
                "review_rate": "(t.review_rows + s.review_rows_inc) / CAST(GREATEST(t.user_window_rows + s.user_window_rows_inc, 1) AS DOUBLE)",
                "updated_at": "s.updated_at",
            })
            .whenNotMatchedInsert(values={
                "kpi_date": "s.kpi_date",
                "user_window_rows": "s.user_window_rows_inc",
                "review_rows": "s.review_rows_inc",
                "review_rate": "CAST(s.review_rows_inc AS DOUBLE) / CAST(GREATEST(s.user_window_rows_inc, 1) AS DOUBLE)",
                "updated_at": "s.updated_at",
            })
            .execute()
        )

    gold_kpis_q = (
        scored.writeStream
        .foreachBatch(upsert_kpis_daily)
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/gold_kpis_daily_v2')
        .start()
    )

    # ---- GOLD (4) Alerts log (append) ----
    # Append-only audit table for reviewed users (useful for investigations).
    GOLD_ALERTS_PATH = "delta/gold/alerts_v1"

    alerts_gold_df = (
        scored.filter(col("decision") == lit("REVIEW"))
        .select("event_id", "window_start", "window_end", "amount", "risk_score",
                "decision", "reasons", "label_fraud")
        .withColumn("processed_at", current_timestamp())
    )

    gold_alerts_q = (
        alerts_gold_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/gold_alerts')
        .start(GOLD_ALERTS_PATH)
    )

    # ---- Alerts (Kafka) ----
    alerts_df = scored.filter(col("risk_score") >= lit(_model_threshold)).selectExpr(
        "to_json(named_struct("
        "'event_id', event_id,"
        "'window_end', window_end,"
        "'amount', amount,"
        "'risk_score', risk_score,"
        "'decision', decision,"
        "'reasons', reasons"
        ")) AS value"
    )

    alerts_q = (
        alerts_df.writeStream.format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", KAFKA["bootstrap_servers"])
        .option("topic", KAFKA["topic_alerts"])
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/alerts')
        .option("kafka.request.timeout.ms", "120000")
        .option("kafka.session.timeout.ms", "60000")
        .start()
    )

    # ---- Redis + Postgres via foreachBatch (NO pandas) ----
    ensure_cases_table(cfg["postgres"])

    def sink_online_and_cases(batch_df, batch_id: int):
        # ---------------- metrics push ----------------
        # 1) Spark streaming progress
        try:
            progress = spark.streams.active[0].lastProgress if spark.streams.active else None
            if progress:
                m = {
                    "spark_input_rows_per_sec": progress.get("inputRowsPerSecond", 0.0),
                    "spark_processed_rows_per_sec": progress.get("processedRowsPerSecond", 0.0),
                    "spark_batch_duration_ms": progress.get("durationMs", {}).get("addBatch", 0.0),
                    "spark_num_input_rows": progress.get("numInputRows", 0.0),
                }
                push_metrics_to_prometheus(m, job="payguard_stream")
        except Exception as e:
            logger.warning(f"metrics push failed: {e}")

        # 2) Delta row counts every 10 batches (local demo acceptable)
        try:
            bid = int(batch_id)
        except Exception:
            bid = batch_id

        if isinstance(bid, int) and bid % 10 == 0:
            try:
                bronze_cnt = spark.read.format("delta").load(PATHS["bronze"]).count()
                silver_cnt = spark.read.format("delta").load(PATHS["silver"]).count()
                gold_feature_cnt = spark.read.format("delta").load(PATHS["gold_features"]).count()
                gold_latest_cnt = spark.read.format("delta").load(GOLD_LATEST_PATH).count()

                push_metrics_to_prometheus(
                    {
                        "delta_bronze_rows": bronze_cnt,
                        "delta_silver_rows": silver_cnt,
                        "delta_gold_rows": gold_latest_cnt,
                        "delta_gold_feature_rows": gold_feature_cnt,
                    },
                    job="payguard_delta_counts",
                )

                # 3) DLQ volume (sampled every 10 batches)
                try:
                    dlq_cnt = spark.read.format("delta").load(PATHS["dlq_delta"]).count()
                    push_metrics_to_prometheus(
                        {"dlq_total_rows": dlq_cnt},
                        job="payguard_dlq_metrics",
                    )
                except Exception as e:
                    logger.warning(f"dlq metrics push failed: {e}")

            except Exception as e:
                logger.warning(f"delta count metrics failed: {e}")

        # keep it light + safe
        if batch_df.rdd.isEmpty():
            return

        latest = (
            batch_df.select("event_id", "window_end", "amount", "risk_score", "decision", "reasons")
            .orderBy(col("window_end").desc())
            .limit(5000)
        )

        now_iso = datetime.now(timezone.utc).isoformat()

        rows = []
        for r in latest.collect():  # ✅ no pandas
            if r["event_id"] is None:
                continue
            rows.append(
                {
                    "event_id": r["event_id"],
                    "amount": float(r["amount"]) if r["amount"] is not None else 0.0,
                    "risk_score": float(r["risk_score"]) if r["risk_score"] is not None else 0.0,
                    "decision": r["decision"] or "ALLOW",
                    "reasons": r["reasons"] or "",
                    "updated_at": now_iso,
                }
            )

        # ---- Fraud rate metrics ----
        try:
            total_records = len(rows)
            threshold = _model_threshold
            fraud_count = 0
            for _r in rows:
                if float(_r["risk_score"]) >= threshold:
                    fraud_count += 1
            fraud_rate = (fraud_count / total_records) if total_records > 0 else 0.0

            push_metrics_to_prometheus(
                {
                    "fraud_total_records": total_records,
                    "fraud_review_count": fraud_count,
                    "fraud_rate": fraud_rate,
                },
                job="payguard_fraud_metrics",
            )
        except Exception as e:
            logger.warning(f"fraud metrics push failed: {e}")

        if not rows:
            return

        write_to_redis(rows, cfg["redis"], int(cfg["redis"]["ttl_seconds"]))

        high = []
        thr = _model_threshold
        for r in rows:
            if float(r["risk_score"]) >= thr:
                high.append(
                    {
                        "case_id": f"{r['event_id']}",
                        "event_id": r["event_id"],
                        "risk_score": r["risk_score"],
                        "decision": "REVIEW",
                        "reasons": r["reasons"],
                    }
                )

        if high:
            insert_cases(high, cfg["postgres"])

    online_q = (
        scored.writeStream.foreachBatch(sink_online_and_cases)
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/online')
        .trigger(processingTime=STREAM["trigger"])
        .start()
    )

    logger.info("Streaming started: bronze + silver + gold + dlq + alerts + redis + cases")
    spark.streams.awaitAnyTermination()
