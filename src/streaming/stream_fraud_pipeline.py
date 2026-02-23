import json
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
        user_id = row.get("user_id")
        if user_id is None:
            continue
        key = f"user:{user_id}"
        payload = json.dumps(
            {
                "user_id": user_id,
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
      user_id INT,
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
            INSERT INTO cases (case_id, event_id, user_id, risk_score, decision, reasons)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (case_id) DO NOTHING
            """,
            (
                row["case_id"],
                row.get("event_id"),
                row["user_id"],
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

    # Schema for incoming JSON
    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("ts", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("merchant_id", IntegerType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("country", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("ip_risk_score", DoubleType(), True),
            StructField("acct_age_days", IntegerType(), True),
            StructField("chargeback_history", IntegerType(), True),
        ]
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
        .withColumn("user_id", col("data.user_id"))
        .withColumn("merchant_id", col("data.merchant_id"))
        .withColumn("merchant_category", col("data.merchant_category"))
        .withColumn("amount", col("data.amount"))
        .withColumn("currency", col("data.currency"))
        .withColumn("country", col("data.country"))
        .withColumn("device_type", col("data.device_type"))
        .withColumn("ip_risk_score", col("data.ip_risk_score"))
        .withColumn("acct_age_days", col("data.acct_age_days"))
        .withColumn("chargeback_history", col("data.chargeback_history"))
        .drop("data")
    )

    allowed_currencies = RULES["currency_allowed"]
    allowed_countries = RULES["countries_allowed"]
    allowed_categories = RULES["categories_allowed"]
    max_amount = float(RULES["max_amount"])

    is_valid = (
        col("event_id").isNotNull()
        & col("event_time").isNotNull()
        & col("user_id").isNotNull()
        & col("merchant_id").isNotNull()
        & col("amount").isNotNull()
        & (col("amount") > lit(0.0))
        & (col("amount") <= lit(max_amount))
        & col("currency").isin(allowed_currencies)
        & col("country").isin(allowed_countries)
        & col("merchant_category").isin(allowed_categories)
        & col("ip_risk_score").isNotNull()
        & (col("ip_risk_score") >= lit(0.0))
        & (col("ip_risk_score") <= lit(1.0))
        & col("acct_age_days").isNotNull()
        & (col("acct_age_days") >= lit(0))
    )

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
            "'user_id', user_id, "
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

    # ---- GOLD features (windowed aggregates) ----
    features = (
        silver.groupBy(window(col("event_time"), STREAM["window"]), col("user_id"))
        .agg(
            fcount(lit(1)).alias("txn_cnt"),
            fsum(col("amount")).alias("amt_sum"),
            favg(col("ip_risk_score")).alias("ip_risk_avg"),
            fsum(when(col("chargeback_history") > lit(0), lit(1)).otherwise(lit(0))).alias("chargeback_cnt"),
        )
        .withColumnRenamed("window", "w")
        .withColumn("window_start", col("w.start"))
        .withColumn("window_end", col("w.end"))
        .drop("w")
    )

    scored = (
        features.withColumn(
            "risk_score",
            expr(
                """
                least(
                  1.0,
                  0.55 * ip_risk_avg +
                  0.25 * least(1.0, txn_cnt / 20.0) +
                  0.20 * least(1.0, amt_sum / 2000.0)
                )
            """
            ),
        )
        .withColumn(
            "decision",
            when(col("risk_score") >= lit(float(RULES["risk_threshold_alert"])), lit("REVIEW")).otherwise(lit("ALLOW")),
        )
        .withColumn(
            "reasons",
            expr(
                """
              concat_ws(
                ',',
                case when ip_risk_avg >= 0.8 then 'HIGH_IP_RISK' end,
                case when txn_cnt >= 15 then 'HIGH_VELOCITY' end,
                case when amt_sum >= 1500 then 'HIGH_AMOUNT' end
              )
            """
            ),
        )
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
    # One row per user_id (fast lookups, stable table size).
    GOLD_LATEST_PATH = "delta/gold/user_risk_latest_v1"

    def upsert_gold_latest(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        # If multiple rows per user in the same micro-batch, keep the latest window_end.
        w = W.partitionBy("user_id").orderBy(col("window_end").desc())
        latest = (
            batch_df.select("user_id", "window_start", "window_end", "risk_score", "decision", "reasons")
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
            .merge(latest.alias("s"), "t.user_id = s.user_id")
            .whenMatchedUpdate(
                set={
                    "last_window_end": "s.last_window_end",
                    "risk_score": "s.risk_score",
                    "decision": "s.decision",
                    "reasons": "s.reasons",
                    "updated_at": "s.updated_at",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "user_id": "s.user_id",
                    "last_window_end": "s.last_window_end",
                    "risk_score": "s.risk_score",
                    "decision": "s.decision",
                    "reasons": "s.reasons",
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

    # ---- GOLD (3) Business KPIs (daily) ----
    # Small table for dashboards: daily review volume + rate (based on gold decision).
    GOLD_KPI_DAILY_PATH = "delta/gold/fraud_kpis_daily_v1"

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
        .select("user_id", "window_start", "window_end", "risk_score", "decision", "reasons")
        .withColumn("processed_at", current_timestamp())
    )

    gold_alerts_q = (
        alerts_gold_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f'{PATHS["checkpoints"]}/gold_alerts')
        .start(GOLD_ALERTS_PATH)
    )

    # ---- Alerts (Kafka) ----
    alerts_df = scored.filter(col("risk_score") >= lit(float(RULES["risk_threshold_alert"]))).selectExpr(
        "to_json(named_struct("
        "'user_id', user_id,"
        "'window_start', window_start,"
        "'window_end', window_end,"
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
            batch_df.select("user_id", "window_end", "risk_score", "decision", "reasons")
            .orderBy(col("window_end").desc())
            .limit(5000)
        )

        now_iso = datetime.now(timezone.utc).isoformat()

        rows = []
        for r in latest.collect():  # ✅ no pandas
            if r["user_id"] is None:
                continue
            rows.append(
                {
                    "user_id": int(r["user_id"]),
                    "risk_score": float(r["risk_score"]) if r["risk_score"] is not None else 0.0,
                    "decision": r["decision"] or "ALLOW",
                    "reasons": r["reasons"] or "",
                    "updated_at": now_iso,
                }
            )

        # ---- Fraud rate metrics ----
        try:
            total_records = len(rows)
            threshold = float(RULES["risk_threshold_alert"])
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
        thr = float(RULES["risk_threshold_alert"])
        for r in rows:
            if float(r["risk_score"]) >= thr:
                high.append(
                    {
                        "case_id": f"{r['user_id']}-{r['updated_at']}",
                        "event_id": None,
                        "user_id": r["user_id"],
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
