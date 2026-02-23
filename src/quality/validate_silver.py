import os
import sys
import json
import time
import subprocess
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


def run(cmd: str) -> str:
    """Run a shell command and return stdout (best-effort)."""
    try:
        out = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
        return out.strip()
    except subprocess.CalledProcessError as e:
        return (e.output or "").strip()


def spark_delta() -> SparkSession:
    """
    Create Spark session able to read Delta locally.
    IMPORTANT: this must match your Delta version used in stream (3.2.0).
    """
    return (
        SparkSession.builder
        .appName("payguard-validate-silver")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def delta_count(spark: SparkSession, path: str) -> int:
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return -1


def find_event(spark: SparkSession, path: str, event_id: str, cols: Optional[list] = None):
    df = spark.read.format("delta").load(path)
    q = df.filter(col("event_id") == event_id)
    if cols:
        q = q.select(*cols)
    return q


def tail_delta_log(path: str, n: int = 3) -> str:
    return run(f'ls "{path}/_delta_log" 2>/dev/null | tail -{n}')


def main():
    # ---- EDITABLE DEFAULTS (your v1 setup) ----
    BRONZE = os.getenv("BRONZE", "delta/bronze/txns_v1")
    SILVER = os.getenv("SILVER", "delta/silver/txns_clean_v1")
    GOLD   = os.getenv("GOLD",   "delta/gold/user_risk_features_v1")
    DLQ    = os.getenv("DLQ",    "delta/bronze/dead_letter_v1")
    CHECKPOINTS = os.getenv("CHECKPOINTS", "delta/_checkpoints_v1")

    TOPIC_RAW = os.getenv("TOPIC_RAW", "txns")
    TOPIC_DLQ = os.getenv("TOPIC_DLQ", "txns_dead_letter")

    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

    # you can pass event id: python validate_silver.py manual-ok-2
    event_id = sys.argv[1] if len(sys.argv) > 1 else None

    print("\n=== Paths ===")
    print("BRONZE:", BRONZE)
    print("SILVER:", SILVER)
    print("GOLD  :", GOLD)
    print("DLQ   :", DLQ)
    print("CHECK :", CHECKPOINTS)

    print("\n=== Delta log tails (shows if tables are appending) ===")
    print("bronze _delta_log:", tail_delta_log(BRONZE))
    print("silver _delta_log:", tail_delta_log(SILVER))
    print("gold   _delta_log:", tail_delta_log(GOLD))
    print("dlq    _delta_log:", tail_delta_log(DLQ))

    # ---- Kafka quick health ----
    print("\n=== Kafka topics ===")
    print(run(f'docker exec -it docker-kafka-1 bash -lc "kafka-topics --bootstrap-server {BOOTSTRAP} --list | head -50"'))

    print("\n=== Kafka offsets (raw topic) ===")
    print(run(f'docker exec -it docker-kafka-1 bash -lc "kafka-get-offsets --bootstrap-server {BOOTSTRAP} --topic {TOPIC_RAW}"'))

    print("\n=== Latest 5 messages from txns (sanity) ===")
    print(run(
        f'docker exec -it docker-kafka-1 bash -lc '
        f'"kafka-console-consumer --bootstrap-server {BOOTSTRAP} --topic {TOPIC_RAW} '
        f'--property print.timestamp=true --max-messages 5"'
    ))

    # ---- Spark delta reads ----
    spark = spark_delta()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=== Delta counts ===")
    bronze_n = delta_count(spark, BRONZE)
    silver_n = delta_count(spark, SILVER)
    gold_n   = delta_count(spark, GOLD)
    dlq_n    = delta_count(spark, DLQ)

    def fmt(n: int) -> str:
        return "ERROR" if n < 0 else str(n)

    print("bronze:", fmt(bronze_n))
    print("silver:", fmt(silver_n))
    print("gold  :", fmt(gold_n))
    print("dlq   :", fmt(dlq_n))

    # show last few rows from silver to confirm it's moving
    try:
        print("\n=== Silver latest 10 (event_time desc) ===")
        sdf = spark.read.format("delta").load(SILVER)
        cols = [c for c in ["event_id", "ts", "event_time", "user_id", "merchant_category", "amount", "country"] if c in sdf.columns]
        sdf.select(*cols).orderBy(desc("event_time")).show(10, truncate=False)
    except Exception as e:
        print("\nCould not read SILVER latest rows:", repr(e))

    # ---- If user provided event_id, locate it across layers ----
    if event_id:
        print(f"\n=== Locate event_id: {event_id} ===")

        # BRONZE
        try:
            b = find_event(spark, BRONZE, event_id, cols=["event_id", "ts", "event_time", "user_id", "merchant_category", "amount", "currency", "country"])
            bn = b.count()
            print(f"BRONZE matches: {bn}")
            if bn > 0:
                b.show(min(5, bn), truncate=False)
        except Exception as e:
            print("BRONZE lookup failed:", repr(e))

        # SILVER
        try:
            s = find_event(spark, SILVER, event_id, cols=["event_id", "ts", "event_time", "event_date", "user_id", "merchant_category", "amount", "currency", "country"])
            sn = s.count()
            print(f"SILVER matches: {sn}")
            if sn > 0:
                s.show(min(5, sn), truncate=False)
        except Exception as e:
            print("SILVER lookup failed:", repr(e))

        # DLQ (Delta)
        try:
            d = spark.read.format("delta").load(DLQ)
            if "event_id" in d.columns:
                dq = d.filter(col("event_id") == event_id)
            else:
                # if DLQ stores only raw json_str, search string
                dq = d.filter(col("json_str").contains(event_id))
            dn = dq.count()
            print(f"DLQ matches: {dn}")
            if dn > 0:
                dq.show(min(5, dn), truncate=False)
        except Exception as e:
            print("DLQ lookup failed (maybe table empty or path wrong):", repr(e))

        print("\nIf event is in BRONZE but not SILVER:")
        print("- check `merchant_category` is allowed (your config only allows retail/digital_goods/travel/gaming/crypto/giftcards)")
        print("- check `event_time` parsed correctly (null event_time => filtered as invalid)")
        print("- check stream is running and checkpoint not stuck")

    # ---- Streaming progress (checkpoints) ----
    print("\n=== Checkpoint folders (quick progress sanity) ===")
    print(run(f'find "{CHECKPOINTS}" -maxdepth 2 -type d | sed "s|^{CHECKPOINTS}||" | head -50'))

    spark.stop()
    print("\nDone.\n")


if __name__ == "__main__":
    main()
