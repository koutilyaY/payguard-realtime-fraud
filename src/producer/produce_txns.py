"""
Replay the REAL ULB credit-card transactions as a live Kafka stream.

This is the ONLY synthetic thing in the pipeline, and even here the data itself
is real: we take the actual ULB transactions (Time, V1..V28, Amount, Class),
sort them by their real elapsed time, and emit them one by one so the streaming
job has a live feed to score. The "real-time" is a replay of a real, static
2-day dataset — not live production traffic. The Class label rides along so
downstream you can measure real detection quality, but the model never sees it.

Run:
    python -m src.producer.produce_txns              # replay whole dataset
    REPLAY_LIMIT=5000 python -m src.producer.produce_txns   # first 5k rows
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

from src.utils.config import load_config
from src.utils.logger import get_logger

log = get_logger("producer")
cfg = load_config()

KAFKA_BOOTSTRAP = cfg["kafka"]["bootstrap_servers"]
TOPIC = cfg["kafka"]["topic_raw"]
SLEEP_S = float(cfg["producer"].get("sleep_seconds", 0.01))
DATA_CSV = os.path.join("data", "creditcard.csv")
REPLAY_LIMIT = int(os.getenv("REPLAY_LIMIT", "0"))  # 0 = whole file

FEATURE_COLS = [f"V{i}" for i in range(1, 29)] + ["Amount"]


def load_transactions():
    import pandas as pd
    if not os.path.exists(DATA_CSV):
        raise FileNotFoundError(
            f"Real dataset not found at {DATA_CSV}. "
            "Run: python scripts/download_data.py"
        )
    df = pd.read_csv(DATA_CSV)
    if "Time" in df.columns:
        df = df.sort_values("Time", kind="mergesort").reset_index(drop=True)
    if REPLAY_LIMIT > 0:
        df = df.head(REPLAY_LIMIT)
    return df


def row_to_event(row) -> dict:
    """Turn one real ULB row into a JSON event for Kafka."""
    evt = {
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "time_offset": float(row["Time"]) if "Time" in row else None,
        "amount": float(row["Amount"]),
        # the 28 PCA components, carried through for scoring
        **{c: float(row[c]) for c in FEATURE_COLS if c != "Amount"},
        # ground-truth label from the real dataset — for offline measurement only;
        # the scoring model never receives this field.
        "label_fraud": int(row["Class"]) if "Class" in row else None,
    }
    return evt


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed: {err}")


if __name__ == "__main__":
    from confluent_kafka import Producer, KafkaException

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    df = load_transactions()
    log.info(
        f"Replaying {len(df):,} REAL ULB transactions to topic={TOPIC} "
        f"bootstrap={KAFKA_BOOTSTRAP} (this is a replay of real data, not live traffic)"
    )
    sent = 0
    try:
        for _, row in df.iterrows():
            evt = row_to_event(row)
            producer.produce(
                TOPIC,
                value=json.dumps(evt).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            sent += 1
            if sent % 10000 == 0:
                log.info(f"  replayed {sent:,} / {len(df):,}")
            if SLEEP_S > 0:
                time.sleep(SLEEP_S)
        log.info(f"Replay complete: {sent:,} transactions sent.")
    except KeyboardInterrupt:
        log.warning("Stopping replay (Ctrl+C)...")
    except KafkaException as e:
        log.exception(f"Kafka error: {e}")
        raise
    finally:
        producer.flush()
        log.info("Producer shutdown complete")
