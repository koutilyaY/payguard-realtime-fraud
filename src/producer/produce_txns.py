import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from confluent_kafka import Producer, KafkaException

from src.utils.config import load_config
from src.utils.logger import get_logger

log = get_logger("producer")
fake = Faker()
cfg = load_config()

KAFKA_BOOTSTRAP = cfg["kafka"]["bootstrap_servers"]
TOPIC = cfg["kafka"]["topic_raw"]
SLEEP_S = float(cfg["producer"]["sleep_seconds"])
EDGE_RATE = float(cfg["producer"]["edge_case_rate"])

MERCHANT_CATS = cfg["rules"]["categories_allowed"]
COUNTRIES = cfg["rules"]["countries_allowed"]
DEVICES = ["ios", "android", "web"]

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def generate_txn():
    user_id = random.randint(1, 20000)
    amount = round(random.expovariate(1/35) + 1, 2)  # long-tail amounts
    country = random.choices(COUNTRIES + ["XX"], weights=[55,8,10,8,6,6,7,0.2])[0]  # rare invalid
    mcc = random.choice(MERCHANT_CATS)
    device = random.choice(DEVICES)
    is_chargeback_history = random.random() < 0.07

    # Edge cases for DQ (rare)
    r = random.random()
    if r < EDGE_RATE / 3:
        amount = 0.0
    elif r < 2 * EDGE_RATE / 3:
        amount = -round(random.random() * 50, 2)
    elif r < EDGE_RATE:
        amount = round(100000 + random.random() * 50000, 2)

    evt = {
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "merchant_id": random.randint(1, 5000),
        "merchant_category": mcc,
        "amount": amount,
        "currency": "USD",
        "country": country,
        "device_type": device,
        "ip_risk_score": round(random.random(), 4),
        "acct_age_days": random.randint(0, 2400),
        "chargeback_history": int(is_chargeback_history),

        # IMPORTANT: realtime systems do NOT have confirmed fraud labels at tx-time.
        # We'll add delayed labeling later via a separate labels stream.
        # "label_fraud": 0
    }
    return evt

def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed: {err}")

if __name__ == "__main__":
    log.info(f"Producing txns to topic={TOPIC} bootstrap={KAFKA_BOOTSTRAP}")
    try:
        while True:
            evt = generate_txn()
            producer.produce(
                TOPIC,
                value=json.dumps(evt).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(SLEEP_S)
    except KeyboardInterrupt:
        log.warning("Stopping producer (Ctrl+C)...")
    except KafkaException as e:
        log.exception(f"Kafka error: {e}")
        raise
    except Exception as e:
        log.exception(f"Unexpected error: {e}")
        raise
    finally:
        producer.flush()
        log.info("Shutdown complete")
