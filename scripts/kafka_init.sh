#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-docker-kafka-1:9092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION="${TOPIC_REPLICATION:-1}"

# All topics required by the pipeline
TOPICS=("txns" "txns_dead_letter" "txns_alerts")

echo "Waiting for Kafka at: $BOOTSTRAP ..."
for i in {1..60}; do
  if kafka-topics --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    echo "Kafka is up."
    break
  fi
  sleep 2
done

ensure_topic() {
  local topic="$1"
  echo "Ensuring topic '$topic' exists with >= $PARTITIONS partitions"
  if kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic" >/dev/null 2>&1; then
    echo "Topic '$topic' exists. Ensuring partitions..."
    kafka-topics --bootstrap-server "$BOOTSTRAP" --alter --topic "$topic" --partitions "$PARTITIONS" || true
  else
    echo "Creating topic '$topic'..."
    kafka-topics --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$topic" \
      --partitions "$PARTITIONS" \
      --replication-factor "$REPLICATION"
  fi
}

for t in "${TOPICS[@]}"; do
  ensure_topic "$t"
done

echo "Final topic states:"
for t in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$t"
done

echo "✅ Topics ensured: ${TOPICS[*]}"
