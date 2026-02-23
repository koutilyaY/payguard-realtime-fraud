#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-docker-kafka-1:9092}"
TOPIC="${TOPIC_NAME:-txns}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION="${TOPIC_REPLICATION:-1}"

echo "Waiting for Kafka at: $BOOTSTRAP ..."
for i in {1..60}; do
  if kafka-topics --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    echo "Kafka is up."
    break
  fi
  sleep 2
done

echo "Ensuring topic '$TOPIC' exists with >= $PARTITIONS partitions"

if kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC" >/dev/null 2>&1; then
  echo "Topic exists. Ensuring partitions..."
  kafka-topics --bootstrap-server "$BOOTSTRAP" --alter --topic "$TOPIC" --partitions "$PARTITIONS" || true
else
  echo "Creating topic..."
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"
fi

echo "Final topic state:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
