#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER=${KAFKA_CONTAINER:-docker-kafka-1}

docker exec -it "$KAFKA_CONTAINER" bash -lc \
'kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic txns --partitions 3 --replication-factor 1'

docker exec -it "$KAFKA_CONTAINER" bash -lc \
'kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic txns_dead_letter --partitions 3 --replication-factor 1'

docker exec -it "$KAFKA_CONTAINER" bash -lc \
'kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic txns_alerts --partitions 3 --replication-factor 1'

echo "✅ Topics ensured: txns, txns_dead_letter, txns_alerts"
