#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
while ! nc -z localhost 9092; do
  sleep 1
done
echo "Kafka is ready. Creating topics..."

docker exec flowforge-kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic raw-trades \
  --partitions 3 \
  --replication-factor 1

docker exec flowforge-kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic raw-quotes \
  --partitions 3 \
  --replication-factor 1

docker exec flowforge-kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic enriched-events \
  --partitions 3 \
  --replication-factor 1

docker exec flowforge-kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic inference-requests \
  --partitions 3 \
  --replication-factor 1

docker exec flowforge-kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic trading-signals \
  --partitions 3 \
  --replication-factor 1

echo "All topics created."
