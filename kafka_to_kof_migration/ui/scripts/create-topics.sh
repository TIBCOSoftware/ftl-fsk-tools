#!/bin/bash
# Create topics on source Kafka cluster and populate with 10,000 messages each
# Topics: orders, inventory, audit-log, metrics, events
#
# Prerequisites:
#   export KAFKA_HOME=/path/to/kafka
#   The 3-broker source Kafka cluster must be running (localhost:9092,9094,9096)

if [ -z "${KAFKA_HOME:-}" ]; then
  echo "ERROR: KAFKA_HOME is not set."
  echo "  export KAFKA_HOME=/rv/msg_share1/tools/kafka/4.1.2"
  exit 1
fi

BOOTSTRAP="localhost:9092,localhost:9094,localhost:9096"
TOPICS=(orders inventory audit-log metrics events)
MSG_COUNT=10000
REPL=3
PARTS=3

echo "=== Creating topics (replication=$REPL, partitions=$PARTS) ==="
for TOPIC in "${TOPICS[@]}"; do
  CLASSPATH= "$KAFKA_HOME/bin/kafka-topics.sh" --create \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --replication-factor "$REPL" \
    --partitions "$PARTS" \
    --if-not-exists \
    && echo "  Created: $TOPIC" || echo "  Already exists (or error): $TOPIC"
done

echo ""
echo "=== Verifying topics ==="
CLASSPATH= "$KAFKA_HOME/bin/kafka-topics.sh" --list --bootstrap-server "$BOOTSTRAP"

echo ""
echo "=== Populating topics with $MSG_COUNT messages each ==="
for TOPIC in "${TOPICS[@]}"; do
  echo "  Populating $TOPIC..."
  CLASSPATH= "$KAFKA_HOME/bin/kafka-producer-perf-test.sh" \
    --topic "$TOPIC" \
    --num-records "$MSG_COUNT" \
    --record-size 256 \
    --throughput -1 \
    --producer-props bootstrap.servers="$BOOTSTRAP" \
      acks=all \
      max.block.ms=30000 \
    2>&1 | tail -3
  echo "    $TOPIC: done"
done

echo ""
echo "=== Verifying message counts ==="
for TOPIC in "${TOPICS[@]}"; do
  CLASSPATH= "$KAFKA_HOME/bin/kafka-run-class.sh" kafka.tools.GetOffsetShell \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --time -1 2>/dev/null | awk -F: "{sum+=\$3} END{print \"  $TOPIC: \" sum \" messages\"}" || true
done

echo ""
echo "All topics populated. Ready to run run-kafka-to-kof.sh"
