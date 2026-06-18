#!/bin/bash
# Stop the 3-broker KRaft Kafka cluster
if [ -z "${KAFKA_HOME:-}" ]; then
  echo "ERROR: KAFKA_HOME is not set."; exit 1
fi

echo "=== Stopping Kafka brokers and controller ==="
for PROP in broker-1.properties broker-2.properties broker-3.properties controller-9.properties; do
  PID=$(pgrep -f "$PROP" 2>/dev/null)
  if [ -n "$PID" ]; then
    echo "Stopping $PROP (PID $PID)"
    kill -9 "$PID" 2>/dev/null || true
  fi
done

sleep 3
echo "Kafka cluster stopped."
