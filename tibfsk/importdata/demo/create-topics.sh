#!/usr/bin/env bash
#
# Copyright (c) 2026 Cloud Software Group, Inc.
# All Rights Reserved.
#
# Create the 10 insurance demo topics on Kafka.
# Each topic is created with 3 partitions and replication-factor 1.
#
# Usage:
#   export KAFKA_HOME=/opt/kafka
#   bash demo/create-topics.sh [--bootstrap-server localhost:9092]

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-}"
BOOTSTRAP="localhost:9092"
PARTITIONS=3
REPLICATION=1

# kafka-run-class.sh builds CLASSPATH starting from whatever $CLASSPATH is in
# the environment. Clear both to avoid snakeyaml/jackson version conflicts.
unset CLASSPATH
unset KAFKA_CLASSPATH

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-server) BOOTSTRAP="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "$KAFKA_HOME" ]]; then
  echo "ERROR: KAFKA_HOME is not set."
  echo "Set it to your Kafka installation, e.g.:"
  echo "  export KAFKA_HOME=/opt/kafka"
  exit 1
fi

if [[ ! -x "$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
  echo "ERROR: kafka-topics.sh not found under KAFKA_HOME=$KAFKA_HOME"
  exit 1
fi

TOPICS=(
  "insurance.auto.claims"
  "insurance.home.claims"
  "insurance.life.events"
  "insurance.health.claims"
  "insurance.commercial.claims"
  "insurance.policy.updates"
  "insurance.customer.profiles"
  "insurance.premium.payments"
  "insurance.fraud.alerts"
  "insurance.audit.log"
)

echo "==> Creating ${#TOPICS[@]} insurance topics on $BOOTSTRAP"
echo "    partitions=$PARTITIONS  replication-factor=$REPLICATION"
echo ""

for topic in "${TOPICS[@]}"; do
  if "$KAFKA_HOME/bin/kafka-topics.sh" \
      --bootstrap-server "$BOOTSTRAP" \
      --create \
      --if-not-exists \
      --topic "$topic" \
      --partitions "$PARTITIONS" \
      --replication-factor "$REPLICATION"; then
    echo "  ✓ $topic"
  else
    echo "  ✗ $topic (failed)"
    exit 1
  fi
done

echo ""
echo "All topics created. Verify with:"
echo "  bash demo/verify-kof.sh --bootstrap-server $BOOTSTRAP --no-sample"
