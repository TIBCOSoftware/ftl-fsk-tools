#!/usr/bin/env bash
# Create the 10 insurance demo topics on Kafka.
# Each topic is created with 3 partitions and replication-factor 1.
#
# Usage:
#   export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec
#   bash demo/create-topics.sh [--bootstrap-server localhost:9092]

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/usr/local/Cellar/kafka/4.2.0/libexec}"
BOOTSTRAP="localhost:9092"
PARTITIONS=3
REPLICATION=1

# kafka-topics.sh uses kafka-run-class.sh which appends $KAFKA_CLASSPATH to
# its own classpath, causing conflicts. Unset it here.
unset KAFKA_CLASSPATH

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-server) BOOTSTRAP="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

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
echo "  \$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list"
