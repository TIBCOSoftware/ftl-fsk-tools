#!/usr/bin/env bash
# Start a 3-broker Kafka cluster in KRaft mode for the insurance migration demo.
# Brokers listen on localhost:9092, localhost:9093, localhost:9094.
#
# Usage:
#   export KAFKA_HOME=/opt/kafka
#   bash demo/setup-kafka-kraft.sh [--clean]
#
# --clean  Delete existing log.dirs data before formatting (full reset).

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KRAFT_DIR="$SCRIPT_DIR/kraft"
LOG_DIR="/tmp/kafka-kraft"
CLEAN=false

# kafka-run-class.sh (used by all Kafka CLI tools) starts building CLASSPATH
# from whatever $CLASSPATH is already set to in the environment. If an older
# snakeyaml is reachable via $CLASSPATH or $KAFKA_CLASSPATH it gets loaded
# before Kafka's own snakeyaml-2.4.jar and causes NoSuchMethodError.
# Unset both so Kafka builds its classpath cleanly from $KAFKA_HOME/libs/.
unset CLASSPATH
unset KAFKA_CLASSPATH

for arg in "$@"; do
  case "$arg" in
    --clean) CLEAN=true ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

if [[ -z "$KAFKA_HOME" ]]; then
  echo "ERROR: KAFKA_HOME is not set."
  echo "Set it to your Kafka installation, e.g.:"
  echo "  export KAFKA_HOME=/opt/kafka"
  exit 1
fi

if [[ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
  echo "ERROR: kafka-server-start.sh not found under KAFKA_HOME=$KAFKA_HOME"
  echo "Set KAFKA_HOME to your Kafka installation, e.g.:"
  echo "  export KAFKA_HOME=/opt/kafka"
  exit 1
fi

# ── Clean existing data ────────────────────────────────────────────────────────
if $CLEAN; then
  echo "==> Removing existing log dirs under $LOG_DIR"
  rm -rf "$LOG_DIR"
fi

mkdir -p "$LOG_DIR/logs"

# ── Stop any running demo brokers ──────────────────────────────────────────────
PID_FILE="$SCRIPT_DIR/kafka-brokers.pid"
if [[ -f "$PID_FILE" ]]; then
  echo "==> Stopping previously started demo brokers"
  while IFS= read -r pid; do
    kill "$pid" 2>/dev/null && echo "  killed PID $pid" || true
  done < "$PID_FILE"
  rm -f "$PID_FILE"
  sleep 2
fi

# ── Cluster ID ────────────────────────────────────────────────────────────────
# Fixed ID so brokers can restart without wiping data dirs.
# Use --clean to fully reset the cluster.
KAFKA_CLUSTER_ID="insurance-demo-cluster-00001"
echo "==> Using fixed KRaft cluster ID: $KAFKA_CLUSTER_ID"

# ── Format storage for each broker ────────────────────────────────────────────
for n in 1 2 3; do
  cfg="$KRAFT_DIR/server-$n.properties"
  echo "==> Formatting storage for broker $n ($cfg)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format \
    --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" \
    -c "$cfg"
done

# ── Start brokers ─────────────────────────────────────────────────────────────
> "$PID_FILE"
for n in 1 2 3; do
  cfg="$KRAFT_DIR/server-$n.properties"
  log="$LOG_DIR/logs/broker-$n.log"
  echo "==> Starting broker $n  (log: $log)"
  "$KAFKA_HOME/bin/kafka-server-start.sh" "$cfg" > "$log" 2>&1 &
  echo "$!" >> "$PID_FILE"
done

echo ""
echo "==> Waiting for brokers to become ready..."
BOOTSTRAP="localhost:9092"
MAX_WAIT=60
ELAPSED=0
until "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list &>/dev/null; do
  sleep 2
  ELAPSED=$((ELAPSED + 2))
  if [[ $ELAPSED -ge $MAX_WAIT ]]; then
    echo "ERROR: Kafka did not become ready within ${MAX_WAIT}s."
    echo "Check logs under $LOG_DIR/logs/"
    exit 1
  fi
  echo "  still waiting... (${ELAPSED}s)"
done

echo ""
echo "✓ Kafka KRaft cluster is up."
echo "  Bootstrap servers: localhost:9092,localhost:9093,localhost:9094"
echo "  Broker PIDs saved to: $PID_FILE"
echo ""
echo "  Next steps:"
echo "    bash demo/create-topics.sh"
echo "    bash demo/populate-kafka.sh"
echo ""
echo "  To stop: bash demo/stop-kafka.sh"
