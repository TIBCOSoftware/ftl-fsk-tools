#!/usr/bin/env bash
# Start a local Kafka 4.x KRaft cluster from the example configs in this directory.
#
# Usage:
#   export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec
#   bash kafka-examples/start-kafka.sh single-node [--clean]
#   bash kafka-examples/start-kafka.sh three-node  [--clean]
#
#   single-node  One broker on localhost:9092
#   three-node   Three brokers on localhost:9092, :9093, :9094
#   --clean      Delete existing log.dirs data before formatting (full reset)
#
# To stop: bash kafka-examples/stop-kafka.sh

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/usr/local/Cellar/kafka/4.2.0/libexec}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="/tmp/kafka-examples"
PID_FILE="$SCRIPT_DIR/kafka-examples.pid"
CLEAN=false
LAYOUT=""

# kafka-run-class.sh (used by all Kafka CLI tools) starts building CLASSPATH
# from whatever $CLASSPATH is already set to in the environment. If an older
# snakeyaml is reachable via $CLASSPATH or $KAFKA_CLASSPATH it gets loaded
# before Kafka's own snakeyaml jar and causes NoSuchMethodError.
# Unset both so Kafka builds its classpath cleanly from $KAFKA_HOME/libs/.
unset CLASSPATH
unset KAFKA_CLASSPATH

for arg in "$@"; do
  case "$arg" in
    single-node|three-node) LAYOUT="$arg" ;;
    --clean)                CLEAN=true ;;
    *) echo "Unknown argument: $arg"; echo "Usage: $0 {single-node|three-node} [--clean]"; exit 1 ;;
  esac
done

if [[ -z "$LAYOUT" ]]; then
  echo "ERROR: choose a layout."
  echo "Usage: $0 {single-node|three-node} [--clean]"
  exit 1
fi

if [[ "$LAYOUT" == "single-node" ]]; then
  CONFIGS=("$SCRIPT_DIR/single-node/server.properties")
  BOOTSTRAP="localhost:9092"
else
  CONFIGS=("$SCRIPT_DIR/three-node/server-1.properties"
           "$SCRIPT_DIR/three-node/server-2.properties"
           "$SCRIPT_DIR/three-node/server-3.properties")
  BOOTSTRAP="localhost:9092,localhost:9093,localhost:9094"
fi

if [[ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
  echo "ERROR: kafka-server-start.sh not found under KAFKA_HOME=$KAFKA_HOME"
  echo "Set KAFKA_HOME to your Kafka installation, e.g.:"
  echo "  export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec"
  exit 1
fi

# ── Stop anything this script started earlier ─────────────────────────────────
# The two layouts share ports 9092/9911, so they cannot run at the same time.
if [[ -f "$PID_FILE" ]]; then
  echo "==> Stopping previously started example brokers"
  while IFS= read -r pid; do
    kill "$pid" 2>/dev/null && echo "  killed PID $pid" || true
  done < "$PID_FILE"
  rm -f "$PID_FILE"
  sleep 2
fi

# ── Clean existing data ───────────────────────────────────────────────────────
if $CLEAN; then
  echo "==> Removing existing log dirs under $LOG_DIR"
  rm -rf "$LOG_DIR"
fi

mkdir -p "$LOG_DIR/logs"

# ── Cluster ID ────────────────────────────────────────────────────────────────
# Fixed ID so brokers can restart without wiping their data dirs.
# Use --clean to fully reset the cluster.
KAFKA_CLUSTER_ID="kafka-examples-$LAYOUT-001"
echo "==> Using fixed KRaft cluster ID: $KAFKA_CLUSTER_ID"

# ── Format storage ────────────────────────────────────────────────────────────
for cfg in "${CONFIGS[@]}"; do
  echo "==> Formatting storage ($cfg)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format \
    --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" \
    -c "$cfg"
done

# ── Start brokers ─────────────────────────────────────────────────────────────
> "$PID_FILE"
n=0
for cfg in "${CONFIGS[@]}"; do
  n=$((n + 1))
  log="$LOG_DIR/logs/broker-$n.log"
  echo "==> Starting broker $n  (log: $log)"
  "$KAFKA_HOME/bin/kafka-server-start.sh" "$cfg" > "$log" 2>&1 &
  echo "$!" >> "$PID_FILE"
done

echo ""
echo "==> Waiting for the cluster to become ready..."
MAX_WAIT=60
ELAPSED=0
until "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list &>/dev/null; do
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
echo "✓ Kafka KRaft cluster is up ($LAYOUT)."
echo "  Bootstrap servers: $BOOTSTRAP"
echo "  Broker PIDs saved to: $PID_FILE"
echo ""
echo "  To stop: bash kafka-examples/stop-kafka.sh"
