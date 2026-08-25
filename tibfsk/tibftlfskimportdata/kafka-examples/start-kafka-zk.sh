#!/usr/bin/env bash
# Start a local Kafka cluster in ZooKeeper mode from the example configs here.
#
# REQUIRES KAFKA 3.9 OR EARLIER. Kafka 4.x removed ZooKeeper support, so
# zookeeper-server-start.sh does not exist in a 4.x installation and this
# script will exit with an explanation.
#
# Usage:
#   export KAFKA_HOME=/path/to/kafka_2.13-3.9.1
#   bash kafka-examples/start-kafka-zk.sh single-node [--clean]
#   bash kafka-examples/start-kafka-zk.sh three-node  [--clean]
#
#   single-node  One broker on localhost:9092
#   three-node   Three brokers on localhost:9092, :9093, :9094
#   --clean      Delete existing data (ZooKeeper + log.dirs) before starting
#
# To stop: bash kafka-examples/stop-kafka-zk.sh

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="/tmp/kafka-examples-zk"
PID_FILE="$SCRIPT_DIR/kafka-examples-zk.pid"
CLEAN=false
LAYOUT=""

# kafka-run-class.sh builds CLASSPATH starting from whatever $CLASSPATH holds.
# An older snakeyaml reachable that way loads before Kafka's own and causes
# NoSuchMethodError. Clear both so Kafka builds its classpath from libs/.
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

if [[ -z "$KAFKA_HOME" ]]; then
  echo "ERROR: KAFKA_HOME is not set."
  echo "ZooKeeper mode needs Kafka 3.9 or earlier, e.g.:"
  echo "  export KAFKA_HOME=/path/to/kafka_2.13-3.9.1"
  exit 1
fi

if [[ ! -x "$KAFKA_HOME/bin/zookeeper-server-start.sh" ]]; then
  echo "ERROR: zookeeper-server-start.sh not found under KAFKA_HOME=$KAFKA_HOME"
  echo ""
  echo "Kafka 4.x removed ZooKeeper support, so 4.x installations do not ship it."
  echo "Either point KAFKA_HOME at a Kafka 3.9-or-earlier installation, or use"
  echo "KRaft mode instead:"
  echo "  bash kafka-examples/start-kafka.sh $LAYOUT"
  exit 1
fi

if [[ "$LAYOUT" == "single-node" ]]; then
  CFG_DIR="$SCRIPT_DIR/zk-single-node"
  CONFIGS=("$CFG_DIR/server.properties")
  BOOTSTRAP="localhost:9092"
else
  CFG_DIR="$SCRIPT_DIR/zk-three-node"
  CONFIGS=("$CFG_DIR/server-1.properties"
           "$CFG_DIR/server-2.properties"
           "$CFG_DIR/server-3.properties")
  BOOTSTRAP="localhost:9092,localhost:9093,localhost:9094"
fi
ZK_CFG="$CFG_DIR/zookeeper.properties"

# ── Stop anything this script started earlier ─────────────────────────────────
if [[ -f "$PID_FILE" ]]; then
  echo "==> Stopping previously started example processes"
  while IFS= read -r pid; do
    kill "$pid" 2>/dev/null && echo "  killed PID $pid" || true
  done < "$PID_FILE"
  rm -f "$PID_FILE"
  sleep 2
fi

# ── Clean existing data ───────────────────────────────────────────────────────
if $CLEAN; then
  echo "==> Removing existing data under $DATA_DIR"
  rm -rf "$DATA_DIR"
fi

mkdir -p "$DATA_DIR/logs"
> "$PID_FILE"

# ── Start ZooKeeper ───────────────────────────────────────────────────────────
echo "==> Starting ZooKeeper  (log: $DATA_DIR/logs/zookeeper.log)"
"$KAFKA_HOME/bin/zookeeper-server-start.sh" "$ZK_CFG" > "$DATA_DIR/logs/zookeeper.log" 2>&1 &
echo "$!" >> "$PID_FILE"

echo "==> Waiting for ZooKeeper on localhost:2181..."
ZK_WAIT=0
until nc -z localhost 2181 2>/dev/null; do
  sleep 1
  ZK_WAIT=$((ZK_WAIT + 1))
  if [[ $ZK_WAIT -ge 30 ]]; then
    echo "ERROR: ZooKeeper did not start within 30s."
    echo "Check $DATA_DIR/logs/zookeeper.log"
    exit 1
  fi
done

# ── Start brokers ─────────────────────────────────────────────────────────────
n=0
for cfg in "${CONFIGS[@]}"; do
  n=$((n + 1))
  log="$DATA_DIR/logs/broker-$n.log"
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
    echo "Check logs under $DATA_DIR/logs/"
    exit 1
  fi
  echo "  still waiting... (${ELAPSED}s)"
done

echo ""
echo "✓ Kafka ZooKeeper-mode cluster is up ($LAYOUT)."
echo "  Bootstrap servers: $BOOTSTRAP"
echo "  ZooKeeper:         localhost:2181"
echo "  PIDs saved to:     $PID_FILE"
echo ""
echo "  To stop: bash kafka-examples/stop-kafka-zk.sh"
