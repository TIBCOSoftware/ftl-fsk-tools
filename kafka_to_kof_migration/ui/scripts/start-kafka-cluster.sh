#!/bin/bash
# Start a 3-broker KRaft Kafka cluster (no ZooKeeper)
# Uses property files from hydra_qa/src/jsons/kof/
#
# Prerequisites:
#   export KAFKA_HOME=/path/to/kafka   (e.g. /rv/msg_share1/tools/kafka/4.1.2)
#
# Cluster layout:
#   Controller : node-9  port 9093  (KRaft only, not a client broker)
#   Broker-1   : node-1  port 9092  (client)
#   Broker-2   : node-2  port 9094  (client)
#   Broker-3   : node-3  port 9096  (client)
#   Bootstrap  : localhost:9092,localhost:9094,localhost:9096

set -e

if [ -z "${KAFKA_HOME:-}" ]; then
  echo "ERROR: KAFKA_HOME is not set."
  echo "  export KAFKA_HOME=/rv/msg_share1/tools/kafka/4.1.2"
  exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $SCRIPT_DIR
CONF_DIR="$SCRIPT_DIR/../../../../../../hydra_qa/src/jsons/kof"
LOG_DIR="${LOG_DIR:-/tmp/kafka-cluster-logs}"
CLUSTER_ID="943wV877QsCzsH_fRfz_qg"

mkdir -p "$LOG_DIR"

echo "=== Formatting Kafka storage (KRaft) ==="
CLASSPATH= "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF_DIR/controller-9.properties" --ignore-formatted
CLASSPATH= "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF_DIR/broker-1.properties"   --ignore-formatted
CLASSPATH= "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF_DIR/broker-2.properties"   --ignore-formatted
CLASSPATH= "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF_DIR/broker-3.properties"   --ignore-formatted

echo "=== Starting KRaft Controller (node-9, port 9093) ==="
CLASSPATH= LOG_DIR="$LOG_DIR/controller-9" \
  "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$CONF_DIR/controller-9.properties"

sleep 2

echo "=== Starting Broker-1 (node-1, port 9092) ==="
CLASSPATH= LOG_DIR="$LOG_DIR/broker-1" \
  "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$CONF_DIR/broker-1.properties"

echo "=== Starting Broker-2 (node-2, port 9094) ==="
CLASSPATH= LOG_DIR="$LOG_DIR/broker-2" \
  "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$CONF_DIR/broker-2.properties"

echo "=== Starting Broker-3 (node-3, port 9096) ==="
CLASSPATH= LOG_DIR="$LOG_DIR/broker-3" \
  "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$CONF_DIR/broker-3.properties"

echo ""
echo "=== Waiting for brokers to be ready... ==="
sleep 5

echo "=== Verifying cluster ==="
CLASSPATH= "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
  --bootstrap-server localhost:9092,localhost:9094,localhost:9096 2>&1 | grep "id:" || true

echo ""
echo "Kafka cluster is UP."
echo "  Bootstrap: localhost:9092,localhost:9094,localhost:9096"
echo "  Logs:      $LOG_DIR"
