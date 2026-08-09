#!/usr/bin/env bash
# Compile InsuranceDataProducer.java and populate Kafka with insurance demo data.
# Sends N messages to each of the 10 insurance topics (default: 100 000 per topic = 1 000 000 total).
#
# Prerequisites:
#   - Kafka running (demo/setup-kafka-kraft.sh)
#   - Topics created (demo/create-topics.sh)
#   - KAFKA_CLASSPATH set to kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar
#
# Usage:
#   export KAFKA_CLASSPATH="/path/kafka-clients.jar:/path/slf4j-api.jar:/path/slf4j-simple.jar"
#   bash demo/populate-kafka.sh [--bootstrap-server localhost:9092] [--messages N]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
BOOTSTRAP="localhost:9092"
MESSAGES=100000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-server) BOOTSTRAP="$2"; shift 2 ;;
    --messages)         MESSAGES="$2";  shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "${KAFKA_CLASSPATH:-}" ]]; then
  echo "ERROR: KAFKA_CLASSPATH is not set."
  echo "Set it to include kafka-clients.jar, slf4j-api.jar, slf4j-simple.jar, e.g.:"
  echo "  export KAFKA_CLASSPATH=\"/path/kafka-clients.jar:/path/slf4j-api.jar:/path/slf4j-simple.jar\""
  exit 1
fi

echo "==> Compiling InsuranceDataProducer.java"
mkdir -p "$BUILD_DIR"
javac -d "$BUILD_DIR" -cp "$KAFKA_CLASSPATH" "$SCRIPT_DIR/InsuranceDataProducer.java"

echo "==> Sending $MESSAGES messages per topic to $BOOTSTRAP"
echo ""
java -cp "$BUILD_DIR:$KAFKA_CLASSPATH" InsuranceDataProducer \
  --bootstrap-server "$BOOTSTRAP" \
  --messages-per-topic "$MESSAGES"
