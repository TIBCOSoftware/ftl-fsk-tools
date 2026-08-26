#!/bin/bash

# Compile and run Apache Kafka -> FSK replication utility.

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SRC_DIR="$SCRIPT_DIR/src/main/java"
BUILD_DIR="$SCRIPT_DIR/build"
CONF_FILE="$SCRIPT_DIR/conf/kafka-to-kof.properties"

if [ -z "${KAFKA_CLASSPATH:-}" ]; then
  echo "KAFKA_CLASSPATH is not set."
  echo "Set it to include kafka-clients and slf4j jars, for example:"
  echo "  export KAFKA_CLASSPATH=\"/path/kafka-clients.jar:/path/slf4j-api.jar:/path/slf4j-simple.jar\""
  exit 1
fi

mkdir -p "$BUILD_DIR"

JAVA_FILES=$(find "$SRC_DIR" -name "*.java")
if [ -z "$JAVA_FILES" ]; then
  echo "No Java files found under $SRC_DIR"
  exit 1
fi

javac -d "$BUILD_DIR" -cp "$KAFKA_CLASSPATH" $JAVA_FILES

if [ "$#" -eq 0 ]; then
  set -- --config "$CONF_FILE"
fi

java -cp "$BUILD_DIR:$KAFKA_CLASSPATH" com.tibco.ftl.fsk.ApacheKafkaToFskReplicatorApp "$@"
