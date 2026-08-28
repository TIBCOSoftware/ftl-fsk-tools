#!/bin/bash
#
# Copyright (c) 2026 Cloud Software Group, Inc.
# All Rights Reserved.
#

# Run the Apache Kafka -> FSK replication utility, compiling it first if needed.
#
# Classes are looked for in two places, in order:
#   classes/  -- compiled by CMake and shipped in the installed package
#   build/    -- compiled here, on demand, from src/main/java
#
# Compilation only happens when neither directory holds the class files, so an
# installed package runs straight away and a source tree compiles once. Set
# FSK_FORCE_REBUILD=1 to recompile after editing the Java sources.

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SRC_DIR="$SCRIPT_DIR/src/main/java"
BUILD_DIR="$SCRIPT_DIR/build"
CLASSES_DIR="$SCRIPT_DIR/classes"
CONF_FILE="$SCRIPT_DIR/conf/kafka-to-kof.properties"
MAIN_CLASS="com.tibco.ftl.fsk.ApacheKafkaToFskReplicatorApp"
MAIN_CLASS_FILE="com/tibco/ftl/fsk/ApacheKafkaToFskReplicatorApp.class"

# Needed to run, not just to compile: the Kafka client jars are not bundled.
if [ -z "${KAFKA_CLASSPATH:-}" ]; then
  echo "KAFKA_CLASSPATH is not set."
  echo "Set it to include kafka-clients and slf4j jars, for example:"
  echo "  export KAFKA_CLASSPATH=\"/path/kafka-clients.jar:/path/slf4j-api.jar:/path/slf4j-simple.jar\""
  exit 1
fi

CLASS_DIR=""
if [ -z "${FSK_FORCE_REBUILD:-}" ]; then
  if [ -f "$CLASSES_DIR/$MAIN_CLASS_FILE" ]; then
    CLASS_DIR="$CLASSES_DIR"
  elif [ -f "$BUILD_DIR/$MAIN_CLASS_FILE" ]; then
    CLASS_DIR="$BUILD_DIR"
  fi
fi

if [ -z "$CLASS_DIR" ]; then
  if [ ! -d "$SRC_DIR" ]; then
    echo "No compiled classes in $CLASSES_DIR or $BUILD_DIR, and no sources at $SRC_DIR."
    exit 1
  fi

  JAVA_FILES=$(find "$SRC_DIR" -name "*.java")
  if [ -z "$JAVA_FILES" ]; then
    echo "No Java files found under $SRC_DIR"
    exit 1
  fi

  echo "Compiling into $BUILD_DIR ..."
  mkdir -p "$BUILD_DIR"
  javac -d "$BUILD_DIR" -cp "$KAFKA_CLASSPATH" $JAVA_FILES
  CLASS_DIR="$BUILD_DIR"
fi

if [ "$#" -eq 0 ]; then
  set -- --config "$CONF_FILE"
fi

java -cp "$CLASS_DIR:$KAFKA_CLASSPATH" "$MAIN_CLASS" "$@"
