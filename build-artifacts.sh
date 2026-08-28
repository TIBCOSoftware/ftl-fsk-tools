#!/usr/bin/env bash
#
# Copyright (c) 2026 Cloud Software Group, Inc.
# All Rights Reserved.
#
# Rebuild the artifacts this repository checks in:
#
#   tibfsk/importconfig/bin/tibftlimportconfig   Go executable, host platform
#   tibfsk/importdata/classes/                   tibftlfskimportdata class files
#   tibfsk/importdata/demo/classes/              InsuranceDataProducer class files
#
# They are committed so the tools run straight from a clone, with no Go toolchain
# and no javac. CMake still builds everything from source into its own binary
# directory and does not read these; they are for people who just want to run.
#
# Re-run this and commit the result whenever the Go or Java sources change --
# sync-to-fsk-tools.sh copies sources only, so a stale binary here is silent.
#
# Usage:
#   ./build-artifacts.sh
#
# Overridable:
#   GO               Go 1.25+ executable                (default: go on PATH)
#   JAVAC            javac executable                   (default: JDK 25 from Homebrew)
#   JAVA_RELEASE     --release passed to javac          (default: 11, the documented minimum)
#   KAFKA_CLASSPATH  kafka-clients.jar:slf4j-api.jar    (default: derived from KAFKA_HOME)
#   KAFKA_HOME       Apache Kafka installation          (used only to find those two jars)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GO="${GO:-go}"
JAVAC="${JAVAC:-/usr/local/opt/openjdk/bin/javac}"
JAVA_RELEASE="${JAVA_RELEASE:-11}"

IMPORTCONFIG="$ROOT/tibfsk/importconfig"
IMPORTDATA="$ROOT/tibfsk/importdata"

# ── tibftlimportconfig ────────────────────────────────────────────────────────
command -v "$GO" >/dev/null 2>&1 || { echo "ERROR: no Go executable ($GO). Set \$GO." >&2; exit 1; }

echo "==> Building tibftlimportconfig with $("$GO" version)"
mkdir -p "$IMPORTCONFIG/bin"
(cd "$IMPORTCONFIG" && "$GO" build -trimpath -o bin/tibftlimportconfig .)
echo "    $(cd "$ROOT" && ls -l tibfsk/importconfig/bin/tibftlimportconfig | awk '{print $5, $NF}')"

# ── tibftlfskimportdata ───────────────────────────────────────────────────────
# javac needs the Kafka client API on the classpath; the jars are not bundled here.
if [[ -z "${KAFKA_CLASSPATH:-}" ]]; then
    if [[ -n "${KAFKA_HOME:-}" ]]; then
        clients="$(ls "$KAFKA_HOME"/libs/kafka-clients-*.jar 2>/dev/null | head -1)"
        slf4j="$(ls "$KAFKA_HOME"/libs/slf4j-api-*.jar 2>/dev/null | head -1)"
        [[ -n "$clients" && -n "$slf4j" ]] && KAFKA_CLASSPATH="$clients:$slf4j"
    fi
fi
if [[ -z "${KAFKA_CLASSPATH:-}" ]]; then
    echo "ERROR: KAFKA_CLASSPATH is not set and could not be derived from \$KAFKA_HOME." >&2
    echo "       Set one of:" >&2
    echo "         export KAFKA_HOME=/path/to/kafka" >&2
    echo "         export KAFKA_CLASSPATH=\"/path/kafka-clients.jar:/path/slf4j-api.jar\"" >&2
    exit 1
fi
[[ -x "$JAVAC" ]] || command -v "$JAVAC" >/dev/null 2>&1 || {
    echo "ERROR: no javac at $JAVAC. Set \$JAVAC." >&2; exit 1; }

# --release keeps the class files loadable on the Java 11 the READMEs promise, even
# though a much newer JDK compiles them.
echo "==> Compiling tibftlfskimportdata with $("$JAVAC" -version 2>&1) --release $JAVA_RELEASE"
rm -rf "$IMPORTDATA/classes"
mkdir -p "$IMPORTDATA/classes"
"$JAVAC" --release "$JAVA_RELEASE" -d "$IMPORTDATA/classes" -cp "$KAFKA_CLASSPATH" \
    $(find "$IMPORTDATA/src/main/java" -name '*.java')

echo "==> Compiling demo InsuranceDataProducer"
rm -rf "$IMPORTDATA/demo/classes"
mkdir -p "$IMPORTDATA/demo/classes"
"$JAVAC" --release "$JAVA_RELEASE" -d "$IMPORTDATA/demo/classes" -cp "$KAFKA_CLASSPATH" \
    "$IMPORTDATA/demo/InsuranceDataProducer.java"

echo ""
echo "Artifacts rebuilt. Review and commit:"
(cd "$ROOT" && git status --short tibfsk/importconfig/bin tibfsk/importdata/classes tibfsk/importdata/demo/classes)
