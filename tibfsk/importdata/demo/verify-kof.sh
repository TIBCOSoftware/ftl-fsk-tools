#!/usr/bin/env bash
# Verify a migration by inspecting the target FSK cluster: list its topics,
# report the record count of each one, and print a few records from a sample
# topic. Point it at the source Kafka cluster instead to compare the two.
#
# Usage:
#   export KAFKA_HOME=/opt/kafka
#   bash demo/verify-kof.sh [--bootstrap-server localhost:19092]
#                           [--topic insurance.fraud.alerts]
#                           [--max-messages 3]
#                           [--no-sample]

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-}"
BOOTSTRAP="localhost:19092"
TOPIC="insurance.fraud.alerts"
MAX_MESSAGES=3
SAMPLE=true

# kafka-run-class.sh builds CLASSPATH starting from whatever $CLASSPATH is in
# the environment. Clear both to avoid snakeyaml/jackson version conflicts.
# KAFKA_CLASSPATH is set for run-apachekafka-to-fsk.sh and must not leak into the
# Kafka CLI scripts.
unset CLASSPATH
unset KAFKA_CLASSPATH

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-server) BOOTSTRAP="$2"; shift 2 ;;
    --topic)            TOPIC="$2"; shift 2 ;;
    --max-messages)     MAX_MESSAGES="$2"; shift 2 ;;
    --no-sample)        SAMPLE=false; shift ;;
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

echo "==> Topics on $BOOTSTRAP"
TOPICS="$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list)"
if [[ -z "$TOPICS" ]]; then
  echo "  (none)"
  echo ""
  echo "No topics found. If the migration has run, check that this is the right"
  echo "bootstrap address — the FSK listener port, not the FTL server port."
  exit 1
fi
echo "$TOPICS" | sed 's/^/  /'

# kafka-get-offsets.sh prints one "topic:partition:endOffset" line per
# partition. Summing the end offsets per topic gives the record count, which is
# what the migration reports as "published".
echo ""
echo "==> Record counts (sum of partition end offsets)"
"$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" 2>/dev/null \
  | awk -F: '$1 !~ /^_/ { count[$1] += $3 } END { for (t in count) print t, count[t] }' \
  | sort \
  | awk '{ printf "  %-32s %10d\n", $1, $2; total += $2; n++ }
         END {
           if (n == 0) { print "  (no non-internal topics)"; exit }
           printf "  %-32s %10d\n", "TOTAL", total
         }'

if [[ "$SAMPLE" == "true" ]]; then
  echo ""
  if echo "$TOPICS" | grep -qx "$TOPIC"; then
    echo "==> First $MAX_MESSAGES records of $TOPIC"
    "$KAFKA_HOME/bin/kafka-console-consumer.sh" \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$TOPIC" \
      --from-beginning \
      --max-messages "$MAX_MESSAGES" \
      --timeout-ms 30000
  else
    echo "==> Skipping sample: topic '$TOPIC' is not on this cluster"
    echo "    Pass --topic <name> to sample a different one."
  fi
fi

echo ""
echo "Done."
