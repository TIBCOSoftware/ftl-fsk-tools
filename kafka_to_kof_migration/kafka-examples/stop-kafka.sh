#!/usr/bin/env bash
# Stop the example Kafka brokers started by start-kafka.sh.
#
# Usage:
#   bash kafka-examples/stop-kafka.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/kafka-examples.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file found at $PID_FILE — brokers may not be running."
  exit 0
fi

echo "==> Stopping example Kafka brokers"
while IFS= read -r pid; do
  if kill "$pid" 2>/dev/null; then
    echo "  stopped PID $pid"
  else
    echo "  PID $pid not found (already stopped?)"
  fi
done < "$PID_FILE"

rm -f "$PID_FILE"
echo "Done."
