#!/usr/bin/env bash
# Stop the ZooKeeper-mode example cluster started by start-kafka-zk.sh.
# Brokers are killed before ZooKeeper, in reverse start order.
#
# Usage:
#   bash kafka-examples/stop-kafka-zk.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/kafka-examples-zk.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file found at $PID_FILE — nothing to stop."
  exit 0
fi

# The PID file lists ZooKeeper first, then the brokers. Stop in reverse so the
# brokers shut down before the ZooKeeper they depend on.
PIDS=()
while IFS= read -r pid; do
  [[ -n "$pid" ]] && PIDS+=("$pid")
done < "$PID_FILE"

echo "==> Stopping example ZooKeeper-mode cluster"
for (( i=${#PIDS[@]}-1; i>=0; i-- )); do
  pid="${PIDS[$i]}"
  if kill "$pid" 2>/dev/null; then
    echo "  stopped PID $pid"
  else
    echo "  PID $pid not found (already stopped?)"
  fi
done

rm -f "$PID_FILE"
echo "Done."
