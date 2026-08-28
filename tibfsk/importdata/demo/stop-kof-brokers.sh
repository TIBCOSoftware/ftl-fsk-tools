#!/usr/bin/env bash
#
# Copyright (c) 2026 Cloud Software Group, Inc.
# All Rights Reserved.
#
# Stop FSK pservers started by demo/start-kof-brokers.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/kof-brokers.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file found at $PID_FILE — pservers may not be running."
  exit 0
fi

echo "==> Stopping FSK pservers"
while IFS= read -r pid; do
  if kill "$pid" 2>/dev/null; then
    echo "  stopped PID $pid"
  else
    echo "  PID $pid not found (already stopped?)"
  fi
done < "$PID_FILE"

rm -f "$PID_FILE"
echo "Done."
