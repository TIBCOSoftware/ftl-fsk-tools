#!/bin/bash
# Stop the KOF cluster started by start-kof-cluster.sh

LOG_DIR="${LOG_DIR:-/tmp/kof-cluster-logs}"
PID_FILE="$LOG_DIR/kof-cluster.pids"

echo "=== Stopping KOF cluster ==="

if [ -f "$PID_FILE" ]; then
  while IFS=' ' read -r NAME PID; do
    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
      echo "Stopping $NAME (PID $PID)"
      kill "$PID" 2>/dev/null || true
    else
      echo "$NAME (PID $PID) already stopped"
    fi
  done < "$PID_FILE"
  rm -f "$PID_FILE"
else
  echo "No PID file at $PID_FILE -- trying fallback by process pattern"
  for NAME in SRV1 SRV2 SRV3 PSRV1 PSRV2 PSRV3; do
    PID=$(pgrep -f "tibftlserver.*-n ${NAME}@" 2>/dev/null || true)
    if [ -n "$PID" ]; then
      echo "Stopping $NAME (PID $PID)"
      kill "$PID" 2>/dev/null || true
    fi
  done
fi

sleep 2
echo "KOF cluster stopped."
