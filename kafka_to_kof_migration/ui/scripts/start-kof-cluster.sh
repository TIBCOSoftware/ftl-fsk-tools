#!/bin/bash
# Start a 6-process KOF cluster: 3 core FTL servers + 3 KOF persistence servers.
#
# Transport mode: AUTO (mux-fronted).
#   kof_listener == PSRV's own FTL public URL, so the mux routes Kafka traffic
#   through the existing FTL socket -- no separate kof_listener port needed.
#
# Port layout:
#   SRV1  (core) : localhost:5633
#   SRV2  (core) : localhost:5643
#   SRV3  (core) : localhost:5653
#   PSRV1 (kof)  : localhost:5663   <-- also Kafka bootstrap
#   PSRV2 (kof)  : localhost:5673
#   PSRV3 (kof)  : localhost:5683
#   KOF Kafka bootstrap : localhost:5663,localhost:5673,localhost:5683
#
# Prerequisites:
#   export FTL_HOME=/path/to/ftl/install

set -e

export TIBFTL_LICENSE="${TIBFTL_LICENSE:-file:///path/to/ftl_any_host_license.bin}"

if [ -n "${FTL_HOME:-}" ] && [ -x "$FTL_HOME/bin/tibftlserver" ]; then
  TIBFTLSERVER="$FTL_HOME/bin/tibftlserver"
  TIBFTLADMIN="$FTL_HOME/bin/tibftladmin"
else
  BUILD_BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../../../../hydra/bin/tibftlserver"
  if [ -x "$BUILD_BIN" ]; then
    TIBFTLSERVER="$BUILD_BIN"
    TIBFTLADMIN="$(dirname "$BUILD_BIN")/tibftladmin"
  else
    echo "ERROR: tibftlserver not found. Set FTL_HOME or build the project."
    exit 1
  fi
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF_DIR="$SCRIPT_DIR/../conf/kof"
LOG_DIR="${LOG_DIR:-/tmp/kof-cluster-logs}"
DATA_DIR="${DATA_DIR:-/tmp/kof-cluster-data}"
PID_FILE="$LOG_DIR/kof-cluster.pids"

mkdir -p "$LOG_DIR" "$DATA_DIR/Common" "$CONF_DIR"

YAML="$CONF_DIR/kof-cluster.yaml"
REALM_JSON="$CONF_DIR/realm-init.json"

cat > "$YAML" << EOF
globals:
  core.servers:
    SRV1: localhost:5633
    SRV2: localhost:5643
    SRV3: localhost:5653

servers:
  SRV1:
  - realm: {}

  SRV2:
  - realm: {}

  SRV3:
  - realm: {}

  PSRV1:
  - ftl:
      server: localhost:5663
  - persistence:
      name: pserver1
      data: $DATA_DIR/Common
      loglevel: connections:debug;producers:debug;consumers:debug;kof:debug;durables:debug;store:debug

  PSRV2:
  - ftl:
      server: localhost:5673
  - persistence:
      name: pserver2
      data: $DATA_DIR/Common
      loglevel: connections:debug;producers:debug;consumers:debug;kof:debug;durables:debug;store:debug

  PSRV3:
  - ftl:
      server: localhost:5683
  - persistence:
      name: pserver3
      data: $DATA_DIR/Common
      loglevel: connections:debug;producers:debug;consumers:debug;kof:debug;durables:debug;store:debug

services:
  persistence:
    data: $DATA_DIR
  realm:
    data: $DATA_DIR
EOF

> "$PID_FILE"

start_server() {
  local NAME="$1"
  local PORT="$2"
  local LOG="$LOG_DIR/$NAME.log"
  echo "=== Starting $NAME (localhost:$PORT) ==="
  "$TIBFTLSERVER" -n "${NAME}@localhost:${PORT}" -c "$YAML" > "$LOG" 2>&1 &
  local PID=$!
  echo "$NAME $PID" >> "$PID_FILE"
  echo "  PID: $PID  log: $LOG"
}

# Step 1: Start core servers
start_server SRV1 5633
sleep 3
start_server SRV2 5643
sleep 3
start_server SRV3 5653

sleep 30

# Step 2: Wait for core cluster to converge, then push realm config
# This registers pserver1/2/3 so PSRVs can find their slot in the cluster
echo "=== Waiting for core cluster to converge (retry loop)... ==="

if [ ! -f "$REALM_JSON" ]; then
  echo "ERROR: realm-init.json not found at $REALM_JSON"
  exit 1
fi

echo "=== Pushing realm config (registering pserver1/2/3) ==="
PUSH_OK=0
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  sleep 3
  OUT=$("$TIBFTLADMIN" --ftlserver http://localhost:5633 --updaterealm "$REALM_JSON" 2>&1)
  echo "$OUT"
  if echo "$OUT" | grep -q "Started deployment"; then
    echo "  Realm config pushed on attempt $attempt."
    PUSH_OK=1
    break
  fi
  echo "  Attempt $attempt: SRV1 not ready, retrying..."
done

if [ "$PUSH_OK" -eq 0 ]; then
  echo "ERROR: Failed to push realm config after 10 attempts."
  exit 1
fi
sleep 2

# Step 3: Start KOF persistence servers
start_server PSRV1 5663
start_server PSRV2 5673
start_server PSRV3 5683
sleep 3

echo ""
echo "=== KOF cluster started ==="
echo "  Core servers        : localhost:5633, localhost:5643, localhost:5653"
echo "  KOF servers         : localhost:5663, localhost:5673, localhost:5683"
echo "  KOF Kafka bootstrap : localhost:5663,localhost:5673,localhost:5683"
echo "  Logs  : $LOG_DIR"
echo "  PIDs  : $PID_FILE"
echo ""
echo "Update the UI form:"
echo "  Source Bootstrap      : localhost:9092,localhost:9094,localhost:9096"
echo "  Target KOF Bootstrap  : localhost:5663,localhost:5673,localhost:5683"
