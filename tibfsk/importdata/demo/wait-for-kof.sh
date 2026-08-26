#!/usr/bin/env bash
# Block until the FSK persistence cluster has formed a quorum and every one of
# its pservers has joined, so a migration never starts against a half-started
# cluster. Use this instead of guessing with sleep.
#
# It polls the realm server's monitoring REST API:
#
#   GET /api/v1/persistence/clusters/<cluster>/quorum   -> QuorumStatus
#
# served by the realm server's monQuorum handler. The response is a QuorumStatus
# object; the fields used here are have_quorum, current_member_count and
# max_member_count.
#
# Usage:
#   bash demo/wait-for-kof.sh [--server localhost:5600]
#                             [--cluster kof.cluster.0]
#                             [--members N]      # default: max_member_count
#                             [--timeout 120]
#
# --server is the FTL SERVER port -- the core.servers port from
# tibftlserver-cluster.yaml -- not the FSK Kafka listener port.
#
# Exits 0 once the cluster is ready, 1 on timeout.

set -euo pipefail

SERVER="localhost:5600"
CLUSTER="kof.cluster.0"
MEMBERS=""
TIMEOUT=120
INTERVAL=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server)  SERVER="$2"; shift 2 ;;
    --cluster) CLUSTER="$2"; shift 2 ;;
    --members) MEMBERS="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if ! command -v curl >/dev/null 2>&1; then
  echo "ERROR: curl is required by this script."
  exit 1
fi

# Accept a bare host:port as well as a full URL.
case "$SERVER" in
  http://*|https://*) BASE="$SERVER" ;;
  *)                  BASE="http://$SERVER" ;;
esac
URL="$BASE/api/v1/persistence/clusters/$CLUSTER/quorum"

# Pull one numeric or boolean field out of the JSON body. Tolerates both the
# compact and the spaced form, and avoids a jq dependency.
field() {
  printf '%s' "$1" | grep -o "\"$2\"[[:space:]]*:[[:space:]]*[^,}]*" | head -1 \
    | sed -E "s/.*:[[:space:]]*//; s/[[:space:]]*$//; s/\"//g"
}

echo "==> Waiting for $CLUSTER to reach quorum (via $URL)"

ELAPSED=0
LAST=""
while true; do
  # Neither a connection failure nor a 404 is fatal: the realm server may not
  # have opened its port yet, and the cluster does not exist until the realm
  # has been seeded. Keep polling until the timeout, but report which it is.
  RESP="$(curl -s -m 5 -w $'\n%{http_code}' "$URL" 2>/dev/null || true)"
  CODE="${RESP##*$'\n'}"
  BODY="${RESP%$'\n'*}"

  if [[ "$CODE" == "200" ]]; then
    HAVE="$(field "$BODY" have_quorum)"
    CURRENT="$(field "$BODY" current_member_count)"
    MAX="$(field "$BODY" max_member_count)"
    LEADER="$(field "$BODY" leader)"
    WANT="${MEMBERS:-$MAX}"

    if [[ "$HAVE" == "true" && -n "$WANT" && "$CURRENT" == "$WANT" ]]; then
      echo "✓ $CLUSTER has quorum: $CURRENT/$WANT members, leader $LEADER"
      exit 0
    fi

    STATE="quorum=$HAVE members=$CURRENT/${WANT:-?}"
  elif [[ "$CODE" == "404" ]]; then
    STATE="cluster '$CLUSTER' not in the realm yet (404)"
  elif [[ -z "$CODE" || "$CODE" == "000" ]]; then
    STATE="realm server not answering yet"
  else
    STATE="realm server returned HTTP $CODE"
  fi

  if [[ "$STATE" != "$LAST" ]]; then
    echo "  $STATE"
    LAST="$STATE"
  fi

  sleep "$INTERVAL"
  ELAPSED=$((ELAPSED + INTERVAL))
  if [[ $ELAPSED -ge $TIMEOUT ]]; then
    echo ""
    echo "ERROR: $CLUSTER did not reach quorum within ${TIMEOUT}s (last: $STATE)."
    echo "Check the server logs, /tmp/kof-SRV*.log by default."
    echo "A stale /var/tmp/kof/data from a run with a different number of"
    echo "servers is the usual cause -- stop the servers, remove it, retry."
    exit 1
  fi
done
