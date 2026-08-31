#!/usr/bin/env bash
#
# Copyright (c) 2026 Cloud Software Group, Inc.
# All Rights Reserved.
#
# Regenerate all tibftlimportconfig example outputs using relative paths.
# (Examples 21 and 22 need live brokers and are documented, not regenerated.)
set -euo pipefail

# Resolve everything relative to this script so this copy regenerates its own
# examples, not another checkout's.
EXAMPLES="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOL_DIR="$(dirname "$EXAMPLES")"

# Build the tool by default; set TOOL=/path/to/tibftlimportconfig to use an existing binary.
if [[ -z "${TOOL:-}" ]]; then
  TOOL="$(mktemp -d)/tibftlimportconfig"
  (cd "$TOOL_DIR" && "${GO:-go}" build -o "$TOOL" .)
fi
if [[ ! -x "$TOOL" ]]; then
  echo "error: TOOL=$TOOL is not an executable binary" >&2
  exit 1
fi

# run_to <example-dir> <output-subdir> <tool flags and input files...>
run_to() {
  local ex="$1"; shift
  local out="$1"; shift
  echo "==> $ex ($out)"
  cd "$EXAMPLES/$ex"
  rm -rf "$out"
  mkdir -p "$out"
  "$TOOL" --output-dir "$out" "$@" 2>&1 || true  # exit 2 is INVALID (ok for regen)
  echo ""
}

run() {
  local ex="$1"; shift
  run_to "$ex" output "$@"
}

# Shared OAuth2 flags used by many examples
OAUTH_COMMON=(
  --oauth-token-url https://auth.example.com/oauth/token
  --oauth-jwks-url file:/etc/ftl/oauth.json
  --oauth-ui-auth-url https://auth.example.com/oauth/authorize
  --oauth-ui-token-url https://auth.example.com/oauth/token
  --oauth-ui-logout-url https://auth.example.com/oauth/logout
  --oauth-ui-client-id ftl-ui
  --oauth-ui-client-secret "env:OAUTH_UI_CLIENT_SECRET"
)
OAUTH_SVR=(
  --oauth-client-id ftl-server
  --oauth-client-secret "env:OAUTH_CLIENT_SECRET"
  --oauth-provider-trust /etc/ftl/certs/oauth-provider.pem
)
MTLS_FLAGS=(
  --tls-server-trust /etc/ftl/certs/client-ca.pem
  --tls-client-cert /etc/ftl/certs/client.pem
  --tls-client-key /etc/ftl/certs/client.key
)

# ── 01: single-node, KRaft, plaintext ─────────────────────────────────────
run 01-single-node-plaintext \
  --core-servers "SRV1=localhost:5663" \
  server-1.properties

# ── 02: 3-broker, KRaft, plaintext ────────────────────────────────────────
run 02-3broker-plaintext \
  --core-servers "SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602" \
  server-1.properties server-2.properties server-3.properties

# ── 02-dr: same inputs and core servers as 02, plus DR ───────────────────
# Written to output-dr/ so `diff output output-dr` shows exactly what --dr-servers adds.
run_to 02-3broker-plaintext output-dr \
  --core-servers "SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602" \
  --dr-servers "DRSRV1=dr-host-1:5800,DRSRV2=dr-host-2:5801,DRSRV3=dr-host-3:5802" \
  --dr-data-dir /var/kof/dr \
  server-1.properties server-2.properties server-3.properties

# ── 03: single-node, ZooKeeper, plaintext ─────────────────────────────────
# broker.id in, node.id out; the zookeeper.* keys land in unsupported.properties.
run 03-zk-single-node-plaintext \
  --core-servers "SRV1=localhost:5664" \
  server-1.properties

# ── 04: 3-broker, ZooKeeper, plaintext ────────────────────────────────────
run 04-zk-3broker-plaintext \
  --core-servers "SRV1=localhost:5610,SRV2=localhost:5611,SRV3=localhost:5612" \
  server-1.properties server-2.properties server-3.properties

# ── 05: single-node, SASL (file-auth+tls) ─────────────────────────────────
run 05-single-node-sasl \
  --core-servers "SRV1=localhost:5689" \
  --tls-cert /etc/ftl/certs/server.pem \
  --auth-users-file /etc/ftl/users.txt \
  server-1.properties

# ── 06: single-node, OAuth2 ───────────────────────────────────────────────
run 06-single-node-oauth \
  --core-servers "SRV1=localhost:5663" \
  "${OAUTH_COMMON[@]}" \
  server-1.properties

# ── 07: 3-broker, SASL (file-auth+tls) ───────────────────────────────────
run 07-3broker-sasl \
  --core-servers "SRV1=localhost:5695,SRV2=localhost:5641,SRV3=localhost:5693" \
  --tls-cert /etc/ftl/certs/server.pem \
  server-1.properties server-2.properties server-3.properties

# ── 08: 3-broker, TLS only ────────────────────────────────────────────────
run 08-3broker-tls-only \
  --core-servers "SRV1=localhost:5680,SRV2=localhost:5626,SRV3=localhost:5616" \
  --tls-cert /etc/ftl/certs/server.pem \
  server-1.properties server-2.properties server-3.properties

# ── 09: 3-broker, multi-SASL (PLAIN+OAUTHBEARER) ──────────────────────────
run 09-3broker-multi-sasl \
  --core-servers "SRV1=localhost:5686,SRV2=localhost:5696,SRV3=localhost:5622" \
  "${OAUTH_COMMON[@]}" \
  server-1.properties server-2.properties server-3.properties

# ── 10: 3-broker, multi-listener ──────────────────────────────────────────
run 10-3broker-multi-listener \
  --core-servers "SRV1=localhost:5695,SRV2=localhost:5654,SRV3=localhost:5616" \
  "${OAUTH_COMMON[@]}" \
  server-1.properties server-2.properties server-3.properties

# ── 11: 9-broker scale-out (3 shards) ─────────────────────────────────────
# The inputs declare SASL_SSL + mTLS listeners, but no FTL security flags are
# passed here on purpose: the output then isolates the sharding behaviour.
# Example 12 is the same nine inputs with the security flags supplied.
run 11-9broker-scale \
  --core-servers "SRV1=localhost:5619,SRV2=localhost:5698,SRV3=localhost:5635" \
  server-1.properties server-2.properties server-3.properties \
  server-4.properties server-5.properties server-6.properties \
  server-7.properties server-8.properties server-9.properties

# ── 12: 9-broker, secure (mTLS+OAuth2) ────────────────────────────────────
run 12-9broker-secure \
  --core-servers "SRV1=localhost:5601,SRV2=localhost:5602,SRV3=localhost:5603" \
  --tls-cert /etc/ftl/certs/server.pem \
  "${MTLS_FLAGS[@]}" \
  "${OAUTH_COMMON[@]}" \
  "${OAUTH_SVR[@]}" \
  server-1.properties server-2.properties server-3.properties \
  server-4.properties server-5.properties server-6.properties \
  server-7.properties server-8.properties server-9.properties

# ── 13: 3-broker, DR ──────────────────────────────────────────────────────
run 13-3broker-dr \
  --core-servers "primary1=primary-host-1:8585,primary2=primary-host-2:8686,primary3=primary-host-3:8787" \
  --dr-servers "drserver1=localhost:9585,drserver2=localhost:9686,drserver3=localhost:9787" \
  server-1.properties server-2.properties server-3.properties

# ── 14: 3-broker, SASL basic auth ─────────────────────────────────────────
run 14-3broker-sasl-basic \
  --core-servers "SRV1=kafka-sasl-1:5680,SRV2=kafka-sasl-2:5681,SRV3=kafka-sasl-3:5682" \
  --auth-users-file /etc/ftl/users.txt \
  server-1.properties server-2.properties server-3.properties

# ── 15: 3-broker, mTLS ────────────────────────────────────────────────────
run 15-3broker-mtls \
  --core-servers "SRV1=kafka-mtls-1:5683,SRV2=kafka-mtls-2:5684,SRV3=kafka-mtls-3:5685" \
  --tls-cert /etc/ftl/certs/server.pem \
  "${MTLS_FLAGS[@]}" \
  --tls-ca /etc/ftl/certs/ca.pem \
  server-1.properties server-2.properties server-3.properties

# ── 16: 3-broker, OAuth2 ──────────────────────────────────────────────────
run 16-3broker-oauth2 \
  --core-servers "SRV1=kafka-oauth-1:5686,SRV2=kafka-oauth-2:5687,SRV3=kafka-oauth-3:5688" \
  --tls-cert /etc/ftl/certs/server.pem \
  "${MTLS_FLAGS[@]}" \
  --tls-ca /etc/ftl/certs/ca.pem \
  "${OAUTH_COMMON[@]}" \
  "${OAUTH_SVR[@]}" \
  server-1.properties server-2.properties server-3.properties

# ── 17: 3-broker, SASL+mTLS ───────────────────────────────────────────────
run 17-3broker-sasl+mtls \
  --core-servers "SRV1=kafka-sasl-mtls-1:5692,SRV2=kafka-sasl-mtls-2:5693,SRV3=kafka-sasl-mtls-3:5694" \
  --tls-cert /etc/ftl/certs/server.pem \
  "${MTLS_FLAGS[@]}" \
  --tls-ca /etc/ftl/certs/ca.pem \
  --auth-users-file /etc/ftl/users.txt \
  server-1.properties server-2.properties server-3.properties

# ── 18: 3-broker, SASL+OAuth2 ─────────────────────────────────────────────
run 18-3broker-sasl+oauth2 \
  --core-servers "SRV1=kafka-sasl-oauth-1:5695,SRV2=kafka-sasl-oauth-2:5696,SRV3=kafka-sasl-oauth-3:5697" \
  --tls-cert /etc/ftl/certs/server.pem \
  --auth-users-file /etc/ftl/users.txt \
  "${OAUTH_COMMON[@]}" \
  server-1.properties server-2.properties server-3.properties

# ── 19: 3-broker, mTLS+OAuth2 ─────────────────────────────────────────────
run 19-3broker-mtls+oauth2 \
  --core-servers "SRV1=kafka-mtls-oauth-1:5701,SRV2=kafka-mtls-oauth-2:5702,SRV3=kafka-mtls-oauth-3:5703" \
  --tls-cert /etc/ftl/certs/server.pem \
  "${MTLS_FLAGS[@]}" \
  --tls-ca /etc/ftl/certs/ca.pem \
  "${OAUTH_COMMON[@]}" \
  "${OAUTH_SVR[@]}" \
  server-1.properties server-2.properties server-3.properties

# ── 20: 3-broker, SASL+mTLS+OAuth2 ───────────────────────────────────────
run 20-3broker-sasl+mtls+oauth2 \
  --core-servers "SRV1=kafka-s-m-o-1:5710,SRV2=kafka-s-m-o-2:5711,SRV3=kafka-s-m-o-3:5712" \
  --tls-cert /etc/ftl/certs/server.pem \
  "${MTLS_FLAGS[@]}" \
  --tls-ca /etc/ftl/certs/ca.pem \
  --auth-users-file /etc/ftl/users.txt \
  "${OAUTH_COMMON[@]}" \
  "${OAUTH_SVR[@]}" \
  server-1.properties server-2.properties server-3.properties

# ── 23: single-node + schema daemon ──────────────────────────────────────
# Same input as 01; --tibschemad is the only difference. cluster.size: 1.
run 23-single-node-tibschemad \
  --core-servers "SRV1=localhost:5663" \
  --tibschemad \
  server-1.properties

# ── 24: 3-broker + schema daemon ─────────────────────────────────────────
# Same input as 02; --tibschemad is the only difference. cluster.size: 3.
run 24-3broker-tibschemad \
  --core-servers "SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602" \
  --tibschemad \
  server-1.properties server-2.properties server-3.properties

echo "==> Done regenerating all examples."
