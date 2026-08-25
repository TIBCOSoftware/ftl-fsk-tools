# Example 20 — Fetch config from live SASL-secured brokers

This example shows how to convert a SASL-secured Kafka cluster to FSK artifacts
by fetching broker configuration directly from live brokers via the Admin API.

## Prerequisites

- A running SASL/PLAIN Kafka cluster accessible from this host.
- `tibftlimportconfig` built: `go build -o tibftlimportconfig .`

## Command

```bash
tibftlimportconfig \
  --from-brokers kafka-sasl-1:9092,kafka-sasl-2:9092,kafka-sasl-3:9092 \
  --core-servers "SRV1=kafka-kof-1:5600,SRV2=kafka-kof-2:5601,SRV3=kafka-kof-3:5602" \
  --tls-cert /etc/ftl/certs/server.pem \
  --auth-users-file /etc/ftl/users.txt \
  --output-dir output
```

## How it differs from the plaintext example

- The SASL-secured brokers return security-related properties from `DescribeConfigs`
  (e.g. `sasl.enabled.mechanisms`, `ssl.keystore.*`).
- These are processed through the same security translation logic as file-based inputs:
  JKS/PKCS12 keystores are flagged as RESOLVE-REQUIRED; PLAIN/OAUTHBEARER mechanisms
  are accepted; custom callback handlers are flagged for manual resolution.
- Use `--tls-cert` and/or `--auth-users-file` (or `--oauth-token-url`) to generate
  `tibftlserver-cluster-secure.yaml` alongside the base `tibftlserver-cluster.yaml`.

## SASL Admin API authentication

> **Note:** the Admin connection `--from-brokers` opens is **plaintext and
> unauthenticated** — the tool uses the sarama library's defaults and has no flags for
> SASL or TLS on the fetch itself. The `--tls-*` and `--oauth-*` flags above configure
> the *generated* FTL servers, not this connection.
>
> So `--from-brokers` must name a listener that accepts an unauthenticated connection.
> On a cluster where every listener requires SASL or TLS, the fetch fails; use the
> file-based input (`tibftlimportconfig [flags] server-1.properties ...`) there instead.

## Output

```
output/tibftlserver-cluster.yaml         (base config; one realm block per server)
output/tibftlserver-cluster-secure.yaml  (FTL server TLS/auth settings; generated when --tls-cert is given)
output/realm.json
output/kof.broker.1.properties
output/kof.broker.2.properties
output/kof.broker.3.properties
output/unsupported.properties
```
