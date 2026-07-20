# Example 20 — Fetch config from live SASL-secured brokers

This example shows how to convert a SASL-secured Kafka cluster to KOF artifacts
by fetching broker configuration directly from live brokers via the Admin API.

## Prerequisites

- A running SASL/PLAIN Kafka cluster accessible from this host.
- `tibkafkatokof` built: `go build -o tibkafkatokof .`

## Command

```bash
tibkafkatokof \
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
  `kof-cluster-secure.yaml` alongside the base `kof-cluster.yaml`.

## SASL Admin API authentication

> **Note:** If your Kafka cluster requires SASL authentication for Admin API access,
> you must configure SASL credentials before calling `tibkafkatokof`. The tool uses
> the sarama library's default plaintext connection. For secured Admin API access,
> set the following environment variables before running the tool, or pre-configure
> your JAAS credentials:
>
> ```bash
> export KAFKA_SASL_USERNAME=admin
> export KAFKA_SASL_PASSWORD=admin-secret
> ```
>
> If your cluster requires TLS for the Admin connection, pass `--from-brokers` with
> the TLS listener port and ensure the broker's CA is trusted by your system.

## Output

```
output/kof-cluster.yaml         (base config, realm: {} per server)
output/kof-cluster-secure.yaml  (FTL server TLS/auth settings; generated when --tls-cert is given)
output/realm.json
output/kof.broker.1.properties
output/kof.broker.2.properties
output/kof.broker.3.properties
output/unsupported.properties
```
