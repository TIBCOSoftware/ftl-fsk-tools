---
id: getting-started
title: Getting Started with tibftlimportconfig
sidebar_label: Getting Started
---

# Getting Started with `tibftlimportconfig`

`tibftlimportconfig` converts Kafka broker `server.properties` files into the FTL artifacts
needed to run a FSK-enabled pserver cluster. Pass one properties file per broker; the
tool generates:

| File | Purpose |
|---|---|
| `tibftlserver-cluster.yaml` | FTL pserver cluster configuration |
| `realm.json` | FTL realm with `kof.cluster` definition |
| `kof.broker.N.properties` | Per-pserver broker properties (1-based) |
| `unsupported.properties` | Settings with no FSK equivalent (reference only) |
| `tibftlserver-cluster-secure.yaml` | TLS/auth overlay (when security flags are provided) |

A single-broker conversion produces one pserver — a standalone server rather than a cluster — so
its YAMLs are named `tibftlserver_standalone.yaml` and `tibftlserver_standalone-secure.yaml`.

:::tip No properties file handy?
If the Kafka cluster is already running, `--from-brokers host:port,...` reads each broker's
effective configuration over the Kafka Admin API instead of from a file, then follows exactly the
same translation path. It is mutually exclusive with the positional arguments, and the Admin
connection itself is plaintext and unauthenticated — see
[Fetching from live brokers](./README.md#fetching-from-live-brokers).
:::

Each scenario below builds on the previous one. Start with the simplest setup and
advance as your environment requires.

---

## Step 1 — Single-node plaintext (development)

The simplest possible configuration: one broker, no authentication, no TLS. Use this
for local development and tool exploration only.

### Kafka server.properties

```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093

listeners=CLIENT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=CLIENT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT

log.dirs=/var/kafka/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

:::tip Listener naming
Name your client-facing listener `CLIENT` (not `PLAINTEXT`). The tool strips the
`CONTROLLER` listener automatically — FTL carries controller traffic natively.
:::

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  server-1.properties
```

### Output

```
Writing file: ./kof-output/kof.broker.1.properties [
Writing file: ./kof-output/unsupported.properties
Writing file: ./kof-output/tibftlserver_standalone.yaml
Writing file: ./kof-output/realm.json

All kof.broker.*.properties files are processed successfully.
```

The `unsupported.properties` file lists any settings that have no FSK equivalent
(such as the `CONTROLLER` listener entry). Review it for reference — those settings
do not affect FSK behavior.

**→ Continue to [Step 2](#step-2--single-node-sasl-plain-over-tls) to add authentication,
or jump to [Step 4](#step-4--3-node-plaintext-cluster) to scale to a 3-node cluster.**

---

## Step 2 — Single-node SASL/PLAIN over TLS

Adds username/password authentication and TLS encryption. This is the most common
starting point for non-production secured environments.

Kafka uses JKS/PKCS12 keystores; FSK reads PEM. The tool flags any keystore it finds
as `RESOLVE-REQUIRED` and provides the exact conversion commands.

### Kafka server.properties

```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093

listeners=BROKER://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=BROKER://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:SSL,BROKER:SASL_SSL

# TLS — JKS keystores (the tool will provide conversion commands)
ssl.keystore.type=JKS
ssl.keystore.location=/etc/kafka/certs/server.keystore.jks
ssl.keystore.password=keystorePassword123
ssl.key.password=keyPassword123
ssl.truststore.type=JKS
ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
ssl.truststore.password=truststorePassword123

# SASL/PLAIN on the BROKER listener
listener.name.broker.sasl.enabled.mechanisms=PLAIN
listener.name.broker.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" \
  user_admin="admin-secret" user_producer="producer-secret" user_consumer="consumer-secret";

log.dirs=/var/kafka/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

### Convert keystores to PEM

The tool emits `RESOLVE-REQUIRED` blocks with the exact commands. For JKS:

```bash
# Convert keystore
keytool -importkeystore \
  -srckeystore /etc/kafka/certs/server.keystore.jks -srcstoretype JKS \
  -destkeystore /etc/kafka/certs/server.keystore.p12 -deststoretype PKCS12
openssl pkcs12 -in /etc/kafka/certs/server.keystore.p12 -nodes \
  -out /etc/kafka/certs/server.keystore.pem

# Convert truststore
keytool -importkeystore \
  -srckeystore /etc/kafka/certs/kafka.truststore.jks -srcstoretype JKS \
  -destkeystore /etc/kafka/certs/kafka.truststore.p12 -deststoretype PKCS12
openssl pkcs12 -in /etc/kafka/certs/kafka.truststore.p12 -nodes -nokeys \
  -out /etc/kafka/certs/kafka.truststore.pem
```

Alternatively, run with `--auto` and the tool performs the conversion for you
(requires `keytool` and `openssl` on `PATH`).

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --tls-cert /etc/kafka/certs/server.keystore.pem \
  --tls-key  /etc/kafka/certs/server.keystore.pem \
  --auth-users-file /etc/ftl/users.txt \
  server-1.properties
```

The `--auth-users-file` points to an FTL users file that maps usernames extracted
from the inline JAAS config. The tool writes a `tibftlserver-cluster-secure.yaml` alongside
the main cluster YAML when TLS or auth flags are supplied.

**→ Continue to [Step 3](#step-3--single-node-oauth2) to replace SASL/PLAIN with OAuth2,
or jump to [Step 5](#step-5--3-node-sasl-plain-cluster) for a 3-node SASL cluster.**

---

## Step 3 — Single-node OAuth2

Replaces username/password authentication with OAuth 2.0 bearer tokens. Kafka clients
present a JWT; the tool wires the OAUTHBEARER mechanism through to FSK's OAuth2
provider.

### Kafka server.properties

```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093

listeners=OAUTH://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=OAUTH://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:SSL,OAUTH:SASL_SSL

ssl.keystore.type=JKS
ssl.keystore.location=/etc/kafka/certs/server.keystore.jks
ssl.keystore.password=keystorePassword123
ssl.key.password=keyPassword123
ssl.truststore.type=JKS
ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
ssl.truststore.password=truststorePassword123

listener.name.oauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.oauth.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
listener.name.oauth.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler

log.dirs=/var/kafka/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --tls-cert /etc/kafka/certs/server.pem \
  --tls-key  /etc/kafka/certs/server.key \
  --oauth-token-url    https://auth.example.com/oauth/token \
  --oauth-jwks-url     https://auth.example.com/.well-known/jwks.json \
  --oauth-client-id    kof-server \
  --oauth-client-secret <secret> \
  server-1.properties
```

The tool detects the `OAUTHBEARER` mechanism and maps the custom callback handler
class to the `oauth` backend automatically. If the handler class is unrecognized, a
`RESOLVE-REQUIRED` block in `kof.broker.1.properties` asks you to choose a backend
(`oauth`, `file`, or `inline`).

**→ Continue to [Step 6](#step-6--3-node-mutual-tls-mtls) for mTLS, or
[Step 7](#step-7--3-node-sasl-plain--oauth2-dual-listener) for a dual SASL+OAuth2 setup.**

---

## Step 4 — 3-node plaintext cluster

Scale out to a 3-broker KRaft cluster. Pass one `server.properties` file per broker;
the tool derives the pserver count from the file count.

### Kafka server.properties (broker 1 of 3)

```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093,localhost:9103,localhost:9113

listeners=CLIENT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=CLIENT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT

log.dirs=/var/kafka/data/broker-1
num.partitions=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

Brokers 2 and 3 use `node.id=2`/`3`, unique ports (`9102`/`9092`, `9112`/`9092`), and
their own `log.dirs`.

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

### Output

```
Writing file: ./kof-output/kof.broker.1.properties [
Writing file: ./kof-output/kof.broker.2.properties [
Writing file: ./kof-output/kof.broker.3.properties [
Writing file: ./kof-output/unsupported.properties
Writing file: ./kof-output/tibftlserver-cluster.yaml
Writing file: ./kof-output/realm.json

All kof.broker.*.properties files are processed successfully.
```

The generated `tibftlserver-cluster.yaml` contains three pserver entries (`SRV1`, `SRV2`,
`SRV3`) with randomly assigned FTL ports in the 5600–5799 range. To pin specific
ports use `--core-servers SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602`.

**→ Continue to [Step 5](#step-5--3-node-sasl-plain-cluster) to secure the cluster.**

---

## Step 5 — 3-node SASL/PLAIN cluster

Add SASL/PLAIN + TLS to a 3-node cluster. Each broker's properties file carries the
same listener and security configuration; per-broker differences are in `node.id`,
ports, and `log.dirs` only.

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --tls-cert /etc/kafka/certs/server.pem \
  --tls-key  /etc/kafka/certs/server.key \
  --auth-users-file /etc/ftl/users.txt \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

The tool reads the inline JAAS `user_X` entries from each broker's properties and
writes them to `ftl-users.txt` (FTL server-to-server auth) and `kafka-users.txt`
(Kafka client principals), both in `--output-dir`.

**→ Continue to [Step 6](#step-6--3-node-mutual-tls-mtls) to add client certificate
authentication.**

---

## Step 6 — 3-node mutual TLS (mTLS)

Client certificates replace username/password. The Kafka `SSL` listener with
`ssl.client.auth=required` maps to FSK's `tls-only` auth mode with
`tls.server.trust.file`.

### Kafka server.properties highlights

```properties
listeners=MTLS://localhost:9094,CONTROLLER://localhost:9093
listener.security.protocol.map=CONTROLLER:SSL,MTLS:SSL

# Require client certificates on the MTLS listener
listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123
```

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --tls-cert         /etc/kafka/certs/server.pem \
  --tls-key          /etc/kafka/certs/server.key \
  --tls-server-trust /etc/kafka/certs/ca.pem \
  --tls-client-cert  /etc/kafka/certs/client.pem \
  --tls-client-key   /etc/kafka/certs/client.key \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

`--tls-server-trust` sets `tls.server.trust.file` (the CA that signs client
certificates). `--tls-client-cert` and `--tls-client-key` are used for
server-to-server connections.

**→ Continue to [Step 7](#step-7--3-node-sasl-plain--oauth2-dual-listener) for a
dual-protocol setup, or [Step 8](#step-8--3-node-sasl-plain--mtls) to combine SASL
and mTLS on separate listeners.**

---

## Step 7 — 3-node SASL/PLAIN + OAuth2 (dual listener)

Two client-facing listeners on the same cluster: legacy clients use SASL/PLAIN; modern
clients use OAUTHBEARER. FSK runs both auth providers concurrently.

### Kafka server.properties highlights

```properties
listeners=SASL_AUTH://localhost:9092,OAUTH://localhost:9095,CONTROLLER://localhost:9093
listener.security.protocol.map=CONTROLLER:SSL,SASL_AUTH:SASL_SSL,OAUTH:SASL_SSL

# SASL/PLAIN on SASL_AUTH listener
listener.name.sasl_auth.sasl.enabled.mechanisms=PLAIN
listener.name.sasl_auth.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" user_admin="admin-secret";

# OAUTHBEARER on OAUTH listener
listener.name.oauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.oauth.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler
```

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --tls-cert           /etc/kafka/certs/server.pem \
  --tls-key            /etc/kafka/certs/server.key \
  --auth-users-file    /etc/ftl/users.txt \
  --oauth-token-url    https://auth.example.com/oauth/token \
  --oauth-jwks-url     https://auth.example.com/.well-known/jwks.json \
  --oauth-client-id    kof-server \
  --oauth-client-secret <secret> \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

The secure YAML sets `auth.providers: file:/etc/ftl/users.txt,oauth2` so both
authentication paths are active simultaneously.

**→ Continue to [Step 8](#step-8--3-node-sasl-plain--mtls) to combine SASL and mTLS.**

---

## Step 8 — 3-node SASL/PLAIN + mTLS

Two listeners on separate ports: one for SASL/PLAIN clients, one for mTLS clients.
Both share the same server certificate; `ssl.client.auth=required` applies only to the
mTLS listener.

### Kafka server.properties highlights

```properties
listeners=SASL_AUTH://localhost:9092,MTLS://localhost:9094,CONTROLLER://localhost:9093
listener.security.protocol.map=CONTROLLER:SSL,SASL_AUTH:SASL_SSL,MTLS:SSL

listener.name.sasl_auth.sasl.enabled.mechanisms=PLAIN
listener.name.sasl_auth.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" user_admin="admin-secret";

listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123
```

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --tls-cert         /etc/kafka/certs/server.pem \
  --tls-key          /etc/kafka/certs/server.key \
  --tls-server-trust /etc/kafka/certs/ca.pem \
  --tls-client-cert  /etc/kafka/certs/client.pem \
  --tls-client-key   /etc/kafka/certs/client.key \
  --auth-users-file  /etc/ftl/users.txt \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

---

## Step 9 — 3-node mTLS + OAuth2

The most secure multi-protocol configuration: mTLS for certificate-bearing clients,
OAUTHBEARER for token-bearing clients.

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --tls-cert           /etc/kafka/certs/server.pem \
  --tls-key            /etc/kafka/certs/server.key \
  --tls-server-trust   /etc/kafka/certs/ca.pem \
  --tls-client-cert    /etc/kafka/certs/client.pem \
  --tls-client-key     /etc/kafka/certs/client.key \
  --oauth-token-url    https://auth.example.com/oauth/token \
  --oauth-jwks-url     https://auth.example.com/.well-known/jwks.json \
  --oauth-client-id    kof-server \
  --oauth-client-secret <secret> \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

---

## Step 10 — 3-node SASL/PLAIN + mTLS + OAuth2

All three auth providers active simultaneously. Each client-facing listener uses a
different mechanism; FSK's `auth.providers` list in the secure YAML activates all of
them.

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --tls-cert           /etc/kafka/certs/server.pem \
  --tls-key            /etc/kafka/certs/server.key \
  --tls-server-trust   /etc/kafka/certs/ca.pem \
  --tls-client-cert    /etc/kafka/certs/client.pem \
  --tls-client-key     /etc/kafka/certs/client.key \
  --auth-users-file    /etc/ftl/users.txt \
  --oauth-token-url    https://auth.example.com/oauth/token \
  --oauth-jwks-url     https://auth.example.com/.well-known/jwks.json \
  --oauth-client-id    kof-server \
  --oauth-client-secret <secret> \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

---

## Common options reference

| Flag | Default | Purpose |
|---|---|---|
| `--output-dir` | `./kof-output` | Directory for all generated files |
| `--realm-name` | `_default_realm` | Realm name in `realm.json` |
| `--data-dir` | `/var/tmp/kof/data` | FSK data directory on pserver hosts |
| `--core-servers` | _(auto)_ | Pin pserver names and ports: `SRV1=host:5600,...` |
| `--from-brokers` | _(none)_ | Fetch the config from running brokers instead of files: `host:port,...` |
| `--transport-type` | `auto` | FTL transport: `auto` (realm server resolves each connection — dynamic TCP within a cluster, static TCP between clusters and to DR) or `dtcp` (dynamic TCP everywhere) |
| `--auto` | off | Convert JKS/PKCS12 keystores to PEM automatically |
| `--migration-config` | off | Also write `kafka-to-kof.properties` for the migration tool |
| `--tibschemad` | off | Add the FTL schema daemon to the generated cluster YAML |
| `--list-properties` | off | Print how each Kafka property is handled, then exit |

### Finding the rest

`tibftlimportconfig -h` prints a short overview — the flags above plus an index of groups. The remaining
flags are organized into groups you can ask for one at a time, so you never have to read the whole
list:

```sh
tibftlimportconfig -h            # overview and group index
tibftlimportconfig -h oauth      # just the OAuth2 flags
tibftlimportconfig -h all        # every flag, grouped
```

| Group | Covers |
|---|---|
| `core` | output location, realm name, data dir, server addresses, transport |
| `brokers` | read the config from running Kafka brokers instead of properties files |
| `tls` | server and client certificates, private keys, trust files |
| `oauth` | token/JWKS endpoints, claims, audience, server and UI client credentials |
| `auth` | users file, role map, and the FTL service credentials |
| `dr` | DR server list and DR data directory |
| `info` | property listing, colorization, automatic keystore conversion |

So the OAuth2 flags used in steps 3, 7, 9 and 10 are all under `tibftlimportconfig -h oauth`, and the
TLS/mTLS flags from steps 2, 6, 8, 9 and 10 are under `tibftlimportconfig -h tls`.

---

## Resolving INVALID output

When the tool cannot fully convert a setting it writes a `RESOLVE-REQUIRED` block in
the generated `kof.broker.N.properties` and exits with code 2. Common causes:

- **JKS/PKCS12 keystore** — convert to PEM with the commands in the block, or re-run
  with `--auto`.
- **Unrecognized SASL handler class** — set the `=<oauth|file|inline>` value in the
  block and re-run.
- **Unsupported SASL mechanism** (e.g. GSSAPI, SCRAM) — switch the listener to `PLAIN`
  or `OAUTHBEARER` in the block.

After editing, re-run the same `tibftlimportconfig` command. The tool recomputes status on
every run; once all blocks are resolved the exit code is 0 and output shows:

```
All kof.broker.*.properties files are processed successfully.
```

Settings with no FSK equivalent (Kerberos families, delegation tokens, per-IP
connection limits) are written to `unsupported.properties` for reference and do not
cause an INVALID status.
