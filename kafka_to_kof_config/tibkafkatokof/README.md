# tibkafkatokof

Translates a Kafka KRaft broker `server.properties` file into the FTL KOF artifacts needed to run a KOF-enabled pserver cluster:

| Output file | Purpose |
|---|---|
| `kof-cluster.yaml` | FTL primary cluster config (realm servers + up to 3 pservers) |
| `kof-cluster-secure.yaml` | Secure variant with TLS/auth blocks (generated when TLS/OAuth flags are provided) |
| `kof-cluster-dr.yaml` | DR replica cluster config (generated when `-dr-servers` is provided) |
| `realm.json` | FTL realm config with `kof.cluster.N` (N is 0-based), stores, and pserver definitions |
| `kof.broker.N.properties` | Per-broker properties file (N is 1-based, one per pserver); only contains properties in the KoF whitelist |
| `unsupported.properties` | Properties from the input not in the KoF whitelist; written when any such properties exist |

---

## Build

Requires Go 1.25+ (`toolchain go1.25.6` is pinned in `go.mod`).

From this directory — the one holding `go.mod`:

```sh
go build .
```

Inside a Go workspace that lists this module, it can also be built by module path from the
workspace root:

```sh
go build tibco.com/ftl-support/tibkafkatokof
```

---

## Usage

```
tibkafkatokof [flags] <server.properties> [<server.properties> ...]
tibkafkatokof [flags] -from-brokers <host:port>[,<host:port> ...]
```

There are two ways to give the tool a broker configuration, and they are mutually exclusive:

- **From files.** Positional arguments are one or more `server.properties` files (1–9).
- **From live brokers.** `-from-brokers` takes a comma-separated `host:port` list (1–9) and reads
  each broker's configuration over the Kafka Admin API instead — see
  [Fetching from live brokers](#fetching-from-live-brokers).

Either way, each broker becomes one pserver — the pserver count is derived from the number of
brokers, not from a flag.

### Getting help

Help is organized in two tiers, so a bare `-h` stays short:

```sh
tibkafkatokof -h            # synopsis, output files, the handful of common flags, group index
tibkafkatokof -h oauth      # just the OAuth2 flags
tibkafkatokof -h all        # every flag, grouped
```

| Group | `-h <group>` covers |
|---|---|
| `core` | output location, realm name, data dir, server addresses, transport |
| `brokers` | read the config from running Kafka brokers instead of properties files |
| `tls` | server and client certificates, private keys, trust files |
| `oauth` | token/JWKS endpoints, claims, audience, server and UI client credentials |
| `auth` | users file, role map, and the FTL service credentials |
| `dr` | DR server list and DR data directory |
| `info` | property listing, colorization, automatic keystore conversion |
| `all` | every flag, grouped |

The sections below list the same flags as the corresponding `-h <group>` topic.

### Core flags (`-h core`)

| Flag | Default | Description |
|---|---|---|
| `-output-dir` | `./kof-output` | Directory where output files are written |
| `-realm-name` | `_default_realm` | Realm name in `realm.json` |
| `-data-dir` | `/var/tmp/kof/data` | KOF data directory path on pserver hosts |
| `-core-servers` | _(auto)_ | Comma-separated `NAME=host:port` list for `globals.core.servers`<br>e.g. `SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602`<br>If omitted, ports are randomly generated in range 5600–5699 |
| `-transport-type` | `auto` | Transport type for all pserver connections in `realm.json`: `auto` or `dtcp`<br>`auto` lets the OS choose; `dtcp` is optimized for low latency |
| `-ftl-loglevel` | `connections:info;kof:info;durables:info;store:info` | `loglevel` written into each generated pserver. This is the *output* FTL servers' logging, not this tool's. |
| `-migration-config` | `false` | Write `kafka-to-kof.properties` to the output directory (configuration for the `kafka_to_kof_migration` data migration tool) |

### Live broker fetch flags (`-h brokers`)

| Flag | Default | Description |
|---|---|---|
| `-from-brokers` | _(none)_ | Comma-separated `host:port` list of running Kafka brokers (1–9) to read the configuration from via the Admin API. Mutually exclusive with positional `server.properties` arguments. |
| `-from-brokers-timeout-ms` | `10000` | Admin API connect/read/write timeout in milliseconds |

See [Fetching from live brokers](#fetching-from-live-brokers) for what the tool asks each broker
for and the current limitations.

### TLS and mTLS flags (`-h tls`)

Used when the input config has any `tls`, `mtls`, `sasl_tls`, or `oauth_tls` listener and you want a `kof-cluster-secure.yaml` emitted. The `-tls-server-trust` / `-tls-client-*` flags are the ones required when a Kafka mTLS listener (`ssl.client.auth=required`) is present and you want FTL server-to-server mutual TLS.

| Flag | Description |
|---|---|
| `-tls-cert` | Server TLS certificate PEM file path |
| `-tls-key` | Server TLS private key PEM file path |
| `-tls-key-password` | TLS private key passphrase |
| `-tls-ca` | CA/trust PEM file path (for connecting to other FTL servers) |
| `-tls-server-trust` | CA PEM used to verify inbound client certificates (`tls.server.trust.file`) |
| `-tls-client-cert` | Client cert PEM presented to other FTL servers (`tls.client.cert`) |
| `-tls-client-key` | Client private key PEM for server-to-server connections (`tls.client.private.key`) |
| `-tls-client-key-password` | Passphrase for `-tls-client-key` |

### OAuth2 flags (`-h oauth`)

| Flag | Default | Description |
|---|---|---|
| `-oauth-token-url` | _(none)_ | OAuth2 token endpoint URL, server-to-server (`oauth2.svr.endpoint.token`) |
| `-oauth-jwks-url` | _(none)_ | OAuth2 JWKS or validation key, `file:` path or URL (`oauth2.validation.key`) |
| `-oauth-client-id` | _(none)_ | OAuth2 client ID for server-to-server (`oauth2.svr.client.id`) |
| `-oauth-client-secret` | _(none)_ | OAuth2 client secret for server-to-server (`oauth2.svr.client.secret`) |
| `-oauth-provider-trust` | _(none)_ | OAuth2 provider trust PEM file (`oauth2.provider.trust.file`) |
| `-oauth-audience` | `ftl` | OAuth2 audience value (`oauth2.audience`) |
| `-oauth-claim-roles` | `group` | Token claim mapped to FTL roles (`oauth2.claim.roles`) |
| `-oauth-claim-username` | `preferred_username` | Token claim mapped to the FTL user (`oauth2.claim.username`) |
| `-oauth-ui-auth-url` | _(none)_ | Auth endpoint for the FTL UI (`oauth2.ui.endpoint.auth`) |
| `-oauth-ui-token-url` | _(none)_ | Token endpoint for the FTL UI (`oauth2.ui.endpoint.token`) |
| `-oauth-ui-logout-url` | _(none)_ | Logout endpoint for the FTL UI (`oauth2.ui.endpoint.logout`) |
| `-oauth-ui-client-id` | _(none)_ | Client ID for the UI authorization code flow (`oauth2.ui.client.id`) |
| `-oauth-ui-client-secret` | _(none)_ | Client secret for the UI (`oauth2.ui.client.secret`) |

### Authentication flags (`-h auth`)

| Flag | Default | Description |
|---|---|---|
| `-auth-users-file` | _(none)_ | Path to FTL `users.txt` for file-based authentication (PLAIN SASL → file auth) |
| `-auth-rolemap` | _(none)_ | Path to an FTL role map file (`auth.rolemap` in `ftlserver.properties`, oauth2 mode) |
| `-server-user` | `internal` | `user` in `ftlserver.properties` for server-to-server connections (non-oauth2 modes) |
| `-server-password` | `internal-pw` | `password` in `ftlserver.properties` for server-to-server connections (non-oauth2 modes) |
| `-realm-service-user` | `primary` | Realm `user` credential for oauth2 mode, written into each per-server realm block |
| `-realm-service-password` | `primary-pw` | Realm `password` credential for oauth2 mode, written into each per-server realm block |

`kof-cluster-secure.yaml` is only emitted when **security is detected** in the input props AND at least one of `-tls-cert`, `-oauth-token-url`, or `-auth-users-file` is provided.

### DR (Disaster Recovery) flags (`-h dr`)

| Flag | Default | Description |
|---|---|---|
| `-dr-servers` | _(none)_ | Comma-separated `DRSRV1=host:port,DRSRV2=host:port,...` DR server list.<br>Providing this flag enables DR mode for all generated files. |
| `-dr-data-dir` | `<data-dir>/dr` | Data directory for DR pservers on DR hosts |

### Inspection and conversion flags (`-h info`)

| Flag | Default | Description |
|---|---|---|
| `-list-properties` | `false` | Print how each Kafka listener/security property is treated, then exit |
| `-color` | `auto` | Colorize `-list-properties` output: `auto`, `always`, or `never` |
| `-auto` | `false` | Run the mechanical conversions automatically (JKS/PKCS12 keystores → PEM via `keytool`/`openssl`); items needing a human stay `RESOLVE-REQUIRED` |

---

## Fetching from live brokers

Instead of collecting a `server.properties` from every broker host, point the tool at the running
cluster:

```sh
tibkafkatokof -output-dir ./kof-output \
  -from-brokers kafka-1:9092,kafka-2:9092,kafka-3:9092
```

For each address the tool connects with the Kafka Admin API, resolves that address to its broker
node ID from cluster metadata, and issues `DescribeConfigs` for that node. The returned entries —
the broker's *effective* configuration, including defaults the operator never wrote down — feed
into exactly the same translation pipeline as a parsed file, so the generated artifacts, the
`RESOLVE-REQUIRED` blocks and the exit codes are identical to the file-based path. Progress is
printed per broker:

```
fetching config from broker kafka-1:9092 ...
fetching config from broker kafka-2:9092 ...
fetching config from broker kafka-3:9092 ...
```

Notes and limitations:

- **Mutually exclusive with positional arguments.** Passing both is an error.
- **1–9 addresses**, same range as the file-based input; each address becomes one pserver.
- **The Admin connection is plaintext and unauthenticated.** The `-tls-*` and `-oauth-*` flags
  configure the *generated* FTL servers, not this fetch. Against a cluster whose listeners all
  require TLS or SASL, the connection will fail — use the file-based input there.
- **Effective config is larger than a hand-written file.** Broker defaults that a
  `server.properties` omits are returned by `DescribeConfigs`, so `unsupported.properties` is
  typically much longer than for the equivalent file input. Those entries are reference-only and
  do not affect the outcome.
- The `SourceFile` recorded in the generated comments is the `host:port` address, not a path.

---

## Multi-cluster split (9 input files → 3 shards)

The number of shards is determined by the number of `server.properties` files passed on the command line. Every 3 input files → 1 KOF cluster (shard). The primary `kof-cluster.yaml` always holds the first 3 pservers with FTL realm servers. Every additional group of up to 3 pservers goes into `kof-cluster-aux1.yaml`, `kof-cluster-aux2.yaml`, etc. Auxiliary files contain **no realm server entries** — pservers connect to the primary realm cluster via `globals.core.servers`.

```
9 input files → kof-cluster.yaml      (pserver1–3 + SRV1–3 realm servers)
                kof-cluster-aux1.yaml  (pserver4–6, no realm)
                kof-cluster-aux2.yaml  (pserver7–9, no realm)
                realm.json             (kof.cluster.0, kof.cluster.1, kof.cluster.2)
```

---

## DR mode (`-dr-servers`)

When `-dr-servers` is provided, DR mode is activated for all output files:

- **`kof-cluster.yaml`** gains `globals.dr:` (pointing to DR servers), `auto.init.primary.on.first.startup: true`, and `label: PRIMARY_SERVER` on each realm block.
- **`kof-cluster-dr.yaml`** is generated with DR servers as `core.servers`, a back-reference `globals.dr:` to the primary servers, and `label: DR_SERVER` on realm blocks. Pservers are named `drpserver1..N`.
- **`realm.json`** clusters get `dr_enabled: true`, a second pserver set `_DRset` with DR replicas, and transport roles swapped (`dr_transport` populated, `inter_cluster_transport` empty for all pservers).

With 9 input files, DR aux files are also produced:

```
9 files + -dr-servers ... →
    kof-cluster.yaml           (primary: pserver1–3)
    kof-cluster-aux1.yaml      (primary: pserver4–6)
    kof-cluster-aux2.yaml      (primary: pserver7–9)
    kof-cluster-dr.yaml        (DR: drpserver1–3)
    kof-cluster-dr-aux1.yaml   (DR: drpserver4–6)
    kof-cluster-dr-aux2.yaml   (DR: drpserver7–9)
    realm.json                 (kof.cluster.0/1/2 with dr_enabled: true)
```

---

## Examples

Each example starts from a `server.properties` file in the `examples/` directory. Run the tool from the `tibkafkatokof/` directory (where `examples/` is located) so that generated file paths are relative and portable.

---

### 01 — Single node, PLAINTEXT

**Kafka config:** 1 node, KRaft (broker+controller), PLAINTEXT, no security. Suitable for local development.

```sh
tibkafkatokof \
  --num-pservers 1 \
  --output-dir ./out/01 \
  examples/01-single-node-plaintext/server-1.properties
```

**Output:** `kof-cluster.yaml` (1 SRV + 1 pserver), `realm.json`, `kof.broker.1.properties`, `unsupported.properties`

---

### 02 — Single node, SASL_SSL PLAIN

**Kafka config:** 1 node, KRaft, SASL_SSL PLAIN on broker listener, SSL on controller.

```sh
tibkafkatokof \
  --num-pservers 1 \
  --output-dir ./out/02 \
  --tls-cert /etc/ftl/certs/server.pem \
  --tls-key /etc/ftl/certs/server.key \
  --auth-users-file /etc/ftl/users.txt \
  examples/02-single-node-sasl/server-1.properties
```

**Output:** `kof-cluster.yaml`, `kof-cluster-secure.yaml` (auth mode: file-auth+tls), `realm.json`, `kof.broker.1.properties`, `unsupported.properties`

---

### 03 — Single node, OAuth2

**Kafka config:** 1 node, KRaft, SASL_SSL OAUTHBEARER on broker listener, SSL on controller.

```sh
tibkafkatokof \
  --num-pservers 1 \
  --output-dir ./out/03 \
  --tls-cert /etc/ftl/certs/server.pem \
  --tls-key /etc/ftl/certs/server.key \
  --oauth-token-url https://auth.example.com/oauth/token \
  --oauth-jwks-url file:/etc/ftl/oauth.json \
  --oauth-client-id ftl-server \
  --oauth-client-secret env:OAUTH_CLIENT_SECRET \
  --oauth-provider-trust /etc/ftl/certs/oauth-provider.pem \
  examples/03-single-node-oauth/server-1.properties
```

**Output:** `kof-cluster.yaml`, `kof-cluster-secure.yaml` (auth mode: oauth2), `realm.json`, `kof.broker.1.properties`, `unsupported.properties`

---

### 04 — 3-broker, PLAINTEXT

**Kafka config:** 3 nodes, KRaft (broker+controller), PLAINTEXT listeners, no security.

```sh
tibkafkatokof \
  --output-dir ./out/04 \
  examples/04-3broker-plaintext/server-1.properties
```

**Output:** `kof-cluster.yaml`, `realm.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 05 — 3-broker, SASL_SSL PLAIN

**Kafka config:** 3 nodes, KRaft, SASL_SSL PLAIN on broker listener, SSL on controller listener.

```sh
tibkafkatokof \
  --output-dir ./out/05 \
  --realm-name prod_realm \
  --data-dir /data/kof \
  --tls-cert /etc/ftl/certs/server.pem \
  --tls-key /etc/ftl/certs/server.key \
  --tls-key-password changeit \
  --tls-ca /etc/ftl/certs/ca.pem \
  --auth-users-file /etc/ftl/users.txt \
  examples/05-3broker-sasl/server-1.properties
```

**Output:** `kof-cluster.yaml`, `kof-cluster-secure.yaml` (auth mode: file-auth+tls), `realm.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 06 — 3-broker, TLS-only (no SASL)

**Kafka config:** 3 nodes, KRaft, SSL listener with `ssl.client.auth=none` — wire encryption only, no authentication mechanism.

```sh
tibkafkatokof \
  --output-dir ./out/06 \
  --tls-cert /etc/ftl/certs/server.pem \
  --tls-key /etc/ftl/certs/server.key \
  --tls-ca /etc/ftl/certs/ca.pem \
  examples/06-3broker-tls-only/server-1.properties
```

**Output:** `kof-cluster.yaml`, `kof-cluster-secure.yaml` (auth mode: tls-only), `realm.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 07 — 3-broker, multi-SASL (PLAIN + OAuth2 + mTLS)

**Kafka config:** 3 nodes, KRaft, four listeners: BASIC_AUTH (SASL_SSL PLAIN), OAUTH (SASL_SSL OAUTHBEARER), MTLS (SSL mutual TLS), CONTROLLER (SSL).

```sh
tibkafkatokof \
  --output-dir ./out/07 \
  --realm-name prod_realm \
  --tls-cert /etc/ftl/certs/server.pem \
  --tls-key /etc/ftl/certs/server.key \
  --tls-ca /etc/ftl/certs/ca.pem \
  --tls-server-trust /etc/ftl/certs/client-ca.pem \
  --tls-client-cert /etc/ftl/certs/internal.pem \
  --tls-client-key /etc/ftl/certs/internal.key \
  --oauth-token-url https://auth.example.com/oauth/token \
  --oauth-jwks-url file:/etc/ftl/oauth.json \
  --oauth-client-id ftl-server \
  --oauth-client-secret env:OAUTH_CLIENT_SECRET \
  --oauth-provider-trust /etc/ftl/certs/oauth-provider.pem \
  examples/07-3broker-multi-sasl/server-1.properties
```

**Output:** `kof-cluster.yaml`, `kof-cluster-secure.yaml` (auth mode: oauth2 + mTLS props), `realm.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 08 — 3-broker, multi-listener (PLAIN + OAuth2 + per-listener mTLS)

**Kafka config:** 3 nodes, KRaft, four listeners: BASIC_AUTH (SASL_SSL PLAIN), OAUTH (SASL_SSL OAUTHBEARER), MTLS (SSL, `listener.name.mtls.ssl.client.auth=required`), CONTROLLER (SSL).

```sh
tibkafkatokof \
  --output-dir ./out/08 \
  --tls-cert /etc/ftl/certs/server.pem \
  --tls-key /etc/ftl/certs/server.key \
  --tls-ca /etc/ftl/certs/ca.pem \
  --tls-server-trust /etc/ftl/certs/client-ca.pem \
  --tls-client-cert /etc/ftl/certs/internal.pem \
  --tls-client-key /etc/ftl/certs/internal.key \
  --oauth-token-url https://auth.example.com/oauth/token \
  --oauth-jwks-url file:/etc/ftl/oauth.json \
  --oauth-client-id ftl-server \
  --oauth-client-secret env:OAUTH_CLIENT_SECRET \
  --oauth-provider-trust /etc/ftl/certs/oauth-provider.pem \
  examples/08-3broker-multi-listener/server-1.properties
```

**Output:** `kof-cluster.yaml`, `kof-cluster-secure.yaml` (auth mode: oauth2; includes all mTLS FTL properties), `realm.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 09 — 10-broker scale-out (3 shards)

**Kafka config:** 10 nodes — nodes 1–3 are broker+controller, nodes 4–10 are broker-only; SASL_SSL PLAIN + OAuth2 + mTLS listeners. 9 input files map to 9 pservers across 3 KOF shards (`kof.cluster.0` / `.1` / `.2`).

Generated reference output: [`examples/09-10broker-scale/output/`](examples/09-10broker-scale/output/)

```sh
tibkafkatokof \
  --output-dir examples/09-10broker-scale/output \
  --core-servers SRV1=kafka-cluster-1:5601,SRV2=kafka-cluster-2:5602,SRV3=kafka-cluster-3:5603 \
  examples/09-10broker-scale/server-1.properties \
  examples/09-10broker-scale/server-2.properties \
  examples/09-10broker-scale/server-3.properties \
  examples/09-10broker-scale/server-4.properties \
  examples/09-10broker-scale/server-5.properties \
  examples/09-10broker-scale/server-6.properties \
  examples/09-10broker-scale/server-7.properties \
  examples/09-10broker-scale/server-8.properties \
  examples/09-10broker-scale/server-9.properties
```

**Output:** `kof-cluster.yaml` (SRV1–3 + pserver1–3), `kof-cluster-aux1.yaml` (pserver4–6), `kof-cluster-aux2.yaml` (pserver7–9), `realm.json` (3 clusters: `kof.cluster.0/1/2`), `kof.broker.{1–9}.properties`, `unsupported.properties`

---

### 10 — 10-broker, full security stack (3 shards)

**Kafka config:** 10 nodes — nodes 1–3 are broker+controller (4 listeners: BASIC_AUTH + OAUTH + MTLS + CONTROLLER), nodes 4–10 are broker-only (3 listeners: BASIC_AUTH + OAUTH + MTLS). 9 input files map to 9 pservers across 3 KOF shards.

Generated reference output: [`examples/10-10broker-secure/output/`](examples/10-10broker-secure/output/)

```sh
tibkafkatokof \
  --output-dir examples/10-10broker-secure/output \
  --core-servers SRV1=kafka-cluster-1:5601,SRV2=kafka-cluster-2:5602,SRV3=kafka-cluster-3:5603 \
  --tls-cert /etc/ftl/certs/server.pem \
  --tls-key /etc/ftl/certs/server.key \
  --tls-ca /etc/ftl/certs/ca.pem \
  --tls-server-trust /etc/ftl/certs/client-ca.pem \
  --tls-client-cert /etc/ftl/certs/internal.pem \
  --tls-client-key /etc/ftl/certs/internal.key \
  --oauth-token-url https://auth.example.com/oauth/token \
  --oauth-jwks-url file:/etc/ftl/oauth.json \
  --oauth-client-id ftl-server \
  --oauth-client-secret env:OAUTH_CLIENT_SECRET \
  --oauth-provider-trust /etc/ftl/certs/oauth-provider.pem \
  examples/10-10broker-secure/server-1.properties \
  examples/10-10broker-secure/server-2.properties \
  examples/10-10broker-secure/server-3.properties \
  examples/10-10broker-secure/server-4.properties \
  examples/10-10broker-secure/server-5.properties \
  examples/10-10broker-secure/server-6.properties \
  examples/10-10broker-secure/server-7.properties \
  examples/10-10broker-secure/server-8.properties \
  examples/10-10broker-secure/server-9.properties
```

**Output:** `kof-cluster.yaml`, `kof-cluster-aux1.yaml`, `kof-cluster-aux2.yaml`, `kof-cluster-secure.yaml` (auth mode: oauth2 + mTLS), `realm.json` (3 clusters), `kof.broker.{1–9}.properties`, `unsupported.properties`

---

### 11 — 3-broker, PLAINTEXT + DR

**Kafka config:** 3 nodes, KRaft (broker+controller), PLAINTEXT. Primary servers named `primary1/2/3`; DR servers named `drserver1/2/3`. Mirrors the layout of the FTL `dr-simple` sample cluster configuration.

Generated reference output: [`examples/11-3broker-dr/output/`](examples/11-3broker-dr/output/)

```sh
tibkafkatokof \
  --output-dir examples/11-3broker-dr/output \
  --core-servers primary1=primary-host-1:8585,primary2=primary-host-2:8686,primary3=primary-host-3:8787 \
  --dr-servers drserver1=dr-host-1:9585,drserver2=dr-host-2:9686,drserver3=dr-host-3:9787 \
  --dr-data-dir /var/kof/dr \
  examples/11-3broker-dr/server-1.properties
```

**Output:**

`kof-cluster.yaml` — primary cluster (start with `tibftlserver -c kof-cluster.yaml -n SRV1`):
```yaml
globals:
  core.servers:
    primary1: primary-host-1:8585
    primary2: primary-host-2:8686
    primary3: primary-host-3:8787
  dr: drserver1@dr-host-1:9585|drserver2@dr-host-2:9686|drserver3@dr-host-3:9787
  auto.init.primary.on.first.startup: true
servers:
  SRV1:
  - realm:
      label: PRIMARY_SERVER
  - persistence:
      name: pserver1  ...
```

`kof-cluster-dr.yaml` — DR replica cluster (start with `tibftlserver -c kof-cluster-dr.yaml -n drserver1`):
```yaml
globals:
  core.servers:
    drserver1: dr-host-1:9585
    drserver2: dr-host-2:9686
    drserver3: dr-host-3:9787
  dr: primary1@primary-host-1:8585|primary2@primary-host-2:8686|primary3@primary-host-3:8787
servers:
  drserver1:
  - realm:
      label: DR_SERVER
  - persistence:
      name: drpserver1  ...
```

`realm.json` — `kof.cluster.N` with `dr_enabled: true`, `_setA` (pserver1–3) + `_DRset` (drpserver1–3)

---

## DR mode examples

Add `-dr-servers` (and optionally `-dr-data-dir`) to any of the commands above to enable Disaster Recovery output.

### Simple 3-broker + DR

Generated reference output: [`examples/04-3broker-plaintext/output-dr/`](examples/04-3broker-plaintext/output-dr/)

```sh
tibkafkatokof \
  --output-dir examples/04-3broker-plaintext/output-dr \
  --dr-servers DRSRV1=dr-host-1:5800,DRSRV2=dr-host-2:5801,DRSRV3=dr-host-3:5802 \
  --dr-data-dir /var/kof/dr \
  examples/04-3broker-plaintext/server-1.properties
```

**Output:**
```
kof-cluster.yaml              (primary: globals.dr + PRIMARY_SERVER labels on realm blocks)
kof-cluster-dr.yaml           (DR replica: DRSRV1–3 as core.servers, DR_SERVER labels, drpserver1–3)
realm.json                    (dr_enabled: true; _setA primary + _DRset DR pserver sets)
kof.broker.{1,2,3}.properties
unsupported.properties
```

### Secure 10-broker + DR (3 shards)

Combine the example 10 flags above with `-dr-servers` to generate DR-enabled output for a 9-pserver, 3-shard deployment. Produces six cluster YAML files (primary + DR, each across three files) and a `realm.json` with `dr_enabled: true` on all three clusters.

---

## Output details

### `kof-cluster.yaml`

Primary cluster with realm servers (SRV1–SRV3) and first 3 pservers. Realm server ports are either from `-core-servers` or randomly chosen in 5600–5699.

```sh
tibftlserver -c kof-cluster.yaml -n SRV1
tibftlserver -c kof-cluster.yaml -n SRV2
tibftlserver -c kof-cluster.yaml -n SRV3
```

**No `services:` section.** Realm settings are written per server, on each `- realm:` entry, rather
than once in a shared `services:` block. That includes `initial.realm.config`, so every server in
the file names the generated `realm.json` itself:

```yaml
servers:
  SRV1:
  - realm:
      data: /var/tmp/kof/data
      initial.realm.config: realm.json
  - persistence:
      name: pserver1
      data: /var/tmp/kof/data/pserver1
      kof.broker.properties: kof.broker.1.properties
      loglevel: connections:info;kof:info;durables:info;store:info
```

This applies to every cluster YAML the tool writes — primary, secure, and DR.

### `kof-cluster-auxN.yaml`

Auxiliary pserver groups. Each file references the same `globals.core.servers` as the primary. No realm entries at all — these pservers join the primary realm cluster.

```sh
tibftlserver -c kof-cluster-aux1.yaml -n PSRV4
```

### `kof-cluster-dr.yaml`

DR replica cluster. Start on the DR hosts using the DR server names from `-dr-servers`:

```sh
tibftlserver -c kof-cluster-dr.yaml -n DRSRV1
tibftlserver -c kof-cluster-dr.yaml -n DRSRV2
tibftlserver -c kof-cluster-dr.yaml -n DRSRV3
```

### `kof-cluster-secure.yaml`

Adds `ftlserver.properties` blocks (TLS, auth) to each realm server. Auth mode is determined by the Kafka listener types:

| Kafka auth | FTL secure YAML mode |
|---|---|
| `sasl_tls` (PLAIN) | `auth.providers: file:<auth-users-file>` + TLS fields |
| `oauth_tls` (OAUTHBEARER) | `auth.providers: oauth2` + `oauth2.*` globals and per-server properties |
| `tls` / `mtls` only | TLS fields only, no `auth.providers` |

When mTLS flags are provided alongside OAuth, the secure YAML includes both sets of FTL properties.

In oauth2 mode the realm credentials (`-realm-service-user` / `-realm-service-password`) are written
onto each per-server `- realm:` entry, since there is no shared `services:` block to hold them.

### `realm.json`

Contains `kof.cluster.N` clusters (`kof_enabled: true`), three stores per cluster (`kof.data.store.N`, `kof.sync.store.N`, `kof.meta.store.N`), and pservers distributed across clusters.

No upload step is needed: every generated cluster YAML names this file through
`initial.realm.config` on each per-server `- realm:` entry (see above), so `tibftlserver` seeds the
realm from it at startup. Upload manually only to push a *hand-edited* `realm.json` to a realm that
is already running:

```sh
tibrealmadmin --server localhost:5600 --realm _default_realm upload-realm realm.json
```

In DR mode, each cluster has `dr_enabled: true` and two pserver sets: `_setA` (primary) and `_DRset` (DR replicas).

### `kof.broker.N.properties`

One file per pserver (N is 1-based). Contains only properties that pass the KoF broker properties whitelist: listener/security keys in the section 1 allowlist, plus general broker/topic/tuning keys. Listener keys appear first, followed by remaining properties in their original order.

### `unsupported.properties`

Written when any input properties are not in the KoF whitelist. Contains KRaft cluster-control keys (`process.roles`, `controller.*`, etc.) and security-domain keys not on the section 1 allowlist (passwords, JAAS configs, handler classes, etc.). Kept for reference — the KoF pserver does not load this file.

---

## Example configs

| Directory | Nodes | Auth | pservers |
|---|---|---|---|
| `examples/01-single-node-plaintext/` | 1 (broker+controller) | PLAINTEXT | 1 |
| `examples/02-single-node-sasl/` | 1 (broker+controller) | SASL_SSL PLAIN | 1 |
| `examples/03-single-node-oauth/` | 1 (broker+controller) | SASL_SSL OAUTHBEARER | 1 |
| `examples/04-3broker-plaintext/` | 3 (broker+controller) | PLAINTEXT | 3 |
| `examples/05-3broker-sasl/` | 3 (broker+controller) | SASL_SSL PLAIN | 3 |
| `examples/06-3broker-tls-only/` | 3 (broker+controller) | SSL only (no SASL) | 3 |
| `examples/07-3broker-multi-sasl/` | 3 (broker+controller) | PLAIN + OAuth2 + mTLS | 3 |
| `examples/08-3broker-multi-listener/` | 3 (broker+controller) | PLAIN + OAuth2 + mTLS (per-listener client auth) | 3 |
| `examples/09-10broker-scale/` | 10 (nodes 1–3 controller) | SASL_SSL PLAIN + OAuth2 + mTLS | 9 (3 shards) |
| `examples/10-10broker-secure/` | 10 (nodes 1–3 controller) | PLAIN + OAuth2 + mTLS (full stack) | 9 (3 shards) |
| `examples/11-3broker-dr/` | 3 (broker+controller) | PLAINTEXT + DR | 3 |
| `examples/12-3broker-sasl-basic/` | 3 (broker+controller) | SASL_SSL PLAIN (single listener) | 3 |
| `examples/13-3broker-mtls/` | 3 (broker+controller) | SSL mTLS only (`ssl.client.auth=required`) | 3 |
| `examples/14-3broker-oauth2/` | 3 (broker+controller) | SASL_SSL OAUTHBEARER (single listener) | 3 |
| `examples/15-3broker-sasl+mtls/` | 3 (broker+controller) | SASL_SSL PLAIN + SSL mTLS | 3 |
| `examples/16-3broker-sasl+oauth2/` | 3 (broker+controller) | SASL_SSL PLAIN + SASL_SSL OAUTHBEARER | 3 |
| `examples/17-3broker-mtls+oauth2/` | 3 (broker+controller) | SSL mTLS + SASL_SSL OAUTHBEARER | 3 |
| `examples/18-3broker-sasl+mtls+oauth2/` | 3 (broker+controller) | SASL_SSL PLAIN + SSL mTLS + SASL_SSL OAUTHBEARER | 3 |
| `examples/19-from-brokers-plaintext/` | N/A (live brokers) | PLAINTEXT — fetch from live brokers | 3 |
| `examples/20-from-brokers-sasl/` | N/A (live brokers) | SASL — fetch from live brokers | 3 |

Examples 01–18 each ship a checked-in `output/` directory, regenerated by
`examples/regen-examples.sh`. Examples 19 and 20 cover the `-from-brokers` mode and hold a
`README.md` only: their input is a running Kafka cluster, so there is nothing reproducible to
check in. Follow the commands in those READMEs against a live cluster of your own.
