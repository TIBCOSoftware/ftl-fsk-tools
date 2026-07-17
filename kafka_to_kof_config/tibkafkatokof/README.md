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

From the workspace root (`hydra/`):

```sh
go build tibco.com/ftl-support/tibkafkatokof
```

Or from the module directory:

```sh
cd hydra/golang/src/tibco.com/ftl/tibkafkatokof
go build .
```

---

## Usage

```
tibkafkatokof [flags] <server.properties>
```

### Core flags

| Flag | Default | Description |
|---|---|---|
| `-output-dir` | `./kof-output` | Directory where output files are written |
| `-realm-name` | `_default_realm` | Realm name in `realm.json` |
| `-data-dir` | `/var/tmp/kof/data` | KOF data directory path on pserver hosts |
| `-num-pservers` | `3` | Number of pservers to generate (must be a positive odd number) |
| `-transport-type` | `auto` | Transport type for all pserver connections in `realm.json`: `auto` or `dtcp`<br>`auto` lets the OS choose; `dtcp` is optimized for low latency |
| `-core-servers` | _(auto)_ | Comma-separated `NAME=host:port` list for `globals.core.servers`<br>e.g. `SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602`<br>If omitted, ports are randomly generated in range 5600–5699 |

### TLS flags

Used when the input config has any `tls`, `mtls`, `sasl_tls`, or `oauth_tls` listener and you want a `kof-cluster-secure.yaml` emitted.

| Flag | Description |
|---|---|
| `-tls-cert` | Server TLS certificate PEM file path |
| `-tls-key` | Server TLS private key PEM file path |
| `-tls-key-password` | TLS private key passphrase |
| `-tls-ca` | CA/trust PEM file path (for connecting to other FTL servers) |

### mTLS flags

Required when a Kafka mTLS listener (`ssl.client.auth=required`) is present and you want FTL server-to-server mutual TLS.

| Flag | Description |
|---|---|
| `-tls-server-trust` | CA PEM used to verify inbound client certificates (`tls.server.trust.file`) |
| `-tls-client-cert` | Client cert PEM presented to other FTL servers (`tls.client.cert`) |
| `-tls-client-key` | Client private key PEM for server-to-server connections (`tls.client.private.key`) |
| `-tls-client-key-password` | Passphrase for `-tls-client-key` |

### OAuth2 flags

| Flag | Description |
|---|---|
| `-oauth-token-url` | OAuth2 token endpoint URL (server-to-server) |
| `-oauth-jwks-url` | OAuth2 JWKS or validation key (`file:` path or URL) |
| `-oauth-client-id` | OAuth2 client ID |
| `-oauth-client-secret` | OAuth2 client secret |
| `-oauth-provider-trust` | OAuth2 provider trust PEM file |

### Basic auth flag

| Flag | Description |
|---|---|
| `-auth-users-file` | Path to FTL `users.txt` for file-based authentication (PLAIN SASL → file auth) |

`kof-cluster-secure.yaml` is only emitted when **security is detected** in the input props AND at least one of `-tls-cert`, `-oauth-token-url`, or `-auth-users-file` is provided.

### DR (Disaster Recovery) flags

| Flag | Default | Description |
|---|---|---|
| `-dr-servers` | _(none)_ | Comma-separated `DRSRV1=host:port,DRSRV2=host:port,...` DR server list.<br>Providing this flag enables DR mode for all generated files. |
| `-dr-data-dir` | `<data-dir>/dr` | Data directory for DR pservers on DR hosts |

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

**Kafka config:** 3 nodes, KRaft (broker+controller), PLAINTEXT. Primary servers named `primary1/2/3`; DR servers named `drserver1/2/3`. Mirrors the layout of `hydra/samples/yaml/dr-simple/`.

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

### `kof-cluster-auxN.yaml`

Auxiliary pserver groups. Each file references the same `globals.core.servers` as the primary. No realm service block.

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

### `realm.json`

Contains `kof.cluster.N` clusters (`kof_enabled: true`), three stores per cluster (`kof.data.store.N`, `kof.sync.store.N`, `kof.meta.store.N`), and pservers distributed across clusters. Upload after the realm server starts:

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
