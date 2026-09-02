# tibftlimportconfig

Translates an Apache Kafka KRaft broker `server.properties` file into the TIBCO FTL(R) Service for Kafka (FSK) artifacts needed to run an FSK-enabled FTL Server cluster:

| Output file | Purpose |
|---|---|
| `tibftlserver-cluster.yaml` | Cluster config for up to 3 FTL Servers (realm service + FSK persistence in each) |
| `tibftlserver-cluster-secure.yaml` | Secure variant with TLS/auth blocks (generated when TLS/OAuth flags are provided) |
| `tibftlserver-cluster-dr.yaml` | DR replica cluster config (generated when `-dr-servers` is provided) |
| `ftlserver.json` | FTL realm config with `kof.cluster.N` (N is 0-based), stores, and FTL Server definitions |
| `kof.broker.N.properties` | Per-broker properties file (N is 1-based, one per FTL Server); only contains properties in the FSK whitelist |
| `unsupported.properties` | Properties from the input not in the FSK whitelist; written when any such properties exist |

A single-broker conversion produces one FTL Server — a standalone server rather than a cluster — so
its YAMLs are named `tibftlserver_standalone.yaml`, `tibftlserver_standalone-secure.yaml` and
`tibftlserver_standalone-dr.yaml`.

---

## Getting the tool

A built binary is checked in at [`bin/tibftlimportconfig`](bin/) (linux/amd64, statically linked),
so a clone needs no Go toolchain. That is the binary every command in this document and in
[getting-started.md](getting-started.md) refers to — put it on your `PATH`, or invoke it by path:

```sh
export PATH="$PWD/bin:$PATH"
tibftlimportconfig -h
```

### Building from source

Only needed for a platform other than linux/amd64, or when changing the tool. Requires Go 1.25+
(`toolchain go1.25.6` is pinned in `go.mod`). From this directory — the one holding `go.mod`:

```sh
go build .
```

Inside a Go workspace that lists this module, it can also be built by module path from the
workspace root:

```sh
go build tibco.com/ftl-support/tibftlimportconfig
```

To refresh the checked-in binary after changing the sources, run `./build-artifacts.sh` at the
repository root and commit the result.

---

## Usage

```
tibftlimportconfig [flags] <server.properties> [<server.properties> ...]
```

Positional arguments are one or more `server.properties` files (1–9). Each broker becomes one
FTL Server — the FTL Server count is derived from the number of input files, not from a flag.

### Getting help

Help is organized in two tiers, so a bare `-h` stays short:

```sh
tibftlimportconfig -h            # synopsis, output files, the handful of common flags, group index
tibftlimportconfig -h oauth      # just the OAuth2 flags
tibftlimportconfig -h all        # every flag, grouped
```

| Group | `-h <group>` covers |
|---|---|
| `core` | output location, data dir, server addresses, transport |
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
| `-data-dir` | `/var/tmp/kof/data` | FSK data directory path on FTL Server hosts |
| `-core-servers` | _(auto)_ | Comma-separated `NAME=host:port` list for `globals.core.servers`<br>e.g. `SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602`<br>If omitted, ports are derived from the cluster in range 5600–5699 — the same brokers always yield the same ports, so re-running the tool does not move them |
| `-transport-type` | `auto` | Transport type for all FTL Server connections in `ftlserver.json`: `auto` or `dtcp`<br>`auto` leaves the choice to the realm server, which resolves each connection at deployment time — dynamic TCP for client and intra-cluster transports, static TCP for inter-cluster and DR transports<br>`dtcp` pins every transport to dynamic TCP |
| `-replication-factor` | `3` | FTL Servers per FSK shard (`kof.cluster.N`): `1`, `3` or `5`. The number of input `server.properties` files must be an exact multiple of it — 9 files at `3` give three shards, 6 give two, 5 files at `5` give one. The FTL realm keeps its own 3 servers either way.<br>The accepted values are odd because a shard needs a majority quorum, so **2 input files are refused**; use `-replication-factor 1` for one unreplicated shard per broker. With a single input file the default is `1` — a standalone server is explicitly unreplicated. See [Multi-cluster split](#multi-cluster-split-9-input-files--3-shards) |
| `-disk-persistence` | `async` | `disk_persistence` for the generated `kof.cluster.N`: `async`, `sync` or `in-memory`<br>`async` writes are buffered for an eventual flush to disk; `sync` flushes every write before acknowledging it; `in-memory` writes nothing to disk and turns off the cluster's `disk_index` and `disk_compact`, which require disk persistence<br>The data store stays `async` and the sync and meta stores `sync` whichever the cluster is — except under `in-memory`, where the stores are in-memory too (see [`ftlserver.json`](#realmjson)) |
| `-ftl-loglevel` | `connections:info;kof:info;durables:info;store:info` | `loglevel` written into each generated FTL Server. This is the *output* FTL Servers' logging, not this tool's. |
| `-migration-config` | `false` | Write `kafka-to-kof.properties` to the output directory (configuration for the `tibftlfskimportdata` data migration tool) |
| `-tibschemad` | `false` | Add the FTL schema daemon to the generated cluster YAML: every server gains a `schemaN` persistence and a `- tibschemad:` entry with `auth.type: none` and `cluster.size` set to the number of realm servers. No extra servers and no extra ports — the schema persistence shares the `tibftlserver` process that already hosts the FSK persistence. |

### TLS and mTLS flags (`-h tls`)

Used when the input config has any `tls`, `mtls`, `sasl_tls`, or `oauth_tls` listener and you want a `tibftlserver-cluster-secure.yaml` emitted. The `-tls-server-trust` / `-tls-client-*` flags are the ones required when an Apache Kafka mTLS listener (`ssl.client.auth=required`) is present and you want FTL Server-to-server mutual TLS.

| Flag | Description |
|---|---|
| `-tls-cert` | Server TLS certificate PEM file path |
| `-tls-key` | Server TLS private key PEM file path |
| `-tls-key-password` | TLS private key passphrase |
| `-tls-ca` | CA/trust PEM file path (for connecting to other FTL Servers) |
| `-tls-server-trust` | CA PEM used to verify inbound client certificates (`tls.server.trust.file`) |
| `-tls-client-cert` | Client cert PEM presented to other FTL Servers (`tls.client.cert`) |
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

`tibftlserver-cluster-secure.yaml` is only emitted when **security is detected** in the input props AND at least one of `-tls-cert`, `-oauth-token-url`, or `-auth-users-file` is provided.

### DR (Disaster Recovery) flags (`-h dr`)

| Flag | Default | Description |
|---|---|---|
| `-dr-servers` | _(none)_ | Comma-separated `DRSRV1=host:port,DRSRV2=host:port,...` DR server list.<br>Providing this flag enables DR mode for all generated files. |
| `-dr-data-dir` | `<data-dir>/dr` | Data directory for DR FTL Servers on DR hosts |

### Inspection and conversion flags (`-h info`)

| Flag | Default | Description |
|---|---|---|
| `-list-properties` | `false` | Print how each Apache Kafka listener/security property is treated, then exit |
| `-color` | `auto` | Colorize `-list-properties` output: `auto`, `always`, or `never` |
| `-auto` | `false` | Actually run the keystore conversions (JKS/PKCS12 → PEM via `keytool`/`openssl`) rather than only printing the commands. The generated config names the `.pem` either way; items needing a human decision stay `RESOLVE-REQUIRED` |

---

## Multi-cluster split (9 input files → 3 shards)

The number of FTL Servers is the number of `server.properties` files passed on the command line; how they are grouped into shards is [`-replication-factor`](#core-flags--h-core). Every *replication factor* input files → 1 FSK cluster (shard), and the file count must be an exact multiple of it, so no shard is ever short of servers.

```
9 input files → tibftlserver-cluster.yaml      (pserver1–3 + SRV1–3 realm servers)
                tibftlserver-cluster-aux1.yaml  (pserver4–6, no realm)
                tibftlserver-cluster-aux2.yaml  (pserver7–9, no realm)
                ftlserver.json             (kof.cluster.0, kof.cluster.1, kof.cluster.2)
```

### Accepted input-file counts

| `-replication-factor` | Input files | Shards |
|---|---|---|
| _(omitted)_ | 1 | one server, unreplicated — a standalone, not a cluster |
| _(omitted)_ or `3` | 3, 6, 9 | one, two or three shards of 3 |
| `5` | 5 | one shard of 5 |
| `1` | 1–9 | one unreplicated shard per broker |

Any other count is refused. In particular **2 input files are refused**: FTL runs 1, 3 or 5 servers, and two have no majority quorum, so losing either one stalls the shard. Use `-replication-factor 1` if you really want two independent unreplicated shards. Counts such as 4, 7 and 8 are refused for the same reason — they would leave a short final shard.

### File layout

The primary `tibftlserver-cluster.yaml` always holds the first 3 FTL Servers, which are the ones carrying FTL realm servers. The rest go into `tibftlserver-cluster-aux1.yaml`, `tibftlserver-cluster-aux2.yaml` and so on, in groups of 3 (or of the replication factor, when that is larger). Auxiliary files contain **no realm server entries** — those FTL Servers connect to the primary realm cluster via `globals.core.servers`.

Which YAML file a server is declared in is packaging only. Shard membership lives entirely in `ftlserver.json`, so at `-replication-factor 5` the one 5-server shard spans the primary file and `aux1.yaml`, and at `-replication-factor 1` a single aux file holds servers belonging to three different shards.

---

## DR mode (`-dr-servers`)

When `-dr-servers` is provided, DR mode is activated for all output files:

- **`tibftlserver-cluster.yaml`** gains `globals.dr:` (pointing to DR servers), `auto.init.primary.on.first.startup: true`, and `label: PRIMARY_SERVER` on each realm block.
- **`tibftlserver-cluster-dr.yaml`** is generated with DR servers as `core.servers`, a back-reference `globals.dr:` to the primary servers, and `label: DR_SERVER` on realm blocks. Their persistence entries are named `drpserver1..N`.
- **`ftlserver.json`** clusters get `dr_enabled: true`, a second persistence set `_DRset` with DR replicas, and transport roles swapped (`dr_transport` populated, `inter_cluster_transport` empty for all FTL Servers).

With 9 input files, DR aux files are also produced:

```
9 files + -dr-servers ... →
    tibftlserver-cluster.yaml           (primary: pserver1–3)
    tibftlserver-cluster-aux1.yaml      (primary: pserver4–6)
    tibftlserver-cluster-aux2.yaml      (primary: pserver7–9)
    tibftlserver-cluster-dr.yaml        (DR: drpserver1–3)
    tibftlserver-cluster-dr-aux1.yaml   (DR: drpserver4–6)
    tibftlserver-cluster-dr-aux2.yaml   (DR: drpserver7–9)
    ftlserver.json                 (kof.cluster.0/1/2 with dr_enabled: true)
```

---

## Examples

Each example is a directory under `examples/` holding one `server-N.properties` per Apache Kafka broker plus a checked-in `output/`. **One input file becomes one FTL Server**, so pass every broker's properties file — the tool has no flag for the FTL Server count.

Each command below is written to be run from inside its own example directory, with `--output-dir output`, which is how the checked-in `output/` was produced. Run it that way and you reproduce the checked-in files (the generated YAML embeds the output directory as a relative path, so a different `--output-dir` changes the result). `examples/regen-examples.sh` runs exactly these commands for every example at once.

---

### 01 — Single node, PLAINTEXT

**Apache Kafka config:** 1 node, KRaft (broker+controller), PLAINTEXT, no security. Suitable for local development.

Generated reference output: [`examples/01-single-node-plaintext/output/`](examples/01-single-node-plaintext/output/)

```sh
cd examples/01-single-node-plaintext
tibftlimportconfig \
  --core-servers SRV1=localhost:5663 \
  --output-dir output \
  server-1.properties
```

**Output:** `tibftlserver_standalone.yaml` (1 SRV + 1 FTL Server), `ftlserver.json`, `kof.broker.1.properties`, `unsupported.properties`

---

### 02 — 3-broker, PLAINTEXT

**Apache Kafka config:** 3 nodes, KRaft (broker+controller), PLAINTEXT listeners, no security.

Generated reference output: [`examples/02-3broker-plaintext/output/`](examples/02-3broker-plaintext/output/)

```sh
cd examples/02-3broker-plaintext
tibftlimportconfig \
  --core-servers SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602 \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties
```

**Output:** `tibftlserver-cluster.yaml`, `ftlserver.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 03 — Single node, ZooKeeper mode, PLAINTEXT

**Apache Kafka config:** 1 node in ZooKeeper mode (Apache Kafka 3.9 or earlier — 4.x removed ZooKeeper), single PLAINTEXT listener, no security.

Two things distinguish a ZooKeeper-mode input from the KRaft examples above:

- The broker identifies itself with `broker.id`, the key KRaft later renamed to `node.id`. FSK reads `node.id` only, so the tool renames it in place and notes the rename in the `# Node ID:` header of the generated `kof.broker.N.properties`. Without this, the FTL Server refuses to start with `kof.broker.properties: node.id is required and must be a non-negative integer`.
- The `zookeeper.*` keys go to `unsupported.properties`. FSK has no ZooKeeper; the cluster membership and metadata ZooKeeper holds for Apache Kafka are FTL-native.

Generated reference output: [`examples/03-zk-single-node-plaintext/output/`](examples/03-zk-single-node-plaintext/output/)

```sh
cd examples/03-zk-single-node-plaintext
tibftlimportconfig \
  --core-servers SRV1=localhost:5664 \
  --output-dir output \
  server-1.properties
```

**Output:** `tibftlserver_standalone.yaml` (1 SRV + 1 FTL Server), `ftlserver.json`, `kof.broker.1.properties` (`node.id=0`, from `broker.id=0`), `unsupported.properties`

---

### 04 — 3-broker, ZooKeeper mode, PLAINTEXT

**Apache Kafka config:** 3 nodes in ZooKeeper mode sharing one ZooKeeper ensemble, PLAINTEXT listeners, no security. Same `broker.id` → `node.id` rename as example 03, applied per broker.

Generated reference output: [`examples/04-zk-3broker-plaintext/output/`](examples/04-zk-3broker-plaintext/output/)

```sh
cd examples/04-zk-3broker-plaintext
tibftlimportconfig \
  --core-servers SRV1=localhost:5610,SRV2=localhost:5611,SRV3=localhost:5612 \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties
```

**Output:** `tibftlserver-cluster.yaml`, `ftlserver.json`, `kof.broker.{1,2,3}.properties` (`node.id=1/2/3`, from `broker.id`), `unsupported.properties`

---

### 05 — Single node, SASL_SSL PLAIN

**Apache Kafka config:** 1 node, KRaft, SASL_SSL PLAIN on broker listener, SSL on controller.

Generated reference output: [`examples/05-single-node-sasl/output/`](examples/05-single-node-sasl/output/)

```sh
cd examples/05-single-node-sasl
tibftlimportconfig \
  --core-servers SRV1=localhost:5689 \
  --tls-cert /etc/ftl/certs/server.pem \
  --auth-users-file /etc/ftl/users.txt \
  --output-dir output \
  server-1.properties
```

**Output:** `tibftlserver_standalone.yaml`, `tibftlserver_standalone-secure.yaml` (auth mode: file-auth+tls), `ftlserver.json`, `kof.broker.1.properties`, `unsupported.properties`

---

### 06 — Single node, OAuth2

**Apache Kafka config:** 1 node, KRaft, SASL_SSL OAUTHBEARER on broker listener, SSL on controller.

Generated reference output: [`examples/06-single-node-oauth/output/`](examples/06-single-node-oauth/output/)

```sh
cd examples/06-single-node-oauth
tibftlimportconfig \
  --core-servers SRV1=localhost:5663 \
  --oauth-token-url https://auth.example.com/oauth/token \
  --oauth-jwks-url file:/etc/ftl/oauth.json \
  --oauth-ui-auth-url https://auth.example.com/oauth/authorize \
  --oauth-ui-token-url https://auth.example.com/oauth/token \
  --oauth-ui-logout-url https://auth.example.com/oauth/logout \
  --oauth-ui-client-id ftl-ui \
  --oauth-ui-client-secret env:OAUTH_UI_CLIENT_SECRET \
  --output-dir output \
  server-1.properties
```

**Output:** `tibftlserver_standalone.yaml`, `tibftlserver_standalone-secure.yaml` (auth mode: oauth2), `ftlserver.json`, `kof.broker.1.properties`, `unsupported.properties`

---

### 07 — 3-broker, SASL_SSL PLAIN

**Apache Kafka config:** 3 nodes, KRaft, SASL_SSL PLAIN on broker listener, SSL on controller listener.

Generated reference output: [`examples/07-3broker-sasl/output/`](examples/07-3broker-sasl/output/)

```sh
cd examples/07-3broker-sasl
tibftlimportconfig \
  --core-servers SRV1=localhost:5695,SRV2=localhost:5641,SRV3=localhost:5693 \
  --tls-cert /etc/ftl/certs/server.pem \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties
```

**Output:** `tibftlserver-cluster.yaml`, `tibftlserver-cluster-secure.yaml` (auth mode: file-auth+tls), `ftlserver.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 08 — 3-broker, TLS-only (no SASL)

**Apache Kafka config:** 3 nodes, KRaft, SSL listener with `ssl.client.auth=none` — wire encryption only, no authentication mechanism.

Generated reference output: [`examples/08-3broker-tls-only/output/`](examples/08-3broker-tls-only/output/)

```sh
cd examples/08-3broker-tls-only
tibftlimportconfig \
  --core-servers SRV1=localhost:5680,SRV2=localhost:5626,SRV3=localhost:5616 \
  --tls-cert /etc/ftl/certs/server.pem \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties
```

**Output:** `tibftlserver-cluster.yaml`, `tibftlserver-cluster-secure.yaml` (auth mode: tls-only), `ftlserver.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 09 — 3-broker, multi-SASL (PLAIN + OAuth2 + mTLS)

**Apache Kafka config:** 3 nodes, KRaft, four listeners: BASIC_AUTH (SASL_SSL PLAIN), OAUTH (SASL_SSL OAUTHBEARER), MTLS (SSL mutual TLS), CONTROLLER (SSL).

Generated reference output: [`examples/09-3broker-multi-sasl/output/`](examples/09-3broker-multi-sasl/output/)

```sh
cd examples/09-3broker-multi-sasl
tibftlimportconfig \
  --core-servers SRV1=localhost:5686,SRV2=localhost:5696,SRV3=localhost:5622 \
  --oauth-token-url https://auth.example.com/oauth/token \
  --oauth-jwks-url file:/etc/ftl/oauth.json \
  --oauth-ui-auth-url https://auth.example.com/oauth/authorize \
  --oauth-ui-token-url https://auth.example.com/oauth/token \
  --oauth-ui-logout-url https://auth.example.com/oauth/logout \
  --oauth-ui-client-id ftl-ui \
  --oauth-ui-client-secret env:OAUTH_UI_CLIENT_SECRET \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties
```

**Output:** `tibftlserver-cluster.yaml`, `tibftlserver-cluster-secure.yaml` (auth mode: oauth2 + mTLS props), `ftlserver.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 10 — 3-broker, multi-listener (PLAIN + OAuth2 + per-listener mTLS)

**Apache Kafka config:** 3 nodes, KRaft, four listeners: BASIC_AUTH (SASL_SSL PLAIN), OAUTH (SASL_SSL OAUTHBEARER), MTLS (SSL, `listener.name.mtls.ssl.client.auth=required`), CONTROLLER (SSL).

Generated reference output: [`examples/10-3broker-multi-listener/output/`](examples/10-3broker-multi-listener/output/)

```sh
cd examples/10-3broker-multi-listener
tibftlimportconfig \
  --core-servers SRV1=localhost:5695,SRV2=localhost:5654,SRV3=localhost:5616 \
  --oauth-token-url https://auth.example.com/oauth/token \
  --oauth-jwks-url file:/etc/ftl/oauth.json \
  --oauth-ui-auth-url https://auth.example.com/oauth/authorize \
  --oauth-ui-token-url https://auth.example.com/oauth/token \
  --oauth-ui-logout-url https://auth.example.com/oauth/logout \
  --oauth-ui-client-id ftl-ui \
  --oauth-ui-client-secret env:OAUTH_UI_CLIENT_SECRET \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties
```

**Output:** `tibftlserver-cluster.yaml`, `tibftlserver-cluster-secure.yaml` (auth mode: oauth2; includes all mTLS FTL properties), `ftlserver.json`, `kof.broker.{1,2,3}.properties`, `unsupported.properties`

---

### 11 — 9-broker scale-out (3 shards)

**Apache Kafka config:** 9 nodes — nodes 1–3 are broker+controller, nodes 4–9 are broker-only; SASL_SSL PLAIN + OAuth2 + mTLS listeners. The 9 input files map to 9 FTL Servers across 3 FSK shards (`kof.cluster.0` / `.1` / `.2`).

The inputs declare secured Apache Kafka listeners, but **no FTL security flags are passed on the command line on purpose**: this example is about the sharding split, so the output stays minimal. That is why there is no `tibftlserver-cluster-secure.yaml` here — only an `ftl-users.txt` derived from the SASL PLAIN users in the inputs. [Example 12](#12--9-broker-full-security-stack-3-shards) is the same nine inputs *with* the security flags supplied, and that is where the secure YAML appears.

Generated reference output: [`examples/11-9broker-scale/output/`](examples/11-9broker-scale/output/)

```sh
cd examples/11-9broker-scale
tibftlimportconfig \
  --core-servers SRV1=localhost:5619,SRV2=localhost:5698,SRV3=localhost:5635 \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties \
  server-4.properties server-5.properties server-6.properties \
  server-7.properties server-8.properties server-9.properties
```

**Output:** `tibftlserver-cluster.yaml` (SRV1–3 + pserver1–3), `tibftlserver-cluster-aux1.yaml` (pserver4–6), `tibftlserver-cluster-aux2.yaml` (pserver7–9), `ftlserver.json` (3 clusters: `kof.cluster.0/1/2`), `kof.broker.{1–9}.properties`, `ftl-users.txt`, `unsupported.properties`

The three shards are the default [`-replication-factor`](#core-flags--h-core) of 3. Adding `--replication-factor 1` to the same nine inputs gives nine unreplicated shards, `kof.cluster.0` through `.8`, with the YAML file layout unchanged.

---

### 12 — 9-broker, full security stack (3 shards)

**Apache Kafka config:** 9 nodes — nodes 1–3 are broker+controller (4 listeners: BASIC_AUTH + OAUTH + MTLS + CONTROLLER), nodes 4–9 are broker-only (3 listeners: BASIC_AUTH + OAUTH + MTLS). The 9 input files map to 9 FTL Servers across 3 FSK shards. Same inputs as example 11, with the FTL security flags supplied.

Generated reference output: [`examples/12-9broker-secure/output/`](examples/12-9broker-secure/output/)

```sh
cd examples/12-9broker-secure
tibftlimportconfig \
  --core-servers SRV1=localhost:5601,SRV2=localhost:5602,SRV3=localhost:5603 \
  --tls-cert /etc/ftl/certs/server.pem \
  --tls-server-trust /etc/ftl/certs/client-ca.pem \
  --tls-client-cert /etc/ftl/certs/client.pem \
  --tls-client-key /etc/ftl/certs/client.key \
  --oauth-token-url https://auth.example.com/oauth/token \
  --oauth-jwks-url file:/etc/ftl/oauth.json \
  --oauth-ui-auth-url https://auth.example.com/oauth/authorize \
  --oauth-ui-token-url https://auth.example.com/oauth/token \
  --oauth-ui-logout-url https://auth.example.com/oauth/logout \
  --oauth-ui-client-id ftl-ui \
  --oauth-ui-client-secret env:OAUTH_UI_CLIENT_SECRET \
  --oauth-client-id ftl-server \
  --oauth-client-secret env:OAUTH_CLIENT_SECRET \
  --oauth-provider-trust /etc/ftl/certs/oauth-provider.pem \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties \
  server-4.properties server-5.properties server-6.properties \
  server-7.properties server-8.properties server-9.properties
```

**Output:** `tibftlserver-cluster.yaml`, `tibftlserver-cluster-aux1.yaml`, `tibftlserver-cluster-aux2.yaml`, `tibftlserver-cluster-secure.yaml` (auth mode: oauth2 + mTLS), `ftlserver.json` (3 clusters), `kof.broker.{1–9}.properties`, `unsupported.properties`

As in example 11, the three shards come from the default [`-replication-factor`](#core-flags--h-core) of 3. The secure YAML covers the primary file's three FTL Servers whatever the factor is — there is no `-secure-auxN.yaml`.

---

### 13 — 3-broker, PLAINTEXT + DR

**Apache Kafka config:** 3 nodes, KRaft (broker+controller), PLAINTEXT. Primary servers named `primary1/2/3`; DR servers named `drserver1/2/3`. Mirrors the layout of the FTL `dr-simple` sample cluster configuration.

Generated reference output: [`examples/13-3broker-dr/output/`](examples/13-3broker-dr/output/)

```sh
cd examples/13-3broker-dr
tibftlimportconfig \
  --core-servers primary1=primary-host-1:8585,primary2=primary-host-2:8686,primary3=primary-host-3:8787 \
  --dr-servers drserver1=localhost:9585,drserver2=localhost:9686,drserver3=localhost:9787 \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties
```

**Output:**

`tibftlserver-cluster.yaml` — primary cluster (start with `tibftlserver -c tibftlserver-cluster.yaml -n primary1`):
```yaml
globals:
  core.servers:
    primary1: primary-host-1:8585
    primary2: primary-host-2:8686
    primary3: primary-host-3:8787
  dr: drserver1@localhost:9585|drserver2@localhost:9686|drserver3@localhost:9787
  auto.init.primary.on.first.startup: true
servers:
  primary1:
  - realm:
      label: PRIMARY_SERVER
  - persistence:
      name: pserver1  ...
```

Note the `servers:` keys are the `-core-servers` names, not a fixed `SRV1/2/3`. These servers carry
no `ftl:` block, so `tibftlserver` resolves each one's listen address by matching `-n` against
`globals.core.servers` — the two name lists have to agree.

`tibftlserver-cluster-dr.yaml` — DR replica cluster (start with `tibftlserver -c tibftlserver-cluster-dr.yaml -n drserver1`):
```yaml
globals:
  core.servers:
    drserver1: localhost:9585
    drserver2: localhost:9686
    drserver3: localhost:9787
  dr: primary1@primary-host-1:8585|primary2@primary-host-2:8686|primary3@primary-host-3:8787
servers:
  drserver1:
  - realm:
      label: DR_SERVER
  - persistence:
      name: drpserver1  ...
```

`ftlserver.json` — `kof.cluster.N` with `dr_enabled: true`, `_setA` (pserver1–3) + `_DRset` (drpserver1–3)

---

## DR mode examples

Add `-dr-servers` (and optionally `-dr-data-dir`) to any of the commands above to enable Disaster Recovery output.

### Simple 3-broker + DR

Generated reference output: [`examples/02-3broker-plaintext/output-dr/`](examples/02-3broker-plaintext/output-dr/)

```sh
cd examples/02-3broker-plaintext
tibftlimportconfig \
  --core-servers SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602 \
  --dr-servers DRSRV1=dr-host-1:5800,DRSRV2=dr-host-2:5801,DRSRV3=dr-host-3:5802 \
  --dr-data-dir /var/kof/dr \
  --output-dir output-dr \
  server-1.properties server-2.properties server-3.properties
```

Same inputs and same `-core-servers` as example 02, so `diff output output-dr` shows exactly what `-dr-servers` adds.

**Output:**
```
tibftlserver-cluster.yaml              (primary: globals.dr + PRIMARY_SERVER labels on realm blocks)
tibftlserver-cluster-dr.yaml           (DR replica: DRSRV1–3 as core.servers, DR_SERVER labels, drpserver1–3)
ftlserver.json                    (dr_enabled: true; _setA primary + _DRset DR persistence sets)
kof.broker.{1,2,3}.properties
unsupported.properties
```

### Secure 9-broker + DR (3 shards)

Combine the example 12 flags above with `-dr-servers` to generate DR-enabled output for a 9-server, 3-shard deployment. Produces six cluster YAML files (primary + DR, each across three files) and a `ftlserver.json` with `dr_enabled: true` on all three clusters.

---

## Output details

### `tibftlserver-cluster.yaml`

Primary cluster with realm servers (SRV1–SRV3) and first 3 FTL Servers. Realm server names and ports
both come from `-core-servers`; if that flag is omitted the names default to `SRV1–SRV3` and the
ports are derived from the cluster in 5600–5699. The `-n` argument is the `servers:` key, which is always the
core-server name:

```sh
tibftlserver -c tibftlserver-cluster.yaml -n SRV1
tibftlserver -c tibftlserver-cluster.yaml -n SRV2
tibftlserver -c tibftlserver-cluster.yaml -n SRV3
```

**No `services:` section.** Realm settings are written per server, on each `- realm:` entry, rather
than once in a shared `services:` block. That includes `initial.realm.config`, so every server in
the file names the generated `ftlserver.json` itself:

```yaml
servers:
  SRV1:
  - realm:
      data: /var/tmp/kof/data
      initial.realm.config: ftlserver.json
  - ftlserver.properties:
      loglevel: info
      #logfile: /var/tmp/kof/data/SRV1.log
      #max.log.size: 10240000
      #max.logs: 10
  - persistence:
      name: pserver1
      data: /var/tmp/kof/data/pserver1
      kof.broker.properties: kof.broker.1.properties
      loglevel: connections:info;kof:info;durables:info;store:info
```

This applies to every cluster YAML the tool writes — primary, secure, and DR.

**Server logging.** Every server carries an `ftlserver.properties` block with the process-wide
logging settings. `loglevel` is set to `info` and reaches each service in the process that does not
name a level of its own — the persistence service above does, and keeps
`connections:info;kof:info;durables:info;store:info`; the realm service does not, so it follows the
server.

The other three lines are commented out because `tibftlserver` logs to stdout by default. To log to
a file instead, uncomment all three: `max.log.size` and `max.logs` are ignored while `logfile` is
unset, and `tibftlserver` rejects a `logfile` given without them. The suggested path is
`<data-dir>/<server-name>.log`, so it matches the `-n` argument that starts the server.

Auxiliary and DR servers get the same block; DR uses `-dr-data-dir` for the suggested path.

**Schema daemon (`-tibschemad`).** With the flag set, every server that carries a realm block also
gets a second persistence service and a `- tibschemad:` entry. No extra `tibftlserver` processes and
no extra ports — the schema persistence rides the process that already hosts the FSK one, so a 3-broker
conversion is still three servers:

```yaml
servers:
  SRV1:
  - realm:
      data: /var/tmp/kof/data
      initial.realm.config: ftlserver.json
  - ftlserver.properties:
      loglevel: info
      #logfile: /var/tmp/kof/data/SRV1.log
      #max.log.size: 10240000
      #max.logs: 10
  - persistence:
      name: pserver1
      data: /var/tmp/kof/data/pserver1
      kof.broker.properties: kof.broker.1.properties
      loglevel: connections:info;kof:info;durables:info;store:info
  - persistence:
      name: schema1
  - tibschemad:
      auth.type: none
      cluster.size: 3
```

`cluster.size` is the number of realm servers in the file — 1 for a standalone server, 3 for a
cluster. The block is written to the primary and secure YAMLs. Auxiliary files never get it, since
their servers have no realm entry to attach a schema daemon to, and DR files are not covered yet.
`auth.type` is always `none` for now; OAuth options for `tibschemad` are a later addition.

### `tibftlserver-cluster-auxN.yaml`

Auxiliary FTL Server groups. Each file references the same `globals.core.servers` as the primary. No realm entries at all — these FTL Servers join the primary realm cluster.

```sh
tibftlserver -c tibftlserver-cluster-aux1.yaml -n PSRV4
```

### `tibftlserver-cluster-dr.yaml`

DR replica cluster. Start on the DR hosts using the DR server names from `-dr-servers`:

```sh
tibftlserver -c tibftlserver-cluster-dr.yaml -n DRSRV1
tibftlserver -c tibftlserver-cluster-dr.yaml -n DRSRV2
tibftlserver -c tibftlserver-cluster-dr.yaml -n DRSRV3
```

### `tibftlserver-cluster-secure.yaml`

Extends each realm server's `ftlserver.properties` block with TLS and auth settings, above the logging settings every cluster YAML already carries. Auth mode is determined by the Apache Kafka listener types:

| Apache Kafka auth | FTL secure YAML mode |
|---|---|
| `sasl_tls` (PLAIN) | `auth.providers: file:<auth-users-file>` + TLS fields |
| `oauth_tls` (OAUTHBEARER) | `auth.providers: oauth2` + `oauth2.*` globals and per-server properties |
| `tls` / `mtls` only | TLS fields only, no `auth.providers` |

When mTLS flags are provided alongside OAuth, the secure YAML includes both sets of FTL properties.

In oauth2 mode the realm credentials (`-realm-service-user` / `-realm-service-password`) are written
onto each per-server `- realm:` entry, since there is no shared `services:` block to hold them.

### `ftlserver.json`

Contains `kof.cluster.N` clusters (`kof_enabled: true`), three stores per cluster (`kof.data.store.N`, `kof.sync.store.N`, `kof.meta.store.N`), and FTL Servers distributed across clusters.

Each cluster is generated with `disk_persistence: async`; `-disk-persistence` selects `sync` or
`in-memory` instead. Every store then overrides the cluster explicitly:

| Store | `disk_persistence` |
|---|---|
| `kof.data.store.N` | `async` — bulk message path, tuned for throughput |
| `kof.sync.store.N` | `sync` |
| `kof.meta.store.N` | `sync` |

The split holds whether the cluster is `async` or `sync`, so the sync and meta stores are durable
even when the bulk path is not. `-disk-persistence in-memory` is the exception: an override would
put the stores back on disk and make the mode a no-op, so there the stores are in-memory too — the
key is omitted and each store inherits the cluster. In-memory also turns off the cluster's
`disk_index` and `disk_compact`, since an index on disk requires `sync` or `async` persistence.

No upload step is needed: every generated cluster YAML names this file through
`initial.realm.config` on each per-server `- realm:` entry (see above), so `tibftlserver` seeds the
realm from it at startup. Upload manually only to push a *hand-edited* `ftlserver.json` to a realm that
is already running:

```sh
tibrealmadmin --server localhost:5600 --realm _default_realm upload-realm ftlserver.json
```

In DR mode, each cluster has `dr_enabled: true` and two persistence sets: `_setA` (primary) and `_DRset` (DR replicas).

### `kof.broker.N.properties`

One file per FTL Server (N is 1-based). Contains only properties that pass the FSK broker properties whitelist: listener/security keys in the section 1 allowlist, plus general broker/topic/tuning keys. Listener keys appear first, followed by remaining properties in their original order.

Every file carries a `node.id`, because the FTL Server refuses to start without one
(`kof.broker.properties: node.id is required and must be a non-negative integer`). The tool
guarantees it, and the `# Node ID:` header says where the value came from:

| Input | `# Node ID:` header | Notes |
|---|---|---|
| `node.id=N` (KRaft) | `# Node ID: N` | used as-is |
| `broker.id=N` (ZooKeeper mode) | `# Node ID: N (from broker.id; FSK reads the KRaft spelling node.id)` | renamed in place, keeping its position in the file; `broker.id` is *not* written to `unsupported.properties`, since its value was used |
| neither, or a negative id such as `broker.id=-1` | `# Node ID: N (assigned by the tool; the source named no usable node.id)` | the tool assigns the lowest id not already taken by another input file |

If both `node.id` and `broker.id` are present, `node.id` wins and `broker.id` is dropped as unsupported.

### `unsupported.properties`

Written when any input properties are not in the FSK whitelist. Contains KRaft cluster-control keys (`process.roles`, `controller.*`, etc.), ZooKeeper-mode keys (`zookeeper.*` — FSK holds cluster membership and metadata in FTL rather than ZooKeeper), and security-domain keys not on the section 1 allowlist (passwords, JAAS configs, handler classes, etc.). Kept for reference — the FTL Server does not load this file.

---

## Example configs

| Directory | Nodes | Auth | FTL Servers |
|---|---|---|---|
| `examples/01-single-node-plaintext/` | 1 (broker+controller) | PLAINTEXT | 1 |
| `examples/02-3broker-plaintext/` | 3 (broker+controller) | PLAINTEXT | 3 |
| `examples/03-zk-single-node-plaintext/` | 1 (ZooKeeper mode) | PLAINTEXT | 1 |
| `examples/04-zk-3broker-plaintext/` | 3 (ZooKeeper mode) | PLAINTEXT | 3 |
| `examples/05-single-node-sasl/` | 1 (broker+controller) | SASL_SSL PLAIN | 1 |
| `examples/06-single-node-oauth/` | 1 (broker+controller) | SASL_SSL OAUTHBEARER | 1 |
| `examples/07-3broker-sasl/` | 3 (broker+controller) | SASL_SSL PLAIN | 3 |
| `examples/08-3broker-tls-only/` | 3 (broker+controller) | SSL only (no SASL) | 3 |
| `examples/09-3broker-multi-sasl/` | 3 (broker+controller) | PLAIN + OAuth2 + mTLS | 3 |
| `examples/10-3broker-multi-listener/` | 3 (broker+controller) | PLAIN + OAuth2 + mTLS (per-listener client auth) | 3 |
| `examples/11-9broker-scale/` | 9 (nodes 1–3 controller) | SASL_SSL PLAIN + OAuth2 + mTLS (no FTL security flags passed) | 9 (3 shards) |
| `examples/12-9broker-secure/` | 9 (nodes 1–3 controller) | PLAIN + OAuth2 + mTLS (full stack) | 9 (3 shards) |
| `examples/13-3broker-dr/` | 3 (broker+controller) | PLAINTEXT + DR | 3 |
| `examples/14-3broker-sasl-basic/` | 3 (broker+controller) | SASL_SSL PLAIN (single listener) | 3 |
| `examples/15-3broker-mtls/` | 3 (broker+controller) | SSL mTLS only (`ssl.client.auth=required`) | 3 |
| `examples/16-3broker-oauth2/` | 3 (broker+controller) | SASL_SSL OAUTHBEARER (single listener) | 3 |
| `examples/17-3broker-sasl+mtls/` | 3 (broker+controller) | SASL_SSL PLAIN + SSL mTLS | 3 |
| `examples/18-3broker-sasl+oauth2/` | 3 (broker+controller) | SASL_SSL PLAIN + SASL_SSL OAUTHBEARER | 3 |
| `examples/19-3broker-mtls+oauth2/` | 3 (broker+controller) | SSL mTLS + SASL_SSL OAUTHBEARER | 3 |
| `examples/20-3broker-sasl+mtls+oauth2/` | 3 (broker+controller) | SASL_SSL PLAIN + SSL mTLS + SASL_SSL OAUTHBEARER | 3 |
| `examples/21-single-node-tibschemad/` | 1 (broker+controller) | PLAINTEXT + `-tibschemad` | 1 |
| `examples/22-3broker-tibschemad/` | 3 (broker+controller) | PLAINTEXT + `-tibschemad` | 3 |

Every example ships a checked-in `output/` directory, regenerated by
`examples/regen-examples.sh`.

Examples 21 and 22 take the same inputs as 01 and 02 and add only `-tibschemad`, so diffing
their outputs shows exactly what the flag contributes — the standalone case at `cluster.size: 1`
and the cluster case at `cluster.size: 3`:

```bash
diff examples/01-single-node-plaintext/output/tibftlserver_standalone.yaml \
     examples/21-single-node-tibschemad/output/tibftlserver_standalone.yaml
diff examples/02-3broker-plaintext/output/tibftlserver-cluster.yaml \
     examples/22-3broker-tibschemad/output/tibftlserver-cluster.yaml
```
