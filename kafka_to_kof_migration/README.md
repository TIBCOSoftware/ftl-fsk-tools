# Kafka to KOF Migration

This tool copies all records from a source Apache Kafka cluster to a target KOF (Kafka-on-FTL) cluster. It discovers all topics and partitions, creates them on the target, and replicates every record from the beginning offset.

## Two ways to run it

| Path | Use when | Section |
|---|---|---|
| **Command line** | Production migrations, headless or remote hosts, scripted and CI runs | [Part 1: Command line (no UI)](#part-1-command-line-no-ui) |
| **Web UI** | Demos, watching progress live, one-off local runs | [Part 2: Web UI](#part-2-web-ui) |

Both paths perform an identical migration: the dashboard is a front end that shells out to the same
`run-kafka-to-kof.sh`. Nothing in the migration requires the UI. Read
[Prerequisites](#prerequisites) first — it applies to both — then pick a part.

---

## Prerequisites

- JDK 11+
- A Kafka installation (for its client JARs). All required JARs ship with Kafka 4.x under `$KAFKA_HOME/libs/`:
  - `kafka-clients-4.2.0.jar`
  - `slf4j-api-1.7.36.jar`
  - `log4j-slf4j-impl-2.25.3.jar`, `log4j-api-2.25.3.jar`, `log4j-core-2.25.3.jar`
- A running source Kafka cluster (the one you are migrating from).
- A running target KOF cluster (the one you are migrating to) — see Step 2.

Set these in your shell before running any commands:

```bash
export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec
export KAFKA_CLASSPATH="$KAFKA_HOME/libs/*"
```

The wildcard picks up all JARs in `libs/` — no need to list them individually.

> **Note:** `KAFKA_CLASSPATH` is only read by the migration tool (`run-kafka-to-kof.sh`,
> `populate-kafka.sh`) and is not needed by the Kafka CLI scripts (`kafka-server-start.sh`,
> `kafka-topics.sh`, etc.). The demo scripts that call Kafka CLI tools unset it automatically
> to avoid classpath conflicts.

---

## Demo scenario — Insurance Provider

The `demo/` directory contains a ready-to-run insurance provider scenario with 10 topics
(3 partitions each, 1 000 messages each — 10 000 messages total):

| Topic | Description |
|---|---|
| `insurance.auto.claims` | Auto accident reports, damage assessments |
| `insurance.home.claims` | Homeowner property damage / theft claims |
| `insurance.life.events` | Policy enrollment, beneficiary changes, death claims |
| `insurance.health.claims` | Medical procedures and prescription claims |
| `insurance.commercial.claims` | Business / commercial insurance claims |
| `insurance.policy.updates` | Renewals, amendments, cancellations |
| `insurance.customer.profiles` | New customer records and profile updates |
| `insurance.premium.payments` | Premium payment transactions |
| `insurance.fraud.alerts` | Fraud detection events and risk scores |
| `insurance.audit.log` | System-wide audit trail |

**Quick start (demo, command line):**

```bash
export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec
export KAFKA_CLASSPATH="$KAFKA_HOME/libs/*"
export TIBFTLSERVER=/opt/tibco/ftl/bin/tibftlserver   # path to your tibftlserver binary

# 1. Start source Kafka (3-broker KRaft cluster)
bash demo/setup-kafka-kraft.sh
bash demo/create-topics.sh
bash demo/populate-kafka.sh             # sends 10 000 sample JSON messages

# 2. Generate KOF config from the demo broker properties
tibkafkatokof \
  --output-dir ./kof-output \
  --realm-name insurance-demo \
  --migration-config \
  demo/kraft/server-1.properties \
  demo/kraft/server-2.properties \
  demo/kraft/server-3.properties

# 3. Start KOF brokers
bash demo/start-kof-brokers.sh --output-dir ./kof-output

# 4. Replace <KOF-HOST-N> placeholders with localhost (demo runs everything locally)
sed -i '' 's/<KOF-HOST-[0-9]*>/localhost/g' kof-output/kafka-to-kof.properties

# 5. Run the migration (dry-run then live)
bash demo/run-migration.sh --output-dir ./kof-output
```

To drive the same demo from the dashboard instead, do steps 1–4 above and then start the UI — see
[Part 2](#part-2-web-ui).

---

# Part 1: Command line (no UI)

Steps 0 through 7 are a complete migration runbook. Nothing here needs Node.js, a browser, or the
`ui/` directory — only a shell, a JDK, and the Kafka client JARs from
[Prerequisites](#prerequisites). For every flag and for running the replicator without the wrapper
script, see the [Command line reference](#command-line-reference) at the end of this part.

---

## Step 0 — Ensure your source Kafka cluster is running

The migration tool reads from a live Kafka cluster, so the brokers must be accessible before you start. If your Kafka cluster is already running, skip to Step 1.

Set `KAFKA_HOME` to your Kafka installation directory first:

```bash
export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec
```

### Option A — Use the demo script (recommended for the insurance scenario)

```bash
bash demo/setup-kafka-kraft.sh          # formats storage + starts 3 KRaft brokers
bash demo/create-topics.sh              # creates 10 insurance topics (3 partitions each)
bash demo/populate-kafka.sh             # sends 10 000 JSON messages
```

To stop: `bash demo/stop-kafka.sh`

### Option B — Manual KRaft setup (Kafka 4.x KRaft, no ZooKeeper)

Format storage on each broker (first time only):

```bash
KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
$KAFKA_HOME/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c /path/to/server.properties
```

Start each broker:

```bash
$KAFKA_HOME/bin/kafka-server-start.sh /path/to/broker-1/server.properties
$KAFKA_HOME/bin/kafka-server-start.sh /path/to/broker-2/server.properties
$KAFKA_HOME/bin/kafka-server-start.sh /path/to/broker-3/server.properties
```

### Option C — ZooKeeper mode (Kafka 2.x – 3.x)

Start ZooKeeper first:

```bash
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```

Then start each broker:

```bash
$KAFKA_HOME/bin/kafka-server-start.sh /path/to/broker-1/server.properties
$KAFKA_HOME/bin/kafka-server-start.sh /path/to/broker-2/server.properties
$KAFKA_HOME/bin/kafka-server-start.sh /path/to/broker-3/server.properties
```

Verify the cluster is healthy before migrating:

```bash
unset CLASSPATH   # required — older JARs in $CLASSPATH cause NoSuchMethodError
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server <broker-host>:9092 --list
```

---

## Step 1 — Generate KOF configuration from your Kafka server.properties

Run `tibkafkatokof` (from the `kafka_to_kof_config/` directory of this repo) against your Kafka
broker `server.properties` files — one file per broker:

```bash
tibkafkatokof \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --migration-config \
  /path/to/broker-1/server.properties \
  /path/to/broker-2/server.properties \
  /path/to/broker-3/server.properties
```

This generates in `./kof-output/`:

| File | Purpose |
|---|---|
| `kof-cluster.yaml` | FTL pserver cluster config (primary 3 pservers) |
| `kof-cluster-aux1.yaml` | Additional pserver groups (one per extra 3 pservers) |
| `realm.json` | FTL realm with `kof.cluster` definitions |
| `kof.broker.N.properties` | Per-pserver Kafka broker properties |
| **`kafka-to-kof.properties`** | **Migration config pre-filled with source broker addresses** |

`kafka-to-kof.properties` has `source.bootstrap.servers` pre-filled from your input files
and `target.bootstrap.servers` with `<KOF-HOST-N>` placeholders for each pserver.

---

## Step 2 — Start the KOF pservers

Deploy `kof-cluster.yaml` (and `kof-cluster-auxN.yaml` if present) to your KOF hosts.
Start one `tibftlserver` process per server entry:

```bash
# On each KOF host — replace SRV1/SRV2/SRV3 with the server name for that host
tibftlserver -c kof-cluster.yaml -n SRV1
tibftlserver -c kof-cluster.yaml -n SRV2
tibftlserver -c kof-cluster.yaml -n SRV3
```

Wait until all pservers report quorum before proceeding.

For the demo scenario there is a helper that starts all three locally — see
[Starting KOF brokers (demo helper)](#starting-kof-brokers-demo-helper).

---

## Step 3 — Upload the realm configuration

On any KOF host (or a machine that can reach the realm server):

```bash
tibrealmadmin \
  --server <KOF-HOST-1>:<realm-port> \
  --realm my-realm \
  upload-realm ./kof-output/realm.json
```

The realm port is the first `core.servers` port in `kof-cluster.yaml` (e.g. `5600`).

---

## Step 4 — Configure the migration

Edit `./kof-output/kafka-to-kof.properties` — it looks like:

```properties
# kafka-to-kof.properties — generated by tibkafkatokof
source.bootstrap.servers=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
target.bootstrap.servers=<KOF-HOST-1>:9092,<KOF-HOST-2>:9092,<KOF-HOST-3>:9092
...
```

Replace each `<KOF-HOST-N>` with the actual hostname of the corresponding KOF pserver:

```properties
source.bootstrap.servers=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
target.bootstrap.servers=kof-host-1:9092,kof-host-2:9092,kof-host-3:9092
```

Every tunable key is listed in the [Config reference](#config-reference). If your source Kafka
cluster uses SASL or TLS, add the security pass-through keys — see [Security](#security-sasltls).

---

## Step 5 — Dry run

Verify the migration scope before copying any data:

```bash
cd kafka_to_kof_migration   # directory containing run-kafka-to-kof.sh
./run-kafka-to-kof.sh \
  --config ../kof-output/kafka-to-kof.properties \
  --dry-run
```

The tool prints per-topic and per-partition record counts from the source cluster.
No records are written to KOF. Review the output and confirm the topic list and
counts look correct.

---

## Step 6 — Run the migration

```bash
./run-kafka-to-kof.sh --config ../kof-output/kafka-to-kof.properties
```

The tool:
1. Discovers all matching topics and partitions on the source.
2. Prints pre-copy message counts.
3. Creates any missing topics on the target KOF cluster.
4. Reads all records from each partition (from offset 0) and publishes them to KOF.
5. Prints post-copy planned vs. published counts.

---

## Step 7 — Verify

The tool exits 0 on success. Confirm the published counts match the pre-copy counts
in the output. If any counts diverge, re-run the migration — the tool reads from
the beginning offset each time, so re-runs are idempotent (records may be
duplicated if KOF already has data; use `--dry-run` first to assess).

---

## Command line reference

`run-kafka-to-kof.sh` is the only entry point the migration needs. It compiles the Java sources into
`build/` and then runs the replicator, forwarding every argument through unchanged:

```bash
./run-kafka-to-kof.sh [options]
```

With no arguments it falls back to `conf/kafka-to-kof.properties` next to the script, so pass
`--config` whenever your properties file lives elsewhere (as it does after Step 1).

`KAFKA_CLASSPATH` must be set or the script exits 1 before compiling. On this path it is **not**
derived from `KAFKA_HOME` — that convenience exists only in the UI server.

### Options

Every config key has a matching flag, and the flag always wins over the file.

| Flag | Config key | Default | Description |
|---|---|---|---|
| `--config <path>` | — | `conf/kafka-to-kof.properties` | Properties file to read |
| `--source-bootstrap <list>` | `source.bootstrap.servers` | _(from file)_ | Source Kafka bootstrap servers |
| `--target-bootstrap <list>` | `target.bootstrap.servers` | _(from file)_ | Target KOF bootstrap servers |
| `--topic-pattern <regex>` | `topic.pattern` | `.*` | Topic name regex filter |
| `--include-internal <bool>` | `include.internal` | `false` | Include internal topics (e.g. `__consumer_offsets`) |
| `--replication-factor <n>` | `target.replication.factor` | `1` | Replication factor for topics created on KOF |
| `--client-id <id>` | `client.id` | `kafka-to-kof-replicator` | Kafka client id prefix |
| `--request-timeout-ms <ms>` | `request.timeout.ms` | `60000` | Kafka request timeout |
| `--batch-size <n>` | `batch.size` | `1000` | Records per partition flushed before the next batch |
| `--max-empty-polls <n>` | `max.empty.polls` | `15` | Empty polls before a partition counts as drained |
| `--dry-run` | `dry.run` | `false` | Print stats only — create nothing, publish nothing |
| `--help`, `-h` | — | — | Print usage and exit |

Flags that take a value require it as the next argument; `--dry-run` and `--help` take none. An
unrecognized flag is a hard error rather than a warning, so a typo stops the run before any data
moves.

### Minimal end-to-end run

Against clusters that already exist, with no demo scripts and no dashboard:

```bash
export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec
export KAFKA_CLASSPATH="$KAFKA_HOME/libs/*"

cd kafka_to_kof_migration

# 1. Confirm scope — reads the source, writes nothing
./run-kafka-to-kof.sh --config /path/to/kafka-to-kof.properties --dry-run

# 2. Migrate
./run-kafka-to-kof.sh --config /path/to/kafka-to-kof.properties
```

Or skip the properties file entirely and pass both endpoints inline:

```bash
./run-kafka-to-kof.sh \
  --source-bootstrap kafka-broker-1:9092,kafka-broker-2:9092 \
  --target-bootstrap kof-host-1:9092,kof-host-2:9092 \
  --topic-pattern 'insurance\..*' \
  --dry-run
```

Note that SASL/TLS settings have no flag equivalents — they are prefixed properties and must come
from a `--config` file. See [Security](#security-sasltls).

### Running without the wrapper script

The wrapper only adds `javac` plus a `java -cp` invocation. Where bash is unavailable, or to
compile once and run many times, call the class directly:

```bash
# Compile once
mkdir -p build
javac -d build -cp "$KAFKA_CLASSPATH" $(find src/main/java -name '*.java')

# Run
java -cp "build:$KAFKA_CLASSPATH" com.tibco.ftl.kof.KafkaToKofReplicatorApp \
  --config /path/to/kafka-to-kof.properties --dry-run
```

The exit code is 0 on success and non-zero on failure, so either form drops straight into a cron
job or CI pipeline.

---

# Part 2: Web UI

An optional dashboard in the `ui/` directory. It does not replace any step in
[Part 1](#part-1-command-line-no-ui) — Steps 0 through 4 (start Kafka, generate the KOF config,
start the pservers, upload the realm, fill in the host placeholders) still happen on the command
line. What the dashboard replaces is Steps 5 and 6: it runs `run-kafka-to-kof.sh` for you and
streams the output back to the browser.

## What the dashboard provides

- **SVG mesh topology** — 3-node Kafka cluster (left) and 3-node KOF cluster (right); nodes turn green as soon as each process starts.
- **Animated data conduit** — canvas particle stream showing topic data flowing from Kafka to KOF in real time.
- **Real-time shell output** — WebSocket-streamed stdout/stderr from `run-kafka-to-kof.sh`.
- **Config form** — pre-populated from `kof-output/kafka-to-kof.properties`; runs Dry Run or Live Migration directly.

## Starting the UI

```bash
cd ui
npm install          # first time only — installs Express, kafkajs, ws
node server.js
```

Open **http://localhost:3000** in your browser.

Node.js is required for this path only; the command line path does not need it.

## Environment variables

These are read by the UI server (`ui/server.js`) and do not apply to
`run-kafka-to-kof.sh`:

| Variable | Default | Description |
|---|---|---|
| `SOURCE_BOOTSTRAP` | read from `kof-output/kafka-to-kof.properties` | Source Kafka bootstrap addresses |
| `TARGET_BOOTSTRAP` | read from `kof-output/kafka-to-kof.properties` | Target KOF bootstrap addresses |
| `KAFKA_HOME` | _(none)_ | Path to your Kafka installation (e.g. `/usr/local/Cellar/kafka/4.2.0/libexec`). Used to auto-build `KAFKA_CLASSPATH` from `$KAFKA_HOME/libs/*` if `KAFKA_CLASSPATH` is not set. |
| `KAFKA_CLASSPATH` | auto-built from `KAFKA_HOME/libs/*` | Kafka client JARs for the migration tool. Explicit value takes precedence over auto-detection from `KAFKA_HOME`. |
| `PORT` | `3000` | HTTP port for the dashboard |

The `KAFKA_HOME` fallback is a UI-server convenience: it resolves a classpath and injects it into
the environment of the `run-kafka-to-kof.sh` child process. Invoking that script yourself gives you
no such fallback — export `KAFKA_CLASSPATH` explicitly.

## Health and metadata reporting

The KOF cluster health check uses TCP probes on the FTL `core.servers` ports (`5635`, `5620`, `5623`),
so KOF nodes turn green as soon as `tibftlserver` starts — before the Kafka protocol layer is ready.
Kafka metadata (topic counts) uses the `kafkajs` Admin API and updates every 10 seconds.

---

# Reference (both paths)

## Config reference

| Key | Default | Description |
|---|---|---|
| `source.bootstrap.servers` | _(from tibkafkatokof)_ | Source Kafka cluster addresses |
| `target.bootstrap.servers` | _(from tibkafkatokof)_ | Target KOF cluster addresses |
| `topic.pattern` | `.*` | Regex filter for topic names |
| `include.internal` | `false` | Include Kafka internal topics (e.g. `__consumer_offsets`) |
| `target.replication.factor` | `1` | Replication factor for newly created topics on KOF |
| `client.id` | `kafka-to-kof-replicator` | Kafka client ID |
| `request.timeout.ms` | `60000` | Kafka request timeout |
| `max.empty.polls` | `15` | Empty poll retries before declaring a partition drained |
| `dry.run` | `false` | Stats only — no topics created, no records published |
| `batch.size` | `1000` | Records per partition flushed to KOF before the next batch |

---

## Security (SASL/TLS)

Pass Kafka security settings with prefixed keys in the config file. The prefix
routes the property to the right Kafka client (`source.admin`, `source.consumer`,
`target.admin`, `target.producer`).

### SASL/PLAIN example (source cluster)

```properties
source.admin.security.protocol=SASL_SSL
source.admin.sasl.mechanism=PLAIN
source.admin.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="secret";
source.consumer.security.protocol=SASL_SSL
source.consumer.sasl.mechanism=PLAIN
source.consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="secret";
```

### TLS-only example (target KOF cluster)

```properties
target.admin.security.protocol=SSL
target.admin.ssl.truststore.location=/path/to/truststore.jks
target.admin.ssl.truststore.password=changeit
target.producer.security.protocol=SSL
target.producer.ssl.truststore.location=/path/to/truststore.jks
target.producer.ssl.truststore.password=changeit
```

KOF pservers use PEM certificates for TLS (generated by `tibkafkatokof` with `--tls-cert`).
If your KOF cluster is TLS-enabled, point the migration tool at the corresponding CA PEM:

```properties
target.admin.security.protocol=SSL
target.admin.ssl.ca.location=/path/to/ca.pem
target.producer.security.protocol=SSL
target.producer.ssl.ca.location=/path/to/ca.pem
```

---

## Starting KOF brokers (demo helper)

For the demo scenario, use:

```bash
export TIBFTLSERVER=/opt/tibco/ftl/bin/tibftlserver   # or put tibftlserver on PATH
bash demo/start-kof-brokers.sh --output-dir ./kof-output
```

This starts SRV1 / SRV2 / SRV3 from `kof-output/kof-cluster.yaml` and saves PIDs to
`demo/kof-brokers.pid`.

To stop: `bash demo/stop-kof-brokers.sh`
