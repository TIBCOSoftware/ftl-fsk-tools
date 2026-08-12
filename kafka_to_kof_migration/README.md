# Kafka to KOF Migration

This tool copies all records from a source Apache Kafka cluster to a target KOF (Kafka-on-FTL) cluster. It discovers all topics and partitions, creates them on the target, and replicates every record from the beginning offset.

Steps 1 through 7 below are a complete migration runbook. Nothing here needs Node.js, a browser, or
the `ui/` directory — only a shell, a JDK, and the Kafka client JARs. For every flag and for
running the replicator without the wrapper script, see the
[Command line reference](#command-line-reference).

> There is also an optional web dashboard that runs the same migration and streams its output to a
> browser. See [README-UI.md](README-UI.md).

---

## Prerequisites

- JDK 11+
- A Kafka installation (for its client JARs). All required JARs ship with Kafka 4.x under `$KAFKA_HOME/libs/`:
  - `kafka-clients-4.2.0.jar`
  - `slf4j-api-1.7.36.jar`
  - `log4j-slf4j-impl-2.25.3.jar`, `log4j-api-2.25.3.jar`, `log4j-core-2.25.3.jar`
- A running source Kafka cluster (the one you are migrating from).
- A running target KOF cluster (the one you are migrating to) — see Step 3.

Set these in your shell before running any commands:

```bash
export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec
export KAFKA_CLASSPATH="$KAFKA_HOME/libs/*"
```

The wildcard picks up all JARs in `libs/` — no need to list them individually.

> **Note:** `KAFKA_CLASSPATH` is only read by the migration tool (`run-kafka-to-kof.sh`) and is not
> needed by the Kafka CLI scripts (`kafka-server-start.sh`, `kafka-topics.sh`, etc.). Keeping it out
> of `CLASSPATH` avoids classpath conflicts when running those CLI tools.

---

## Step 1 — Ensure your source Kafka cluster is running

The migration tool reads from a live Kafka cluster, so the brokers must be accessible before you
start. **If your Kafka cluster is already running and reachable, skip to Step 2** — you do not need
to restart or reconfigure anything to migrate from it.

To bring up a cluster yourself, set `KAFKA_HOME` first:

```bash
export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec
```

### Option A — KRaft mode (Kafka 4.x, no ZooKeeper)

The `kafka-examples/` directory here holds ready-to-run KRaft configurations, so you do not have to
supply your own to get started:

| Layout | Config files | Bootstrap servers |
|---|---|---|
| `single-node` | `kafka-examples/single-node/server.properties` | `localhost:9092` |
| `three-node` | `kafka-examples/three-node/server-1.properties` … `server-3.properties` | `localhost:9092,localhost:9093,localhost:9094` |

Start either one with:

```bash
bash kafka-examples/start-kafka.sh single-node    # or: three-node
```

The script formats the KRaft storage directories, starts one broker per config file, waits until
the cluster answers, and records the PIDs in `kafka-examples/kafka-examples.pid`. Pass `--clean` to
wipe the data directories and start from empty. Stop with:

```bash
bash kafka-examples/stop-kafka.sh
```

Both layouts bind the same ports, so only one can run at a time. Broker data lives under
`/tmp/kafka-examples/` and broker logs under `/tmp/kafka-examples/logs/`.

Use `single-node` for the quickest path through this runbook; use `three-node` to exercise a
multi-broker source that maps onto a 3-pserver KOF cluster.

**To bring up your own brokers instead**, format each one and start it — all brokers in a KRaft
cluster must be formatted with the *same* cluster ID:

```bash
KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
for n in 1 2 3; do
  $KAFKA_HOME/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c /path/to/broker-$n/server.properties
done

$KAFKA_HOME/bin/kafka-server-start.sh /path/to/broker-1/server.properties
$KAFKA_HOME/bin/kafka-server-start.sh /path/to/broker-2/server.properties
$KAFKA_HOME/bin/kafka-server-start.sh /path/to/broker-3/server.properties
```

Formatting is a first-time-only step; restarting a formatted broker just needs
`kafka-server-start.sh`.

### Option B — ZooKeeper mode (Kafka 2.x – 3.x)

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

### Verify the cluster

Whichever mode you use, confirm the cluster is healthy before migrating:

```bash
unset CLASSPATH   # required — older JARs in $CLASSPATH cause NoSuchMethodError
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server <broker-host>:9092 --list
```

---

## Step 2 — Generate KOF configuration from your Kafka server.properties

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

If you started one of the example clusters in Step 1, point the tool at those config files:

```bash
# single-node
tibkafkatokof --output-dir ./kof-output --realm-name my-realm --migration-config \
  kafka-examples/single-node/server.properties

# three-node
tibkafkatokof --output-dir ./kof-output --realm-name my-realm --migration-config \
  kafka-examples/three-node/server-1.properties \
  kafka-examples/three-node/server-2.properties \
  kafka-examples/three-node/server-3.properties
```

This generates in `./kof-output/`:

| File | Purpose |
|---|---|
| `kof-cluster.yaml` | FTL pserver cluster config (primary 3 pservers); also seeds the realm |
| `kof-cluster-aux1.yaml` | Additional pserver groups (one per extra 3 pservers) |
| `realm.json` | FTL realm with `kof.cluster` definitions |
| `kof.broker.N.properties` | Per-pserver Kafka broker properties |
| **`kafka-to-kof.properties`** | **Migration config pre-filled with source broker addresses** |

`kafka-to-kof.properties` has `source.bootstrap.servers` pre-filled from your input files
and `target.bootstrap.servers` with `<KOF-HOST-N>` placeholders for each pserver.

Every server entry in `kof-cluster.yaml` names `realm.json` through the `initial.realm.config`
parameter, so the realm configuration is loaded for you when the servers start — there is no
separate upload step. See Step 3.

---

## Step 3 — Start the KOF servers

Deploy `kof-cluster.yaml` (and `kof-cluster-auxN.yaml` if present) to your KOF hosts, then start one
`tibftlserver` process per `SRV` entry in that file. Step 2 decides how many there are:
`tibkafkatokof` emits one server per source broker `server.properties` you passed it. The generated
YAML also lists the exact commands in its header comment.

### Single server

One source broker produces a one-server cluster:

```yaml
# kof-cluster.yaml
globals:
  core.servers:
    SRV1: localhost:5606

servers:
  SRV1:
  - realm:
      data: /var/tmp/kof/data
      initial.realm.config: realm.json
  - persistence:
      name: pserver1
      data: /var/tmp/kof/data/pserver1
      kof.broker.properties: kof.broker.1.properties
```

Start it with a single command:

```bash
tibftlserver -c kof-cluster.yaml -n SRV1
```

That one process hosts both the realm and `pserver1`. There is no quorum to wait for — the cluster
is ready once the pserver reports started. This pairs with the `single-node` Kafka example from
Step 1 and is the quickest way through the rest of this runbook.

> **Running KOF on the same host as the source Kafka?** Change the KOF broker port first.
> `tibkafkatokof` copies each source broker's `listeners` verbatim into
> `kof.broker.N.properties`, so a source broker on `localhost:9092` produces a KOF broker on
> `localhost:9092` too — they fight over the port, and the migration would read from and write to
> the same endpoint. There is no flag for this; edit the generated file before starting the server:
>
> ```bash
> # in kof-output/kof.broker.1.properties
> listeners=PLAINTEXT://localhost:19092
> advertised.listeners=PLAINTEXT://localhost:19092
> ```
>
> Then use the new port in `target.bootstrap.servers` in Step 4. This applies to the three-server
> layout as well (`9092/9093/9094` on both sides). It is not an issue when KOF runs on its own
> hosts, which is the normal production case.

### Three servers

Three source brokers produce `SRV1`, `SRV2`, and `SRV3`, each hosting one pserver:

```bash
# On each KOF host — replace SRV1/SRV2/SRV3 with the server name for that host
tibftlserver -c kof-cluster.yaml -n SRV1
tibftlserver -c kof-cluster.yaml -n SRV2
tibftlserver -c kof-cluster.yaml -n SRV3
```

Wait until all pservers report quorum before proceeding.

**No realm upload is needed.** Each `- realm:` entry in the generated YAML carries
`initial.realm.config`, pointing at the generated `realm.json`:

```yaml
servers:
  SRV1:
  - realm:
      data: /var/tmp/kof/data
      initial.realm.config: realm.json
```

`tibftlserver` reads that file at startup and seeds the realm itself, so running
`tibrealmadmin upload-realm` is redundant here. You only need a manual upload to push a
*hand-edited* `realm.json` to a realm that is already running:

```bash
tibrealmadmin --server <KOF-HOST-1>:<realm-port> --realm my-realm upload-realm ./kof-output/realm.json
```

The realm port is that server's `core.servers` port in `kof-cluster.yaml` — `5606` in the
single-server example above. `tibkafkatokof` picks these randomly from the range 5600–5699, so read
them out of the generated file rather than assuming a value. Pass `--core-servers` in Step 2 to pin
them instead:

```bash
--core-servers "SRV1=localhost:5600"                                        # single server
--core-servers "SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602"  # three servers
```

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

For a one-server KOF cluster this is a single entry. If you moved the KOF broker off 9092 to avoid
the same-host collision described in Step 3, use the port you chose — the ports here must match
`advertised.listeners` in `kof.broker.N.properties`:

```properties
source.bootstrap.servers=localhost:9092
target.bootstrap.servers=localhost:19092
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
`--config` whenever your properties file lives elsewhere (as it does after Step 2).

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

Against clusters that already exist:

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
