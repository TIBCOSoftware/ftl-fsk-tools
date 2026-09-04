---
id: getting-started
title: Getting Started with tibftlimportconfig
sidebar_label: Getting Started
---

# Getting Started with `tibftlimportconfig`

`tibftlimportconfig` converts Kafka broker `server.properties` files into the FTL artifacts
needed to run a FSK-enabled FTL Server cluster. Pass one properties file per broker; the
tool generates:

| File | Purpose |
|---|---|
| `tibftlserver-cluster.yaml` | FTL Server cluster configuration |
| `ftlserver.json` | FTL realm with `kof.cluster` definition |
| `kof.broker.N.properties` | Broker properties, one per FTL Server (1-based) |
| `unsupported.properties` | Settings with no FSK equivalent (reference only) |
| `tibftlserver-cluster-secure.yaml` | TLS/auth overlay (when security flags are provided) |

A single-broker conversion produces one FTL Server — a standalone server rather than a cluster — so
its YAMLs are named `tibftlserver-standalone.yaml` and `tibftlserver-standalone-secure.yaml`.

Each scenario below builds on the previous one. Start with the simplest setup and
advance as your environment requires.

## Before you start

Every scenario runs end to end: bring up the Apache Kafka brokers the `server.properties`
describes, stop them, convert the configuration, then start the FTL Servers on the result. Each
scenario is a numbered sequence of steps, so *Scenario 5, Step 2* means the second step of the
SASL/PLAIN-over-TLS walkthrough. Three things hold for all twelve.

**`tibftlimportconfig`.** Every `tibftlimportconfig` command below is the binary checked in at
[`bin/tibftlimportconfig`](bin/) (linux/amd64, statically linked) — a clone needs no Go toolchain
and no build step. Put it on your `PATH`, or invoke it by path:

```bash
export PATH=/path/to/ftl-fsk-tools/tibfsk/importconfig/bin:$PATH
```

**`KAFKA_HOME`.** The Kafka commands assume a Kafka 4.x installation. Scenarios 3 and 4 are the
exception: they run Kafka in ZooKeeper mode, which 4.x removed, so those two need `KAFKA_HOME`
pointed at Kafka 3.9 or earlier.

```bash
export KAFKA_HOME=/opt/kafka
```

**Clear any inherited `CLASSPATH`.** `kafka-run-class.sh` *appends* Kafka's own `libs/` to whatever
`CLASSPATH` your shell already exports, so those jars are searched first and can shadow the ones
Kafka ships — an older `snakeyaml` or `jackson-dataformat-yaml` is enough to kill the broker before
it reads a line of your `server.properties`. Start Kafka from a shell where the variable is unset:

```bash
unset CLASSPATH
```

---

## Scenario 1 — Single-node plaintext (KRaft)

The simplest possible configuration: one broker, no authentication, no TLS. Use this
for local development and tool exploration only.

:::tip Already running a single-node KRaft broker?
Steps 1 and 2 only build and start the example broker. If you already have a running Kafka
broker or brokers, skip them and start at **Step 3 — Stop Apache Kafka**, then hand your own
`server.properties` to the tool in Step 4 instead of `server-1.properties`. The tool reads the
file; it never contacts the running broker.
:::

### Step 1 — Kafka server.properties

Save this as **`server-1.properties`**, in the directory you will work from. Step 2 formats and
starts the broker with it, and Step 4 passes that same file to `tibftlimportconfig` by name.

```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093

listeners=CLIENT://localhost:9092,CONTROLLER://localhost:9093
inter.broker.listener.name=CLIENT
advertised.listeners=CLIENT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT

log.dirs=/var/tmp/kafka/scenario1/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

:::tip Listener naming
Name your client-facing listener `CLIENT` (not `PLAINTEXT`). The tool strips the
`CONTROLLER` listener automatically — FTL carries controller traffic natively.
:::

### Step 2 — Start Apache Kafka (KRaft)

Format the storage directory once, then start the broker. KRaft needs a cluster ID; generate one
and keep it — reformatting with a different ID discards the log directory's contents.

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

"$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted --standalone \
  -t "$KAFKA_CLUSTER_ID" -c server-1.properties

"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon server-1.properties
```

Confirm it is up:

```bash
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
  --bootstrap-server localhost:9092 | grep 'id:'
```

```
localhost:9092 (id: 1 rack: null isFenced: false) -> (
```

`--ignore-formatted` makes the format step a no-op on an already-formatted directory, so the
sequence is safe to re-run. `--standalone` is what declares this node the sole member of the
controller quorum; without it, `kafka-storage.sh` refuses to format a config that names
`controller.quorum.bootstrap.servers` but no voters, and the broker then dies on startup with
*No readable meta.properties files found*.

### Step 3 — Stop Apache Kafka

The FTL Server that replaces the broker binds port 9092, the port the broker is on, so the two
cannot run at once:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

Stopping the broker here rather than after the conversion keeps the rest of the scenario in one
direction: everything from this point on is FSK.

### Step 4 — Run the config migration tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario1/data \
  server-1.properties
```

### Output

```
Writing file: ./kof-output/kof.broker.1.properties [
Writing file: ./kof-output/unsupported.properties
Writing file: ./kof-output/tibftlserver-standalone.yaml
Writing file: ./kof-output/ftlserver.json

All kof.broker.*.properties files are processed successfully.
```

The `unsupported.properties` file lists any settings that have no FSK equivalent
(such as the `CONTROLLER` listener entry). Review it for reference — those settings
do not affect FSK behavior.

`--data-dir` does not appear in that list: it names the directories the FTL Server will create at
startup, not files the tool writes now. In the generated YAML the realm service takes
`/var/tmp/kof/scenario1/data/srv1` and the persistence service `/var/tmp/kof/scenario1/data/pserver1`.

### Step 5 — Start the FTL Server

A single broker converts to a standalone server rather than a cluster, so there is one process to
start, named for the single entry under `servers:`:

```bash
tibftlserver -c kof-output/tibftlserver-standalone.yaml -n SRV1
```

Run this from the directory you ran `tibftlimportconfig` in: the YAML records `--output-dir` exactly
as it was passed, so `kof-output/ftlserver.json` resolves against the working directory and not
against the YAML's own location — `cd kof-output` first and the server will not find its realm.

No realm upload step: the YAML points `initial.realm.config` at the generated `ftlserver.json`, so the
server seeds the realm itself on first startup. Kafka clients can now connect to `localhost:9092`
as before.

Confirm both halves are up — the FTL Server, then the Kafka port it now serves:

```bash
FTLS="http://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-standalone.yaml)"

tibftladmin --ftlserver "$FTLS" --available
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
  --bootstrap-server localhost:9092 | grep 'id:'
```

```
FTLserver is available
localhost:9092 (id: 1 rack: null isFenced: false) -> (
```

That second line is the same command that checked the broker earlier, against the same port,
now answered by FSK.

### Step 6 — Shut down the FTL Server

Nothing carries over to the next scenario, so stop the FTL Server: it is holding both the realm port and the Kafka port the next scenario wants.

```bash
FTLS="http://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-standalone.yaml)"

tibftladmin --ftlserver "$FTLS" -x
```

`-x` (`--shutdown`) stops the FTL Server process.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

**→ Continue to [Scenario 2](#scenario-2--3-node-plaintext-cluster-kraft) to scale the same configuration to three brokers,
or [Scenario 3](#scenario-3--single-node-plaintext-zookeeper) if your brokers still run under ZooKeeper.**

---

## Scenario 2 — 3-node plaintext cluster (KRaft)

Scale out to a 3-broker KRaft cluster. Pass one `server.properties` file per broker;
the tool derives the FTL Server count from the file count.

:::tip Already running a 3-node KRaft cluster?
Steps 1–3 only build and start the three example brokers. If you already have a running Kafka
broker or brokers, skip them and start at **Step 4 — Stop Apache Kafka**, then hand your own
three `server.properties` files to the tool in Step 5. The tool reads the files; it never
contacts the running brokers.
:::

### Step 1 — The properties all three brokers share

Save these lines as **`server-1.properties`**, **`server-2.properties`** and
**`server-3.properties`** — the same content in all three. Step 2 adds the four lines that differ.

```properties
process.roles=broker,controller
controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113

inter.broker.listener.name=CLIENT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT

num.partitions=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

`controller.quorum.voters` is identical everywhere — each broker needs the address of all three
controllers, its own included, and a node that lists only itself forms its own quorum and never
joins the others.

### Step 2 — What differs in each broker file

Append the matching block to each file. The ports are the ones the voter list already names, and
broker *n* takes broker 1's ports plus `10 × (n − 1)`:

```properties
# server-1.properties
node.id=1
listeners=CLIENT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=CLIENT://localhost:9092
log.dirs=/var/tmp/kafka/scenario2/data/broker-1
```

```properties
# server-2.properties
node.id=2
listeners=CLIENT://localhost:9102,CONTROLLER://localhost:9103
advertised.listeners=CLIENT://localhost:9102
log.dirs=/var/tmp/kafka/scenario2/data/broker-2
```

```properties
# server-3.properties
node.id=3
listeners=CLIENT://localhost:9112,CONTROLLER://localhost:9113
advertised.listeners=CLIENT://localhost:9112
log.dirs=/var/tmp/kafka/scenario2/data/broker-3
```

Nothing else differs: the shared block from Step 1 plus one of these four-line blocks is a complete
broker configuration.

### Step 3 — Start Apache Kafka (KRaft)

All three brokers must be formatted with the **same** cluster ID — that is what makes them one
cluster rather than three. Generate it once, outside the loop:

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/scenario2/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

No `--standalone` here: the static `controller.quorum.voters` list supplies the initial voter set,
which is what that flag stands in for on a single node. (Configure the quorum with
`controller.quorum.bootstrap.servers` instead and each node has to be formatted with
`--initial-controllers` or `--no-initial-controllers`.) `LOG_DIR` keeps the three brokers from
writing over each other's `server.log`, which they otherwise all place under `$KAFKA_HOME/logs`.
`-daemon` is a plain `nohup ... &` with no PID file; each broker's stdout and stderr land in
`$LOG_DIR/kafkaServer.out`, which is the first place to look when one of them does not come up.

The quorum forms once a majority of controllers are up. Confirm:

```bash
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
  --bootstrap-server localhost:9092,localhost:9102,localhost:9112 | grep id:
```

### Step 4 — Stop Apache Kafka

The three FTL Servers take over ports 9092, 9102 and 9112, so the brokers have to give them up:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

One call stops all three. Kafka writes no PID files: `kafka-server-stop.sh` runs
`ps ax | grep ' kafka.Kafka '` and sends `SIGTERM` to every match, which is every broker JVM on the
host — including any belonging to a cluster you did not start here. It prints `No kafka server to
stop` and exits 1 when it finds none. To take down a single broker, name it:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh" --node-id=2
```

That form reads `node.id` out of each running broker's configuration file, and it finds that file
by the path the broker was started with — so run it from the directory you ran
`kafka-server-start.sh` in, or the relative `server-2.properties` will not resolve.

### Step 5 — Run the config migration tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario2/data \
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
Writing file: ./kof-output/ftlserver.json

All kof.broker.*.properties files are processed successfully.
```

The generated `tibftlserver-cluster.yaml` contains three FTL Server entries (`SRV1`, `SRV2`,
`SRV3`) with FTL ports derived from the cluster in the 5600–5799 range — the same brokers
always yield the same ports, so re-running the tool does not move them. To pin specific
ports use `--core-servers SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602`.

### Step 6 — Start the FTL Servers

One `tibftlserver` per entry under `servers:`, each in its own shell (the header comment of the
generated YAML lists these same three commands):

```bash
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV3
```

All three share one YAML and one `ftlserver.json`; `-n` is what selects which entry a process runs.
Each carries the same `initial.realm.config`, so whichever starts first seeds the realm and the
other two join it. The cluster is available once two of the three are up.

`--status` is the check worth running here, because it reports the whole cluster from whichever
member you ask:

```bash
FTLS="http://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-cluster.yaml)"

tibftladmin --ftlserver "$FTLS" --status
```

```
-----------------------
Cluster Members
-----------------------
Leader:                   SRV1

----
Name:                     SRV1
Host:                     localhost
Port:                     5600
Status:                   online
...
```

One block per FTL Server, each with its own `Status:`, and a single `Leader:` above them — so a
member that never joined shows up as a missing block rather than as silence. The Kafka side is
the same three-broker check as before, now answered by the FTL Servers:

```bash
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
  --bootstrap-server localhost:9092,localhost:9102,localhost:9112 | grep 'id:'
```

### Step 7 — Shut down the FTL Servers

Nothing carries over to the next scenario, so stop the FTL Servers: between them they hold the realm port and all three Kafka ports the next scenario wants.

```bash
FTLS="http://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-cluster.yaml)"

tibftladmin --ftlserver "$FTLS" -xc
```

`-xc` (`--shutdown_cluster`) stops every FTL Server in the cluster, so it does not matter which member you address — one call replaces three Ctrl-C's.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

**→ Continue to [Scenario 3](#scenario-3--single-node-plaintext-zookeeper) for the ZooKeeper equivalents of Scenarios 1 and 2,
or skip to [Scenario 5](#scenario-5--single-node-sasl-plain-over-tls) to start adding security.**

---

## Scenario 3 — Single-node plaintext (ZooKeeper)

Kafka 3.9 and earlier keep their cluster metadata in ZooKeeper rather than in a KRaft quorum.
The conversion is the same work — `tibftlimportconfig` reads the same file and writes the same
artifacts — but two things differ from Scenario 1, and both are worth seeing before you convert a
real ZooKeeper cluster.

**FSK has no ZooKeeper.** The cluster membership and metadata ZooKeeper holds for Kafka are
FTL-native, so every `zookeeper.*` key is routed to `unsupported.properties`. Nothing is lost
in the translation; there is simply nothing for FSK to do with them.

**ZooKeeper mode spells the broker's identity `broker.id`**, the key KRaft later renamed to
`node.id`. FSK reads `node.id` only, so the tool renames it on the way through and records
where the value came from.

:::note Kafka 4.x cannot run this scenario
ZooKeeper support was removed in Kafka 4.0, so a 4.x installation ships no
`zookeeper-server-start.sh`. Point `KAFKA_HOME` at Kafka 3.9 or earlier for Scenarios 3 and 4. The
*conversion* works whatever Kafka version is installed — only starting the brokers needs the
older release.
:::

:::tip Already running a single-node ZooKeeper broker?
Steps 1 and 2 only build and start the example broker and its ZooKeeper. If you already have a
running Kafka broker or brokers, skip them and start at **Step 3 — Stop Apache Kafka and
ZooKeeper**, then hand your own `server.properties` to the tool in Step 4. Only starting the
example broker needs Kafka 3.9 or earlier; the conversion itself works whatever version you run.
:::

### Step 1 — Kafka server.properties

Save this as **`server-1.properties`**, in the directory you will work from. Step 2 starts the
broker with it, and Step 4 passes that same file to `tibftlimportconfig` by name.

```properties
broker.id=0

zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000

listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT

log.dirs=/var/tmp/kafka/scenario3/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

No `process.roles` and no `CONTROLLER` listener: the controller is elected through ZooKeeper.
`PLAINTEXT` is the only listener and also carries inter-broker traffic, which is the one case
where the tool keeps a listener that `inter.broker.listener.name` names — stripping it would
leave the FTL Server nothing to bind. It is also why Scenario 1's advice to call the client listener
`CLIENT` rather than `PLAINTEXT` does not apply here; a ZooKeeper-mode broker conventionally
has exactly this listener.

### Step 2 — Start Apache Kafka (ZooKeeper)

No storage formatting and no cluster ID — ZooKeeper mode has neither. Start ZooKeeper first,
then the broker:

```bash
cat > zookeeper.properties <<'EOF'
dataDir=/var/tmp/zookeeper/scenario3
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF

"$KAFKA_HOME/bin/zookeeper-server-start.sh" -daemon zookeeper.properties

"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon server-1.properties
```

`admin.enableServer=false` turns off the ZooKeeper AdminServer, which otherwise binds port 8080
and collides with whatever else on the host wants it. Confirm the broker is up:

```bash
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
  --bootstrap-server localhost:9092 | grep 'id:'
```

```
localhost:9092 (id: 0 rack: null isFenced: false) -> (
```

The id is 0 here rather than 1: it is this broker's `broker.id`, and the ZooKeeper examples number
from zero.

### Step 3 — Stop Apache Kafka and ZooKeeper

The FTL Server that replaces the broker binds port 9092, so stop the broker, then ZooKeeper:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
"$KAFKA_HOME/bin/zookeeper-server-stop.sh"
```

That order matters only for the log: a broker whose ZooKeeper session drops while it is still
running writes a stream of connection failures on the way down.

### Step 4 — Run the config migration tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario3/data \
  server-1.properties
```

Identical to Scenario 1 — the tool is not told which metadata mode the source cluster used, and
does not need to be.

### Output

```
Writing file: ./kof-output/kof.broker.1.properties [
Writing file: ./kof-output/unsupported.properties
Writing file: ./kof-output/tibftlserver-standalone.yaml
Writing file: ./kof-output/ftlserver.json

All kof.broker.*.properties files are processed successfully.
```

The renamed identity is recorded in the header of `kof.broker.1.properties`, so the value can
be traced back to the key it came from:

```
# TIBCO FTL(R) Service for Kafka Server Properties
# Generated by conversion tool from: server-1.properties
# Node ID: 0 (from broker.id; FSK reads the KRaft spelling node.id)
```

and `unsupported.properties` collects the ZooKeeper keys:

```
inter.broker.listener.name=PLAINTEXT
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
```

### Step 5 — Start the FTL Server

```bash
tibftlserver -c kof-output/tibftlserver-standalone.yaml -n SRV1
```

Nothing takes ZooKeeper's place on the FSK side — the realm server the YAML starts holds the
cluster metadata itself.

### Step 6 — Shut down the FTL Server

Nothing carries over to the next scenario, so stop the FTL Server: it is holding both the realm port and the Kafka port the next scenario wants.

```bash
FTLS="http://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-standalone.yaml)"

tibftladmin --ftlserver "$FTLS" -x
```

`-x` (`--shutdown`) stops the FTL Server process.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

**→ Continue to [Scenario 4](#scenario-4--3-node-plaintext-cluster-zookeeper) for the same configuration across three brokers.**

---

## Scenario 4 — 3-node plaintext cluster (ZooKeeper)

The ZooKeeper counterpart of Scenario 2. One ZooKeeper serves all three brokers, which differ only
in `broker.id`, listener port and `log.dirs`.

Kafka 3.9 or earlier is required here too, for the same reason as Scenario 3.

:::tip Already running a 3-node ZooKeeper cluster?
Steps 1–3 only build and start ZooKeeper and the three example brokers. If you already have a
running Kafka broker or brokers, skip them and start at **Step 4 — Stop Apache Kafka and
ZooKeeper**, then hand your own three `server.properties` files to the tool in Step 5.
:::

### Step 1 — The properties all three brokers share

Save these lines as **`server-1.properties`**, **`server-2.properties`** and
**`server-3.properties`** — the same content in all three. Step 2 adds the four lines that differ.

```properties
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000

inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT

num.partitions=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

`zookeeper.connect` is identical in all three — that shared ZooKeeper is what makes them one
cluster, the way a shared cluster ID does under KRaft. A single ZooKeeper is fine for an
example; production runs an ensemble of three or five.

### Step 2 — What differs in each broker file

Append the matching block to each file. Broker *n* takes broker 1's port plus `10 × (n − 1)`:

```properties
# server-1.properties
broker.id=1
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
log.dirs=/var/tmp/kafka/scenario4/data/broker-1
```

```properties
# server-2.properties
broker.id=2
listeners=PLAINTEXT://localhost:9102
advertised.listeners=PLAINTEXT://localhost:9102
log.dirs=/var/tmp/kafka/scenario4/data/broker-2
```

```properties
# server-3.properties
broker.id=3
listeners=PLAINTEXT://localhost:9112
advertised.listeners=PLAINTEXT://localhost:9112
log.dirs=/var/tmp/kafka/scenario4/data/broker-3
```

Nothing else differs: the shared block from Step 1 plus one of these four-line blocks is a complete
broker configuration. There is no voter list to keep in step, because there is no KRaft quorum.

### Step 3 — Start Apache Kafka (ZooKeeper)

```bash
cat > zookeeper.properties <<'EOF'
dataDir=/var/tmp/zookeeper/scenario4
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF

"$KAFKA_HOME/bin/zookeeper-server-start.sh" -daemon zookeeper.properties

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/scenario4/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

Give ZooKeeper a moment to accept connections on 2181 before starting the brokers; one that
starts first will retry, but noisily. `LOG_DIR` keeps the three brokers from writing over each
other's `server.log`, which they otherwise all place under `$KAFKA_HOME/logs`. Confirm all
three registered:

```bash
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
  --bootstrap-server localhost:9092,localhost:9102,localhost:9112 | grep id:
```

### Step 4 — Stop Apache Kafka and ZooKeeper

The three FTL Servers take over ports 9092, 9102 and 9112, so the brokers have to give them up —
then ZooKeeper, which nothing on the FSK side replaces:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
"$KAFKA_HOME/bin/zookeeper-server-stop.sh"
```

`kafka-server-stop.sh` stops all three at once: it sends `SIGTERM` to every broker JVM on the
host, so run it before ZooKeeper and check nothing else you care about was caught in it.

### Step 5 — Run the config migration tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario4/data \
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
Writing file: ./kof-output/ftlserver.json

All kof.broker.*.properties files are processed successfully.
```

Three FTL Servers, exactly as in Scenario 2. Each broker's `broker.id` becomes the `node.id` of the
matching `kof.broker.N.properties`, and the `zookeeper.*` keys are collected once in
`unsupported.properties` rather than repeated per broker.

### Step 6 — Start the FTL Servers

One `tibftlserver` per entry under `servers:`, each in its own shell:

```bash
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV3
```

All three share one YAML and one `ftlserver.json`; whichever starts first seeds the realm and the
other two join it. The cluster is available once two of the three are up.

### Step 7 — Shut down the FTL Servers

Nothing carries over to the next scenario, so stop the FTL Servers: between them they hold the realm port and all three Kafka ports the next scenario wants.

```bash
FTLS="http://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-cluster.yaml)"

tibftladmin --ftlserver "$FTLS" -xc
```

`-xc` (`--shutdown_cluster`) stops every FTL Server in the cluster, so it does not matter which member you address — one call replaces three Ctrl-C's.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

**→ Continue to [Scenario 5](#scenario-5--single-node-sasl-plain-over-tls) to add authentication and TLS.**

---

## Scenario 5 — Single-node SASL/PLAIN over TLS

Adds username/password authentication and TLS encryption. This is the most common
starting point for non-production secured environments.

Kafka uses JKS/PKCS12 keystores; FSK reads PEM. The tool rewrites `ssl.keystore.type` to
`PEM` and repoints `ssl.keystore.location` at the `.pem` path, then prints the exact
commands that create that file — creating it is a real conversion, not a rename, and it
is the one step below (Step 2) that the tool leaves to you.

### The certificates this scenario needs

Two separate sets of certificates are in play, and mixing them up is the usual reason this
scenario stalls.

**The Kafka side — you create these.** A keystore holding the broker's certificate and its
private key, and a truststore holding the certificate authority that signed it. They have to be
JKS or PKCS12, because that is all Apache Kafka reads, and the certificate's subject alternative
name must cover the advertised host (`localhost` here) or clients reject the connection during the
handshake. The three passwords have to match `ssl.keystore.password`, `ssl.key.password` and
`ssl.truststore.password` in Step 1. FSK reads PEM rather than JKS, so Step 2 converts the same
material a second time — same certificate and key, different container.

**The FTL side — already on disk.** The FTL Server's own TLS material and its user accounts come
from the installed samples in `/opt/tibco/ftl/current-version/samples/yaml/tls-user`, so there is
nothing to generate for the realm service.

| | Apache Kafka reads | FSK reads | Where it comes from |
|---|---|---|---|
| Broker certificate + private key | `server.keystore.jks` | `server.keystore.pem` | you create it; Step 2 converts it |
| Trust anchor for that certificate | `kafka.truststore.jks` | `kafka.truststore.pem` | you create it; Step 2 converts it |
| FTL Server certificate, key, CA | — | `server_cert.pem`, `server_key.pem`, `client_trust.pem` | shipped in `samples/yaml/tls-user` |
| FTL realm user accounts | — | `users.txt` | shipped in `samples/yaml/tls-user` |

No keystores yet? [Creating self-signed Kafka certificates](#creating-self-signed-kafka-certificates)
produces the two this scenario names, with the passwords Step 1 expects.

:::tip Already running a secured single-node broker?
Steps 1 and 3 only build and start the example broker. If you already have a running Kafka
broker or brokers, skip them and start at **Step 4 — Stop Apache Kafka**, then hand your own
`server.properties` to the tool in Step 5. **Step 2 still applies**: FSK reads PEM, so the `.pem`
files have to exist before the FTL Server starts, even though your brokers are already running
on JKS.
:::

### Step 1 — Kafka server.properties

```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093

listeners=BROKER://localhost:9092,CONTROLLER://localhost:9093
inter.broker.listener.name=BROKER
advertised.listeners=BROKER://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:SSL,BROKER:SASL_SSL

# TLS — JKS keystores (the tool rewrites these to PEM and gives you the commands)
ssl.keystore.type=JKS
ssl.keystore.location=/var/tmp/kafka/scenario5/certs/server.keystore.jks
ssl.keystore.password=keystorePassword123
ssl.key.password=keyPassword123
ssl.truststore.type=JKS
ssl.truststore.location=/var/tmp/kafka/scenario5/certs/kafka.truststore.jks
ssl.truststore.password=truststorePassword123

# SASL/PLAIN on the BROKER listener, used for client and inter-broker traffic alike
sasl.mechanism.inter.broker.protocol=PLAIN
listener.name.broker.sasl.enabled.mechanisms=PLAIN
listener.name.broker.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" \
  user_admin="admin-secret" user_producer="producer-secret" user_consumer="consumer-secret";

log.dirs=/var/tmp/kafka/scenario5/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

Save this as **server-1.properties** in your working directory — the commands below name that
file.

### Step 2 — Convert keystores to PEM

The generated `kof.broker.1.properties` already names the `.pem` files; these are the
commands, printed in the file above each setting and again as a `SEVERE WARNING` at the end
of the run, that actually create them. For JKS:

```bash
# Convert keystore
keytool -importkeystore \
  -srckeystore /var/tmp/kafka/scenario5/certs/server.keystore.jks -srcstoretype JKS \
  -destkeystore /var/tmp/kafka/scenario5/certs/server.keystore.p12 -deststoretype PKCS12
openssl pkcs12 -in /var/tmp/kafka/scenario5/certs/server.keystore.p12 -nodes \
  -out /var/tmp/kafka/scenario5/certs/server.keystore.pem

# Convert truststore
keytool -importkeystore \
  -srckeystore /var/tmp/kafka/scenario5/certs/kafka.truststore.jks -srcstoretype JKS \
  -destkeystore /var/tmp/kafka/scenario5/certs/kafka.truststore.p12 -deststoretype PKCS12
openssl pkcs12 -in /var/tmp/kafka/scenario5/certs/kafka.truststore.p12 -nodes -nokeys \
  -out /var/tmp/kafka/scenario5/certs/kafka.truststore.pem
```

Both `keytool` commands prompt for the source and destination store passwords; add
`-srcstorepass`, `-srckeypass` and `-deststorepass` to run them unattended. A truststore holding
only a trusted certificate — no private key — cannot be run through `keytool -importkeystore` on
every JDK; `keytool -exportcert -rfc` writes the same PEM directly, as the appendix shows.

Alternatively, run with `--auto` and the tool performs the conversion for you
(requires `keytool` and `openssl` on `PATH`).

### Step 3 — Start Apache Kafka (KRaft)

Same sequence as Scenario 1. The broker reads the JKS keystores named in its `server.properties`, so
those must exist before it will start:

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

"$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted --standalone \
  -t "$KAFKA_CLUSTER_ID" -c server-1.properties

"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon server-1.properties
```

A plain `kafka-topics.sh --list` will not reach a SASL_SSL listener — every Kafka CLI needs the
truststore and the SASL credentials before it can complete the handshake. Save this as
**client.properties**:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret";
ssl.truststore.type=JKS
ssl.truststore.location=/var/tmp/kafka/scenario5/certs/kafka.truststore.jks
ssl.truststore.password=truststorePassword123
```

Then:

```bash
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
  --bootstrap-server localhost:9092 --command-config client.properties
```

A line beginning `localhost:9092 (id: 1 rack: null …) ->` means the broker is up and the
certificates are good. Keep the file: the same one reaches FSK on port 9092 after Step 6, which is
the most direct proof the converted PEMs took over.

### Step 4 — Stop Apache Kafka

The FTL Server that replaces the broker binds port 9092, so the broker has to give it up:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

### Step 5 — Run the config migration tool

```bash
export FTL_SAMPLES=/opt/tibco/ftl/current-version/samples/yaml/tls-user

tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario5/data \
  --tls-cert $FTL_SAMPLES/server_cert.pem \
  --tls-key  $FTL_SAMPLES/server_key.pem \
  --tls-key-password password \
  --tls-ca   $FTL_SAMPLES/client_trust.pem \
  --auth-users-file $FTL_SAMPLES/users.txt \
  server-1.properties
```

These four flags configure the **FTL** side — the realm service's own TLS and the accounts that
may log in to it. They have nothing to do with the Kafka listener, which FSK serves using the PEMs
converted in Step 2 and named in `kof.broker.1.properties`. Taking them from the installed
`tls-user` sample means the only certificates you create in this scenario are the Kafka ones.
`--tls-key-password` is required here because the sample `server_key.pem` is encrypted with the
passphrase `password`.

`--auth-users-file` supplies the FTL realm's own users file — the sample grants `admin` the
`ftl-admin` role and `internal` the `ftl-internal` role, which is what the generated
`ftlserver.properties` logs in as. The Kafka clients from the inline JAAS config are a separate
list: the tool extracts them into `kafka-users.txt` in the output directory and layers both files
into `auth.providers`. The tool writes a `tibftlserver-standalone-secure.yaml` alongside the plain
standalone YAML whenever TLS or auth flags are supplied.

:::note The `SEVERE WARNING` about missing `.pem` files
The run ends with `SEVERE WARNING -- … WILL NOT RUN WITH FSK AS IT STANDS` and a claim that the
converted `.pem` files do not exist. The tool only tracks conversions it performed itself, under
`--auto`, so the warning fires even when you did Step 2 by hand and the files are there. Confirm
with `ls /var/tmp/kafka/scenario5/certs/*.pem`; if both exist, carry on.
:::

`--tls-ca` writes `tls.client.trust.file`, the trust anchor the FTL Server's own internal
client uses to verify the certificate the server presents to it. Omit it and the server
starts, runs for about twenty seconds and then dies with
`Certificate verification failed … (18:self-signed certificate)` followed by
`tibmux exited … exit status 99` — so it is required, not optional, whenever `--tls-cert`
is used.

### Step 6 — Start the FTL Server

```bash
tibftlserver -c kof-output/tibftlserver-standalone-secure.yaml -n SRV1
```

Start from the `-secure` YAML, not the plain one: it is the file that carries the TLS certificate
paths and the `auth.providers` list. The plain YAML is written too, and is the one to use if you
want the same topology without security.

### Step 7 — Shut down the FTL Server

Stop the FTL Server before the next scenario. The `-secure` YAML puts TLS and authentication on the realm service, so this needs `https://`, a trust flag and an account holding the `ftl-admin` role.

```bash
FTLS="https://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-standalone-secure.yaml)"

tibftladmin --ftlserver "$FTLS" --tls.trust.file $FTL_SAMPLES/client_trust.pem \
  -u admin -pw admin-pw -x
```

`-x` (`--shutdown`) stops the FTL Server process.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

`admin`/`admin-pw` is the account the sample `users.txt` grants `ftl-admin` to; a user without that role authenticates and is then refused with *403 Forbidden*. `--tls.trust.file` names the CA that signed the certificate the server presents — the same `client_trust.pem` given to `--tls-ca` in Step 5. `-te` in its place trusts any certificate at all, which is quicker and looser.

**→ Continue to [Scenario 6](#scenario-6--single-node-oauth2) to replace SASL/PLAIN with OAuth2,
or jump to [Scenario 7](#scenario-7--3-node-sasl-plain-cluster) for a 3-node SASL cluster.**

---

## Scenario 6 — Single-node OAuth2

Replaces username/password authentication with OAuth 2.0 bearer tokens. Kafka clients
present a JWT; the tool wires the OAUTHBEARER mechanism through to FSK's OAuth2
provider.

### What you must supply — OAuth2

This is the first scenario that cannot run entirely on material that ships with the product. An
OAuth2 deployment needs an authorization server, and only you know where yours is. The list is
short, and everything not on it comes from the installed samples:

1. **The Kafka keystore and truststore**, exactly as in Scenario 5 — JKS or PKCS12, SAN covering
   `localhost`, placed in `/var/tmp/kafka/scenario6/certs`.
   [Creating self-signed Kafka certificates](#creating-self-signed-kafka-certificates) makes them.
2. **The token endpoint URL** of your authorization server — `--oauth-token-url`, and the same URL
   in the broker's JAAS line in Step 1.
3. **The JWKS URL**, or a local validation key file — `--oauth-jwks-url`. FSK validates incoming
   tokens against this; it accepts a `file:` path as well as a URL.
4. **A client ID and secret for FSK itself** — `--oauth-client-id` and `--oauth-client-secret`.
   This is how the FTL Servers get their own tokens for server-to-server traffic.
5. **A client ID and secret for the Kafka broker** — only for Step 1, and only until the broker is
   retired at the end of the scenario.
6. **The Strimzi OAuth callback jars**, on the broker's classpath in Step 2. Apache Kafka has no
   built-in OAUTHBEARER validator; FSK does, and needs no jar.

Three more flags matter if your authorization server does not use the defaults the tool assumes:
`--oauth-audience` (default `ftl`), `--oauth-claim-roles` (default `group`) and
`--oauth-claim-username` (default `preferred_username`). If tokens are rejected with the roles
empty, one of those three is usually why. `--oauth-provider-trust` names a CA PEM when the
authorization server presents a certificate your host does not already trust.

Everything on the FTL side — the realm service's own certificate, key, CA and user accounts —
comes from `/opt/tibco/ftl/current-version/samples/yaml/tls-user`, as in Scenario 5. No
authorization server to hand? Run [Scenario 5](#scenario-5--single-node-sasl-plain-over-tls)
instead; it needs nothing but the two keystores.

:::tip Already running a single-node broker with OAuth2?
Steps 1 and 2 only build and start the example broker. If you already have a running Kafka
broker or brokers, skip them and start at **Step 3 — Stop Apache Kafka**, then hand your own
`server.properties` to the tool in Step 4.
:::

### Step 1 — Kafka server.properties

```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093

listeners=OAUTH://localhost:9092,CONTROLLER://localhost:9093
inter.broker.listener.name=OAUTH
advertised.listeners=OAUTH://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:SSL,OAUTH:SASL_SSL

ssl.keystore.type=JKS
ssl.keystore.location=/var/tmp/kafka/scenario6/certs/server.keystore.jks
ssl.keystore.password=keystorePassword123
ssl.key.password=keyPassword123
ssl.truststore.type=JKS
ssl.truststore.location=/var/tmp/kafka/scenario6/certs/kafka.truststore.jks
ssl.truststore.password=truststorePassword123

sasl.mechanism.inter.broker.protocol=OAUTHBEARER
listener.name.oauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.oauth.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
  oauth.token.endpoint.uri="https://auth.example.com/oauth/token" \
  oauth.client.id="kafka-broker-1" oauth.client.secret="broker-client-secret";
listener.name.oauth.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler
listener.name.oauth.oauthbearer.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler

log.dirs=/var/tmp/kafka/scenario6/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

### Step 2 — Start Apache Kafka (KRaft)

The OAUTHBEARER listener needs the Strimzi OAuth callback handler jar on the broker's classpath,
in addition to the keystores:

```bash
export CLASSPATH="/opt/strimzi-oauth/*"

KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

"$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted --standalone \
  -t "$KAFKA_CLUSTER_ID" -c server-1.properties

"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon server-1.properties
```

FSK has its own OAuth2 provider and needs no such jar — the handler class is a Kafka-side detail
that the tool reads and maps, as described below.

### Step 3 — Stop Apache Kafka

The FTL Server that replaces the broker binds port 9092, so the broker has to give it up:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

### Step 4 — Run the config migration tool

```bash
export FTL_SAMPLES=/opt/tibco/ftl/current-version/samples/yaml/tls-user

tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario6/data \
  --tls-cert $FTL_SAMPLES/server_cert.pem \
  --tls-key  $FTL_SAMPLES/server_key.pem \
  --tls-key-password password \
  --tls-ca   $FTL_SAMPLES/client_trust.pem \
  --auth-users-file $FTL_SAMPLES/users.txt \
  --oauth-token-url    https://auth.example.com/oauth/token \
  --oauth-jwks-url     https://auth.example.com/.well-known/jwks.json \
  --oauth-client-id    kof-server \
  --oauth-client-secret <secret> \
  server-1.properties
```

The four TLS and auth flags come from the installed sample, exactly as in Scenario 5 — they
configure the realm service, not the Kafka listener. The four `--oauth-*` flags are the values
from the list above.

`--auth-users-file` is not strictly required here, and the scenario would work without it: FSK
would then validate every caller, administrators included, by OAuth2 token. Passing it costs
nothing and writes `auth.providers: file:…,oauth2`, so Kafka clients still authenticate by token
while `tibftladmin` can use the sample's `admin`/`admin-pw` account — which is one fewer token to
mint every time you want to look at the server. Drop the flag if you would rather prove the
token path end to end.

The tool detects the `OAUTHBEARER` mechanism and maps the custom callback handler
class to the `oauth` backend automatically. If the handler class is unrecognized, a
`RESOLVE-REQUIRED` block in `kof.broker.1.properties` asks you to choose a backend
(`oauth`, `file`, or `inline`).

### Step 5 — Start the FTL Server

```bash
tibftlserver -c kof-output/tibftlserver-standalone-secure.yaml -n SRV1
```

The secure YAML carries the `oauth2.*` globals and the per-server validation key, so the server
reaches the authorization server on its own at startup — check the log for the JWKS fetch if
tokens are rejected.

### Step 6 — Shut down the FTL Server

Stop the FTL Server before the next scenario. Step 4 passed the sample users file alongside the
OAuth2 configuration, so the realm service accepts either an account or a token here.

```bash
tibftladmin --ftlserver "$FTLS" --tls.trust.file $FTL_SAMPLES/client_trust.pem \
  -u admin -pw admin-pw -x
```

where `FTLS` is the address the server prints at startup:

```bash
FTLS="https://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-standalone-secure.yaml)"
```

`-x` (`--shutdown`) stops the FTL Server process.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

Without `--auth-users-file` the same call becomes `--oauth2.token <token>` in place of `-u`/`-pw`. The token must come from the same issuer the scenario configured with `--oauth-token-url`, and the claim named by `--oauth-claim-roles` has to carry `ftl-admin`. (The tool would also generate `kof-output/ftl-users.txt` with an `admin: ftl-admin-pw` in it — usable, but a generated password is not a thing to leave in place.)

**→ Continue to [Scenario 8](#scenario-8--3-node-mutual-tls-mtls) for mTLS, or
[Scenario 9](#scenario-9--3-node-sasl-plain--oauth2-dual-listener) for a dual SASL+OAuth2 setup.**

---

## Scenario 7 — 3-node SASL/PLAIN cluster

Add SASL/PLAIN + TLS to a 3-node cluster. Every broker carries the Scenario 5 listener and security
configuration unchanged — the TLS keystores, the `BROKER` listener at `SASL_SSL`,
`inter.broker.listener.name=BROKER`, `sasl.mechanism.inter.broker.protocol=PLAIN`, and the inline
JAAS users.

### What you must supply — SASL/PLAIN

One item, and it is the one you already made:

1. **The Kafka keystore and truststore.** All three brokers are `localhost`, so the single
   certificate pair from [Scenario 5](#scenario-5--single-node-sasl-plain-over-tls) serves the
   whole cluster — keep `/var/tmp/kafka/scenario5/certs` in the `ssl.*` lines of all three files,
   including the `.pem` copies. Starting fresh?
   [Creating self-signed Kafka certificates](#creating-self-signed-kafka-certificates) makes them.

The FTL Servers' own certificate, key, CA and user accounts come from
`/opt/tibco/ftl/current-version/samples/yaml/tls-user`, so there is nothing else to create. A
cluster spread over real hosts is the one case that needs more: either a certificate per host, or
one certificate whose SAN lists every advertised name.

:::tip Already running a 3-node SASL/PLAIN cluster?
Steps 1 and 2 only describe and start the three example brokers. If you already have a running
Kafka broker or brokers, skip them and start at **Step 3 — Stop Apache Kafka**, then hand your
own three `server.properties` files to the tool in Step 4. Whatever `.pem` files your configuration
names still have to exist before the FTL Servers start — see Scenario 5, Step 2 if yours are
Java keystores.
:::

### Step 1 — What changes in each broker file

```properties
# server-1.properties
node.id=1
listeners=BROKER://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=BROKER://localhost:9092
log.dirs=/var/tmp/kafka/scenario7/data/broker-1

# server-2.properties
node.id=2
listeners=BROKER://localhost:9102,CONTROLLER://localhost:9103
advertised.listeners=BROKER://localhost:9102
log.dirs=/var/tmp/kafka/scenario7/data/broker-2

# server-3.properties
node.id=3
listeners=BROKER://localhost:9112,CONTROLLER://localhost:9113
advertised.listeners=BROKER://localhost:9112
log.dirs=/var/tmp/kafka/scenario7/data/broker-3
```

Two things change identically in all three files, because Scenario 5 was a single node: swap
`controller.quorum.bootstrap.servers` for the voter list
`controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113`, and raise the
replication settings to `offsets.topic.replication.factor=3`,
`transaction.state.log.replication.factor=3` and `transaction.state.log.min.isr=2` — the values
Scenario 2 uses. The three brokers share one server certificate, so nothing about the TLS configuration
differs per file.

### Step 2 — Start Apache Kafka (KRaft)

Same three-broker sequence as Scenario 2 — one cluster ID shared by all three:

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/scenario7/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

The keystores and truststore named in the properties files must exist before the brokers will
start. The listeners are SASL_SSL, so the plain `kafka-topics.sh --list` check does not apply
here — reuse the `client.properties` written in
[Scenario 5](#scenario-5--single-node-sasl-plain-over-tls), Step 3, which already carries the
truststore and the `admin`/`admin-secret` JAAS line, and point it at each broker in turn:

```bash
for port in 9092 9102 9112; do
  "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server "localhost:$port" --command-config client.properties | head -1
done
```

### Step 3 — Stop Apache Kafka

The three FTL Servers take over the brokers' client ports, so the brokers have to give them up:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

One call stops all three — it signals every broker JVM on the host.

### Step 4 — Run the config migration tool

```bash
export FTL_SAMPLES=/opt/tibco/ftl/current-version/samples/yaml/tls-user

tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario7/data \
  --tls-cert $FTL_SAMPLES/server_cert.pem \
  --tls-key  $FTL_SAMPLES/server_key.pem \
  --tls-key-password password \
  --tls-ca   $FTL_SAMPLES/client_trust.pem \
  --auth-users-file $FTL_SAMPLES/users.txt \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

Identical to Scenario 5's invocation but for the data directory and the three properties files —
the FTL side does not change with the size of the cluster.

The tool reads the inline JAAS `user_X` entries from each broker's properties and writes them to
`kafka-users.txt` in `--output-dir`, which `auth.providers` lists after the sample `users.txt` —
Kafka client principals in one file, FTL accounts in the other. Leave `--auth-users-file` out and
the tool generates a third file, `ftl-users.txt`, to give the servers accounts of their own.

### Step 5 — Start the FTL Servers

```bash
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

Three shells, one per server. The `-secure` YAML is the one that carries the certificate paths and
`auth.providers`; the users files are referenced from it by the paths they had at generation time,
so keep `kafka-users.txt` where the tool wrote it and leave the sample directory alone.

### Step 6 — Shut down the FTL Servers

Stop the FTL Servers before the next scenario. The `-secure` YAML puts TLS and authentication on the realm service, so this needs `https://`, a trust flag and an account holding the `ftl-admin` role.

```bash
FTLS="https://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-cluster-secure.yaml)"

tibftladmin --ftlserver "$FTLS" --tls.trust.file $FTL_SAMPLES/client_trust.pem \
  -u admin -pw admin-pw -xc
```

`-xc` (`--shutdown_cluster`) stops every FTL Server in the cluster, so it does not matter which member you address — one call replaces three Ctrl-C's.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

`admin`/`admin-pw` is the account the sample `users.txt` grants `ftl-admin` to; a user without that role authenticates and is then refused with *403 Forbidden*. `--tls.trust.file` names the CA that signed the certificate the server presents — the same one given to `--tls-ca` when the configuration was generated. `-te` in its place trusts any certificate at all, which is quicker and looser.

**→ Continue to [Scenario 8](#scenario-8--3-node-mutual-tls-mtls) to add client certificate
authentication.**

---

## Scenario 8 — 3-node mutual TLS (mTLS)

Client certificates replace username/password. The Kafka `SSL` listener with
`ssl.client.auth=required` maps to FSK's `tls-only` auth mode with
`tls.server.trust.file`.

### What you must supply — mTLS

Still one item, because FTL ships a complete mTLS sample:

1. **The Kafka keystore and truststore.** Same pair as Scenario 5, reused unchanged — with
   `ssl.client.auth=required` the truststore takes on a second job, since it is now also the CA
   against which the broker verifies *client* certificates. A self-signed pair works for this
   scenario because the same certificate signs both sides.

The FTL side comes from `/opt/tibco/ftl/current-version/samples/yaml/mtls` rather than
`tls-user` — mTLS needs two more files than the earlier scenarios, and that directory has them:

| Sample file | Flag | What it is |
|---|---|---|
| `server_cert.pem`, `server_key.pem` | `--tls-cert`, `--tls-key` | what the realm service presents (`CN=server`, key passphrase `password`) |
| `client_trust.pem` | `--tls-ca` | CA the server's own client verifies against |
| `server_trust.pem` | `--tls-server-trust` | CA used to verify *inbound* client certificates |
| `internal_cert.pem`, `internal_key.pem` | `--tls-client-cert`, `--tls-client-key` | what each server presents to the others (`CN=internal:ftl-internal`) |
| `admin_cert.pem`, `admin_key.pem` | `tibftladmin` in Step 7 | an administrator certificate (`CN=admin:ftl-admin`) |

With mTLS there is no users file: the certificate *is* the account. FTL takes the user and its
roles from the certificate's common name, which is why the sample CNs read
`internal:ftl-internal` and `admin:ftl-admin`. A certificate whose CN carries no role
authenticates and is then refused everything.

:::tip Already running a 3-node mTLS cluster?
Steps 1–3 only describe and start the three example brokers. If you already have a running
Kafka broker or brokers, skip them and start at **Step 4 — Stop Apache Kafka**, then hand your own
three `server.properties` files to the tool in Step 5. Whatever `.pem` files your configuration
names still have to exist before the FTL Servers start — see Scenario 5, Step 2 if yours are
Java keystores.
:::

### Step 1 — Kafka server.properties highlights

```properties
inter.broker.listener.name=MTLS
listener.security.protocol.map=CONTROLLER:SSL,MTLS:SSL

# Require client certificates on the MTLS listener
listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/var/tmp/kafka/scenario5/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123
```

Those lines, plus `controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113`,
are the same in all three files.

### Step 2 — What changes in each broker file

```properties
# server-1.properties
node.id=1
listeners=MTLS://localhost:9094,CONTROLLER://localhost:9093
advertised.listeners=MTLS://localhost:9094
log.dirs=/var/tmp/kafka/scenario8/data/broker-1

# server-2.properties
node.id=2
listeners=MTLS://localhost:9104,CONTROLLER://localhost:9103
advertised.listeners=MTLS://localhost:9104
log.dirs=/var/tmp/kafka/scenario8/data/broker-2

# server-3.properties
node.id=3
listeners=MTLS://localhost:9114,CONTROLLER://localhost:9113
advertised.listeners=MTLS://localhost:9114
log.dirs=/var/tmp/kafka/scenario8/data/broker-3
```

### Step 3 — Start Apache Kafka (KRaft)

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/scenario8/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

Both the keystore and the client-CA truststore must be in place — with
`ssl.client.auth=required` the MTLS listener rejects every connection that arrives without a
certificate it can verify, including your own verification attempts.

### Step 4 — Stop Apache Kafka

The three FTL Servers take over the brokers' client ports, so the brokers have to give them up:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

One call stops all three — it signals every broker JVM on the host.

### Step 5 — Run the config migration tool

```bash
export FTL_MTLS=/opt/tibco/ftl/current-version/samples/yaml/mtls

tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario8/data \
  --tls-cert                $FTL_MTLS/server_cert.pem \
  --tls-key                 $FTL_MTLS/server_key.pem \
  --tls-key-password        password \
  --tls-ca                  $FTL_MTLS/client_trust.pem \
  --tls-server-trust        $FTL_MTLS/server_trust.pem \
  --tls-client-cert         $FTL_MTLS/internal_cert.pem \
  --tls-client-key          $FTL_MTLS/internal_key.pem \
  --tls-client-key-password password \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

Both sample keys are encrypted with the passphrase `password`, so both password flags are
required; omitting either leaves the server unable to read its own key at startup.

`--tls-server-trust` sets `tls.server.trust.file` (the CA that signs client
certificates). `--tls-client-cert` and `--tls-client-key` are used for
server-to-server connections.

The two trust files are different things: `--tls-server-trust`
(`tls.server.trust.file`) is the CA that signs *inbound* client certificates, while
`--tls-ca` (`tls.client.trust.file`) is what the server's own client verifies the
certificates it *connects to* against — see Scenario 5, Step 5. The sample keeps them genuinely
separate: `CN=client_trust` signed `server_cert.pem`, `CN=server_trust` signed the client
certificates, and neither verifies the other's. Swapping the two flags therefore fails outright
rather than quietly working, which is the useful way round. Where one CA really does sign
everything, name the same file twice.

### Step 6 — Start the FTL Servers

```bash
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

The FTL Servers bind each broker's MTLS port (9094 for broker 1) rather than 9092 — there is no
plaintext listener in this configuration to translate. The servers present
`--tls-client-cert` to each other, so that certificate has to be one the CA in
`--tls-server-trust` signed, or the cluster will not form.

### Step 7 — Shut down the FTL Servers

Stop the FTL Servers before the next scenario. This scenario passed no `--auth-users-file`, so the natural way in is the client certificate.

```bash
FTLS="https://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-cluster-secure.yaml)"

tibftladmin --ftlserver "$FTLS" --tls.trust.file $FTL_MTLS/client_trust.pem \
  --tls.client.cert $FTL_MTLS/admin_cert.pem \
  --tls.client.private.key $FTL_MTLS/admin_key.pem \
  --tls.client.private.key.password password -xc
```

`-xc` (`--shutdown_cluster`) stops every FTL Server in the cluster, so it does not matter which member you address — one call replaces three Ctrl-C's.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

`admin_cert.pem` is used rather than the `internal_cert.pem` the servers use between themselves: its common name is `admin:ftl-admin`, and `ftl-admin` is the role a shutdown needs. The realm service checks the certificate against `tls.server.trust.file` — `server_trust.pem`, the CA that signed it — and `--tls.trust.file` is the other direction, this command verifying the server.

With no users file of your own, the tool writes one: `kof-output/ftl-users.txt`, holding the accounts the servers use between themselves and an `admin: ftl-admin-pw, ftl-admin`. So `-u admin -pw ftl-admin-pw` in place of the three client-certificate flags also works — change that password before the configuration goes anywhere real.

**→ Continue to [Scenario 9](#scenario-9--3-node-sasl-plain--oauth2-dual-listener) for a
dual-protocol setup, or [Scenario 10](#scenario-10--3-node-sasl-plain--mtls) to combine SASL
and mTLS on separate listeners.**

---

## Scenario 9 — 3-node SASL/PLAIN + OAuth2 (dual listener)

Two client-facing listeners on the same cluster: legacy clients use SASL/PLAIN; modern
clients use OAUTHBEARER. FSK runs both auth providers concurrently.

### What you must supply — SASL/PLAIN + OAuth2

Scenario 7's list plus Scenario 6's, with nothing new of its own:

1. **The Kafka keystore and truststore** — Scenario 5's pair, reused unchanged.
2. **The authorization server values** — token endpoint, JWKS URL, and a client ID and secret for
   FSK, exactly as itemised under
   [Scenario 6](#what-you-must-supply--oauth2). The broker additionally needs its own client
   credentials and the Strimzi callback jars.

The FTL Servers' certificate, key, CA and user accounts come from
`/opt/tibco/ftl/current-version/samples/yaml/tls-user`. This scenario is the one where the users
file earns its place regardless of preference: SASL/PLAIN clients authenticate against it, so it
is not optional here the way it is in Scenario 6.

:::tip Already running a 3-node SASL/PLAIN + OAuth2 cluster?
Steps 1–3 only describe and start the three example brokers. If you already have a running
Kafka broker or brokers, skip them and start at **Step 4 — Stop Apache Kafka**, then hand your own
three `server.properties` files to the tool in Step 5. Whatever `.pem` files your configuration
names still have to exist before the FTL Servers start — see Scenario 5, Step 2 if yours are
Java keystores.
:::

### Step 1 — Kafka server.properties highlights

```properties
inter.broker.listener.name=INTERNAL
listener.security.protocol.map=CONTROLLER:SSL,SASL_AUTH:SASL_SSL,OAUTH:SASL_SSL,INTERNAL:SSL

# SASL/PLAIN on SASL_AUTH listener
listener.name.sasl_auth.sasl.enabled.mechanisms=PLAIN
listener.name.sasl_auth.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" user_admin="admin-secret";

# OAUTHBEARER on OAUTH listener
listener.name.oauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.oauth.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler
```

`INTERNAL` exists only to carry inter-broker traffic, on an SSL listener of its own. Naming one of
the two client listeners there would work for Kafka, but the tool would then read that listener as
internal and leave it out of the generated broker properties — and a scenario about serving SASL/PLAIN
and OAUTHBEARER side by side would end up with one client port. `INTERNAL` is dropped instead,
which is what you want, and both client listeners come through.

### Step 2 — What changes in each broker file

Four listeners means four ports to move per broker; the JAAS and OAuth lines above stay identical,
as does `controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113`.

```properties
# server-1.properties
node.id=1
listeners=SASL_AUTH://localhost:9092,OAUTH://localhost:9095,INTERNAL://localhost:9098,CONTROLLER://localhost:9093
advertised.listeners=SASL_AUTH://localhost:9092,OAUTH://localhost:9095,INTERNAL://localhost:9098
log.dirs=/var/tmp/kafka/scenario9/data/broker-1

# server-2.properties
node.id=2
listeners=SASL_AUTH://localhost:9102,OAUTH://localhost:9105,INTERNAL://localhost:9108,CONTROLLER://localhost:9103
advertised.listeners=SASL_AUTH://localhost:9102,OAUTH://localhost:9105,INTERNAL://localhost:9108
log.dirs=/var/tmp/kafka/scenario9/data/broker-2

# server-3.properties
node.id=3
listeners=SASL_AUTH://localhost:9112,OAUTH://localhost:9115,INTERNAL://localhost:9118,CONTROLLER://localhost:9113
advertised.listeners=SASL_AUTH://localhost:9112,OAUTH://localhost:9115,INTERNAL://localhost:9118
log.dirs=/var/tmp/kafka/scenario9/data/broker-3
```

`INTERNAL` has to be advertised as well as bound: the other two brokers reach this one at the
address it advertises for the inter-broker listener, so leaving it out of `advertised.listeners`
stops the cluster forming.

### Step 3 — Start Apache Kafka (KRaft)

The OAUTHBEARER callback handler is a third-party class, so it has to be on the broker's classpath
before the brokers start:

```bash
export CLASSPATH="/opt/strimzi-oauth/*"

KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/scenario9/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

FSK needs no equivalent jar — token validation is built in and driven by `--oauth-jwks-url`.

### Step 4 — Stop Apache Kafka

The three FTL Servers take over the brokers' client ports, so the brokers have to give them up:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

One call stops all three — it signals every broker JVM on the host.

### Step 5 — Run the config migration tool

```bash
export FTL_SAMPLES=/opt/tibco/ftl/current-version/samples/yaml/tls-user

tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario9/data \
  --tls-cert            $FTL_SAMPLES/server_cert.pem \
  --tls-key             $FTL_SAMPLES/server_key.pem \
  --tls-key-password    password \
  --tls-ca              $FTL_SAMPLES/client_trust.pem \
  --auth-users-file     $FTL_SAMPLES/users.txt \
  --oauth-token-url     https://auth.example.com/oauth/token \
  --oauth-jwks-url      https://auth.example.com/.well-known/jwks.json \
  --oauth-client-id     kof-server \
  --oauth-client-secret <secret> \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

The four `--tls-*` and `--auth-users-file` values come from the shipped sample and need no editing;
the four `--oauth-*` values are yours. The secure YAML ends up with
`auth.providers: file:<samples>/users.txt,file:kof-output/kafka-users.txt,oauth2`, so both
authentication paths are active at once — a SASL/PLAIN client on 9092 is checked against the files,
a bearer token on 9095 against the JWKS.

### Step 6 — Start the FTL Servers

```bash
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

Each FTL Server serves both client ports, 9092 and 9095, from one process — the dual listener carries
over from the broker configuration. The servers reach the token endpoint at startup, so a failure
to fetch the JWKS shows up in the startup log rather than at first client connect.

### Step 7 — Shut down the FTL Servers

Stop the FTL Servers before the next scenario. The `-secure` YAML puts TLS and authentication on the realm service, so this needs `https://`, a trust flag and an account holding the `ftl-admin` role.

```bash
FTLS="https://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-cluster-secure.yaml)"

tibftladmin --ftlserver "$FTLS" --tls.trust.file $FTL_SAMPLES/client_trust.pem \
  -u admin -pw admin-pw -xc
```

`-xc` (`--shutdown_cluster`) stops every FTL Server in the cluster, so it does not matter which member you address — one call replaces three Ctrl-C's.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

`admin`/`admin-pw` is the account the sample `users.txt` grants `ftl-admin` to; a user without that role authenticates and is then refused with *403 Forbidden*. `--tls.trust.file` names the CA that signed the certificate the server presents — the same one given to `--tls-ca` when the configuration was generated. `-te` in its place trusts any certificate at all, which is quicker and looser.

**→ Continue to [Scenario 10](#scenario-10--3-node-sasl-plain--mtls) to combine SASL and mTLS.**

---

## Scenario 10 — 3-node SASL/PLAIN + mTLS

Two listeners on separate ports: one for SASL/PLAIN clients, one for mTLS clients.
Both share the same server certificate; `ssl.client.auth=required` applies only to the
mTLS listener.

### What you must supply — SASL/PLAIN + mTLS

One item, the same as Scenario 8:

1. **The Kafka keystore and truststore** — Scenario 5's pair, reused unchanged. The truststore is
   what the `MTLS` listener checks Kafka clients against, so in a real deployment it holds the CA
   that issued their certificates rather than the broker's own.

Everything on the FTL side is shipped. This scenario draws on **both** sample directories, because
`samples/yaml/mtls` has the six certificates the mTLS flags need but no users file, and
`samples/yaml/tls-user` has the users file the SASL/PLAIN listener authenticates against:

| Flag | File |
|---|---|
| `--tls-cert`, `--tls-key` | `$FTL_MTLS/server_cert.pem`, `$FTL_MTLS/server_key.pem` |
| `--tls-ca` | `$FTL_MTLS/client_trust.pem` |
| `--tls-server-trust` | `$FTL_MTLS/server_trust.pem` |
| `--tls-client-cert`, `--tls-client-key` | `$FTL_MTLS/internal_cert.pem`, `$FTL_MTLS/internal_key.pem` |
| `--auth-users-file` | `$FTL_SAMPLES/users.txt` |

:::tip Already running a 3-node SASL/PLAIN + mTLS cluster?
Steps 1–3 only describe and start the three example brokers. If you already have a running
Kafka broker or brokers, skip them and start at **Step 4 — Stop Apache Kafka**, then hand your own
three `server.properties` files to the tool in Step 5. Whatever `.pem` files your configuration
names still have to exist before the FTL Servers start — see Scenario 5, Step 2 if yours are
Java keystores.
:::

### Step 1 — Kafka server.properties highlights

```properties
inter.broker.listener.name=INTERNAL
listener.security.protocol.map=CONTROLLER:SSL,SASL_AUTH:SASL_SSL,MTLS:SSL,INTERNAL:SSL

listener.name.sasl_auth.sasl.enabled.mechanisms=PLAIN
listener.name.sasl_auth.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" user_admin="admin-secret";

listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/var/tmp/kafka/scenario5/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123
```

As in Scenario 9, inter-broker traffic gets its own `INTERNAL` listener so that both client listeners
survive the translation. `INTERNAL` is plain SSL — the brokers already trust each other's
certificates, so there is no reason to make them authenticate over SASL as well.

### Step 2 — What changes in each broker file

```properties
# server-1.properties
node.id=1
listeners=SASL_AUTH://localhost:9092,MTLS://localhost:9094,INTERNAL://localhost:9098,CONTROLLER://localhost:9093
advertised.listeners=SASL_AUTH://localhost:9092,MTLS://localhost:9094,INTERNAL://localhost:9098
log.dirs=/var/tmp/kafka/scenario10/data/broker-1

# server-2.properties
node.id=2
listeners=SASL_AUTH://localhost:9102,MTLS://localhost:9104,INTERNAL://localhost:9108,CONTROLLER://localhost:9103
advertised.listeners=SASL_AUTH://localhost:9102,MTLS://localhost:9104,INTERNAL://localhost:9108
log.dirs=/var/tmp/kafka/scenario10/data/broker-2

# server-3.properties
node.id=3
listeners=SASL_AUTH://localhost:9112,MTLS://localhost:9114,INTERNAL://localhost:9118,CONTROLLER://localhost:9113
advertised.listeners=SASL_AUTH://localhost:9112,MTLS://localhost:9114,INTERNAL://localhost:9118
log.dirs=/var/tmp/kafka/scenario10/data/broker-3
```

Everything else is identical across the three files, `controller.quorum.voters` and the truststore
settings included.

### Step 3 — Start Apache Kafka (KRaft)

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/scenario10/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

### Step 4 — Stop Apache Kafka

The three FTL Servers take over the brokers' client ports, so the brokers have to give them up:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

One call stops all three — it signals every broker JVM on the host.

### Step 5 — Run the config migration tool

```bash
export FTL_MTLS=/opt/tibco/ftl/current-version/samples/yaml/mtls
export FTL_SAMPLES=/opt/tibco/ftl/current-version/samples/yaml/tls-user

tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario10/data \
  --tls-cert                $FTL_MTLS/server_cert.pem \
  --tls-key                 $FTL_MTLS/server_key.pem \
  --tls-key-password        password \
  --tls-ca                  $FTL_MTLS/client_trust.pem \
  --tls-server-trust        $FTL_MTLS/server_trust.pem \
  --tls-client-cert         $FTL_MTLS/internal_cert.pem \
  --tls-client-key          $FTL_MTLS/internal_key.pem \
  --tls-client-key-password password \
  --auth-users-file         $FTL_SAMPLES/users.txt \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

Both key passwords are `password` — every private key in the two sample directories is encrypted
with that passphrase. `--tls-ca` and `--tls-server-trust` are two different CAs and are not
interchangeable: `client_trust.pem` signed the server certificate, `server_trust.pem` signed the
client ones. See [Scenario 8](#what-you-must-supply--mtls) for what each of the six certificates is.

### Step 6 — Start the FTL Servers

```bash
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

`auth.providers` lists `file:` and `mtls` together, so a client authenticates with either a
username/password or a certificate, depending on which port it connects to — 9092 or 9094.

### Step 7 — Shut down the FTL Servers

Stop the FTL Servers before the next scenario. The `-secure` YAML puts TLS and authentication on the realm service, so this needs `https://`, a trust flag and an account holding the `ftl-admin` role.

```bash
FTLS="https://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-cluster-secure.yaml)"

tibftladmin --ftlserver "$FTLS" --tls.trust.file $FTL_MTLS/client_trust.pem \
  -u admin -pw admin-pw -xc
```

`-xc` (`--shutdown_cluster`) stops every FTL Server in the cluster, so it does not matter which member you address — one call replaces three Ctrl-C's.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

`admin`/`admin-pw` is the account the sample `users.txt` grants `ftl-admin` to; a user without that role authenticates and is then refused with *403 Forbidden*. `--tls.trust.file` is `client_trust.pem` from the **mtls** sample here, not the `tls-user` one — it has to be the CA that signed the certificate the server presents, which is the file this scenario gave to `--tls-ca`. The certificate route works too, since `auth.providers` also lists `mtls`: add the three `--tls.client.*` flags from [Scenario 8](#scenario-8--3-node-mutual-tls-mtls), Step 7, and drop `-u`/`-pw`.

---

## Scenario 11 — 3-node mTLS + OAuth2

The most secure multi-protocol configuration: mTLS for certificate-bearing clients,
OAUTHBEARER for token-bearing clients.

### What you must supply — mTLS + OAuth2

1. **The Kafka keystore and truststore** — Scenario 5's pair, reused unchanged.
2. **The authorization server values** — token endpoint, JWKS URL, and a client ID and secret for
   FSK, as itemised under [Scenario 6](#what-you-must-supply--oauth2). The broker needs its own
   client credentials and the Strimzi callback jars on top of that.

The six FTL-side certificates come from `/opt/tibco/ftl/current-version/samples/yaml/mtls`, listed
in [Scenario 8](#what-you-must-supply--mtls). No users file is passed: the tool generates
`kof-output/ftl-users.txt` for the FTL Servers' own accounts, and that file also carries an
`admin`, so administering the cluster needs no token minted by hand.

:::tip Already running a 3-node mTLS + OAuth2 cluster?
Steps 1–3 only describe and start the three example brokers. If you already have a running
Kafka broker or brokers, skip them and start at **Step 4 — Stop Apache Kafka**, then hand your own
three `server.properties` files to the tool in Step 5. Whatever `.pem` files your configuration
names still have to exist before the FTL Servers start — see Scenario 5, Step 2 if yours are
Java keystores.
:::

### Step 1 — Kafka server.properties highlights

```properties
inter.broker.listener.name=INTERNAL
listener.security.protocol.map=CONTROLLER:SSL,MTLS:SSL,OAUTH:SASL_SSL,INTERNAL:SSL

listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/var/tmp/kafka/scenario5/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123

listener.name.oauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.oauth.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler
```

Two client listeners again, so inter-broker traffic takes a dedicated `INTERNAL` listener for the
reason Scenario 9 gives: name either client listener there and the tool reads it as internal and leaves
it out of the generated broker properties.

### Step 2 — What changes in each broker file

```properties
# server-1.properties
node.id=1
listeners=MTLS://localhost:9094,OAUTH://localhost:9095,INTERNAL://localhost:9098,CONTROLLER://localhost:9093
advertised.listeners=MTLS://localhost:9094,OAUTH://localhost:9095,INTERNAL://localhost:9098
log.dirs=/var/tmp/kafka/scenario11/data/broker-1

# server-2.properties
node.id=2
listeners=MTLS://localhost:9104,OAUTH://localhost:9105,INTERNAL://localhost:9108,CONTROLLER://localhost:9103
advertised.listeners=MTLS://localhost:9104,OAUTH://localhost:9105,INTERNAL://localhost:9108
log.dirs=/var/tmp/kafka/scenario11/data/broker-2

# server-3.properties
node.id=3
listeners=MTLS://localhost:9114,OAUTH://localhost:9115,INTERNAL://localhost:9118,CONTROLLER://localhost:9113
advertised.listeners=MTLS://localhost:9114,OAUTH://localhost:9115,INTERNAL://localhost:9118
log.dirs=/var/tmp/kafka/scenario11/data/broker-3
```

`controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113` and everything under
*highlights* above are identical in all three files.

### Step 3 — Start Apache Kafka (KRaft)

The OAUTHBEARER listener needs the callback handler jars, as in Scenario 9:

```bash
export CLASSPATH="/opt/strimzi-oauth/*"

KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/scenario11/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

### Step 4 — Stop Apache Kafka

The three FTL Servers take over the brokers' client ports, so the brokers have to give them up:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

One call stops all three — it signals every broker JVM on the host.

### Step 5 — Run the config migration tool

```bash
export FTL_MTLS=/opt/tibco/ftl/current-version/samples/yaml/mtls

tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario11/data \
  --tls-cert                $FTL_MTLS/server_cert.pem \
  --tls-key                 $FTL_MTLS/server_key.pem \
  --tls-key-password        password \
  --tls-ca                  $FTL_MTLS/client_trust.pem \
  --tls-server-trust        $FTL_MTLS/server_trust.pem \
  --tls-client-cert         $FTL_MTLS/internal_cert.pem \
  --tls-client-key          $FTL_MTLS/internal_key.pem \
  --tls-client-key-password password \
  --oauth-token-url         https://auth.example.com/oauth/token \
  --oauth-jwks-url          https://auth.example.com/.well-known/jwks.json \
  --oauth-client-id         kof-server \
  --oauth-client-secret     <secret> \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

The eight `--tls-*` values are the sample files as shipped; the four `--oauth-*` values are the only
ones to edit. Add `--oauth-provider-trust <ca.pem>` if the authorization server presents a
certificate the system trust store does not already carry.

### Step 6 — Start the FTL Servers

```bash
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

There is no `--auth-users-file` here, so the tool supplies one of its own:
`kof-output/ftl-users.txt`, holding `internal`, `admin`, `all` and `monitoring` with generated
passwords, and `auth.providers` reads `file:kof-output/ftl-users.txt,mtls,oauth2`. Change those
passwords before the configuration goes anywhere real. The servers also authenticate to the
authorization server as OAuth2 clients, fetching a token from `--oauth-token-url` with
`--oauth-client-id` and `--oauth-client-secret` — written into the YAML as `oauth2.svr.client.id`
and `oauth2.svr.client.secret`.

### Step 7 — Shut down the FTL Servers

Stop the FTL Servers before the next scenario. All three of this configuration's authentication routes can carry the call; the shortest is the `admin` account in the generated users file.

```bash
FTLS="https://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-cluster-secure.yaml)"

tibftladmin --ftlserver "$FTLS" --tls.trust.file $FTL_MTLS/client_trust.pem \
  -u admin -pw ftl-admin-pw -xc
```

`-xc` (`--shutdown_cluster`) stops every FTL Server in the cluster, so it does not matter which member you address — one call replaces three Ctrl-C's.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

`ftl-admin-pw` is the password the tool wrote into `kof-output/ftl-users.txt` for `admin`; read the file rather than trusting this page if a later release changes it. The two alternatives are the sample certificate — the three `--tls.client.*` flags of [Scenario 8](#scenario-8--3-node-mutual-tls-mtls), Step 7, whose `admin_cert.pem` carries the common name `admin:ftl-admin` — and a bearer token, `tibftladmin --ftlserver "$FTLS" -te --oauth2.token <token> -xc`, which must come from the issuer named by `--oauth-token-url` and whose account needs the `ftl-admin` role.

---

## Scenario 12 — 3-node SASL/PLAIN + mTLS + OAuth2

All three auth providers active simultaneously. Each client-facing listener uses a
different mechanism; FSK's `auth.providers` list in the secure YAML activates all of
them.

### What you must supply — SASL/PLAIN + mTLS + OAuth2

The union of Scenarios 7, 8 and 6, and still only two things of your own:

1. **The Kafka keystore and truststore** — Scenario 5's pair, reused unchanged.
2. **The authorization server values** — token endpoint, JWKS URL, and a client ID and secret for
   FSK, as itemised under [Scenario 6](#what-you-must-supply--oauth2), plus the broker's own client
   credentials and the Strimzi callback jars.

The FTL side needs both sample directories, as in Scenario 10: the six certificates from
`samples/yaml/mtls` for the mTLS flags, and `users.txt` from `samples/yaml/tls-user` for the
SASL/PLAIN listener.

:::tip Already running a 3-node SASL/PLAIN + mTLS + OAuth2 cluster?
Steps 1–3 only describe and start the three example brokers. If you already have a running
Kafka broker or brokers, skip them and start at **Step 4 — Stop Apache Kafka**, then hand your own
three `server.properties` files to the tool in Step 5. Whatever `.pem` files your configuration
names still have to exist before the FTL Servers start — see Scenario 5, Step 2 if yours are
Java keystores.
:::

### Step 1 — Kafka server.properties highlights

```properties
inter.broker.listener.name=INTERNAL
listener.security.protocol.map=CONTROLLER:SSL,SASL_AUTH:SASL_SSL,MTLS:SSL,OAUTH:SASL_SSL,INTERNAL:SSL

listener.name.sasl_auth.sasl.enabled.mechanisms=PLAIN
listener.name.sasl_auth.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" user_admin="admin-secret";

listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/var/tmp/kafka/scenario5/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123

listener.name.oauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.oauth.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler
```

### Step 2 — What changes in each broker file

Three client listeners and one internal one — five ports per broker, all moved together:

```properties
# server-1.properties
node.id=1
listeners=SASL_AUTH://localhost:9092,MTLS://localhost:9094,OAUTH://localhost:9095,INTERNAL://localhost:9098,CONTROLLER://localhost:9093
advertised.listeners=SASL_AUTH://localhost:9092,MTLS://localhost:9094,OAUTH://localhost:9095,INTERNAL://localhost:9098
log.dirs=/var/tmp/kafka/scenario12/data/broker-1

# server-2.properties
node.id=2
listeners=SASL_AUTH://localhost:9102,MTLS://localhost:9104,OAUTH://localhost:9105,INTERNAL://localhost:9108,CONTROLLER://localhost:9103
advertised.listeners=SASL_AUTH://localhost:9102,MTLS://localhost:9104,OAUTH://localhost:9105,INTERNAL://localhost:9108
log.dirs=/var/tmp/kafka/scenario12/data/broker-2

# server-3.properties
node.id=3
listeners=SASL_AUTH://localhost:9112,MTLS://localhost:9114,OAUTH://localhost:9115,INTERNAL://localhost:9118,CONTROLLER://localhost:9113
advertised.listeners=SASL_AUTH://localhost:9112,MTLS://localhost:9114,OAUTH://localhost:9115,INTERNAL://localhost:9118
log.dirs=/var/tmp/kafka/scenario12/data/broker-3
```

`controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113` and the security
lines above are identical in all three files.

### Step 3 — Start Apache Kafka (KRaft)

```bash
export CLASSPATH="/opt/strimzi-oauth/*"

KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/scenario12/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

### Step 4 — Stop Apache Kafka

The three FTL Servers take over the brokers' client ports, so the brokers have to give them up:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

One call stops all three — it signals every broker JVM on the host.

### Step 5 — Run the config migration tool

```bash
export FTL_MTLS=/opt/tibco/ftl/current-version/samples/yaml/mtls
export FTL_SAMPLES=/opt/tibco/ftl/current-version/samples/yaml/tls-user

tibftlimportconfig \
  --output-dir ./kof-output \
  --data-dir /var/tmp/kof/scenario12/data \
  --tls-cert                $FTL_MTLS/server_cert.pem \
  --tls-key                 $FTL_MTLS/server_key.pem \
  --tls-key-password        password \
  --tls-ca                  $FTL_MTLS/client_trust.pem \
  --tls-server-trust        $FTL_MTLS/server_trust.pem \
  --tls-client-cert         $FTL_MTLS/internal_cert.pem \
  --tls-client-key          $FTL_MTLS/internal_key.pem \
  --tls-client-key-password password \
  --auth-users-file         $FTL_SAMPLES/users.txt \
  --oauth-token-url         https://auth.example.com/oauth/token \
  --oauth-jwks-url          https://auth.example.com/.well-known/jwks.json \
  --oauth-client-id         kof-server \
  --oauth-client-secret     <secret> \
  server-1.properties \
  server-2.properties \
  server-3.properties
```

Everything but the four `--oauth-*` values is a shipped sample path. This is the longest command
line in the guide, and every flag in it earns its place: eight for the two certificate directions,
one for the users file, four for the authorization server.

### Step 6 — Start the FTL Servers

```bash
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

Check the `auth.providers` line at the top of the secure YAML before starting: it should name the
sample `users.txt`, then the generated `kof-output/kafka-users.txt`, then `mtls` and `oauth2`. All
three mechanisms are active at once, so a client that authenticates by any one of them is accepted.

### Step 7 — Shut down the FTL Servers

The last scenario, so nothing follows this cluster — but leaving it running keeps three Kafka ports
and a realm port occupied. The `-secure` YAML puts TLS and authentication on the realm service,
so stopping it needs `https://`, a trust flag and an account holding the `ftl-admin` role.

```bash
FTLS="https://$(awk '/^ *SRV1:/ {print $2; exit}' kof-output/tibftlserver-cluster-secure.yaml)"

tibftladmin --ftlserver "$FTLS" --tls.trust.file $FTL_MTLS/client_trust.pem \
  -u admin -pw admin-pw -xc
```

`-xc` (`--shutdown_cluster`) stops every FTL Server in the cluster, so it does not matter which member you address — one call replaces three Ctrl-C's.

The command is asynchronous: it returns as soon as the server accepts the request, not when the process is gone. `--status` is how you confirm — it fails to connect once the server is down.

Any of the three mechanisms can carry this call, since all three are active: `admin`/`admin-pw` from the sample `users.txt`, the `--tls.client.*` flags of [Scenario 8](#scenario-8--3-node-mutual-tls-mtls), Step 7, or `--oauth2.token <token>`. All three need the `ftl-admin` role — without it the caller authenticates and is then refused with *403 Forbidden*. `--tls.trust.file` is unrelated to the choice: it names the CA that signed the certificate the server presents, the file given to `--tls-ca`, which in this scenario is the **mtls** sample's `client_trust.pem`.

---

## Common options reference

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

The sections below list the same flags as the corresponding `-h <group>` topic: the OAuth2 flags
used in scenarios 6, 9, 11 and 12 are all under `tibftlimportconfig -h oauth`, and the TLS/mTLS
flags from scenarios 5, 8, 10, 11 and 12 under `tibftlimportconfig -h tls`.

Both spellings work: Go's `flag` package accepts `-flag` and `--flag` alike. The scenarios above
use `--`, the tables below use `-`.

### Core flags (`-h core`)

| Flag | Default | Description |
|---|---|---|
| `-output-dir` | `./kof-output` | Directory where output files are written |
| `-data-dir` | `/var/tmp/kof/data` | FSK data directory path on FTL Server hosts |
| `-core-servers` | _(auto)_ | Comma-separated `NAME=host:port` list for `globals.core.servers`<br/>e.g. `SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602`<br/>If omitted, ports are derived from the cluster in range 5600–5699 — the same brokers always yield the same ports, so re-running the tool does not move them |
| `-transport-type` | `auto` | Transport type for all FTL Server connections in `ftlserver.json`: `auto` or `dtcp`<br/>`auto` leaves the choice to the realm server, which resolves each connection at deployment time — dynamic TCP for client and intra-cluster transports, static TCP for inter-cluster and DR transports<br/>`dtcp` pins every transport to dynamic TCP |
| `-replication-factor` | `3` | FTL Servers per FSK shard (`kof.cluster.N`): `1`, `3` or `5`. The number of input `server.properties` files must be an exact multiple of it — 9 files at `3` give three shards, 6 give two, 5 files at `5` give one. The FTL realm keeps its own 3 servers either way.<br/>The accepted values are odd because a shard needs a majority quorum, so **2 input files are refused**; use `-replication-factor 1` for one unreplicated shard per broker. With a single input file the default is `1` — a standalone server is explicitly unreplicated. |
| `-disk-persistence` | `async` | `disk_persistence` for the generated `kof.cluster.N`: `async`, `sync` or `in-memory`<br/>`async` writes are buffered for an eventual flush to disk; `sync` flushes every write before acknowledging it; `in-memory` writes nothing to disk and turns off the cluster's `disk_index` and `disk_compact`, which require disk persistence<br/>The data store stays `async` and the sync and meta stores `sync` whichever the cluster is — except under `in-memory`, where the stores are in-memory too (see [`ftlserver.json`](#ftlserverjson)) |
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
| `-dr-servers` | _(none)_ | Comma-separated `DRSRV1=host:port,DRSRV2=host:port,...` DR server list.<br/>Providing this flag enables DR mode for all generated files. |
| `-dr-data-dir` | `<data-dir>/dr` | Data directory for DR FTL Servers on DR hosts |

### Inspection and conversion flags (`-h info`)

| Flag | Default | Description |
|---|---|---|
| `-list-properties` | `false` | Print how each Apache Kafka listener/security property is treated, then exit |
| `-color` | `auto` | Colorize `-list-properties` output: `auto`, `always`, or `never` |
| `-auto` | `false` | Actually run the keystore conversions (JKS/PKCS12 → PEM via `keytool`/`openssl`) rather than only printing the commands. The generated config names the `.pem` either way; items needing a human decision stay `RESOLVE-REQUIRED` |

---

## Disaster recovery mode

When `-dr-servers` is provided, DR mode is activated for all output files:

- **`tibftlserver-cluster.yaml`** gains `globals.dr:` (pointing to DR servers), `auto.init.primary.on.first.startup: true`, and `label: PRIMARY_SERVER` on each realm block.
- **`tibftlserver-cluster-dr.yaml`** is generated with DR servers as `core.servers`, a back-reference `globals.dr:` to the primary servers, and `label: DR_SERVER` on realm blocks. Their persistence entries are named `drpserver1..N`.
- **`ftlserver.json`** clusters get `dr_enabled: true`, a second persistence set `_DRset` with DR replicas, and transport roles swapped (`dr_transport` populated, `inter_cluster_transport` empty for all FTL Servers).

The DR YAML covers every DR server in one file, exactly as the primary YAML does:

```
9 files + -dr-servers ... →
    tibftlserver-cluster.yaml      (primary: pserver1–9)
    tibftlserver-cluster-dr.yaml   (DR: drpserver1–9)
    ftlserver.json                 (kof.cluster.0/1/2 with dr_enabled: true)
```

Pass one `-dr-servers` entry per input file. When the list is shorter, the extra replicas are named `DRSRV<n>` and fall back to the primary's addresses, which is only usable if the DR cluster runs on other hosts.

### Adding DR to an existing conversion

Add `-dr-servers` (and optionally `-dr-data-dir`) to any `tibftlimportconfig` command — any of the scenarios above, or any of the [example configurations](#example-configurations) — to enable Disaster Recovery output.

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

Combine the flags of [example 12](#12--9-broker-full-security-stack-3-shards) with `-dr-servers` to generate DR-enabled output for a 9-server, 3-shard deployment. Produces three cluster YAML files — plain, secure and DR, each carrying all nine servers — and a `ftlserver.json` with `dr_enabled: true` on all three clusters.

---

## Output details

### `tibftlserver-cluster.yaml`

Every FTL Server, in one file. Names and ports for the first
shard's servers come from `-core-servers`; if that flag is omitted the names default to `SRV1–SRV3`
and the ports are derived from the cluster in 5600–5699. Servers past the first shard are named
`SRV4`, `SRV5`, … and take their ports from 5700–5799. The `-n` argument is the `servers:` key:

```sh
tibftlserver -c tibftlserver-cluster.yaml -n SRV1
tibftlserver -c tibftlserver-cluster.yaml -n SRV2
tibftlserver -c tibftlserver-cluster.yaml -n SRV3
```

**No `services:` section.** Realm settings are written per server, on each `- realm:` entry, rather
than once in a shared `services:` block. That includes `initial.realm.config`, so every server in
the file names the generated `ftlserver.json` itself, and `data`, which is `<-data-dir>/srv<n>` —
its own directory per server, separate from the pserver's `<-data-dir>/pserver<n>`:

```yaml
servers:
  SRV1:
  - realm:
      data: /var/tmp/kof/data/srv1
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

DR servers get the same block, with `-dr-data-dir` as the suggested path.

**Schema daemon (`-tibschemad`).** With the flag set, the first 3 servers each get a second
persistence service and a `- tibschemad:` entry. No extra `tibftlserver` processes and no extra
ports — the schema persistence rides the process that already hosts the FSK one, so a 3-broker
conversion is still three servers:

```yaml
servers:
  SRV1:
  - realm:
      data: /var/tmp/kof/data/srv1
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

`cluster.size` is the size of the schema daemon's own cluster — 1 for a standalone server, 3
otherwise. It does not scale with the pservers: a 9-server, 3-shard conversion still puts the
schema daemon on SRV1–3 only. The block is written to the plain and secure YAMLs; DR files are not
covered yet. `auth.type` is always `none` for now; OAuth options for `tibschemad` are a later
addition.

### `tibftlserver-cluster-dr.yaml`

DR replica cluster, laid out like the plain YAML and likewise covering every server. Start on the
DR hosts using the DR server names from `-dr-servers`:

```sh
tibftlserver -c tibftlserver-cluster-dr.yaml -n drserver1
tibftlserver -c tibftlserver-cluster-dr.yaml -n drserver2
tibftlserver -c tibftlserver-cluster-dr.yaml -n drserver3
```

### `tibftlserver-cluster-secure.yaml`

The same servers as `tibftlserver-cluster.yaml` — run one file or the other, not both — with each `ftlserver.properties` block extended by TLS and auth settings, above the logging settings every cluster YAML already carries. Auth mode is determined by the Apache Kafka listener types:

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

## Resolving INVALID output

When the tool cannot fully convert a setting it writes a `RESOLVE-REQUIRED` block in
the generated `kof.broker.N.properties` and exits with code 2. Common causes:

- **Unrecognized SASL handler class** — set the `=<oauth|file|inline>` value in the
  block and re-run.
- **Unsupported SASL mechanism** (e.g. GSSAPI, SCRAM) on a listener that speaks SASL —
  switch the listener to `PLAIN` or `OAUTHBEARER` in the block.
- **Unrecognized `authorizer.class.name`** — a custom Java authorizer FSK cannot run.
  Set the value to `KofAuthorizer` to use FSK's own ACL enforcement.

After editing, re-run the same `tibftlimportconfig` command. The tool recomputes status on
every run; once all blocks are resolved the exit code is 0 and output shows:

```
All kof.broker.*.properties files are processed successfully.
```

### Keystores: ACCEPTED but not yet runnable

PEM is FSK's keystore format, so a JKS/PKCS12 keystore is translated rather than refused:
the type becomes `PEM`, the location is repointed at the `.pem`, and the file is stamped
ACCEPTED. **Creating that `.pem` is a separate step, and until it is done the configuration
will not run.** The tool is explicit about it:

```
*** SEVERE WARNING -- kof-output/kof.broker.1.properties WILL NOT RUN WITH FSK AS IT STANDS ***
2 Java keystore(s) were rewritten to PEM, but the .pem file(s) do not exist.
tibftlserver will fail at startup on the missing file. Create them first:
```

followed by the `keytool`/`openssl` commands for each one — the same commands the generated
file carries above each setting. Run them, or re-run with `--auto` on a host that holds the
`.jks`, and the warning disappears. When `--auto` cannot do a conversion it says so per
keystore and the SEVERE WARNING still stands.

### Settings that are present but doing nothing

These are commented out with a reason, not flagged. An empty `authorizer.class.name`, a
`sasl.enabled.mechanisms` list on a broker where no listener speaks SASL, an
`ssl.keystore.type` with no matching `.location` — each had no effect on the source broker
either, so none of them makes the file INVALID.

Settings with no FSK equivalent (Kerberos families, delegation tokens, per-IP
connection limits) are written to `unsupported.properties` for reference and do not
cause an INVALID status.

---

## Example configurations

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

**Output:** `tibftlserver-standalone.yaml` (1 SRV + 1 FTL Server), `ftlserver.json`, `kof.broker.1.properties`, `unsupported.properties`

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

**Output:** `tibftlserver-standalone.yaml` (1 SRV + 1 FTL Server), `ftlserver.json`, `kof.broker.1.properties` (`node.id=0`, from `broker.id=0`), `unsupported.properties`

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

**Output:** `tibftlserver-standalone.yaml`, `tibftlserver-standalone-secure.yaml` (auth mode: file-auth+tls), `ftlserver.json`, `kof.broker.1.properties`, `unsupported.properties`

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

**Output:** `tibftlserver-standalone.yaml`, `tibftlserver-standalone-secure.yaml` (auth mode: oauth2), `ftlserver.json`, `kof.broker.1.properties`, `unsupported.properties`

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

**Output:** `tibftlserver-cluster.yaml` (SRV1–9, pserver1–9), `ftlserver.json` (3 clusters: `kof.cluster.0/1/2`), `kof.broker.{1–9}.properties`, `ftl-users.txt`, `unsupported.properties`

All nine FTL Servers are in the one YAML: `globals.core.servers` lists SRV1–3, the first shard, and SRV4–9 each carry their own `ftl: server:` address.

The three shards are the default [`-replication-factor`](#core-flags--h-core) of 3. Adding `--replication-factor 1` to the same nine inputs gives nine unreplicated shards, `kof.cluster.0` through `.8`, with the YAML unchanged.

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

**Output:** `tibftlserver-cluster.yaml`, `tibftlserver-cluster-secure.yaml` (auth mode: oauth2 + mTLS), `ftlserver.json` (3 clusters), `kof.broker.{1–9}.properties`, `unsupported.properties`

As in example 11, the three shards come from the default [`-replication-factor`](#core-flags--h-core) of 3, and both YAMLs carry all nine FTL Servers. Run the secure one instead of the plain one — it is the same cluster with the TLS and OAuth settings added.

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

### All example directories

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
diff examples/01-single-node-plaintext/output/tibftlserver-standalone.yaml \
     examples/21-single-node-tibschemad/output/tibftlserver-standalone.yaml
diff examples/02-3broker-plaintext/output/tibftlserver-cluster.yaml \
     examples/22-3broker-tibschemad/output/tibftlserver-cluster.yaml
```

---

## Creating self-signed Kafka certificates

[Scenario 5](#scenario-5--single-node-sasl-plain-over-tls) and the secured scenarios after it
need a Kafka keystore and truststore that you supply. If you have none, these commands produce
the pair those scenarios name, with the passwords their `server.properties` expects. This is a
demo PKI — one self-signed certificate acting as its own authority, no CA hierarchy, no
revocation. Do not model a production deployment on it.

### 1. The broker keystore

```bash
mkdir -p /var/tmp/kafka/scenario5/certs

keytool -genkeypair -alias kafka-server \
  -keyalg RSA -keysize 2048 -validity 365 \
  -dname "CN=localhost, OU=FSK, O=Example, L=Palo Alto, ST=CA, C=US" \
  -ext "SAN=DNS:localhost,IP:127.0.0.1" \
  -keystore /var/tmp/kafka/scenario5/certs/server.keystore.jks -storetype JKS \
  -storepass keystorePassword123 -keypass keyPassword123
```

`SAN` is the part that matters and the part most often left out: a Kafka client verifies the
hostname it dialled against the certificate's subject alternative name, not against `CN`. The
scenarios advertise `localhost`, so `DNS:localhost` has to be in there. Advertising a real
hostname means naming that hostname instead.

`-storepass` and `-keypass` become `ssl.keystore.password` and `ssl.key.password` in Step 1.
Modern JDKs print a warning that JKS is a proprietary format and suggest migrating to PKCS12 —
harmless here, since Step 2 converts the store anyway.

### 2. The truststore

A self-signed certificate is its own authority, so the truststore holds that same certificate:

```bash
keytool -exportcert -alias kafka-server -rfc \
  -keystore /var/tmp/kafka/scenario5/certs/server.keystore.jks \
  -storepass keystorePassword123 \
  -file /var/tmp/kafka/scenario5/certs/server.cer

keytool -importcert -noprompt -alias kafka-server \
  -file /var/tmp/kafka/scenario5/certs/server.cer \
  -keystore /var/tmp/kafka/scenario5/certs/kafka.truststore.jks -storetype JKS \
  -storepass truststorePassword123
```

`-storepass` here is `ssl.truststore.password` in Step 1. Kafka clients need this truststore too —
it is what `ssl.truststore.location` points at in the `client.properties` of Step 3.

### 3. The PEM copies FSK reads

Apache Kafka reads the JKS stores above; FSK reads PEM. This is Step 2 of the scenario, with the
passwords filled in so nothing prompts:

```bash
keytool -importkeystore -alias kafka-server \
  -srckeystore /var/tmp/kafka/scenario5/certs/server.keystore.jks -srcstoretype JKS \
  -srcstorepass keystorePassword123 -srckeypass keyPassword123 \
  -destkeystore /var/tmp/kafka/scenario5/certs/server.keystore.p12 -deststoretype PKCS12 \
  -deststorepass keystorePassword123 -destkeypass keystorePassword123

openssl pkcs12 -in /var/tmp/kafka/scenario5/certs/server.keystore.p12 \
  -passin pass:keystorePassword123 -nodes \
  -out /var/tmp/kafka/scenario5/certs/server.keystore.pem

cp /var/tmp/kafka/scenario5/certs/server.cer \
   /var/tmp/kafka/scenario5/certs/kafka.truststore.pem
```

Two details differ from the generic commands the tool prints. `keytool -importkeystore` refuses
`-srckeypass`/`-destkeypass` unless `-alias` names a single entry, so the alias is given
explicitly. And the truststore needs no conversion at all: it holds one trusted certificate and no
private key, which `keytool -importkeystore` declines to migrate on several JDKs — the
`-exportcert -rfc` output from step 2 above already *is* the PEM, so it is simply copied.

Check that the FTL installation's `openssl` is the one on your `PATH`, or call `/usr/bin/openssl`
explicitly; a mismatched shared library gives `Library not loaded: libssl.3.dylib` rather than a
useful error.
