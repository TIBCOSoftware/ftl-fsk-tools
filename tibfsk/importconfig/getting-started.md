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

Each scenario below builds on the previous one. Start with the simplest setup and
advance as your environment requires.

## Before you start

Every step runs end to end: bring up the Apache Kafka brokers the `server.properties` describes,
convert the configuration, then start the FSK servers on the result. Five things hold for all twelve.

**`tibftlimportconfig`.** Every `tibftlimportconfig` command below is the binary checked in at
`bin/tibftlimportconfig` (linux/amd64) — no build step is needed. Put it on your `PATH`:

```bash
export PATH=/path/to/ftl-fsk-tools/tibfsk/importconfig/bin:$PATH
```

Building from source is only necessary on another platform or when changing the tool; see
[README.md](./README.md#building-from-source).

**`KAFKA_HOME`.** The Kafka commands assume a Kafka 4.x installation:

```bash
export KAFKA_HOME=/opt/kafka
```

Steps 3 and 4 are the exception: they run Kafka in ZooKeeper mode, which 4.x removed, so those
two need `KAFKA_HOME` pointed at Kafka 3.9 or earlier. The conversion itself does not care —
`tibftlimportconfig` reads a `server.properties`, not a running broker.

**Every broker needs an `inter.broker.listener.name`.** Kafka defaults it to a listener called
`PLAINTEXT`, and none of the KRaft configurations here has one, so leaving it out stops the broker
before it opens a port:

```
java.lang.IllegalArgumentException: requirement failed: inter.broker.listener.name must be
a listener name defined in advertised.listeners.
```

Which listener to name matters to the translation. FSK carries inter-broker traffic over FTL, so
the tool treats the named listener as internal and drops it from the generated broker properties —
but only when another client listener remains. Where a step has a single client listener it points
`inter.broker.listener.name` at that listener and the listener survives; where a step has two, it
adds a separate `INTERNAL` listener for inter-broker traffic so that both client listeners come
through. Either way the key itself is reported in `unsupported.properties`.

**Stop Kafka before starting FSK.** The tool translates the client-facing listeners faithfully, so
the FSK pservers bind the *same* ports the brokers were just using — 9092 in the single-node steps,
9092/9102/9112 across the three brokers below, plus 9094/9104/9114 or 9095/9105/9115 wherever a
second client listener is configured. Every 3-node step follows that convention: broker *n* takes
each of broker 1's ports plus `10 × (n − 1)`. On one host the two cannot run at once:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

Running the brokers first is not required to convert a file — it proves the `server.properties` is
a valid Kafka configuration before you translate it.

**Start `tibftlserver` from the directory you ran `tibftlimportconfig` in.** The generated YAML
records `initial.realm.config` and `kof.broker.properties` exactly as they were passed —
`kof-output/realm.json` for `--output-dir ./kof-output` — so those paths resolve against the working
directory, not against the YAML's own location. `cd kof-output` first and the server will not find
its realm. Pass an absolute `--output-dir` if you would rather not care.

---

## Step 1 — Single-node plaintext (KRaft)

The simplest possible configuration: one broker, no authentication, no TLS. Use this
for local development and tool exploration only.

### Kafka server.properties

```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093

listeners=CLIENT://localhost:9092,CONTROLLER://localhost:9093
inter.broker.listener.name=CLIENT
advertised.listeners=CLIENT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT

log.dirs=/var/tmp/kafka/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

:::tip Listener naming
Name your client-facing listener `CLIENT` (not `PLAINTEXT`). The tool strips the
`CONTROLLER` listener automatically — FTL carries controller traffic natively.
:::

### Start Apache Kafka (KRaft)

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
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list
```

`--ignore-formatted` makes the format step a no-op on an already-formatted directory, so the
sequence is safe to re-run. `--standalone` is what declares this node the sole member of the
controller quorum; without it, `kafka-storage.sh` refuses to format a config that names
`controller.quorum.bootstrap.servers` but no voters, and the broker then dies on startup with
*No readable meta.properties files found*.

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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

### Start the FSK servers

Stop Kafka first — the pserver is about to bind port 9092:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
```

A single broker converts to a standalone server rather than a cluster, so there is one process to
start, named for the single entry under `servers:`:

```bash
tibftlserver -c kof-output/tibftlserver_standalone.yaml -n SRV1
```

No realm upload step: the YAML points `initial.realm.config` at the generated `realm.json`, so the
server seeds the realm itself on first startup. Kafka clients can now connect to `localhost:9092`
as before.

**→ Continue to [Step 2](#step-2--3-node-plaintext-cluster-kraft) to scale the same configuration to three brokers,
or [Step 3](#step-3--single-node-plaintext-zookeeper) if your brokers still run under ZooKeeper.**

---

## Step 2 — 3-node plaintext cluster (KRaft)

Scale out to a 3-broker KRaft cluster. Pass one `server.properties` file per broker;
the tool derives the pserver count from the file count.

### Kafka server.properties (broker 1 of 3)

```properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113

listeners=CLIENT://localhost:9092,CONTROLLER://localhost:9093
inter.broker.listener.name=CLIENT
advertised.listeners=CLIENT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT

log.dirs=/var/tmp/kafka/data/broker-1
num.partitions=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

### What changes in each broker file

Copy the file above to `server-2.properties` and `server-3.properties`, then change these four
lines in each — the ports are the ones the voter list already names:

```properties
# server-1.properties
node.id=1
listeners=CLIENT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=CLIENT://localhost:9092
log.dirs=/var/tmp/kafka/data/broker-1

# server-2.properties
node.id=2
listeners=CLIENT://localhost:9102,CONTROLLER://localhost:9103
advertised.listeners=CLIENT://localhost:9102
log.dirs=/var/tmp/kafka/data/broker-2

# server-3.properties
node.id=3
listeners=CLIENT://localhost:9112,CONTROLLER://localhost:9113
advertised.listeners=CLIENT://localhost:9112
log.dirs=/var/tmp/kafka/data/broker-3
```

Every other line stays as it is in all three files. `controller.quorum.voters` in particular is
identical everywhere — each broker needs the address of all three controllers, its own included,
and a node that lists only itself forms its own quorum and never joins the others.

### Start Apache Kafka (KRaft)

All three brokers must be formatted with the **same** cluster ID — that is what makes them one
cluster rather than three. Generate it once, outside the loop:

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/logs/broker-$n" \
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

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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
`SRV3`) with FTL ports derived from the cluster in the 5600–5799 range — the same brokers
always yield the same ports, so re-running the tool does not move them. To pin specific
ports use `--core-servers SRV1=host1:5600,SRV2=host2:5601,SRV3=host3:5602`.

### Start the FSK servers

Stop the brokers first — the three pservers take over ports 9092, 9102 and 9112:

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

One `tibftlserver` per entry under `servers:`, each in its own shell (the header comment of the
generated YAML lists these same three commands):

```bash
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV3
```

All three share one YAML and one `realm.json`; `-n` is what selects which entry a process runs.
Each carries the same `initial.realm.config`, so whichever starts first seeds the realm and the
other two join it. The cluster is available once two of the three are up.

**→ Continue to [Step 3](#step-3--single-node-plaintext-zookeeper) for the ZooKeeper equivalents of Steps 1 and 2,
or skip to [Step 5](#step-5--single-node-sasl-plain-over-tls) to start adding security.**

---

## Step 3 — Single-node plaintext (ZooKeeper)

Kafka 3.9 and earlier keep their cluster metadata in ZooKeeper rather than in a KRaft quorum.
The conversion is the same work — `tibftlimportconfig` reads the same file and writes the same
artifacts — but two things differ from Step 1, and both are worth seeing before you convert a
real ZooKeeper cluster.

**FSK has no ZooKeeper.** The cluster membership and metadata ZooKeeper holds for Kafka are
FTL-native, so every `zookeeper.*` key is routed to `unsupported.properties`. Nothing is lost
in the translation; there is simply nothing for FSK to do with them.

**ZooKeeper mode spells the broker's identity `broker.id`**, the key KRaft later renamed to
`node.id`. FSK reads `node.id` only, so the tool renames it on the way through and records
where the value came from.

:::note Kafka 4.x cannot run this step
ZooKeeper support was removed in Kafka 4.0, so a 4.x installation ships no
`zookeeper-server-start.sh`. Point `KAFKA_HOME` at Kafka 3.9 or earlier for Steps 3 and 4. The
*conversion* works whatever Kafka version is installed — only starting the brokers needs the
older release.
:::

### Kafka server.properties

```properties
broker.id=0

zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000

listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT

log.dirs=/var/tmp/kafka/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

No `process.roles` and no `CONTROLLER` listener: the controller is elected through ZooKeeper.
`PLAINTEXT` is the only listener and also carries inter-broker traffic, which is the one case
where the tool keeps a listener that `inter.broker.listener.name` names — stripping it would
leave the pserver nothing to bind. It is also why Step 1's advice to call the client listener
`CLIENT` rather than `PLAINTEXT` does not apply here; a ZooKeeper-mode broker conventionally
has exactly this listener.

### Start Apache Kafka (ZooKeeper)

No storage formatting and no cluster ID — ZooKeeper mode has neither. Start ZooKeeper first,
then the broker:

```bash
cat > zookeeper.properties <<'EOF'
dataDir=/var/tmp/zookeeper
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
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list
```

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  server-1.properties
```

Identical to Step 1 — the tool is not told which metadata mode the source cluster used, and
does not need to be.

### Output

```
Writing file: ./kof-output/kof.broker.1.properties [
Writing file: ./kof-output/unsupported.properties
Writing file: ./kof-output/tibftlserver_standalone.yaml
Writing file: ./kof-output/realm.json

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

### Start the FSK servers

Stop the broker, then ZooKeeper, then start the standalone server:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
"$KAFKA_HOME/bin/zookeeper-server-stop.sh"

tibftlserver -c kof-output/tibftlserver_standalone.yaml -n SRV1
```

That order matters only for the log: a broker whose ZooKeeper session drops while it is still
running writes a stream of connection failures on the way down. Nothing takes ZooKeeper's place
on the FSK side — the realm server the YAML starts holds the cluster metadata itself.

**→ Continue to [Step 4](#step-4--3-node-plaintext-cluster-zookeeper) for the same configuration across three brokers.**

---

## Step 4 — 3-node plaintext cluster (ZooKeeper)

The ZooKeeper counterpart of Step 2. One ZooKeeper serves all three brokers, which differ only
in `broker.id`, listener port and `log.dirs` — there is no voter list to keep in step, because
there is no KRaft quorum.

Kafka 3.9 or earlier is required here too, for the same reason as Step 3.

### Kafka server.properties (broker 1 of 3)

```properties
broker.id=1

zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000

listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT

log.dirs=/var/tmp/kafka/data/broker-1
num.partitions=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

### What changes in each broker file

Copy the file above to `server-2.properties` and `server-3.properties`, then change these four
lines in each:

```properties
# server-1.properties
broker.id=1
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
log.dirs=/var/tmp/kafka/data/broker-1

# server-2.properties
broker.id=2
listeners=PLAINTEXT://localhost:9102
advertised.listeners=PLAINTEXT://localhost:9102
log.dirs=/var/tmp/kafka/data/broker-2

# server-3.properties
broker.id=3
listeners=PLAINTEXT://localhost:9112
advertised.listeners=PLAINTEXT://localhost:9112
log.dirs=/var/tmp/kafka/data/broker-3
```

`zookeeper.connect` is identical in all three — that shared ZooKeeper is what makes them one
cluster, the way a shared cluster ID does under KRaft. A single ZooKeeper is fine for an
example; production runs an ensemble of three or five.

### Start Apache Kafka (ZooKeeper)

```bash
cat > zookeeper.properties <<'EOF'
dataDir=/var/tmp/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF

"$KAFKA_HOME/bin/zookeeper-server-start.sh" -daemon zookeeper.properties

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/logs/broker-$n" \
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

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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

Three pservers, exactly as in Step 2. Each broker's `broker.id` becomes the `node.id` of the
matching `kof.broker.N.properties`, and the `zookeeper.*` keys are collected once in
`unsupported.properties` rather than repeated per broker.

### Start the FSK servers

Stop the brokers and ZooKeeper — the three pservers are about to take over 9092, 9102 and 9112:

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"
"$KAFKA_HOME/bin/zookeeper-server-stop.sh"
```

`kafka-server-stop.sh` stops all three at once: it sends `SIGTERM` to every broker JVM on the
host, so run it before ZooKeeper and check nothing else you care about was caught in it. Then
one `tibftlserver` per entry under `servers:`, each in its own shell:

```bash
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster.yaml -n SRV3
```

All three share one YAML and one `realm.json`; whichever starts first seeds the realm and the
other two join it. The cluster is available once two of the three are up.

**→ Continue to [Step 5](#step-5--single-node-sasl-plain-over-tls) to add authentication and TLS.**

---

## Step 5 — Single-node SASL/PLAIN over TLS

Adds username/password authentication and TLS encryption. This is the most common
starting point for non-production secured environments.

Kafka uses JKS/PKCS12 keystores; FSK reads PEM. The tool rewrites `ssl.keystore.type` to
`PEM` and repoints `ssl.keystore.location` at the `.pem` path, then prints the exact
commands that create that file — creating it is a real conversion, not a rename, and it
is the one step the tool leaves to you.

### Kafka server.properties

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
ssl.keystore.location=/etc/kafka/certs/server.keystore.jks
ssl.keystore.password=keystorePassword123
ssl.key.password=keyPassword123
ssl.truststore.type=JKS
ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
ssl.truststore.password=truststorePassword123

# SASL/PLAIN on the BROKER listener, used for client and inter-broker traffic alike
sasl.mechanism.inter.broker.protocol=PLAIN
listener.name.broker.sasl.enabled.mechanisms=PLAIN
listener.name.broker.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" \
  user_admin="admin-secret" user_producer="producer-secret" user_consumer="consumer-secret";

log.dirs=/var/tmp/kafka/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

### Convert keystores to PEM

The generated `kof.broker.1.properties` already names the `.pem` files; these are the
commands, printed in the file above each setting and again as a `SEVERE WARNING` at the end
of the run, that actually create them. For JKS:

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

### Start Apache Kafka (KRaft)

Same sequence as Step 1. The broker reads the JKS keystores named in its `server.properties`, so
those must exist before it will start:

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

"$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted --standalone \
  -t "$KAFKA_CLUSTER_ID" -c server-1.properties

"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon server-1.properties
```

A plain `kafka-topics.sh --list` will not reach a SASL_SSL listener; verify with a client
properties file carrying the truststore and JAAS settings, or just check the broker log.

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
  --tls-cert /etc/kafka/certs/server.keystore.pem \
  --tls-key  /etc/kafka/certs/server.keystore.pem \
  --auth-users-file /etc/ftl/users.txt \
  server-1.properties
```

The `--auth-users-file` points to an FTL users file that maps usernames extracted
from the inline JAAS config. The tool writes a `tibftlserver-cluster-secure.yaml` alongside
the main cluster YAML when TLS or auth flags are supplied.

### Start the FSK servers

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"

tibftlserver -c kof-output/tibftlserver_standalone-secure.yaml -n SRV1
```

Start from the `-secure` YAML, not the plain one: it is the file that carries the TLS certificate
paths and the `auth.providers` list. The plain YAML is written too, and is the one to use if you
want the same topology without security.

**→ Continue to [Step 6](#step-6--single-node-oauth2) to replace SASL/PLAIN with OAuth2,
or jump to [Step 7](#step-7--3-node-sasl-plain-cluster) for a 3-node SASL cluster.**

---

## Step 6 — Single-node OAuth2

Replaces username/password authentication with OAuth 2.0 bearer tokens. Kafka clients
present a JWT; the tool wires the OAUTHBEARER mechanism through to FSK's OAuth2
provider.

### Kafka server.properties

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
ssl.keystore.location=/etc/kafka/certs/server.keystore.jks
ssl.keystore.password=keystorePassword123
ssl.key.password=keyPassword123
ssl.truststore.type=JKS
ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
ssl.truststore.password=truststorePassword123

sasl.mechanism.inter.broker.protocol=OAUTHBEARER
listener.name.oauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.oauth.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
  oauth.token.endpoint.uri="https://auth.example.com/oauth/token" \
  oauth.client.id="kafka-broker-1" oauth.client.secret="broker-client-secret";
listener.name.oauth.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler
listener.name.oauth.oauthbearer.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler

log.dirs=/var/tmp/kafka/data/broker-1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

### Start Apache Kafka (KRaft)

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

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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

### Start the FSK servers

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"

tibftlserver -c kof-output/tibftlserver_standalone-secure.yaml -n SRV1
```

The secure YAML carries the `oauth2.*` globals and the per-server validation key, so the server
reaches the authorization server on its own at startup — check the log for the JWKS fetch if
tokens are rejected.

**→ Continue to [Step 8](#step-8--3-node-mutual-tls-mtls) for mTLS, or
[Step 9](#step-9--3-node-sasl-plain--oauth2-dual-listener) for a dual SASL+OAuth2 setup.**

---

## Step 7 — 3-node SASL/PLAIN cluster

Add SASL/PLAIN + TLS to a 3-node cluster. Every broker carries the Step 5 listener and security
configuration unchanged — the TLS keystores, the `BROKER` listener at `SASL_SSL`,
`inter.broker.listener.name=BROKER`, `sasl.mechanism.inter.broker.protocol=PLAIN`, and the inline
JAAS users.

### What changes in each broker file

```properties
# server-1.properties
node.id=1
listeners=BROKER://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=BROKER://localhost:9092
log.dirs=/var/tmp/kafka/data/broker-1

# server-2.properties
node.id=2
listeners=BROKER://localhost:9102,CONTROLLER://localhost:9103
advertised.listeners=BROKER://localhost:9102
log.dirs=/var/tmp/kafka/data/broker-2

# server-3.properties
node.id=3
listeners=BROKER://localhost:9112,CONTROLLER://localhost:9113
advertised.listeners=BROKER://localhost:9112
log.dirs=/var/tmp/kafka/data/broker-3
```

Two things change identically in all three files, because Step 5 was a single node: swap
`controller.quorum.bootstrap.servers` for the voter list
`controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113`, and raise the
replication settings to `offsets.topic.replication.factor=3`,
`transaction.state.log.replication.factor=3` and `transaction.state.log.min.isr=2` — the values
Step 2 uses. The three brokers share one server certificate, so nothing about the TLS configuration
differs per file.

### Start Apache Kafka (KRaft)

Same three-broker sequence as Step 2 — one cluster ID shared by all three:

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

The keystores and truststore named in the properties files must exist before the brokers will
start. The listeners are SASL_SSL, so the plain `kafka-topics.sh --list` check does not apply here;
check the broker logs under `$KAFKA_HOME/logs` instead.

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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

### Start the FSK servers

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"

tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

Three shells, one per server. The `-secure` YAML is the one that carries the certificate paths and
`auth.providers`; the two generated users files are referenced from it by the paths they had at
generation time, so keep them where the tool wrote them.

**→ Continue to [Step 8](#step-8--3-node-mutual-tls-mtls) to add client certificate
authentication.**

---

## Step 8 — 3-node mutual TLS (mTLS)

Client certificates replace username/password. The Kafka `SSL` listener with
`ssl.client.auth=required` maps to FSK's `tls-only` auth mode with
`tls.server.trust.file`.

### Kafka server.properties highlights

```properties
inter.broker.listener.name=MTLS
listener.security.protocol.map=CONTROLLER:SSL,MTLS:SSL

# Require client certificates on the MTLS listener
listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123
```

Those lines, plus `controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113`,
are the same in all three files.

### What changes in each broker file

```properties
# server-1.properties
node.id=1
listeners=MTLS://localhost:9094,CONTROLLER://localhost:9093
advertised.listeners=MTLS://localhost:9094
log.dirs=/var/tmp/kafka/data/broker-1

# server-2.properties
node.id=2
listeners=MTLS://localhost:9104,CONTROLLER://localhost:9103
advertised.listeners=MTLS://localhost:9104
log.dirs=/var/tmp/kafka/data/broker-2

# server-3.properties
node.id=3
listeners=MTLS://localhost:9114,CONTROLLER://localhost:9113
advertised.listeners=MTLS://localhost:9114
log.dirs=/var/tmp/kafka/data/broker-3
```

### Start Apache Kafka (KRaft)

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

Both the keystore and the client-CA truststore must be in place — with
`ssl.client.auth=required` the MTLS listener rejects every connection that arrives without a
certificate it can verify, including your own verification attempts.

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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

### Start the FSK servers

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"

tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

The pservers bind each broker's MTLS port (9094 for broker 1) rather than 9092 — there is no
plaintext listener in this configuration to translate. The servers present
`--tls-client-cert` to each other, so that certificate has to be one the CA in
`--tls-server-trust` signed, or the cluster will not form.

**→ Continue to [Step 9](#step-9--3-node-sasl-plain--oauth2-dual-listener) for a
dual-protocol setup, or [Step 10](#step-10--3-node-sasl-plain--mtls) to combine SASL
and mTLS on separate listeners.**

---

## Step 9 — 3-node SASL/PLAIN + OAuth2 (dual listener)

Two client-facing listeners on the same cluster: legacy clients use SASL/PLAIN; modern
clients use OAUTHBEARER. FSK runs both auth providers concurrently.

### Kafka server.properties highlights

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
internal and leave it out of the generated broker properties — and a step about serving SASL/PLAIN
and OAUTHBEARER side by side would end up with one client port. `INTERNAL` is dropped instead,
which is what you want, and both client listeners come through.

### What changes in each broker file

Four listeners means four ports to move per broker; the JAAS and OAuth lines above stay identical,
as does `controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113`.

```properties
# server-1.properties
node.id=1
listeners=SASL_AUTH://localhost:9092,OAUTH://localhost:9095,INTERNAL://localhost:9098,CONTROLLER://localhost:9093
advertised.listeners=SASL_AUTH://localhost:9092,OAUTH://localhost:9095,INTERNAL://localhost:9098
log.dirs=/var/tmp/kafka/data/broker-1

# server-2.properties
node.id=2
listeners=SASL_AUTH://localhost:9102,OAUTH://localhost:9105,INTERNAL://localhost:9108,CONTROLLER://localhost:9103
advertised.listeners=SASL_AUTH://localhost:9102,OAUTH://localhost:9105,INTERNAL://localhost:9108
log.dirs=/var/tmp/kafka/data/broker-2

# server-3.properties
node.id=3
listeners=SASL_AUTH://localhost:9112,OAUTH://localhost:9115,INTERNAL://localhost:9118,CONTROLLER://localhost:9113
advertised.listeners=SASL_AUTH://localhost:9112,OAUTH://localhost:9115,INTERNAL://localhost:9118
log.dirs=/var/tmp/kafka/data/broker-3
```

`INTERNAL` has to be advertised as well as bound: the other two brokers reach this one at the
address it advertises for the inter-broker listener, so leaving it out of `advertised.listeners`
stops the cluster forming.

### Start Apache Kafka (KRaft)

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
  LOG_DIR="/var/tmp/kafka/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

FSK needs no equivalent jar — token validation is built in and driven by `--oauth-jwks-url`.

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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

### Start the FSK servers

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"

tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

Each pserver serves both client ports, 9092 and 9095, from one process — the dual listener carries
over from the broker configuration. The servers reach the token endpoint at startup, so a failure
to fetch the JWKS shows up in the startup log rather than at first client connect.

**→ Continue to [Step 10](#step-10--3-node-sasl-plain--mtls) to combine SASL and mTLS.**

---

## Step 10 — 3-node SASL/PLAIN + mTLS

Two listeners on separate ports: one for SASL/PLAIN clients, one for mTLS clients.
Both share the same server certificate; `ssl.client.auth=required` applies only to the
mTLS listener.

### Kafka server.properties highlights

```properties
inter.broker.listener.name=INTERNAL
listener.security.protocol.map=CONTROLLER:SSL,SASL_AUTH:SASL_SSL,MTLS:SSL,INTERNAL:SSL

listener.name.sasl_auth.sasl.enabled.mechanisms=PLAIN
listener.name.sasl_auth.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" user_admin="admin-secret";

listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123
```

As in Step 9, inter-broker traffic gets its own `INTERNAL` listener so that both client listeners
survive the translation. `INTERNAL` is plain SSL — the brokers already trust each other's
certificates, so there is no reason to make them authenticate over SASL as well.

### What changes in each broker file

```properties
# server-1.properties
node.id=1
listeners=SASL_AUTH://localhost:9092,MTLS://localhost:9094,INTERNAL://localhost:9098,CONTROLLER://localhost:9093
advertised.listeners=SASL_AUTH://localhost:9092,MTLS://localhost:9094,INTERNAL://localhost:9098
log.dirs=/var/tmp/kafka/data/broker-1

# server-2.properties
node.id=2
listeners=SASL_AUTH://localhost:9102,MTLS://localhost:9104,INTERNAL://localhost:9108,CONTROLLER://localhost:9103
advertised.listeners=SASL_AUTH://localhost:9102,MTLS://localhost:9104,INTERNAL://localhost:9108
log.dirs=/var/tmp/kafka/data/broker-2

# server-3.properties
node.id=3
listeners=SASL_AUTH://localhost:9112,MTLS://localhost:9114,INTERNAL://localhost:9118,CONTROLLER://localhost:9113
advertised.listeners=SASL_AUTH://localhost:9112,MTLS://localhost:9114,INTERNAL://localhost:9118
log.dirs=/var/tmp/kafka/data/broker-3
```

Everything else is identical across the three files, `controller.quorum.voters` and the truststore
settings included.

### Start Apache Kafka (KRaft)

```bash
KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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

### Start the FSK servers

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"

tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

`auth.providers` lists `file:` and `mtls` together, so a client authenticates with either a
username/password or a certificate, depending on which port it connects to — 9092 or 9094.

---

## Step 11 — 3-node mTLS + OAuth2

The most secure multi-protocol configuration: mTLS for certificate-bearing clients,
OAUTHBEARER for token-bearing clients.

### Kafka server.properties highlights

```properties
inter.broker.listener.name=INTERNAL
listener.security.protocol.map=CONTROLLER:SSL,MTLS:SSL,OAUTH:SASL_SSL,INTERNAL:SSL

listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123

listener.name.oauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.oauth.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler
```

Two client listeners again, so inter-broker traffic takes a dedicated `INTERNAL` listener for the
reason Step 9 gives: name either client listener there and the tool reads it as internal and leaves
it out of the generated broker properties.

### What changes in each broker file

```properties
# server-1.properties
node.id=1
listeners=MTLS://localhost:9094,OAUTH://localhost:9095,INTERNAL://localhost:9098,CONTROLLER://localhost:9093
advertised.listeners=MTLS://localhost:9094,OAUTH://localhost:9095,INTERNAL://localhost:9098
log.dirs=/var/tmp/kafka/data/broker-1

# server-2.properties
node.id=2
listeners=MTLS://localhost:9104,OAUTH://localhost:9105,INTERNAL://localhost:9108,CONTROLLER://localhost:9103
advertised.listeners=MTLS://localhost:9104,OAUTH://localhost:9105,INTERNAL://localhost:9108
log.dirs=/var/tmp/kafka/data/broker-2

# server-3.properties
node.id=3
listeners=MTLS://localhost:9114,OAUTH://localhost:9115,INTERNAL://localhost:9118,CONTROLLER://localhost:9113
advertised.listeners=MTLS://localhost:9114,OAUTH://localhost:9115,INTERNAL://localhost:9118
log.dirs=/var/tmp/kafka/data/broker-3
```

`controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113` and everything under
*highlights* above are identical in all three files.

### Start Apache Kafka (KRaft)

The OAUTHBEARER listener needs the callback handler jars, as in Step 9:

```bash
export CLASSPATH="/opt/strimzi-oauth/*"

KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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

### Start the FSK servers

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"

tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

There is no `--auth-users-file` here, so the servers do not carry an internal username and
password. They authenticate to each other as OAuth2 clients instead, fetching a token from
`--oauth-token-url` with `--oauth-client-id` and `--oauth-client-secret` — which the secure YAML
writes as `oauth2.svr.client.id` and `oauth2.svr.client.secret`.

---

## Step 12 — 3-node SASL/PLAIN + mTLS + OAuth2

All three auth providers active simultaneously. Each client-facing listener uses a
different mechanism; FSK's `auth.providers` list in the secure YAML activates all of
them.

### Kafka server.properties highlights

```properties
inter.broker.listener.name=INTERNAL
listener.security.protocol.map=CONTROLLER:SSL,SASL_AUTH:SASL_SSL,MTLS:SSL,OAUTH:SASL_SSL,INTERNAL:SSL

listener.name.sasl_auth.sasl.enabled.mechanisms=PLAIN
listener.name.sasl_auth.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" password="admin-secret" user_admin="admin-secret";

listener.name.mtls.ssl.client.auth=required
listener.name.mtls.ssl.truststore.type=JKS
listener.name.mtls.ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
listener.name.mtls.ssl.truststore.password=truststorePassword123

listener.name.oauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.oauth.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler
```

### What changes in each broker file

Three client listeners and one internal one — five ports per broker, all moved together:

```properties
# server-1.properties
node.id=1
listeners=SASL_AUTH://localhost:9092,MTLS://localhost:9094,OAUTH://localhost:9095,INTERNAL://localhost:9098,CONTROLLER://localhost:9093
advertised.listeners=SASL_AUTH://localhost:9092,MTLS://localhost:9094,OAUTH://localhost:9095,INTERNAL://localhost:9098
log.dirs=/var/tmp/kafka/data/broker-1

# server-2.properties
node.id=2
listeners=SASL_AUTH://localhost:9102,MTLS://localhost:9104,OAUTH://localhost:9105,INTERNAL://localhost:9108,CONTROLLER://localhost:9103
advertised.listeners=SASL_AUTH://localhost:9102,MTLS://localhost:9104,OAUTH://localhost:9105,INTERNAL://localhost:9108
log.dirs=/var/tmp/kafka/data/broker-2

# server-3.properties
node.id=3
listeners=SASL_AUTH://localhost:9112,MTLS://localhost:9114,OAUTH://localhost:9115,INTERNAL://localhost:9118,CONTROLLER://localhost:9113
advertised.listeners=SASL_AUTH://localhost:9112,MTLS://localhost:9114,OAUTH://localhost:9115,INTERNAL://localhost:9118
log.dirs=/var/tmp/kafka/data/broker-3
```

`controller.quorum.voters=1@localhost:9093,2@localhost:9103,3@localhost:9113` and the security
lines above are identical in all three files.

### Start Apache Kafka (KRaft)

```bash
export CLASSPATH="/opt/strimzi-oauth/*"

KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

for n in 1 2 3; do
  "$KAFKA_HOME/bin/kafka-storage.sh" format --ignore-formatted \
    -t "$KAFKA_CLUSTER_ID" -c "server-$n.properties"
done

for n in 1 2 3; do
  LOG_DIR="/var/tmp/kafka/logs/broker-$n" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "server-$n.properties"
done
```

### Run the tool

```bash
tibftlimportconfig \
  --output-dir ./kof-output \
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

### Start the FSK servers

```bash
"$KAFKA_HOME/bin/kafka-server-stop.sh"

tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV1
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV2
tibftlserver -c kof-output/tibftlserver-cluster-secure.yaml -n SRV3
```

Check the `auth.providers` line at the top of the secure YAML before starting: it should read
`file:/etc/ftl/users.txt,mtls,oauth2` (plus a second `file:` entry for the generated
`kafka-users.txt` when the broker properties carried inline JAAS users). All three providers are
active at once, so a client that authenticates by any one of them is accepted.

---

## Common options reference

| Flag | Default | Purpose |
|---|---|---|
| `--output-dir` | `./kof-output` | Directory for all generated files |
| `--data-dir` | `/var/tmp/kof/data` | FSK data directory on pserver hosts |
| `--core-servers` | _(auto)_ | Pin pserver names and ports: `SRV1=host:5600,...` |
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
| `core` | output location, data dir, server addresses, transport |
| `tls` | server and client certificates, private keys, trust files |
| `oauth` | token/JWKS endpoints, claims, audience, server and UI client credentials |
| `auth` | users file, role map, and the FTL service credentials |
| `dr` | DR server list and DR data directory |
| `info` | property listing, colorization, automatic keystore conversion |

So the OAuth2 flags used in steps 6, 9, 11 and 12 are all under `tibftlimportconfig -h oauth`, and the
TLS/mTLS flags from steps 5, 8, 10, 11 and 12 are under `tibftlimportconfig -h tls`.

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
