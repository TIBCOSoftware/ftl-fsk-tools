# Kafka to KOF Migration

This tool copies all records from a source Apache Kafka cluster to a target KOF (Kafka-on-FTL)
cluster. It discovers every topic and partition, creates them on the target, and replicates every
record from the beginning offset.

Pick one of the four paths below and paste the commands in order. Each path is self-contained —
start a Kafka cluster, load it with demo data, generate the KOF configuration, start KOF, migrate,
verify, shut down. Nothing here needs Node.js or a browser.

> **NOTE — steps 1, 2 and part of 9 are demo scaffolding.** They start a throwaway Kafka cluster
> and fill it with sample records so there is something to migrate. If you already have a Kafka
> broker or cluster running **and it already holds the data you want to move**, skip them: start
> each path at **step 3**, and in step 9 stop only KOF. Every step from 3 onward is the same
> whether the source cluster came from `kafka-examples/` or from your own deployment.

| Path | Source cluster | Kafka version | Verified |
|---|---|---|---|
| **Path 1** | Single-node, KRaft | 4.x | Yes, end to end |
| **Path 2** | Three-node, KRaft | 4.x | Yes, end to end |
| **Path 3** | Single-node, ZooKeeper | 3.9 or earlier | **No — written from Kafka docs, not executed** |
| **Path 4** | Three-node, ZooKeeper | 3.9 or earlier | **No — written from Kafka docs, not executed** |

Already have a Kafka cluster you want to migrate? Skip to
[Migrating from an existing cluster](#migrating-from-an-existing-cluster).

> There is also an optional web dashboard that runs the same migration and streams its output to a
> browser. See [README-UI.md](README-UI.md).

---

## Prerequisites

- JDK 11+
- A Kafka installation, for its client JARs and CLI scripts
- `tibkafkatokof`, `tibftlserver`, and `tibftladmin` on your `PATH`
- `curl`, used by `demo/wait-for-kof.sh` to poll the realm server

Every command below is run **from this directory** — the one holding `run-kafka-to-kof.sh`. `cd`
here first, then set:

```bash
export KAFKA_HOME=/opt/kafka   # your Kafka installation
export KAFKA_CLASSPATH="$KAFKA_HOME/libs/*"
export FTL_HOME=/opt/tibco/ftl/current-version            # directory holding bin/ and lib/
export DYLD_LIBRARY_PATH=$FTL_HOME/lib                    # Linux: LD_LIBRARY_PATH
export PATH=$FTL_HOME/bin:$PATH
```

`KAFKA_CLASSPATH` is read by `run-kafka-to-kof.sh` and by `demo/populate-kafka.sh`; both exit 1
without it. It is deliberately kept out of `CLASSPATH`, because the Kafka CLI scripts build their
own classpath starting from whatever `CLASSPATH` holds, and an older `snakeyaml` reachable that way
makes them fail with `NoSuchMethodError`. Every script here that calls the Kafka CLI clears both
variables itself, so you do not have to — only a Kafka CLI command you type by hand needs
`unset CLASSPATH KAFKA_CLASSPATH` first.

---

## Before you start

Four things that will bite you if you skip them:

1. **KOF cannot share a port with the source Kafka.** `tibkafkatokof` copies each source broker's
   `listeners` verbatim, so a source broker on `localhost:9092` produces a KOF broker on
   `localhost:9092` too. Both must run at the same time during a migration, so every path below
   moves KOF to `19092`/`19093`/`19094` with a `sed` command. This is only an issue when both run
   on one host; in production KOF has its own hosts and no edit is needed.

2. **Clear the KOF data directory when the cluster shape changes.** `initial.realm.config` seeds
   the realm only on a *first* start with an empty data directory. If `/var/tmp/kof/data` still
   holds state from an earlier run — especially one with a different number of servers — the new
   `realm.json` is ignored, and you get a realm with the old pserver count or a
   `Quorum contains an inadequate number of members` failure. Every path below runs
   `rm -rf /var/tmp/kof/data` before starting KOF.

3. **Use a fresh output directory per run.** `tibkafkatokof` writes into `--output-dir` without
   clearing it, so a 1-broker run into a directory left over from a 3-broker run leaves stale
   `kof.broker.2.properties` and `kof.broker.3.properties` sitting next to a correct 1-pserver
   `realm.json`. Every path below runs `rm -rf ./kof-output` first.

4. **Stop KOF with `tibftladmin`, never `kill`.** `tibftlserver` runs its work in child processes —
   `tibmux`, `tibpserver`, `tibrserver`. Killing the parent orphans them, and they keep holding the
   FTL server port, so the *next* server you start fails its mux with
   `exit status 99`/`fatal startup error` and every `tibftladmin` command against it dies with
   `EOF`. The symptom looks like a broken admin tool; the cause is a leftover from the previous
   run. Use `-x` (one server) or `-xc` (whole cluster), as each path below does. If you already
   killed one, clean up with:

   ```bash
   pkill -f tibmux; pkill -f tibpserver; pkill -f tibrserver
   ```

   To check a single server is up, ask it — `--status` answers only for the server you point at,
   which is why step 5 of each path uses `demo/wait-for-kof.sh` instead to wait on the whole
   persistence cluster:

   ```bash
   tibftladmin -ftls http://localhost:5600 --status
   ```

---

## Path 1 — Single-node Kafka (KRaft)

One Kafka broker on `localhost:9092`, one KOF server on `localhost:19092`. This is the quickest
path and the one to use if you are trying the tool for the first time.

### 1. Start the Kafka broker

> **NOTE — skip this step if a Kafka broker is already running.** `--clean` wipes the example
> cluster's log directories, so do not point this at anything you care about. Against your own
> broker, go straight to step 3 and pass its real `server.properties` instead of the
> `kafka-examples/` one.

```bash
bash kafka-examples/start-kafka.sh single-node --clean
```

### 2. Create topics and load demo data

> **NOTE — skip this step if your broker already has the data you want to migrate.** This only
> manufactures something to move. Running it against a real cluster would add ten unwanted
> `insurance.*` topics to it.

```bash
bash demo/create-topics.sh --bootstrap-server localhost:9092
bash demo/populate-kafka.sh --bootstrap-server localhost:9092 --messages 1000
```

Ten `insurance.*` topics with 3 partitions each, 1000 messages per topic.

### 3. Generate the KOF configuration

```bash
rm -rf ./kof-output
tibkafkatokof \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --migration-config \
  --core-servers "SRV1=localhost:5600" \
  kafka-examples/single-node/server.properties
```

### 4. Move the KOF broker off port 9092

```bash
sed -E -i.bak 's/:909([0-9])/:1909\1/' kof-output/kof.broker.*.properties
sed -E -i.bak 's|^target\.bootstrap\.servers=.*|target.bootstrap.servers=localhost:19092|' \
  kof-output/kafka-to-kof.properties
rm -f kof-output/*.bak
```

### 5. Start the KOF server

```bash
rm -rf /var/tmp/kof/data
tibftlserver -c kof-output/kof-cluster.yaml -n SRV1 > /tmp/kof-SRV1.log 2>&1 &
bash demo/wait-for-kof.sh --server localhost:5600
```

That one process hosts the realm and `pserver1`. `wait-for-kof.sh` polls the realm server's
monitoring REST API — `GET /api/v1/persistence/clusters/kof.cluster.0/quorum` — and returns as
soon as the cluster has quorum with every pserver joined, so there is nothing to guess at. It
prints `✓ kof.cluster.0 has quorum: 1/1 members` and exits 0; on failure it exits 1, and the
migration in step 6 never runs against a half-started cluster.

`--server` is the **FTL server** port from `core.servers` in `kof-cluster.yaml`, not the KOF
Kafka listener port.

No realm upload is needed. Every `- realm:` entry in the generated YAML carries
`initial.realm.config: realm.json`, so `tibftlserver` seeds the realm at startup.

### 6. Dry run

```bash
./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties --dry-run
```

Reads the source and writes nothing. Confirm the topic list and the per-topic counts.

### 7. Migrate

```bash
./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties
```

Check that `planned` equals `published` in the summary.

### 8. Verify against KOF directly

```bash
bash demo/verify-kof.sh --bootstrap-server localhost:19092
```

Lists the topics on KOF, sums each one's partition end offsets to get its record count, and prints
the first few records of `insurance.fraud.alerts`. To compare against the source, point it there
too — the counts should match what the migration reported:

```bash
bash demo/verify-kof.sh --bootstrap-server localhost:9092 --no-sample
```

### 9. Shut down

> **NOTE — run the second line only if step 1 started the broker.** If you migrated from your own
> Kafka, stop KOF and leave the source cluster alone; the migration never modified it.

```bash
tibftladmin -ftls http://localhost:5600 -x
bash kafka-examples/stop-kafka.sh
```

`-x` asks the FTL server to shut itself down, and it takes the **FTL server port** — `5600`, the
`core.servers` port pinned in step 3 — not the KOF listener port `19092`. Do not `kill` the
`tibftlserver` process instead: it leaves `tibmux`, `tibpserver`, and `tibrserver` orphaned and
still holding 5600, and the next run's mux then dies with `exit status 99`.

---

## Path 2 — Three-node Kafka (KRaft)

Three Kafka brokers on `localhost:9092`, `:9093`, `:9094`, and three KOF servers on `:19092`,
`:19093`, `:19094`. Same steps as Path 1, three of everything. Running all six processes on one
host is fine for a trial.

### 1. Start the three Kafka brokers

> **NOTE — skip this step if your Kafka cluster is already running.** `--clean` wipes the example
> cluster's log directories, so do not point this at anything you care about. Against your own
> cluster, go straight to step 3 and pass one real `server.properties` per broker instead of the
> `kafka-examples/` ones.

```bash
bash kafka-examples/start-kafka.sh three-node --clean
```

Both layouts bind the same ports, so stop a `single-node` cluster before starting this one.

### 2. Create topics and load demo data

> **NOTE — skip this step if your cluster already has the data you want to migrate.** This only
> manufactures something to move. Running it against a real cluster would add ten unwanted
> `insurance.*` topics to it.

```bash
bash demo/create-topics.sh --bootstrap-server localhost:9092
bash demo/populate-kafka.sh --bootstrap-server localhost:9092 --messages 1000
```

`create-topics.sh` creates the topics with replication factor 1. That is intentional — it keeps the
script identical across layouts. Partitions still spread across all three brokers.

### 3. Generate the KOF configuration

```bash
rm -rf ./kof-output
tibkafkatokof \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --migration-config \
  --core-servers "SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602" \
  kafka-examples/three-node/server-1.properties \
  kafka-examples/three-node/server-2.properties \
  kafka-examples/three-node/server-3.properties
```

One source broker file in, one KOF server (`SRV1`, `SRV2`, `SRV3`) and one pserver out.

### 4. Move the KOF brokers off 9092–9094

```bash
sed -E -i.bak 's/:909([0-9])/:1909\1/' kof-output/kof.broker.*.properties
sed -E -i.bak 's|^target\.bootstrap\.servers=.*|target.bootstrap.servers=localhost:19092,localhost:19093,localhost:19094|' \
  kof-output/kafka-to-kof.properties
rm -f kof-output/*.bak
```

### 5. Start the three KOF servers

```bash
rm -rf /var/tmp/kof/data
for n in 1 2 3; do
  tibftlserver -c kof-output/kof-cluster.yaml -n SRV$n > /tmp/kof-SRV$n.log 2>&1 &
done
bash demo/wait-for-kof.sh --server localhost:5600
```

`wait-for-kof.sh` polls the realm server's monitoring REST API —
`GET /api/v1/persistence/clusters/kof.cluster.0/quorum` — and returns as soon as the cluster has
quorum with all three pservers joined. It prints `✓ kof.cluster.0 has quorum: 3/3 members` and
exits 0. Note `3/3`: quorum alone is reached with two of three members, and migrating then leaves
the third to catch up, so the script waits for the full set.

`--server` is the **FTL server** port from `core.servers` in `kof-cluster.yaml`, not the KOF
Kafka listener port. Any of the three works; SRV1 is just the first.

On failure the script exits 1 rather than letting step 6 migrate into a half-started cluster. The
usual cause is a `/var/tmp/kof/data` left over from a run with a different number of servers,
which shows up in the logs as `Quorum contains an inadequate number of members` — stop the servers
(step 9), `rm -rf /var/tmp/kof/data`, and repeat this step.

In production each `tibftlserver` runs on its own host, with the same `kof-cluster.yaml` deployed
to all three.

### 6. Dry run

```bash
./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties --dry-run
```

### 7. Migrate

```bash
./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties
```

### 8. Verify against KOF directly

```bash
bash demo/verify-kof.sh --bootstrap-server localhost:19092
```

Lists the topics on KOF, sums each one's partition end offsets to get its record count, and prints
the first few records of `insurance.fraud.alerts`. To compare against the source, point it there
too — the counts should match what the migration reported:

```bash
bash demo/verify-kof.sh --bootstrap-server localhost:9092 --no-sample
```

### 9. Shut down

> **NOTE — run the second line only if step 1 started the brokers.** If you migrated from your own
> Kafka, stop KOF and leave the source cluster alone; the migration never modified it.

```bash
tibftladmin -ftls http://localhost:5600 -xc
bash kafka-examples/stop-kafka.sh
```

`-xc` stops the whole cluster, so one command against any one server brings down all three —
`5600` here is the **FTL server port** for `SRV1` from step 3, not the KOF listener port `19092`.
Do not `kill` the `tibftlserver` processes instead: that leaves `tibmux`, `tibpserver`, and
`tibrserver` orphaned and still holding 5600–5602, and the next run's mux then dies with
`exit status 99`.

---

## Path 3 — Single-node Kafka (ZooKeeper)

> **Not executed.** The configuration files and the start/stop scripts for Paths 3 and 4 were
> written from the standard Kafka 3.x configuration and have not been run against a live
> ZooKeeper — no Kafka 3.x installation was available. **Kafka 4.x removed ZooKeeper entirely**, so
> these paths need Kafka 3.9 or earlier; `start-kafka-zk.sh` detects a 4.x installation and exits
> with an explanation rather than failing obscurely. What *was* verified is the part that matters
> most here: `tibkafkatokof` translates both ZooKeeper example layouts correctly, emitting the
> right pserver count and routing `zookeeper.connect` and `zookeeper.connection.timeout.ms` to
> `unsupported.properties`. Steps 3 onward are therefore the same commands verified in Path 1.

Point `KAFKA_HOME` at a Kafka 3.9-or-earlier installation for this path:

```bash
export KAFKA_HOME=/path/to/kafka_2.13-3.9.1
export KAFKA_CLASSPATH="$KAFKA_HOME/libs/*"
```

### 1. Start ZooKeeper and the broker

> **NOTE — skip this step if your ZooKeeper-backed broker is already running.** `--clean` wipes
> the example cluster's log directories, so do not point this at anything you care about. Against
> your own broker, go straight to step 3 and pass its real `server.properties` instead of the
> `kafka-examples/` one.

```bash
bash kafka-examples/start-kafka-zk.sh single-node --clean
```

ZooKeeper on `localhost:2181`, one broker on `localhost:9092`. The script starts ZooKeeper first,
waits for it to accept connections, then starts the broker.

### 2. Create topics and load demo data

> **NOTE — skip this step if your broker already has the data you want to migrate.** This only
> manufactures something to move. Running it against a real cluster would add ten unwanted
> `insurance.*` topics to it.

```bash
bash demo/create-topics.sh --bootstrap-server localhost:9092
bash demo/populate-kafka.sh --bootstrap-server localhost:9092 --messages 1000
```

### 3. Generate the KOF configuration

```bash
rm -rf ./kof-output
tibkafkatokof \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --migration-config \
  --core-servers "SRV1=localhost:5600" \
  kafka-examples/zk-single-node/server.properties
```

The `zookeeper.*` keys have no KOF equivalent — KOF has no ZooKeeper. They are written to
`kof-output/unsupported.properties` and dropped from the generated broker config. That is expected
and does not affect the migration: the replicator talks to the source brokers, not to ZooKeeper.

### 4. Move the KOF broker off port 9092

```bash
sed -E -i.bak 's/:909([0-9])/:1909\1/' kof-output/kof.broker.*.properties
sed -E -i.bak 's|^target\.bootstrap\.servers=.*|target.bootstrap.servers=localhost:19092|' \
  kof-output/kafka-to-kof.properties
rm -f kof-output/*.bak
```

### 5. Start the KOF server

```bash
rm -rf /var/tmp/kof/data
tibftlserver -c kof-output/kof-cluster.yaml -n SRV1 > /tmp/kof-SRV1.log 2>&1 &
bash demo/wait-for-kof.sh --server localhost:5600
```

Waits for `✓ kof.cluster.0 has quorum: 1/1 members`, then exits 0. See
[Path 1 step 5](#path-1--single-node-kafka-kraft) for what it is polling.

### 6. Dry run

```bash
./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties --dry-run
```

### 7. Migrate

```bash
./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties
```

### 8. Verify against KOF directly

```bash
bash demo/verify-kof.sh --bootstrap-server localhost:19092
```

Lists the topics on KOF, sums each one's partition end offsets to get its record count, and prints
the first few records of `insurance.fraud.alerts`. To compare against the source, point it there
too — the counts should match what the migration reported:

```bash
bash demo/verify-kof.sh --bootstrap-server localhost:9092 --no-sample
```

### 9. Shut down

> **NOTE — run the second line only if step 1 started ZooKeeper and the broker.** If you migrated
> from your own Kafka, stop KOF and leave the source cluster alone; the migration never modified
> it.

```bash
tibftladmin -ftls http://localhost:5600 -x
bash kafka-examples/stop-kafka-zk.sh
```

`-x` takes the **FTL server port** — `5600`, the `core.servers` port pinned in step 3 — not the KOF
listener port `19092`. Do not `kill` the `tibftlserver` process instead: it leaves `tibmux`,
`tibpserver`, and `tibrserver` orphaned and still holding 5600. `stop-kafka-zk.sh` stops the
brokers before ZooKeeper.

---

## Path 4 — Three-node Kafka (ZooKeeper)

> **Not executed** — same caveat as Path 3. Requires Kafka 3.9 or earlier.

```bash
export KAFKA_HOME=/path/to/kafka_2.13-3.9.1
export KAFKA_CLASSPATH="$KAFKA_HOME/libs/*"
```

### 1. Start ZooKeeper and the three brokers

> **NOTE — skip this step if your ZooKeeper-backed cluster is already running.** `--clean` wipes
> the example cluster's log directories, so do not point this at anything you care about. Against
> your own cluster, go straight to step 3 and pass one real `server.properties` per broker
> instead of the `kafka-examples/` ones.

```bash
bash kafka-examples/start-kafka-zk.sh three-node --clean
```

One ZooKeeper on `localhost:2181` serving all three brokers (`:9092`, `:9093`, `:9094`). A single
ZooKeeper is a deliberate simplification for a local example — production uses an ensemble of
three or five.

### 2. Create topics and load demo data

> **NOTE — skip this step if your cluster already has the data you want to migrate.** This only
> manufactures something to move. Running it against a real cluster would add ten unwanted
> `insurance.*` topics to it.

```bash
bash demo/create-topics.sh --bootstrap-server localhost:9092
bash demo/populate-kafka.sh --bootstrap-server localhost:9092 --messages 1000
```

### 3. Generate the KOF configuration

```bash
rm -rf ./kof-output
tibkafkatokof \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --migration-config \
  --core-servers "SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602" \
  kafka-examples/zk-three-node/server-1.properties \
  kafka-examples/zk-three-node/server-2.properties \
  kafka-examples/zk-three-node/server-3.properties
```

### 4. Move the KOF brokers off 9092–9094

```bash
sed -E -i.bak 's/:909([0-9])/:1909\1/' kof-output/kof.broker.*.properties
sed -E -i.bak 's|^target\.bootstrap\.servers=.*|target.bootstrap.servers=localhost:19092,localhost:19093,localhost:19094|' \
  kof-output/kafka-to-kof.properties
rm -f kof-output/*.bak
```

### 5. Start the three KOF servers

```bash
rm -rf /var/tmp/kof/data
for n in 1 2 3; do
  tibftlserver -c kof-output/kof-cluster.yaml -n SRV$n > /tmp/kof-SRV$n.log 2>&1 &
done
bash demo/wait-for-kof.sh --server localhost:5600
```

Waits for `✓ kof.cluster.0 has quorum: 3/3 members`, then exits 0. See
[Path 2 step 5](#path-2--three-node-kafka-kraft) for what it is polling.

### 6. Dry run

```bash
./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties --dry-run
```

### 7. Migrate

```bash
./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties
```

### 8. Verify against KOF directly

```bash
bash demo/verify-kof.sh --bootstrap-server localhost:19092
```

Lists the topics on KOF, sums each one's partition end offsets to get its record count, and prints
the first few records of `insurance.fraud.alerts`. To compare against the source, point it there
too — the counts should match what the migration reported:

```bash
bash demo/verify-kof.sh --bootstrap-server localhost:9092 --no-sample
```

### 9. Shut down

> **NOTE — run the second line only if step 1 started ZooKeeper and the brokers.** If you migrated
> from your own Kafka, stop KOF and leave the source cluster alone; the migration never modified
> it.

```bash
tibftladmin -ftls http://localhost:5600 -xc
bash kafka-examples/stop-kafka-zk.sh
```

`-xc` stops the whole cluster, so one command against any one server brings down all three —
`5600` here is the **FTL server port** for `SRV1` from step 3, not the KOF listener port `19092`.
Do not `kill` the `tibftlserver` processes instead: that leaves `tibmux`, `tibpserver`, and
`tibrserver` orphaned and still holding 5600–5602. `stop-kafka-zk.sh` stops the brokers before
ZooKeeper.

---

## Migrating from an existing cluster

The paths above start a Kafka cluster only so there is something to migrate. Against a cluster you
already run, the procedure is Path 1 or Path 2 with steps 1 and 2 dropped and step 9 reduced to
stopping KOF — nothing about the source cluster needs to change, and it keeps serving traffic
throughout.

```bash
# 1. Generate, from the real server.properties — one file per broker
rm -rf ./kof-output
tibkafkatokof \
  --output-dir ./kof-output \
  --realm-name my-realm \
  --migration-config \
  /path/to/broker-1/server.properties \
  /path/to/broker-2/server.properties \
  /path/to/broker-3/server.properties
```

`tibkafkatokof` emits one KOF server and one pserver per file you pass. Three files in, `SRV1`
through `SRV3` out; one file in, `SRV1` alone.

This writes into `./kof-output/`:

| File | Purpose |
|---|---|
| `kof-cluster.yaml` | FTL server cluster config; also seeds the realm via `initial.realm.config` |
| `kof-cluster-aux1.yaml` | Additional pserver groups, one file per extra three pservers |
| `realm.json` | FTL realm with the `kof.cluster` definitions |
| `kof.broker.N.properties` | Per-pserver Kafka broker properties |
| `unsupported.properties` | Source keys with no KOF equivalent, for review |
| **`kafka-to-kof.properties`** | **Migration config, pre-filled with the source broker addresses** |

Then:

2. Deploy `kof-cluster.yaml` (plus any `kof-cluster-auxN.yaml`), `realm.json`, and the
   `kof.broker.N.properties` files to your KOF hosts, and start one `tibftlserver -c
   kof-cluster.yaml -n SRVn` per server entry. On separate hosts there is no port collision, so no
   `sed` step. Wait for the cluster to form with
   `bash demo/wait-for-kof.sh --server <kof-host-1>:5600`, pointing at the `core.servers` port of
   any one of them.

3. Edit `kof-output/kafka-to-kof.properties` and replace each `<KOF-HOST-N>` placeholder in
   `target.bootstrap.servers` with the real hostname. The ports there must match
   `advertised.listeners` in the corresponding `kof.broker.N.properties`.

   ```properties
   source.bootstrap.servers=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
   target.bootstrap.servers=kof-host-1:9092,kof-host-2:9092,kof-host-3:9092
   ```

   If the source cluster uses SASL or TLS, add the pass-through keys — see
   [Security](#security-sasltls). Everything else tunable is in the
   [Config reference](#config-reference).

4. Dry run, then migrate:

   ```bash
   ./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties --dry-run
   ./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties
   ```

To narrow the scope, add `--topic-pattern` — useful for migrating one topic family at a time:

```bash
./run-kafka-to-kof.sh --config ./kof-output/kafka-to-kof.properties \
  --topic-pattern 'insurance\.auto\..*'
```

The tool always reads from offset 0, so a re-run is safe in the sense that it never loses data, but
it is **not** deduplicating — records already on KOF are published again. Use `--dry-run` to check
before re-running a topic that partially completed.

### Realm ports and `--core-servers`

Unless you pass `--core-servers`, `tibkafkatokof` picks each server's realm port randomly from
5600–5699, and a regenerated configuration gets different ports. Pin them when the ports appear in
firewall rules, scripts, or the UI dashboard:

```bash
--core-servers "SRV1=localhost:5600"                                          # one server
--core-servers "SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602"  # three servers
```

To push a *hand-edited* `realm.json` to an already-running realm — the only case that needs a
manual upload — use that port:

```bash
tibrealmadmin --server <KOF-HOST-1>:5600 --realm my-realm upload-realm ./kof-output/realm.json
```

---

## Command line reference

`run-kafka-to-kof.sh` is the only entry point the migration needs. It compiles the Java sources into
`build/` and then runs the replicator, forwarding every argument through unchanged:

```bash
./run-kafka-to-kof.sh [options]
```

With no arguments it falls back to `conf/kafka-to-kof.properties` next to the script, so pass
`--config` whenever your properties file lives elsewhere — as it does after generation.

`KAFKA_CLASSPATH` must be set or the script exits 1 before compiling. On this path it is **not**
derived from `KAFKA_HOME`; that convenience exists only in the UI server.

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

### Without a properties file

Both endpoints can be passed inline:

```bash
./run-kafka-to-kof.sh \
  --source-bootstrap kafka-broker-1:9092,kafka-broker-2:9092 \
  --target-bootstrap kof-host-1:9092,kof-host-2:9092 \
  --topic-pattern 'insurance\..*' \
  --dry-run
```

SASL/TLS settings have no flag equivalents — they are prefixed properties and must come from a
`--config` file. See [Security](#security-sasltls).

### Without the wrapper script

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

Pass Kafka security settings with prefixed keys in the config file. The prefix routes the property
to the right Kafka client (`source.admin`, `source.consumer`, `target.admin`, `target.producer`).

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
