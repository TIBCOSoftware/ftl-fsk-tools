# Kafka example clusters

Ready-to-run Kafka configurations for trying the Kafka-to-FSK migration without having to write
`server.properties` from scratch. Everything runs on localhost.

| Layout | Mode | Config files | Bootstrap servers | Other ports |
|---|---|---|---|---|
| `single-node` | KRaft | `single-node/server.properties` | `localhost:9092` | controller 9911 |
| `three-node` | KRaft | `three-node/server-1.properties` … `server-3.properties` | `localhost:9092,localhost:9093,localhost:9094` | controllers 9911–9913 |
| `zk-single-node` | ZooKeeper | `zk-single-node/server.properties` | `localhost:9092` | ZooKeeper 2181 |
| `zk-three-node` | ZooKeeper | `zk-three-node/server-1.properties` … `server-3.properties` | `localhost:9092,localhost:9093,localhost:9094` | ZooKeeper 2181 |

The KRaft layouts need Kafka 4.x. The ZooKeeper layouts need **Kafka 3.9 or earlier** — 4.x removed
ZooKeeper support entirely.

## Usage

KRaft:

```bash
export KAFKA_HOME=/opt/kafka   # a Kafka 4.x installation

bash kafka-examples/start-kafka.sh single-node     # or: three-node
bash kafka-examples/stop-kafka.sh
```

ZooKeeper:

```bash
export KAFKA_HOME=/path/to/kafka_2.13-3.9.1                # must be 3.9 or earlier

bash kafka-examples/start-kafka-zk.sh single-node  # or: three-node
bash kafka-examples/stop-kafka-zk.sh
```

Both start scripts start one broker per config file, wait until the cluster answers, and write the
PIDs to a file in this directory (`kafka-examples.pid` / `kafka-examples-zk.pid`). Add `--clean` to
delete the data directories and start from empty. `start-kafka.sh` also formats the KRaft storage
directories; the cluster ID is fixed per layout, so brokers can be stopped and restarted without
reformatting. `start-kafka-zk.sh` starts ZooKeeper first and waits for it before starting brokers;
`stop-kafka-zk.sh` stops the brokers before ZooKeeper.

If `KAFKA_HOME` points at a 4.x installation, `start-kafka-zk.sh` refuses to run and says so —
`zookeeper-server-start.sh` does not exist there.

Data lives under `/tmp/kafka-examples/` (KRaft) and `/tmp/kafka-examples-zk/` (ZooKeeper), broker
logs under the `logs/` subdirectory of each.

## Which one to use

`single-node` is the quickest way through the migration runbook. `three-node` exercises a real
multi-broker source and maps 1:1 onto a 3-pserver FSK cluster, so it is the better shape for
rehearsing a production migration. Use the `zk-*` layouts only to rehearse migrating off a
ZooKeeper-mode cluster; nothing about the migration itself differs.

All four layouts bind port 9092, so **only one can run at a time**. They also collide with the
insurance demo cluster in `demo/` — stop one before starting the other.

The `zk-three-node` layout runs a single ZooKeeper for all three brokers. That is a deliberate
simplification for a local example; production uses an ensemble of three or five.

## Replication settings

Beyond node count and ports, the replication settings differ between the one- and three-broker
layouts, and they have to:

| Setting | 1 broker | 3 brokers |
|---|---|---|
| `default.replication.factor` | 1 | 3 |
| `min.insync.replicas` | 1 | 2 |
| `offsets.topic.replication.factor` | 1 | 3 |
| `transaction.state.log.replication.factor` | 1 | 3 |
| `transaction.state.log.min.isr` | 1 | 2 |
| `share.coordinator.state.topic.replication.factor` | 1 | 3 |
| `share.coordinator.state.topic.min.isr` | 1 | 2 |

Kafka defaults these to 3. On a one-broker cluster that leaves internal topics such as
`__consumer_offsets` permanently under-replicated, so the single-broker files override all of them
to 1.

The `share.coordinator.*` keys are Kafka 4.x only and appear in the KRaft files alone — setting
them on 3.9 would make the broker reject the config.

## Feeding them to tibftlimportconfig

```bash
tibftlimportconfig --output-dir ./kof-output --migration-config \
  kafka-examples/three-node/server-1.properties \
  kafka-examples/three-node/server-2.properties \
  kafka-examples/three-node/server-3.properties
```

All four layouts translate to `KOF-CONFIG-STATUS: ACCEPTED`, emitting one pserver per input file.
Keys with no FSK equivalent land in `unsupported.properties` — the KRaft-internal ones
(`process.roles`, `controller.quorum.voters`, `controller.listener.names`), the ZooKeeper ones
(`zookeeper.connect`, `zookeeper.connection.timeout.ms`), and the JVM/OS tuning keys. That is
expected: FSK uses an FTL-native quorum rather than KRaft or ZooKeeper.

## Verified status

The KRaft layouts have been started, populated, and migrated end to end. The ZooKeeper layouts and
their scripts were written from the standard Kafka 3.x configuration and **have not been run
against a live ZooKeeper** — no Kafka 3.x installation was available. Their translation through
`tibftlimportconfig` was verified.

See [../README.md](../README.md) for the full migration runbook.
