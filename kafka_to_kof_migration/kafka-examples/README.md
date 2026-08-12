# Kafka KRaft example clusters

Ready-to-run Kafka 4.x KRaft configurations for trying the Kafka-to-KOF migration without having to
write `server.properties` from scratch. Everything runs on localhost.

| Layout | Config files | Bootstrap servers | Controller ports |
|---|---|---|---|
| `single-node` | `single-node/server.properties` | `localhost:9092` | 9911 |
| `three-node` | `three-node/server-1.properties` … `server-3.properties` | `localhost:9092,localhost:9093,localhost:9094` | 9911, 9912, 9913 |

## Usage

```bash
export KAFKA_HOME=/usr/local/Cellar/kafka/4.2.0/libexec   # your Kafka installation

bash kafka-examples/start-kafka.sh single-node     # or: three-node
bash kafka-examples/stop-kafka.sh
```

`start-kafka.sh` formats the KRaft storage directories, starts one broker per config file, waits
until the cluster answers, and writes the PIDs to `kafka-examples.pid` in this directory. Add
`--clean` to delete the data directories and start from empty.

The cluster ID is fixed per layout, so brokers can be stopped and restarted without reformatting.

Data lives under `/tmp/kafka-examples/`, broker logs under `/tmp/kafka-examples/logs/`.

## Which one to use

`single-node` is the quickest way through the migration runbook. `three-node` exercises a real
multi-broker source and maps 1:1 onto a 3-pserver KOF cluster, so it is the better shape for
rehearsing a production migration.

Both layouts bind ports 9092 and 9911, so **only one can run at a time**. They also collide with
the insurance demo cluster in `demo/` — stop one before starting the other.

## Differences between the two

Beyond node count and ports, the replication settings differ, and they have to:

| Setting | single-node | three-node |
|---|---|---|
| `default.replication.factor` | 1 | 3 |
| `min.insync.replicas` | 1 | 2 |
| `offsets.topic.replication.factor` | 1 | 3 |
| `transaction.state.log.replication.factor` | 1 | 3 |
| `transaction.state.log.min.isr` | 1 | 2 |
| `share.coordinator.state.topic.replication.factor` | 1 | 3 |
| `share.coordinator.state.topic.min.isr` | 1 | 2 |

Kafka defaults these to 3. On a one-broker cluster that leaves internal topics such as
`__consumer_offsets` permanently under-replicated, so the single-node file overrides all of them
to 1.

## Feeding them to tibkafkatokof

```bash
tibkafkatokof --output-dir ./kof-output --realm-name my-realm --migration-config \
  kafka-examples/three-node/server-1.properties \
  kafka-examples/three-node/server-2.properties \
  kafka-examples/three-node/server-3.properties
```

Both layouts translate to `KOF-CONFIG-STATUS: ACCEPTED`. The KRaft-internal keys
(`process.roles`, `controller.quorum.voters`, `controller.listener.names`) and the JVM/OS tuning
keys land in `unsupported.properties` — expected, since KOF uses an FTL-native quorum rather than
KRaft.

See [../README.md](../README.md) for the full migration runbook.
