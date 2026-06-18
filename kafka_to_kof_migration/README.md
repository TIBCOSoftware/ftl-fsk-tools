# Kafka to KOF Replicator

This utility reads all records from source Kafka topics/partitions and republishes
those records to target KOF brokers.

## Location

- Root: `hydra/servers/store/kafka_to_kof`
- Main class: `com.tibco.ftl.kof.KafkaToKofReplicatorApp`
- Source file: `src/main/java/com/tibco/ftl/kof/KafkaToKofReplicatorApp.java`
- Config: `conf/kafka-to-kof.properties`
- Runner: `run-kafka-to-kof.sh`

## Features

1. Discovers source topics and partitions.
2. Computes and prints pre-copy message counts per topic and partition.
3. Creates missing topics on target KOF with matching partition count.
4. Copies records from source snapshot offsets to target KOF.
5. Prints post-copy planned vs published counts.

## Usage

Set classpath with Kafka and logging client jars:

```bash
export KAFKA_CLASSPATH="/path/kafka-clients.jar:/path/slf4j-api.jar:/path/slf4j-simple.jar"
```

Run with default config:

```bash
./run-kafka-to-kof.sh
```

Run with custom config:

```bash
./run-kafka-to-kof.sh --config conf/kafka-to-kof.properties
```

Override brokers from CLI:

```bash
./run-kafka-to-kof.sh \
  --config conf/kafka-to-kof.properties \
  --source-bootstrap localhost:9092 \
  --target-bootstrap localhost:9093
```

Dry-run (stats only, no create/publish):

```bash
./run-kafka-to-kof.sh --config conf/kafka-to-kof.properties --dry-run
```

Show help:

```bash
./run-kafka-to-kof.sh --help
```

## Config keys

- `source.bootstrap.servers`
- `target.bootstrap.servers`
- `topic.pattern`
- `include.internal`
- `target.replication.factor`
- `client.id`
- `request.timeout.ms`
- `max.empty.polls`
- `dry.run`

Pass-through Kafka client keys are supported via prefixes:

- `source.admin.<kafka.property>`
- `source.consumer.<kafka.property>`
- `target.admin.<kafka.property>`
- `target.producer.<kafka.property>`
