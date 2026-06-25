# kof-tools

Tools for configuring and migrating to KOF (Kafka-on-FTL) persistence clusters.

| Tool | Language | Description |
|------|----------|-------------|
| [kafka_to_kof_config/tibkafkatokof](kafka_to_kof_config/tibkafkatokof/README.md) | Go | Translates Kafka KRaft `server.properties` into FTL KOF artifacts (`realm.json`, `kof-cluster.yaml`, etc.) |
| [kafka_to_kof_migration](kafka_to_kof_migration/README.md) | Java | Replicates records from a source Kafka cluster to a target KOF cluster |

---

## Building

### Prerequisites

| Requirement | Minimum | Notes |
|-------------|---------|-------|
| CMake | 3.15 | |
| Go | 1.24 | For `tibkafkatokof`. Set `GO_EXECUTABLE` if the right version is not first on `PATH`. |
| Java (JDK) | 11 | For `kafka_to_kof_migration`. |
| Kafka client jars | — | `kafka-clients.jar` + `slf4j-api.jar`; see below. |

### Build (both tools)

```sh
# Create a build directory outside the source tree.
mkdir build && cd build

# Configure — provide KAFKA_CLASSPATH for the Java tool.
cmake .. \
  -DKAFKA_CLASSPATH="/path/kafka-clients.jar:/path/slf4j-api.jar"

# Build.
cmake --build .
```

Outputs:
- `build/tibkafkatokof` — ready-to-run binary
- `build/kafka_to_kof_migration/classes/` — compiled Java class files (run via `run-kafka-to-kof.sh`)

### Build tibkafkatokof only (no Java required)

```sh
mkdir build && cd build
cmake ..   # omit -DKAFKA_CLASSPATH; Java tool is skipped with a warning
cmake --build . --target tibkafkatokof_build
```

### Selecting the Go toolchain

If Go 1.24+ is not on your `PATH` but is installed elsewhere:

```sh
cmake .. -DGO_EXECUTABLE=/usr/local/go/bin/go
```

### Obtaining Kafka client jars

The Kafka client jars are not bundled in this repository. Common sources:

- **Kafka installation**: `$KAFKA_HOME/libs/kafka-clients-*.jar` and `$KAFKA_HOME/libs/slf4j-api-*.jar`
- **Maven Central**: `org.apache.kafka:kafka-clients` and `org.slf4j:slf4j-api`

At runtime, `run-kafka-to-kof.sh` also needs a logging binding such as `slf4j-simple.jar`.

### Install

```sh
cmake --install . --prefix /usr/local
```

Installs:
- `/usr/local/bin/tibkafkatokof`
- `/usr/local/share/kafka_to_kof_migration/` (classes, conf, run script)
