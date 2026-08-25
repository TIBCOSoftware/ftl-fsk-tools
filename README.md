# ftl-fsk-tools

Tools for configuring and migrating to TIBCO FTL(R) Service for Kafka (FSK) persistence clusters.

| Tool | Language | Description |
|------|----------|-------------|
| [tibfsk/tibftlimportconfig](tibfsk/tibftlimportconfig/README.md) | Go | Translates Apache Kafka KRaft `server.properties` into FSK artifacts (`realm.json`, `tibftlserver-cluster.yaml`, etc.) |
| [tibfsk/tibftlfskimportdata](tibfsk/tibftlfskimportdata/README.md) | Java | Replicates records from a source Apache Kafka cluster to a target FSK cluster |

The migration tool also ships an optional web dashboard and a runnable demo scenario —
see [tibfsk/tibftlfskimportdata/README-UI.md](tibfsk/tibftlfskimportdata/README-UI.md).

---

## Building

### Prerequisites

| Requirement | Minimum | Notes |
|-------------|---------|-------|
| CMake | 3.15 | |
| Go | 1.25 | For `tibftlimportconfig`. Set `GO_EXECUTABLE` if the right version is not first on `PATH`. |
| Java (JDK) | 11 | For `tibftlfskimportdata`. |
| Apache Kafka client jars | — | `kafka-clients.jar` + `slf4j-api.jar`; see below. |

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

> List the jars explicitly, colon-separated. A `libs/*` wildcard does **not** work here — the shell
> expands it inside the `javac` rule and every jar after the first is passed as a flag
> (`error: invalid flag: .../activation-1.1.1.jar`). The wildcard form is only valid for the
> *runtime* `KAFKA_CLASSPATH` read by `run-apachekafka-to-fsk.sh`.

Outputs:
- `build/tibftlimportconfig` — ready-to-run binary
- `build/tibfsk/tibftlfskimportdata/classes/` — compiled Java class files (run via `run-apachekafka-to-fsk.sh`)

### Build tibftlimportconfig only (no Java required)

```sh
mkdir build && cd build
cmake ..   # omit -DKAFKA_CLASSPATH; Java tool is skipped with a warning
cmake --build . --target tibftlimportconfig_build
```

### Selecting the Go toolchain

If Go 1.25+ is not on your `PATH` but is installed elsewhere:

```sh
cmake .. -DGO_EXECUTABLE=/usr/local/go/bin/go
```

### Obtaining Apache Kafka client jars

The Apache Kafka client jars are not bundled in this repository. Common sources:

- **Apache Kafka installation**: `$KAFKA_HOME/libs/kafka-clients-*.jar` and `$KAFKA_HOME/libs/slf4j-api-*.jar`
- **Maven Central**: `org.apache.kafka:kafka-clients` and `org.slf4j:slf4j-api`

At runtime, `run-apachekafka-to-fsk.sh` also needs a logging binding such as `slf4j-simple.jar`.

### Install

```sh
cmake --install . --prefix /usr/local
```

Installs:
- `/usr/local/bin/tibftlimportconfig`
- `/usr/local/share/tibftlfskimportdata/` (classes, conf, run script)
