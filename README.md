# ftl-fsk-tools

Tools for configuring and migrating to TIBCO FTL(R) Service for Kafka (FSK) persistence clusters.

| Tool | Language | Description |
|------|----------|-------------|
| [tibfsk/importconfig](tibfsk/importconfig/README.md) | Go | Translates Apache Kafka KRaft `server.properties` into FSK artifacts (`realm.json`, `tibftlserver-cluster.yaml`, etc.) |
| [tibfsk/importdata](tibfsk/importdata/README.md) | Java | Replicates records from a source Apache Kafka cluster to a target FSK cluster |

The migration tool also ships a runnable demo scenario — see
[tibfsk/importdata/README.md](tibfsk/importdata/README.md) for the four
end-to-end paths.

---

## Prebuilt artifacts

The repository checks in a built copy of each tool, so a clone runs with no Go toolchain and no
`javac`:

| Artifact | Built from | Used by |
|----------|------------|---------|
| `tibfsk/importconfig/bin/tibftlimportconfig` | the Go sources | run it directly |
| `tibfsk/importdata/classes/` | `tibfsk/importdata/src/main/java` | `run-apachekafka-to-fsk.sh` |
| `tibfsk/importdata/demo/classes/` | `tibfsk/importdata/demo/InsuranceDataProducer.java` | `demo/populate-kafka.sh` |

Both scripts prefer `classes/` and fall back to compiling into `build/` only when it is missing.
Set `FSK_FORCE_REBUILD=1` to compile from source anyway after editing the Java.

The binary is built for one platform — the checked-in one is **macOS x86_64**. Build from source
(below) for any other target.

To refresh the artifacts after changing a source file:

```sh
export KAFKA_HOME=/path/to/kafka      # or set KAFKA_CLASSPATH directly
./build-artifacts.sh
```

The Java is compiled with `--release 11` whatever JDK does the compiling, so the class files load
on the Java 11 listed below. `GO`, `JAVAC`, `JAVA_RELEASE` and `KAFKA_CLASSPATH` override the
defaults. Nothing refreshes these automatically — `sync-to-fsk-tools.sh` copies sources only, so
re-run the script and commit the result whenever the sources move.

CMake ignores the checked-in artifacts and builds from source into its own binary directory.

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
- `build/tibfsk/importdata/classes/` — compiled Java class files (run via `run-apachekafka-to-fsk.sh`)

`javac` runs only when the classes are missing or a `.java` source has changed, so repeat builds
are no-ops. Because the compiled classes are installed next to `run-apachekafka-to-fsk.sh`, the
script finds them and skips compiling entirely at run time — set `FSK_FORCE_REBUILD=1` if you edit
the sources and want the script to recompile.

### Build tibftlimportconfig only (no Java required)

```sh
mkdir build && cd build
cmake ..   # omit -DKAFKA_CLASSPATH; Java tool is skipped with a warning
cmake --build . --target tibftlimportconfig_build
```

Omitting `-DKAFKA_CLASSPATH` is not an error: the Java tool's compile step is skipped and the
sources ship as-is, so `run-apachekafka-to-fsk.sh` compiles them into `build/` on first use.

### Build the Java tool only

```sh
cmake --build . --target tibftlfskimportdata
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
