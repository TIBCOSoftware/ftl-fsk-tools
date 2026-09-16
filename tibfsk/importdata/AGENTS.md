# AGENTS.md — tibftlfskimportdata

Scoped guidance for this directory. Read `../../AGENTS.md` first for the repo-wide rules,
especially the list of generated-but-checked-in files.

## Layout

| Path | What is in it |
|---|---|
| `src/main/java/com/tibco/ftl/fsk/ApacheKafkaToFskReplicatorApp.java` | the whole tool, one file |
| `classes/` | **checked-in build output** of the above |
| `demo/` | a runnable end-to-end demo: KRaft setup, a producer, FSK brokers, verification |
| `demo/classes/` | **checked-in build output** of `demo/InsuranceDataProducer.java` |
| `kafka-examples/` | ready-made Apache Kafka configurations (single-node, three-node, ZooKeeper variants) |
| `conf/kafka-to-kof.properties` | the tool's configuration template |
| `run-apachekafka-to-fsk.sh` | the launcher |
| `README.md` | the user-facing documentation for this tool |

## Rebuilding after a Java change

Use the repo-root script, not `javac` by hand — it compiles both the tool and the demo producer
and prints what to commit:

```sh
export KAFKA_HOME=/path/to/kafka      # or KAFKA_CLASSPATH="/path/kafka-clients.jar:/path/slf4j-api.jar"
cd ../.. && ./build-artifacts.sh
```

The Apache Kafka client jars are not bundled in this repository, so one of those variables is
required. Compilation targets `--release 17` even on a newer JDK, which is what keeps the
checked-in classes loadable on the Java 17 the README promises.

`run-apachekafka-to-fsk.sh` prefers `classes/` and only compiles into `build/` when it is missing,
so after editing the Java either rerun `build-artifacts.sh` or set `FSK_FORCE_REBUILD=1` — the
script will otherwise keep running the stale checked-in classes.

## Not committed

`.gitignore` here already covers `build/`, `demo/build/`, `demo/*.pid`, `demo/*.log`,
`kafka-examples/*.pid`, `kof-output/` and `*.swp`. Demo runs drop PID and log files in place; they
are ignored, but check `git status` before committing after a demo run.

## Notes

- `demo/` and `kafka-examples/` are runnable operator scenarios, not a test suite. There are no
  unit tests in this directory and no test runner.
- The shell scripts carry the Cloud Software Group copyright header, same as the Java.
