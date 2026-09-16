# AGENTS.md — ftl-fsk-tools

Guidance for AI coding agents working in this repository. Humans: `README.md` is the place to
start; this file only covers the things that are easy to get wrong.

## What this repository is

Two independent tools for TIBCO FTL(R) Service for Kafka (FSK):

| Directory | Language | Job |
|---|---|---|
| `tibfsk/importconfig` | Go | Translate an Apache Kafka `server.properties` into FSK artifacts |
| `tibfsk/importdata` | Java | Replicate records from a source Apache Kafka cluster into FSK |

This is a **published** repository, which is why every `.go`, `.java` and `.sh` file opens with a
Cloud Software Group copyright header. Keep it on new files.

Branch `trunk`, remote `origin`. There is no CI, no Makefile and no pre-commit hook: nothing
checks your work automatically, so run the commands below yourself.

## Generated files that are checked in

This is the single most important thing to know about the repo. Each of these is build output
that is also committed, so it looks hand-written and edits to it appear to work — until the next
regeneration silently discards them.

| Path | Generated from | Regenerate with |
|---|---|---|
| `tibfsk/importconfig/examples/*/output*/` | the `.properties` files beside them | `tibfsk/importconfig/examples/regen-examples.sh` |
| `tibfsk/importconfig/bin/tibftlimportconfig` | the Go sources, for linux/amd64 | `./build-artifacts.sh` |
| `tibfsk/importdata/classes/` | `tibfsk/importdata/src/main/java` | `./build-artifacts.sh` |
| `tibfsk/importdata/demo/classes/` | `tibfsk/importdata/demo/InsuranceDataProducer.java` | `./build-artifacts.sh` |

**Never hand-edit any of them.** Edit the source, rerun the command, and commit source and
regenerated output together in one commit. Nothing regenerates automatically.

## Never commit

- `tibfsk/importconfig/bin/macosx_x86_64/` — a local macOS build of the tool. The shipped binary
  is linux/amd64 only. This is excluded via `.git/info/exclude`, which git does **not** push, so a
  fresh clone has no record of the rule. Check `git status` before committing if you built
  natively.
- Anything under `kof-output/`, `build/`, `*.pid`, `*.log` in `tibfsk/importdata` — already
  covered by `tibfsk/importdata/.gitignore`.

## Build and test

`README.md` covers building properly (CMake, prerequisites, Kafka client jars). The two commands
that come up most:

```sh
# Refresh the checked-in binary and class files after changing a source file.
export KAFKA_HOME=/path/to/kafka        # or KAFKA_CLASSPATH directly
./build-artifacts.sh

# Go tests.
cd tibfsk/importconfig && go test ./...
```

Building is optional for documentation work. It is required whenever a `.go` or `.java` source
changes, because the artifacts are checked in.

## Commit conventions

From `git log`: a lowercase scope prefix (`doc:`, `importconfig:`) or a plain imperative sentence,
then a body explaining *why* in full sentences. Source change and regenerated output belong in the
same commit.

## Where the documentation lives

`tibfsk/importconfig/getting-started.md` is the one substantive user document for the Go tool —
scenarios, flag reference, output formats and worked examples are all sections of it. The
`README.md` files are pointers to it. `tibfsk/importdata/README.md` is the user doc for the Java
tool. See `tibfsk/importconfig/AGENTS.md` before editing any of it.
