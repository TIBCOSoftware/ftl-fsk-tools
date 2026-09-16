# AGENTS.md — tibftlimportconfig

Scoped guidance for this directory. Read `../../AGENTS.md` first for the repo-wide rules,
especially the list of generated-but-checked-in files.

## Layout

| Path | What is in it |
|---|---|
| `main.go` | flag definitions, validation, orchestration |
| `help.go` | the `-h <group>` topics — `flagGroups` at `help.go:29-112` |
| `translator/` | the translation itself: parsing, status, YAML/JSON emission |
| `getting-started.md` | the user documentation, ~3100 lines, single source (see below) |
| `examples/` | 22 worked examples, inputs hand-written, outputs generated |
| `bin/` | the checked-in linux/amd64 binary |

Module is `tibco.com/ftl-support/tibftlimportconfig`, Go 1.25.

## Tests

```sh
go test ./...
```

Seven `_test.go` files sit beside the code in `translator/`. `translator/test/translator_test.go`
is the end-to-end one: it runs every `testdata/*.properties` through the translator and diffs the
result against `testdata/golden/`. A golden file changes only when the output is *meant* to
change — if a test fails, work out which it is before touching the golden.

## The documentation

`getting-started.md` is the single user document for this tool: the scenarios, the flag reference,
the output formats and the worked examples are all sections of it. The `README.md` files are
pointers to it. Edit it directly — nothing generates it and nothing is generated from it.

## Examples

`examples/regen-examples.sh` regenerates the output of all 22 examples. It builds the tool into a
temporary directory itself unless you point it at one:

```sh
TOOL=/path/to/tibftlimportconfig examples/regen-examples.sh
```

The `--core-servers` values in that script are **hand-written pins**, not values the tool derived.
They do not have to match what the tool would generate on its own, and several no longer do.

Example inputs use `/etc/kafka/certs/...` paths; the scenarios in `getting-started.md` use
`/var/tmp/kafka/scenarioN/certs/...`. The two sets are independent — do not "fix" one to match the
other.

## Prose conventions in getting-started.md

- **"Apache Kafka"**, not bare "Kafka", on a standalone reference. ("Apache Kafka listener",
  but `server.properties` and `kafka-users.txt` stay as they are.)
- **En-dash ranges**: `Steps 1–3`, `scenarios 5–12`.
- **Scenario cross-references** as `[Scenario N](#scenario-n--<slug>)` — same-file anchors,
  not file paths.
- Wrap prose at roughly 100 columns. Some long single-line paragraphs exist; match the
  neighbourhood you are editing rather than reflowing the file.
- The flag tables under "Common options reference" mirror the `-h <group>` topics in `help.go`.
  Change one and check the other.
