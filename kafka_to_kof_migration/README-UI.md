# Kafka to KOF Migration — Web UI

An optional dashboard in the `ui/` directory for running the Kafka-to-KOF migration from a browser,
with live topology and streaming output. It is a front end that shells out to the same
`run-kafka-to-kof.sh` the command line uses — the migration itself is identical either way.

This page also documents the ready-to-run **insurance provider demo scenario**, which is the
quickest way to see the dashboard doing real work.

For the production runbook, every config key, and the SASL/TLS reference, see
**[README.md](README.md)**. Nothing here duplicates it.

---

## Prerequisites

Everything in [README.md § Prerequisites](README.md#prerequisites) — JDK 11+, a Kafka installation,
and `KAFKA_HOME` / `KAFKA_CLASSPATH` exported — **plus**:

- **Node.js** (for the dashboard only; the command line path does not need it)
- `tibftlserver` on `PATH`, or `TIBFTLSERVER` pointing at the binary

---

## What the dashboard replaces

The dashboard does **not** replace the whole runbook. Steps 1 through 4 of
[README.md](README.md) — start Kafka, generate the KOF config, start the KOF servers, and fill in
the `<KOF-HOST-N>` placeholders — still happen on the command line.

What the dashboard replaces is **Steps 5 and 6** (dry run and live migration): it runs
`run-kafka-to-kof.sh` for you and streams the output back to the browser.

---

## Demo scenario — Insurance Provider

The `demo/` directory contains a ready-to-run insurance provider scenario with 10 topics
(3 partitions each, 1 000 messages each — 10 000 messages total):

| Topic | Description |
|---|---|
| `insurance.auto.claims` | Auto accident reports, damage assessments |
| `insurance.home.claims` | Homeowner property damage / theft claims |
| `insurance.life.events` | Policy enrollment, beneficiary changes, death claims |
| `insurance.health.claims` | Medical procedures and prescription claims |
| `insurance.commercial.claims` | Business / commercial insurance claims |
| `insurance.policy.updates` | Renewals, amendments, cancellations |
| `insurance.customer.profiles` | New customer records and profile updates |
| `insurance.premium.payments` | Premium payment transactions |
| `insurance.fraud.alerts` | Fraud detection events and risk scores |
| `insurance.audit.log` | System-wide audit trail |

### Quick start

> **NOTE — skip block 1 if you already have a Kafka cluster running with data.** Those three
> commands only stand up a throwaway cluster and fill it with sample records. Against your own
> cluster, start at block 2 and pass one real broker `server.properties` per node in place of the
> `demo/kraft/` files, then skip `demo/stop-kafka.sh` at the end. `setup-kafka-kraft.sh` and
> `create-topics.sh` write to the cluster you point them at, so do not run them against anything
> you care about. The dashboard's topology draws three Kafka nodes, so a source cluster with a
> different node count will not line up with the diagram — the migration itself is unaffected.

```bash
export KAFKA_HOME=/opt/kafka
export KAFKA_CLASSPATH="$KAFKA_HOME/libs/*"
export TIBFTLSERVER=/opt/tibco/ftl/current-version/bin/tibftlserver   # path to your tibftlserver binary

# 1. Start source Kafka (3-broker KRaft cluster) — skip if yours is already running
bash demo/setup-kafka-kraft.sh
bash demo/create-topics.sh
bash demo/populate-kafka.sh             # sends 10 000 sample JSON messages

# 2. Generate KOF config from the demo broker properties.
#    The --core-servers ports match what the dashboard probes — see
#    "Health and metadata reporting" below.
tibkafkatokof \
  --output-dir ./kof-output \
  --realm-name insurance-demo \
  --migration-config \
  --core-servers "SRV1=localhost:5635,SRV2=localhost:5620,SRV3=localhost:5623" \
  demo/kraft/server-1.properties \
  demo/kraft/server-2.properties \
  demo/kraft/server-3.properties

# 3. Start KOF brokers (the realm is seeded from the generated YAML — no upload needed)
bash demo/start-kof-brokers.sh --output-dir ./kof-output

# 4. Replace <KOF-HOST-N> placeholders with localhost (demo runs everything locally)
sed -i '' 's/<KOF-HOST-[0-9]*>/localhost/g' kof-output/kafka-to-kof.properties
```

Then start the dashboard (below) and run the migration from the browser. To run it headless
instead, use `bash demo/run-migration.sh --output-dir ./kof-output`, which does a dry run followed
by the live migration.

The demo scripts that call Kafka CLI tools unset `KAFKA_CLASSPATH` automatically to avoid classpath
conflicts.

To stop everything: `bash demo/stop-kafka.sh` and `bash demo/stop-kof-brokers.sh`. Run the first
only if block 1 started the cluster — if you migrated from your own Kafka, stop KOF and leave the
source alone.

---

## Starting the UI

```bash
cd ui
npm install          # first time only — installs Express, kafkajs, ws
node server.js
```

Open **http://localhost:3000** in your browser.

---

## What the dashboard provides

- **SVG mesh topology** — 3-node Kafka cluster (left) and 3-node KOF cluster (right); nodes turn green as soon as each process starts.
- **Animated data conduit** — canvas particle stream showing topic data flowing from Kafka to KOF in real time.
- **Real-time shell output** — WebSocket-streamed stdout/stderr from `run-kafka-to-kof.sh`.
- **Config form** — pre-populated from `kof-output/kafka-to-kof.properties`; runs Dry Run or Live Migration directly.

---

## Environment variables

These are read by the UI server (`ui/server.js`) and do not apply to
`run-kafka-to-kof.sh`:

| Variable | Default | Description |
|---|---|---|
| `SOURCE_BOOTSTRAP` | read from `kof-output/kafka-to-kof.properties` | Source Kafka bootstrap addresses |
| `TARGET_BOOTSTRAP` | read from `kof-output/kafka-to-kof.properties` | Target KOF bootstrap addresses |
| `KAFKA_HOME` | _(none)_ | Path to your Kafka installation (e.g. `/opt/kafka`). Used to auto-build `KAFKA_CLASSPATH` from `$KAFKA_HOME/libs/*` if `KAFKA_CLASSPATH` is not set. |
| `KAFKA_CLASSPATH` | auto-built from `KAFKA_HOME/libs/*` | Kafka client JARs for the migration tool. Explicit value takes precedence over auto-detection from `KAFKA_HOME`. |
| `PORT` | `3000` | HTTP port for the dashboard |

The `KAFKA_HOME` fallback is a UI-server convenience: it resolves a classpath and injects it into
the environment of the `run-kafka-to-kof.sh` child process. Invoking that script yourself gives you
no such fallback — export `KAFKA_CLASSPATH` explicitly.

---

## Health and metadata reporting

The KOF cluster health check uses TCP probes on the FTL `core.servers` ports, so KOF nodes turn
green as soon as `tibftlserver` starts — before the Kafka protocol layer is ready. Kafka metadata
(topic counts) uses the `kafkajs` Admin API and updates every 10 seconds.

> **Known constraint — the probed ports are hardcoded.** The dashboard probes `5635`, `5620`, and
> `5623` (`ui/server.js:72-74`, `ui/public/index.html:429-430`). `tibkafkatokof` assigns realm
> ports *randomly* in the range 5600–5699 unless you pass `-core-servers`, so with a
> default-generated config the KOF nodes stay grey even though the cluster is healthy. Pin the
> ports when generating the config to make the topology light up:
>
> ```
> --core-servers "SRV1=localhost:5635,SRV2=localhost:5620,SRV3=localhost:5623"
> ```

---

## Starting KOF brokers (demo helper)

```bash
export TIBFTLSERVER=/opt/tibco/ftl/current-version/bin/tibftlserver   # or put tibftlserver on PATH
bash demo/start-kof-brokers.sh --output-dir ./kof-output
```

This starts SRV1 / SRV2 / SRV3 from `kof-output/kof-cluster.yaml` and saves PIDs to
`demo/kof-brokers.pid`.

To stop: `bash demo/stop-kof-brokers.sh`
