# Example 19 — Fetch config from live plaintext brokers

This example shows how to convert a running plaintext Kafka cluster to KOF artifacts
**without** a `server.properties` file, by fetching the effective broker configuration
directly from the live brokers via the Kafka Admin API.

## Prerequisites

- A running Kafka cluster (e.g. started with
  `../../../tibkafkatokof_migration/demo/setup-kafka-kraft.sh`, or your own).
- `tibkafkatokof` built: `go build -o tibkafkatokof .`

## Command

```bash
tibkafkatokof \
  --from-brokers localhost:9092,localhost:9093,localhost:9094 \
  --core-servers "SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602" \
  --output-dir output
```

If all three brokers share the same configuration (a typical KRaft cluster where
broker config is pushed cluster-wide), you can pass a single broker address and get a
single-pserver output, then scale up by repeating the address:

```bash
# Single pserver (good for a quick sanity check):
tibkafkatokof --from-brokers localhost:9092 --output-dir output

# Three pservers, same broker config fetched once per address:
tibkafkatokof --from-brokers localhost:9092,localhost:9093,localhost:9094 --output-dir output
```

## How it works

1. `tibkafkatokof` connects to each broker address in turn.
2. It calls the Kafka Admin `DescribeConfigs` API to download the effective
   broker configuration (equivalent to the contents of `server.properties`).
3. The downloaded configuration is processed through the same translation pipeline
   used for file-based inputs — stripping internal listeners, mapping security
   protocols, checking the whitelist, and generating KOF artifacts.

## Output

The `output/` directory will contain the same artifacts as file-based invocations:

```
output/kof-cluster.yaml
output/realm.json
output/kof.broker.1.properties   (one per broker address)
output/kof.broker.2.properties
output/kof.broker.3.properties
output/unsupported.properties    (only if unsupported keys are found)
```

## Notes

- The `--from-brokers` flag and positional `server.properties` arguments are mutually exclusive.
- Use `--from-brokers-timeout-ms` (default 10000) to adjust the Admin API timeout.
- The generated `SourceFile` in comments is the broker address instead of a file path.
