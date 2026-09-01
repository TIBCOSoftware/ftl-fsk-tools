# Example 21 — Single node with the FTL schema daemon

Single-node plaintext Kafka converted with `-tibschemad`. The input is byte-for-byte the
same `server-1.properties` as [example 01](../01-single-node-plaintext/), so the only
difference in the output is what the flag adds.

## Command

```bash
tibftlimportconfig \
  --core-servers "SRV1=localhost:5663" \
  --tibschemad \
  --output-dir output \
  server-1.properties
```

## What the flag adds

A single broker is a standalone server rather than a cluster, so the YAML is named
`tibftlserver_standalone.yaml` and the schema daemon is sized `cluster.size: 1`:

```yaml
servers:
  SRV1:
  - realm:
      data: /var/tmp/kof/data
      initial.realm.config: output/realm.json
  - persistence:
      name: pserver1
      data: /var/tmp/kof/data/pserver1
      kof.broker.properties: output/kof.broker.1.properties
      loglevel: connections:info;kof:info;durables:info;store:info
  - persistence:          # <-- added by --tibschemad
      name: schema1
  - tibschemad:
      auth.type: none
      cluster.size: 1
```

`SRV1` is still one `tibftlserver` process on one port. The schema pserver shares the process
that already hosts the FSK pserver — the flag adds no servers and no ports.

## Verifying

Everything except the schema daemon block is unchanged from example 01:

```bash
diff -r ../01-single-node-plaintext/output output
```

The only hunk is the five lines above; `realm.json`, `kof.broker.1.properties` and
`unsupported.properties` are identical.

## Start

```bash
tibftlserver -c output/tibftlserver_standalone.yaml -n SRV1
```

## Notes

- `auth.type: none` is what phase 1 emits. Wiring the schema daemon to OAuth2 is phase 2.
- See [example 22](../22-3broker-tibschemad/) for the three-broker form, where the same flag
  produces `cluster.size: 3`.
