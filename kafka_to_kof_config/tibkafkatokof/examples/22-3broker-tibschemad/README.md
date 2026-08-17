# Example 22 — Three brokers with the FTL schema daemon

Three-broker plaintext Kafka converted with `-tibschemad`. The inputs are byte-for-byte the
same `server-{1,2,3}.properties` as [example 04](../04-3broker-plaintext/), so the only
difference in the output is what the flag adds.

## Command

```bash
tibkafkatokof \
  --core-servers "SRV1=localhost:5600,SRV2=localhost:5601,SRV3=localhost:5602" \
  --tibschemad \
  --output-dir output \
  server-1.properties server-2.properties server-3.properties
```

## What the flag adds

Every server that carries a `- realm:` entry gains a `schemaN` persistence and a `tibschemad`
entry. `cluster.size` is the number of realm servers, so a three-broker conversion is sized 3:

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
      cluster.size: 3

  SRV2:
    ... schema2, cluster.size: 3
  SRV3:
    ... schema3, cluster.size: 3
```

This is still three `tibftlserver` processes on three ports. Each one hosts both a KOF pserver
and a schema pserver; the flag adds no servers and no ports.

## Verifying

Everything except the three schema daemon blocks is unchanged from example 04:

```bash
diff -r ../04-3broker-plaintext/output output
```

`realm.json`, the three `kof.broker.N.properties` files and `unsupported.properties` are
identical.

## Start

```bash
tibftlserver -c output/tibftlserver-cluster.yaml -n SRV1
tibftlserver -c output/tibftlserver-cluster.yaml -n SRV2
tibftlserver -c output/tibftlserver-cluster.yaml -n SRV3
```

## Notes

- `auth.type: none` is what phase 1 emits. Wiring the schema daemon to OAuth2 is phase 2.
- Auxiliary YAMLs (`tibftlserver-cluster-auxN.yaml`, produced above three pservers) get no
  schema daemon: their `PSRV*` servers have no `- realm:` entry, so there is nothing for it to
  attach to. DR YAMLs are likewise left alone in phase 1.
- See [example 21](../21-single-node-tibschemad/) for the standalone form, where the same flag
  produces `cluster.size: 1`.
