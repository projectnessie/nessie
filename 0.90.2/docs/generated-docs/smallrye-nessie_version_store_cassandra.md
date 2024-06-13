---
search:
  exclude: true
---
<!--start-->

When setting `nessie.version.store.type=CASSANDRA` which enables Apache Cassandra or  ScyllaDB as the version store used by the Nessie server, the following configurations are  applicable.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.version.store.cassandra.dml-timeout` | `PT3S` | `duration` | Timeout used for queries and updates.  |
| `nessie.version.store.cassandra.ddl-timeout` | `PT5S` | `duration` | Timeout used when creating tables.  |
