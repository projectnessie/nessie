When setting `nessie.version.store.type=CASSANDRA` which enables Apache Cassandra or  ScyllaDB as the version store used by the Nessie server, the following configurations are  applicable.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.version.store.cassandra.dml-timeout` | `PT3S` | `Duration` | Timeout used for queries and updates.  |
| `nessie.version.store.cassandra.ddl-timeout` | `PT5S` | `Duration` | Timeout used when creating tables.  |
