Version store configuration.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.version.store.type` | `IN_MEMORY` | `IN_MEMORY, ROCKSDB, DYNAMODB, MONGODB, CASSANDRA, JDBC, BIGTABLE` | Sets which type of version store to use by Nessie.  |
| `nessie.version.store.events.enable` | `true` | `boolean` | Sets whether events for the version-store are enabled. In order for events to be published,  it's not enough to enable them in the configuration; you also need to provide at least one  implementation of Nessie's EventListener SPI.  |
