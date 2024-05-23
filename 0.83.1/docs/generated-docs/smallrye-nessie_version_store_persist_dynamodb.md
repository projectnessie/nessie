When setting `nessie.version.store.type=DYNAMODB` which enables DynamoDB as the version  store used by the Nessie server, the following configurations are applicable.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.version.store.persist.dynamodb.table-prefix` |  | `String` | Prefix for tables, default is no prefix.  |
