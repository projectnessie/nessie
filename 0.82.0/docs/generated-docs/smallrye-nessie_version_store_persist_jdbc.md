---
search:
  exclude: true
---
<!--start-->

Setting `nessie.version.store.type=JDBC` enables transactional/RDBMS as the version store  used by the Nessie server.  Configuration of the datastore will be done by Quarkus and depends on  many factors, such as the actual database in use. A complete set of JDBC configuration options  can be found on [quarkus.io](https://quarkus.io/guides/datasource).

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.version.store.persist.jdbc.catalog` |  | `String` | The JDBC catalog name. If not provided, will be inferred from the datasource. |
| `nessie.version.store.persist.jdbc.schema` |  | `String` | The JDBC schema name. If not provided, will be inferred from the datasource. |
