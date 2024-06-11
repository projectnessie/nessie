Setting `nessie.version.store.type=JDBC` enables transactional/RDBMS as the version store  used by the Nessie server.  

Configuration of the datastore will be done by Quarkus and depends on many factors, such as  the actual database to use. The property `nessie.version.store.persist.jdbc.datasource` will be used to select one of the built-in datasources; currently supported values are: `postgresql` (which activates the PostgresQL driver) and `mariadb` (which activates the  MariaDB driver, compatible with MySQL servers).   

For example, to configure a PostgresQL connection, the following configuration should be used:   

 * `nessie.version.store.type=JDBC` 
 * `nessie.version.store.persist.jdbc.datasource=postgresql` 
 * `quarkus.datasource.postgresql.jdbc.url=jdbc:postgresql://localhost:5432/my_database` 
 * `quarkus.datasource.postgresql.username=<your username>` 
 * `quarkus.datasource.postgresql.password=<your password>` 
 * Other PostgresQL-specific properties can be set using `quarkus.datasource.postgresql.*` 

To connect to a MariaDB database instead, the following configuration should be used:   

 * `nessie.version.store.type=JDBC` 
 * `nessie.version.store.persist.jdbc.datasource=mariadb` 
 * `quarkus.datasource.mariadb.jdbc.url=jdbc:mariadb://localhost:3306/my_database` 
 * `quarkus.datasource.mariadb.username=<your username>` 
 * `quarkus.datasource.mariadb.password=<your password>` 
 * Other MariaDB-specific properties can be set using `quarkus.datasource.mariadb.*` 

A complete set of JDBC configuration options can be found on [quarkus.io](https://quarkus.io/guides/datasource).

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.version.store.persist.jdbc.datasource` |  | `String` | The name of the datasource to use. Must correspond to a configured datasource under `quarkus.datasource.<name>` . Supported values are: `postgresql` and `mariadb`. If  not provided, the default Quarkus datasource, defined using the `quarkus.datasource.*` configuration keys, will be used (the corresponding driver is PostgresQL). Note that it is  recommended to define "named" JDBC datasources, see [Quarkus JDBC config  reference ](https://quarkus.io/guides/datasource#jdbc-configuration). |
| `nessie.version.store.persist.jdbc.catalog` |  | `String` | The JDBC catalog name. If not provided, will be inferred from the datasource. |
| `nessie.version.store.persist.jdbc.schema` |  | `String` | The JDBC schema name. If not provided, will be inferred from the datasource. |
