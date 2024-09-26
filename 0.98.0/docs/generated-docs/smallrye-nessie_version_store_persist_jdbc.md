---
search:
  exclude: true
---
<!--start-->

Setting `nessie.version.store.type=JDBC2` enables transactional/RDBMS as the version store  used by the Nessie server.  

Configuration of the datastore will be done by Quarkus and depends on many factors, such as  the actual database to use. The property `nessie.version.store.persist.jdbc.datasource` will be used to select one of the built-in datasources; currently supported values are: `postgresql` (which activates the PostgresQL driver), `mariadb` (which activates the MariaDB  driver), and `mysql` (which targets MySQL backends, but using the MariaDB driver).   

For example, to configure a PostgresQL connection, the following configuration should be used:   

 * `nessie.version.store.type=JDBC2` 
 * `nessie.version.store.persist.jdbc.datasource=postgresql` 
 * `quarkus.datasource.postgresql.jdbc.url=jdbc:postgresql://localhost:5432/my_database` 
 * `quarkus.datasource.postgresql.username=<your username>` 
 * `quarkus.datasource.postgresql.password=<your password>` 
 * Other PostgresQL-specific properties can be set using `quarkus.datasource.postgresql.*` 

To connect to a MariaDB database instead, the following configuration should be used:   

 * `nessie.version.store.type=JDBC2` 
 * `nessie.version.store.persist.jdbc.datasource=mariadb` 
 * `quarkus.datasource.mariadb.jdbc.url=jdbc:mariadb://localhost:3306/my_database` 
 * `quarkus.datasource.mariadb.username=<your username>` 
 * `quarkus.datasource.mariadb.password=<your password>` 
 * Other MariaDB-specific properties can be set using `quarkus.datasource.mariadb.*` 

To connect to a MySQL database instead, the following configuration should be used:   

 * `nessie.version.store.type=JDBC2` 
 * `nessie.version.store.persist.jdbc.datasource=mysql` 
 * `quarkus.datasource.mysql.jdbc.url=jdbc:mysql://localhost:3306/my_database` 
 * `quarkus.datasource.mysql.username=<your username>` 
 * `quarkus.datasource.mysql.password=<your password>` 
 * Other MySQL-specific properties can be set using `quarkus.datasource.mysql.*` 

To connect to an H2 in-memory database, the following configuration should be used (note that  H2 is not recommended for production):   

 * `nessie.version.store.type=JDBC2` 
 * `nessie.version.store.persist.jdbc.datasource=h2` 

Note: for MySQL, the MariaDB driver is used, as it is compatible with MySQL. You can use either  `jdbc:mysql` or `jdbc:mariadb` as the URL prefix.   

A complete set of JDBC configuration options can be found on [quarkus.io](https://quarkus.io/guides/datasource).

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.version.store.persist.jdbc.datasource` |  | `string` | The name of the datasource to use. Must correspond to a configured datasource under `quarkus.datasource.<name>` . Supported values are: `postgresql` `mariadb`, `mysql` and `h2`. If not provided, the default Quarkus datasource, defined using the  `quarkus.datasource.*` configuration keys, will be used (the corresponding driver is  PostgresQL). Note that it is recommended to define "named" JDBC datasources, see [Quarkus JDBC config  reference ](https://quarkus.io/guides/datasource#jdbc-configuration). |
