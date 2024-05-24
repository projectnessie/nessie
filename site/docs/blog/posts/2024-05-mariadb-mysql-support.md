---
date: 2024-05-24
authors:
  - adutra
---

# Support for MariaDB and MySQL

We are happy to announce that Nessie now supports MariaDB and MySQL as backends. This is a
significant milestone for Nessie, as it opens up new possibilities for Nessie users.

A big thank you to [Vayuj Rajan](https://github.com/vyj7) for his contribution to this feature!

<!-- more -->

## Why MariaDB and MySQL?

MariaDB and MySQL are popular open-source relational databases that are widely used in the industry.
By adding support for MariaDB and MySQL, we are making it easier for users to get started with
Nessie.

**Important note**: MariaDB and MySQL support is considered experimental at this time, and we are
looking for feedback from the community to improve it further.

## How does it work?

Nessie now ships with the MariaDB driver and the corresponding Quarkus extension. The MariaDB driver
allows Nessie to work not only with MariaDB servers, but also with MySQL ones, thanks to MariaDB's
protocol & schema compatibility with MySQL.

## How to get started with MariaDB or MySQL

To get started with MariaDB or MySQL, you can follow the instructions in the [Nessie
documentation](https://projectnessie.org/nessie-latest/configuration/#jdbc-version-store-settings),
but here is a quick step-by-step guide:

1. Select the `JDBC` version store type as usual:

```properties
nessie.version.store.type=JDBC
```

2. Declare the JDBC datasource to use as either `mariadb` or `mysql` depending on your case:

For MariaDB:

```properties
nessie.version.store.persist.jdbc.datasource=mariadb
```

For MySQL:

```properties
nessie.version.store.persist.jdbc.datasource=mysql
```

3. Configure the datasource using either the prefix `quarkus.datasource.mariadb.*` or
   `quarkus.datasource.mysql.*`, depending on your case:

For MariaDB:

```properties
quarkus.datasource.mariadb.jdbc.url=jdbc:mariadb://example.com:3306/my_db
quarkus.datasource.mariadb.username=my_user
quarkus.datasource.mariadb.password=${env:DB_PASSWORD}
```

For MySQL:

```properties
quarkus.datasource.mysql.jdbc.url=jdbc:mysql://example.com:3306/my_db
quarkus.datasource.mysql.username=my_user
quarkus.datasource.mysql.password=${env:DB_PASSWORD}
```

Check the [Quarkus documentation](https://quarkus.io/guides/datasource#datasource-reference) for more details on how to configure the datasource.

**Important Notes**: 

* When connecting to a MySQL server, the actual driver being used is the MariaDB driver, as it is
  compatible with MySQL. You can use either `jdbc:mariadb:` or `jdbc:mysql` in your JDBC URL.
  Generally, all MySQL JDBC URL parameters should still work, but your mileage may vary – we would
  love to hear your feedback!

* When connecting to MariaDB, please do NOT set the following options to `true`; they are
  incompatible with Nessie's current implementation and may cause somme commits to be rejected:

```properties
useBulkStmts=false
useBulkStmtsForInserts=false
```

## What does this mean for existing Nessie users?

If you are already using Nessie with a version store type other than `JDBC`, then this change does
not affect you.

If you are using the `JDBC` version store type, you are probably using a PostgreSQL database. You
can continue to use PostgreSQL without any modifications to your configuration, but please note that
_some properties are now deprecated and will be removed in a future release_. We recommend that you
update your configuration to use the new properties as soon as possible. Here is what you need to
do:

1. Include the new property below in your configuration:

```properties
nessie.version.store.persist.jdbc.datasource=postgresql
```

2. Migrate any property with the prefix `quarkus.datasource.*` to `quarkus.datasource.postgresql.*`.
   For example, the below configuration:

```properties
quarkus.datasource.jdbc.url=jdbc:postgresql://example.com:5432/my_db
quarkus.datasource.username=my_user
quarkus.datasource.password=${env:DB_PASSWORD}
```

should be migrated to:

```properties
quarkus.datasource.postgresql.jdbc.url=jdbc:postgresql://example.com:5432/my_db
quarkus.datasource.postgresql.username=my_user
quarkus.datasource.postgresql.password=${env:DB_PASSWORD}
```

## What about the Helm chart?

The Nessie Helm chart has been updated to support MariaDB and MySQL. You can now deploy Nessie with
MariaDB or MySQL as the version store backend. 

The main change is that the section called `postgres` in the `values.yaml` file has been renamed to
`jdbc`. The old properties are still supported for backward compatibility, but we recommend that you
update your configuration to use the new properties as soon as possible.

Check the [Nessie Helm chart
documentation](https://projectnessie.org/guides/kubernetes/#installing-the-helm-chart) for more
details, but here is a quick example of how to configure MariaDB or MySQL as the version store
backend:

```yaml
jdbc:
  jdbcUrl: jdbc:mariadb://example.com:3306/my_db
  secret:
    name: mariadb-credentials
    username: mariadb_user
    password: mariadb_password
```

It's that simple! The Helm chart will take care of the rest.

## What about the GC tool?

We have good news for you: the Nessie GC tool now supports MariaDB and MySQL. Check the [Nessie GC
tool documentation](https://projectnessie.org/guides/management/#nessie-gc-tool) for more details.
Again, please note: support for GC with these databases is also considered experimental, and we are
looking for feedback from the community.

As an example, here is how to create the SQL schema for the Nessie GC tool using MariaDB or MySQL:

```bash
java -jar nessie-gc.jar create-sql-schema \
  --jdbc-url jdbc:mariadb://example.com:3306/my_db \
  --jdbc-user my_user \
  --jdbc-password $DB_PASSWORD
```

That's it! You can now use the Nessie GC tool with MariaDB or MySQL.

## What about the Server Admin tool?

The Nessie Server Admin tool has also been updated to support MariaDB and MySQL. Check the [Nessie
Server Admin tool documentation](https://projectnessie.org/nessie-latest/export_import/) for more
details.

## Conclusion

We hope that you are as excited as we are about this new feature. We are looking forward to your
feedback and suggestions on how we can improve the MariaDB and MySQL support in Nessie. Please feel
free to reach out to us on the [Project Nessie Zulip](https://project-nessie.zulipchat.com/) chat or
[GitHub](https://github.com/projectnessie/nessie) with your thoughts or questions.