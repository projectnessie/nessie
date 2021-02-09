# Testing the Nessie JDBC Store against different databases

The Nessie JDBC Store implementation currently supports SQL databases via JDBC that support the
`ARRAY` data type. Current implementations include experimental support for PostgresQL, CockroachDB,
Oracle RDBMS and H2 (mostly for in-JVM testing).

The following system properties control how integration tests are run:

| System property | Meaning |
| --------------- | ------- |
| `it.nessie.store.jdbc.databaseAdapter` | The database being tested. One of `H2`, `HSQL`, `PostgresQL`, `Cockroach`, `Oracle` |
| `it.nessie.store.jdbc.table_prefix` | The table prefix to use, set via `JdbcStoreConfig.tablePrefix`  |
| `it.nessie.store.jdbc.schema` | The table prefix to use, set via `JdbcStoreConfig.schema` |
| `it.nessie.store.jdbc.catalog` | The table prefix to use, set via `JdbcStoreConfig.catalog` | 
| `it.nessie.store.jdbc.testcontainer` | Whether [testcontainers](https://www.testcontainers.org/) is used to run the database in a Docker container (not for `H2` and `HSQL`). |
| `it.nessie.store.jdbc.url` | Valid w/o testcontainers: The JDBC connect URL |
| `it.nessie.store.jdbc.username` | Valid w/o testcontainers: The JDBC connect username |
| `it.nessie.store.jdbc.password` | Valid w/o testcontainers: The JDBC connect password |
| `it.nessie.store.jdbc.container-image-version` | Valid w/ testcontainers: The Docker image version, valid for `PostgresQL` and `Cockroach` |
| `it.nessie.store.jdbc.container-image` | Valid w/ testcontainers & `Oracle`: The Docker image name |
| `it.nessie.store.jdbc.oracle-port` | Valid w/ testcontainers & `Oracle`: Optional, the fixed connect TCP port (defaults to `1521`) |
| `it.nessie.store.jdbc.oracle-sid` | Valid w/ testcontainers & `Oracle`: Optional, the Oracle SID (defaults to `xe`) |
| `it.nessie.store.jdbc.oracle-username` | Valid w/ testcontainers & `Oracle`: Optional, the database user's name (defaults to `system`) |
| `it.nessie.store.jdbc.oracle-password` | Valid w/ testcontainers & `Oracle`: Optional, the database user's password (defaults to `oracle`) |
| `it.nessie.store.jdbc.oracle-wait-regex` | Valid w/ testcontainers & `Oracle`: Optional, the regex that must appear in the container's log output before the instance is considered "ready". Recommended to set to `.*DATABASE IS READY TO USE.*` for the vendor's image. |
| `it.nessie.store.jdbc.oracle-container-ip-url` | Valid w/ testcontainers & `Oracle`: Optional, use the container's IP address + unmapped in the JDBC connect URL instead of a mapped port, because some vendor images have issues when connecting via "localhost" |

## Testing in-JVM H2 & HSQL

Integration tests against h2 and hsql are executed and run in the local JVM.

## Testing PostgresQL + CockroachDB

Integration testing via [testcontainers](https://www.testcontainers.org/) is straight forward:

1. Set the system property `it.nessie.store.jdbc.testcontainer` to `true`
2. Set the system property `it.nessie.store.jdbc.databaseAdapter` to `PostgresQL`/`Cockroach`
3. Optional: Set the system property `it.nessie.store.jdbc.container-image-version` to the desired version (PostgresQL tested w/ `9.6.12`, CockroachDB tested w/ `v20.1.11`)

Note: Newer CockroachDB images (aka v20.2.x) have issues with testcontainers (or vice versa?)

Both databases are exercised if the `ossDatabases` Maven profile is activated.

## Testing Oracle

This is not exactly straight forward. One approach that works is this:

1. Go to [Oracle Container Registry](https://container-registry.oracle.com/)
    1. Choose the RDBMS/enterprise image.
    2. Accept the license agreement for the image you want to use.
    3. Do the `docker login container-registry.oracle.com`.
    4. Do the `docker pull` for the image you want to use.
2. Prepare a pre-configured Docker image:
    1. Start the vendor image: `docker run -ti --name oracle -p 1521:1521 -e ORACLE_PWD=oracle -e ORACLE_SID=XE -e ORACLE_EDITION=standard container-registry.oracle.com/database/enterprise:latest`, this takes quite a while
    2. Once that's done, stop the Docker image
    3. Save the preconfigured container as an image: `docker commit oracle oracle-testnessie`
3. Configure Nessie integration-tests by setting the following system properties:
   | System property | Value | Notes |
   | --------------- | ----- | ----- |
   | `it.nessie.store.jdbc.testcontainer` | `true` |  |
   | `it.nessie.store.jdbc.databaseAdapter` | `Oracle` |  |
   | `it.nessie.store.jdbc.container-image` | `oracle-testnessie` | (The image name you specified in step 2.3. above) |
   | `it.nessie.store.jdbc.oracle-password` | `oracle` | (The password you specified in step 2.1. above) |
   | `it.nessie.store.jdbc.oracle-sid` | `XE` | (The SID you specified in step 2.1. above) |
   | `it.nessie.store.jdbc.oracle-wait-regex` | `.*DATABASE IS READY TO USE.*` | Note: when specified on the command line, it needs to be quoted! |
   | `it.nessie.store.jdbc.oracle-container-ip-url` | `true` |  |

Notes: The `it.nessie.store.jdbc.oracle-container-ip-url` setting is required, because connecting via "localhost" + mapped-Docker-port does *not* work (JDBC requests just time out).

Links:
* [Oracle Container Registry](https://container-registry.oracle.com/)
* [Oracle Docker images github repo](https://github.com/oracle/docker-images) (not checked)

Oracle database adapter is only built if the `commercialOracle` Maven profile is activated.
Integration tests against Oracle are exercised if the `commercialOracleIT` Maven profile is activated.

# Example intergration-tests invocation

```
# All OSS databases
./mvnw integration-test -pl :nessie-versioned-tiered-jdbc -P ossDatabases

# Oracle
./mvnw integration-test -pl :nessie-versioned-tiered-jdbc-oracle \
   -P commercialOracle -P commercialOracleIT \
   -Dit.nessie.store.jdbc.container-image=oracle-testnessie \
   -Dit.nessie.store.jdbc.oracle-password=oracle \
   -Dit.nessie.store.jdbc.oracle-sid=XE \
   "-Dit.nessie.store.jdbc.oracle-wait-regex=.*DATABASE IS READY TO USE.*" \
   -Dit.nessie.store.jdbc.oracle-container-ip-url=true
```
