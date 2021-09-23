# Nessie Persist Version Store Microbenchmarks

**DISCLAIMER** THIS IS NOT A BENCHMARK TOOL FOR PRODUCTION WORKLOADS!!!

The JMH based microbenchmarks exist to get an idea of the potential commit-performance of a
database-adapter with a specific configuration. These microbenchmarks do neither validate
linearizability nor the commit-contents-model, but focus on the pure "commit performance" to find
bottlenecks.

## Usage

JMH code parameter defaults:

* number of threads: 4
* number of forks: 1
* warmups: 2 x 2s
* iterations: 3 * 5s

Benchmark defaults:

* exercised database adapters via `adapter`: H2, In-Memory, RocksDB
* tables per commit via `tablesPerCommit`: 1, 3, 5

Benchmarks:

* `CommitBench.singleBranchSharedKeys`: threads use a single branch and shared content-keys,
  contention on the branch, contention on the content-keys
* `CommitBench.branchPerThreadSharedKeys`: threads use their own branch and shared content-keys, no
  contention on the branch, contention on the content-keys
* `CommitBench.singleBranchUnsharedKeys`: threads use a single branch and own content-keys,
  contention on the branch, no contention on the content-keys
* `CommitBench.branchPerThreadUnsharedKeys`: threads use their own branch and own content-keys, no
  contention on the branch, no contention on the content-keys

## Database-adapter configuration options

Some database-adapters need extra configuration. These options are "injected" via
[SystemPropertiesConfigurer](../adapter/src/main/java/org/projectnessie/versioned/persist/adapter/SystemPropertiesConfigurer.java)
.

Basically all database-adapters, except those intended for testing, require configuration options
provided via system properties.

* RocksDB:
  * `nessie.store.db.path`, the path where the RocksDB is persisted.
    Example: `-Dnessie.store.db.path=/tmp/rocks-nessie-jmh`
* MongoDB:
  * `nessie.store.connection.string`, the MongoDB connection URL.
    Example: `-Dnessie.store.connection.string=mongodb://localhost:1234`
  * `nessie.store.database.name`, the Mongo database name.
* JDBC based database-adapters:
  * `nessie.store.jdbc.url`, the JDBC URL
  * `nessie.store.jdbc.user`, the JDBC username
  * `nessie.store.jdbc.pass`, the JDBC password

The "In-Memory" and "H2" database adapters do not require additional configuration, because those
are mainly intended for testing purposes.

## Running the microbenchmarks

Running microbenchmarks against in-memory, RocksDB and H2-in-memory do not require an external
process/service to be started, so those work "out of the box".

Other database adapters like Mongo, Postgres or Cockroach need a running database service. The
following sections shall help to start a database locally. Please refer to the official docs of the
projects/vendors how to install a "production grade" database, if needed.

The snippets below assume that you have built Nessie from source. If not, run
```bash
./mvnw package -am -pl :nessie-versioned-persist-bench -DskipTests
```

### In-Memory

```bash
java \
  -jar versioned/persist/bench/target/nessie-versioned-persist-bench-0.9.3-SNAPSHOT-jmh.jar \
  -p adapter=in-memory
```

### RocksDB

```bash
java \
  -jar versioned/persist/bench/target/nessie-versioned-persist-bench-0.9.3-SNAPSHOT-jmh.jar \
  -p adapter=RocksDB
```

### MongoDB

```bash
docker run -d \
  --name nessie-mongodb-bench \
  -p 27017:27017 \
  mongo:latest

java \
  -Dnessie.store.connection.string=mongodb://localhost:27017 \
  -Dnessie.store.database.name=nessie-mongodb \
  -jar versioned/persist/bench/target/nessie-versioned-persist-bench-0.9.3-SNAPSHOT-jmh.jar \
  -p adapter=MongoDB
```

### H2 / In-Memory

```bash
java \
  -Dnessie.store.jdbc.url=jdbc:h2:mem:nessie \
  -jar versioned/persist/bench/target/nessie-versioned-persist-bench-0.9.3-SNAPSHOT-jmh.jar \
  -p adapter=H2:h2
```

### H2 / Remote

(There does not seem to be an officially maintained Docker image for H2.)

```bash
java \
  -Dnessie.store.jdbc.url=<JDBC_URL_HERE> \
  -jar versioned/persist/bench/target/nessie-versioned-persist-bench-0.9.3-SNAPSHOT-jmh.jar \
  -p adapter=H2:generic
```

### PostgreSQL

```bash
docker run -d \
  -e POSTGRES_PASSWORD=nessie-bench \
  --name nessie-postgres-bench \
  -p 5432:5432 \
  postgres:latest

java \
  -Dnessie.store.jdbc.url=jdbc:postgresql://localhost:5432/postgres \
  -Dnessie.store.jdbc.user=postgres \
  -Dnessie.store.jdbc.pass=nessie-bench \
  -jar versioned/persist/bench/target/nessie-versioned-persist-bench-0.9.3-SNAPSHOT-jmh.jar \
  -p adapter=PostgreSQL
```

### Cockroach

```bash
docker run -d \
  --name nessie-cockroach-bench \
  -p 26257:26257 -p 8080:8080 \
  cockroachdb/cockroach:latest \
  start-single-node \
  --insecure

java \
  -Dnessie.store.jdbc.url=jdbc:postgresql://localhost:26257/postgres \
  -Dnessie.store.jdbc.user=root \
  -Dnessie.store.jdbc.pass= \
  -jar versioned/persist/bench/target/nessie-versioned-persist-bench-0.9.3-SNAPSHOT-jmh.jar \
  -p adapter=PostgreSQL
```

Docs: [Cockroach](https://www.cockroachlabs.com/docs/stable/start-a-local-cluster-in-docker-linux.html)
