# Nessie Tiered Version Store Microbenchmarks

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

* exercised database adapters via `adapter`: H2, HSQL, In-Memory, RocksDB
* tables per commit via `tablesPerCommi`: 1, 3, 5

Benchmarks:

* `CommitBench.singleBranchSharedKeys`: threads use a single branch and shared content-keys,
  contention on the branch, contention on the content-keys
* `CommitBench.branchPerThreadSharedKeys`: threads use their own branch and own content-keys, no
  contention on the branch, contention on the content-keys
* `CommitBench.singleBranchUnsharedKeys`: threads use a single branch and shared content-keys,
  contention on the branch, no contention on the content-keys
* `CommitBench.branchPerThreadUnsharedKeys`: threads use their own branch and own content-keys, no
  contention on the branch, no contention on the content-keys

## Database-adapter configuration options

Some database-adapters need extra configuration. These options are "injected" via
[SystemPropertiesConfigurer](../adapter/src/main/java/org/projectnessie/versioned/tiered/adapter/SystemPropertiesConfigurer.java)
.

Basically all database-adapters, except those intended for testing, require configuration options
provided via system properties.

* RocksDB:
  * `nessie.store.db.path`, the path where the RocksDB is persisted.
    Example: `-Dnessie.store.db.path=/tmp/rocks-nessie-jmh`
* JDBC based database-adapters:
  * `nessie.store.jdbc.url`, the JDBC URL
  * `nessie.store.jdbc.user`, the JDBC username
  * `nessie.store.jdbc.pass`, the JDBC password

The "In-Memory", "H2" and "HSQL" database adapters do not require additional configuration, because
those are mainly intended for testing purposes.
