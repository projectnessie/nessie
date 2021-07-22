# Commit Kernel

Nessie's production commit kernel is optimized to provide high commit throughput against a
distributed key value store that provides record-level compare-and-swap capability or
transactional/relational databases. The commit kernel is the heart of Nessie's operations and
enables it to provide lightweight creation of new tags and branches, merges, rebases all with a very
high concurrent commit rate.

## Implemented database adapters

All current implementations are based on the abstractions in the Maven modules
`:nessie-versioned-persist-adapter` + either `:nessie-versioned-persist-non-transactional`
(for key-value stores) or `:nessie-versioned-persist-transactional` (for relational/transactional
databases).

* Non-transactional
  * In-Memory (testing and prototyping)
  * RocksDB
  * MongoDB
  * DynamoDB (planned)
* Transactional
  * H2
  * Postgres

Note: not all database adapters are available via Nessie running via Quarkus!

## Nessie logic vs database-adapters

The whole logic around commits, merges, transplants, fetching keys and values resides in
[AbstractDatabaseAdapter](https://github.com/projectnessie/nessie/blob/main/versioned/persist/adapter/src/main/java/org/projectnessie/versioned/persist/adapter/spi/AbstractDatabaseAdapter.java)
and is shared across all kinds of database adapters.

Database adapters, for both transactional and non-transactional databases, have the database
specific implementations around the CAS-loop for non-transactional, catching
integrity-constraint-violations for transactional, the concrete physical data model and the concrete
read & write implementations.

## Logical Data model

The [DatabaseAdapter](https://github.com/projectnessie/nessie/blob/main/versioned/persist/adapter/src/main/java/org/projectnessie/versioned/persist/adapter/DatabaseAdapter.java)
interface defines the functions needed by the tiered-version-store implementation to access the
data.

Implementations of `DatabaseAdapter` are free to implement their own optimizations.

### Non-transactional

Implementations are based
on [NonTransactionalDatabaseAdapter](https://github.com/projectnessie/nessie/blob/main/versioned/persist/nontx/src/main/java/org/projectnessie/versioned/persist/nontx/NonTransactionalDatabaseAdapter.java)
and only implement the database specific "primitives" to unconditionally read and write records and
perform the mandatory CAS (compare-and-swap) operation.

Key-value stores are all non-transactional as those are built for scale-out. Most key-value stores
support atomic CAS (compare-and-swap) operations against a single row/record, but atomic and
conditional updates to multiple rows/records is either not supported at all or extremely slow.

Within Nessie we differ between content-types that do require so called "global-state" and those
that do not. Apache Iceberg is currently the only content-type that supports global-state:
the pointer to the Iceberg "Table Metadata" is tracked as "global-state" and the Iceberg snapshot-ID
is tracker per Nessie-named-reference. For Nessie-commits, which are atomic, this means that Nessie
has to update both the global-state and the on-reference-state for the Iceberg table. While this is
not an issue with a relational/transactional database, it is an issue in a key-value store. Nessie
solves this with a single "global-pointer", which is updated using a CAS operation.

Nessie-commits (and similar operations like "transplant" and "merge") optimistically write all the
data to the commit-log and global-state-log first and then try to perform the CAS-operation against
the global-pointer. If the CAS-operation succeeds, the Nessie-commit operation has succeeded. If the
CAS-operation failed, all optimistically written rows are deleted and the whole Nessie-commit is
retried.

The logical data model shared by all non-transactional database adapters consists of four entities:

* _Global-pointer_ a single "table row" that points to the current _global-state-log_ and all HEADs
  for all named references. Consistent updates are guaranteed via a CAS operation on this entity
  comparing the HEAD of the global-state-log.
* _Commit-log_ contains all commit-log-entries, identified by a deterministic hash. This is the same
  as for transactional databases.
* _Global-state-log_ contains all changes to the global state for content-types that do require
  global state (currently Apache Iceberg). The row-keys are random IDs.
* _Key-lists_ acts as an "overflow" for large key-lists that do not fit entirely into a single
  commit-log's embedded-key-list.

### Transactional

Implementations are based
on [TxDatabaseAdapter](https://github.com/projectnessie/nessie/blob/main/versioned/persist/tx/src/main/java/org/projectnessie/versioned/persist/tx/TxDatabaseAdapter.java)
and currently only implement the database specific nuances in the SQL syntax and Nessie-data-type
mappings.

The data for transactional database adapters consists of four tables:

* _Named-references_ contains all named-references and their current HEAD, the latter is used to
  guarantee consistent updates.
* _Global-state_ contains the current global state for a contents-id for content-types that require
  global state (currently Apache Iceberg). Consistent changes are guaranteed by tracking a checksum
  + value of the contents of the global-state-value.
* _Commit-log_ contains all commit-log-entries, identified by a deterministic hash. This is the same
  as for non-transactional databases.
* _Key-lists_ acts as an "overflow" for large key-lists that do not fit entirely into a single
  commit-log's embedded-key-list.

## Performance

The non-transactional and transactional variants have different performance characteristics. As
outlined above, the non-transactional variant uses a central global-pointer and the transactional
variant leverages the transaction-manager of the database.

The implementation can perform many hundred to many thousand commits per second, depending on the
performance of the backend database and the characteristics of the use-case. The two important
factors are:

* Concurrent commits against different branches are "faster" than concurrent commits against a
  single branch
* Concurrent commits against the same table (think: Iceberg or Deltalake table) are slower than
  concurrent commits against different tables.

### Gatling Benchmarks

Nessie has a framework to simulate "higher level use cases" using Gatling. See the readmes
[here](https://github.com/projectnessie/nessie/blob/main/perftest/gatling/README.md) and
[here](https://github.com/projectnessie/nessie/blob/main/perftest/simulations/README.md). Please
note that all kinds of performance tests are only meaningful in production-like environments using
production-live use cases.

### Microbenchmarks

There are microbenchmarks available, which can be useful to investigate the overall performance of a
database. Please note that performance tests, even microbenchmarks, are only meaningful in
production-like environments using production-live use cases.
See [Nessie Persistence Microbenchmarks README.me](https://github.com/projectnessie/nessie/blob/main/versioned/persist/bench/README.md)
.

## Retry Mechanism

All write operations do support retries. Retries happen, if a non-transactional CAS-operation failed
or a transactional DML operation ran into a "integrity constraint violation". Both the number of
retries and total time for the operation are bounded. There is an (exponentially increasing) sleep
time between two tries. The actual values for the retry mechanism are configurable. 
