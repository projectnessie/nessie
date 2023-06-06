# Commit Kernel

Nessie's production commit kernel is optimized to provide high commit throughput against a
distributed key value store that provides record level CAS (compare-and-swap) capability or
transactional/relational databases. The commit kernel is the heart of Nessie's operations and
enables it to provide lightweight creation of new tags/branches, merges, and rebases, all with very
high concurrent commit rate.

## High level abstract

Nessie 1.0 comes with a version store (aka commit kernel) implementation that is different from both
Git and older Nessie version store implementations in Nessie versions before 1.0 and is abstracted
as illustrated below. Nessie generally supports both non-transactional key-value databases and
transactional databases (relational).

The goal of all implementations is to spread the keys as much as possible, so data can be
properly distributed, and to keep the number of operations against a database low to reduce
operation time.

Contention by itself is not avoidable, because operations against Nessie are guaranteed to be
atomic and consistent.

### Nessie Content Types

The state of so called `Content` objects like `IcebergTable` or `DeltaLakeTable` represents the
current state of a table in a data lake. Whenever a table has changed via for example Iceberg,
a so-called commit operation instructs Nessie to record the new state in a Nessie commit, which
carries the `Content` object(s).

`IcebergTable` contains the pointer to Iceberg's table metadata plus the
IDs of the snapshot, schema, partition spec, sort order defined in the table metadata.
- Iceberg's table metadata manages information is stored in the Nessie commit.
- The value of the snapshot-ID, schema-ID, partition-spec-ID, sort-order-ID is stored per Nessie named reference (branch or tag).
For more information, please refer the spec [On Reference State vs Global State](spec.md#on-reference-state-vs-global-state)

Updating _global-state_ and _on-reference-state_ are technically operations against two different
entities in Nessie's backend database. Classic, relational databases (usually) come with a
transaction manager, which ensures that changes to different tables appear atomically to other
users. Much more scalable key-value stores do not have a transaction manager, but usually only
provide so-called "Compare-and-Swap" (CAS) operations, which conditionally update a single key-value
pair. This means, that the data model has to be fundamentally different for non-transactional
key-value stores and transactional databases. Support for non-transactional databases, the
data model, is designed in a way that only requires a single CAS operation to ensure atomicity and
consistency even when committing two logical entities, namely the _global-state_ and the
_on-reference-state_, respectively the update to the "HEAD" of the updated branch. Some more details
are outlined below.

### Version Store and Database Adapters

**The information on this page is outdated. `DatabaseAdapter` has been replaced with `Persist`
along with its own `VersionStore` implementation.**

Nessie's REST API implementation works against the `VersionStore` interface, which defines the
contract for the REST API, deals with concrete contents objects like `IcebergTable` or
`DeltaLakeTable`.

`PersistVersionStore` is an implementation of `VersionStore` and translates between the
content type objects like `IcebergTable` or `DeltaLakeTable` and the "binary" (think: "BLOB")
representation in the database adapters.

`DatabaseAdapter` interface defining the content type independent mechanisms to perform Nessie
operations like commit, transplants and merges as well as retrieving data.

`AbstractDatabaseAdapter` implements the commit logic, commit conflict detection and operations
to retrieve information. There are these subclasses: 

* `NonTransactionalDatabaseAdapter` is used as a base for key-value stores.
  * Implementation for DynamoDB
  * Implementation for MongoDB
  * Implementation for RocksDB
  * Implementation for InMemory
* `TransactionalDatabaseAdapter` JDBC based implementation relying on relational database
  transactions for conflict resolution (rollback).
  * SQL/DDL/type definitions for Postgres, Cockroach, H2

### Non-transactional key-value databases

**The information on this page is outdated. "global state" has been removed from Nessie.**

The data model for non-transactional key-value databases relies on a single _global-state-pointer_,
which is technically a table with a single row pointing to the _current_ entry in the _global-log_,
_current_ entry in the _ref-log_ and the "HEAD"s of all named references (branches and tags).

The _global-log_ contains changes to _global-state_, which is needed for backwards compatibility.

The _ref-log_ contains the history with details of operations 
like COMMIT, MERGE, TRANSPLANT, CREATE_REFERENCE, DELETE_REFERENCE, ASSIGN_REFERENCE.

The _commit-log_ contains the individual Nessie commits.

All commit, transplant and merge operations as well as other write operations like creating,
reassigning or deleting a named reference work inside a so-called "CAS loop", which technically
works like the following pseudocode. A CAS operation can be imagined as an SQL like
`UPDATE global_pointer SET value = :new_value WHERE primary_key = 1 AND value = :expected_value`.

```java 
// Pseudo definition of a Nessie write operation like a commit, merge, transplant, createReference,
// assignReference, deleteReference.
FunctionResult nessieWriteOperation(parameters...) {
  while (true) {
    globalPointer = loadGlobalPointer();

    // Try the actual operation.
    //
    // Return the keys of the optimistically written rows in the commit log and global log,
    // the changes to the global pointer and the result to be returned to the caller.
    optimisticallyWrittenRows, updatesToGlobalPointer, functionResult
      = performNessieWriteOperation(globalPointer, parameters);

    // Try the CAS operation on the global pointer.
    success = tryUpdateGlobalPointer(globalPointer, updatesToGlobalPointer);
    
    if (success) {
      // If the CAS oepration was successfully applied, return the function's result to the user.
      return functionResult;
    }

    // CAS was not successful
    deleteOptimisticallyWrittenRows(optimisticallyWrittenRows);
    if (!retryPolicy.allowsRetry()) {
      throw new RetryFailureException();
    }
  }
}
```

### Transactional databases

The data model for transactional databases defines tables for

* the _global-state_, where the primary key is the globally unique _content-id_ and the
  value of the _global-state_,
* the _named-references_, which define the commit hash/id of the "HEAD" of each named reference,
* the _commit-log_, which contains all commits
* the _ref-log_ contains the history with details of operations
  like COMMIT, MERGE, TRANSPLANT, CREATE_REFERENCE, DELETE_REFERENCE, ASSIGN_REFERENCE.
* the _ref-log-head_ contains current head of the _ref_log_ entry.

All commit, transplant and merge operations as well as other write operations like creating,
reassigning or deleting a named reference work inside a so-called "operation loop", which is
rather somewhat similar to the "CAS loop" for non-transactional databases, but does not need to
keep track of optimistically written data and can directly use conditional SQL DML statements like
`UPDATE table SET col = :value WHERE key = :key AND col = :expected_value` resp. `INSERT INTO...`.
The database then comes back with either an _update count_ > 0 to indicate success or an
_update count_ = 0 to indicate failure or an _integrity constraint violation_ error.

### Tracing & Metrics

Two delegating implementations of the `VersionStore` interface exist to provide metrics and
tracing using Micrometer and OpenTracing.

## Implemented database adapters

All current implementations are based on the abstractions in the Maven modules
`:nessie-versioned-persist-adapter` + either `:nessie-versioned-persist-non-transactional`
(for key-value stores) or `:nessie-versioned-persist-transactional` (for relational/transactional
databases).

* Non-transactional
  * InMemory (testing and prototyping)
  * RocksDB
  * MongoDB
  * DynamoDB (planned)
* Transactional
  * H2
  * Postgres

Note: not all database adapters are available via Nessie running via Quarkus!

## Nessie logic vs database specific adapters

The whole logic around commits, merges, transplants, fetching keys and values resides in
[AbstractDatabaseAdapter](https://github.com/projectnessie/nessie/blob/main/versioned/persist/adapter/src/main/java/org/projectnessie/versioned/persist/adapter/spi/AbstractDatabaseAdapter.java)
and is shared across all kinds of database adapters.

Database adapters, for both transactional and non-transactional databases, have the database
specific implementations around the CAS loop for non-transactional, catching
_integrity constraint violations_ for transactional, the concrete physical data model and the concrete
read & write implementations.

## Logical Data model

The [DatabaseAdapter](https://github.com/projectnessie/nessie/blob/main/versioned/persist/adapter/src/main/java/org/projectnessie/versioned/persist/adapter/DatabaseAdapter.java)
interface defines the functions needed by the version store implementation to access the data.

Implementations of `DatabaseAdapter` are free to implement their own optimizations.

### Non-transactional

Implementations are based
on [NonTransactionalDatabaseAdapter](https://github.com/projectnessie/nessie/blob/main/versioned/persist/nontx/src/main/java/org/projectnessie/versioned/persist/nontx/NonTransactionalDatabaseAdapter.java)
and only implement the database specific "primitives" to unconditionally read and write records and
perform the mandatory CAS (compare-and-swap) operation.

Key-value stores are all non-transactional as those are built for scale-out. Most key-value stores
support atomic CAS (compare-and-swap) operations against a single row/record, but atomic and
conditional updates to multiple rows/records is either not supported at all or extremely slow.

Nessie differentiates between content types that do require so called _global-state_ and those
that do not. _Global-state_ is maintained globally and evaluated when a content value object is
being retrieved, combined with the requested on-reference state on a Nessie commit.
For _Nessie commits_, which are atomic, this means that
Nessie has to update both the global-state and the on-reference-state for a content type that
requires _global state_. While
this is not an issue with a relational/transactional database, it is an issue in a key-value store.
Nessie solves this with a single "global pointer", which is updated using a CAS operation.

Nessie commits (and similar operations like "transplant" and "merge") optimistically write all the
data to the commit log and _global state log_ first and then try to perform the CAS operation against
the global pointer. If the CAS operation succeeds, the Nessie commit operation has succeeded. If the
CAS operation failed, all optimistically written rows are deleted and the whole Nessie commit is
retried.

The logical data model shared by all non-transactional database adapters consists of five entities:

* _Global-pointer_ a single "table row" that points to the current _global-state-log_ and all HEADs
  for all named references. Consistent updates are guaranteed via a CAS operation on this entity
  comparing the HEAD of the _global-state-log_.
* _Commit-log_ contains all commit log entries, identified by a deterministic hash. This is the same
  as for transactional databases.
* _Global-state-log_ contains all changes to the global state for content types that do require
  global state. The row keys are random IDs.
* _Key-lists_ acts as an "overflow" for large key lists that do not fit entirely into a single
  commit log entry's embedded key list.
* _Ref-log_ contains the history with details of operations
    like COMMIT, MERGE, TRANSPLANT, CREATE_REFERENCE, DELETE_REFERENCE, ASSIGN_REFERENCE.

### Transactional

Implementations are based
on [TxDatabaseAdapter](https://github.com/projectnessie/nessie/blob/main/versioned/persist/tx/src/main/java/org/projectnessie/versioned/persist/tx/TxDatabaseAdapter.java)
and currently only implement the database specific nuances in the SQL syntax and Nessie data type
mappings.

The data for transactional database adapters consists of six tables:

* _Named-references_ contains all named references and their current HEAD, the latter is used to
  guarantee consistent updates.
* _Global-state_ contains the current global state for a contents ID for content types that require
  global state. Consistent changes are guaranteed by tracking a checksum
  + value of the contents of the value representing the global state.
* _Commit-log_ contains all commit log entries, identified by a deterministic hash. This is the same
  as for non-transactional databases.
* _Key-lists_ acts as an "overflow" for large key lists that do not fit entirely into a single
  commit log entry's embedded key list.
* _Ref-log_ contains the history with details of operations
  like COMMIT, MERGE, TRANSPLANT, CREATE_REFERENCE, DELETE_REFERENCE, ASSIGN_REFERENCE.
* _Ref-log-head_ contains current head of the _ref_log_ entry.

## Performance

The non-transactional and transactional variants have different performance characteristics. As
outlined above, the non-transactional variant uses a central global pointer and the transactional
variant leverages the transaction manager of the database.

The implementation can perform many hundred to many thousand commits per second, depending on the
performance of the backend database and the characteristics of the use case. The two important
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
production-like use cases.

### Microbenchmarks

There are microbenchmarks available, which can be useful to investigate the overall performance of a
database. Please note that performance tests, even microbenchmarks, are only meaningful in
production-like environments using production-like use cases.
See [Nessie Persistence Microbenchmarks README.me](https://github.com/projectnessie/nessie/blob/main/versioned/persist/bench/README.md)
.

## Retry Mechanism

All write operations do support retries. Retries happen, if a non-transactional CAS operation failed
or a transactional DML operation ran into an "integrity constraint violation". Both the number of
retries and total time for the operation are bounded. There is an (exponentially increasing) sleep
time between two tries. The actual values for the retry mechanism are configurable. 
