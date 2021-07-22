# Nessie Tiered Version Store

Nessie's Tiered-Version-Store consists of an implementation
of [VersionStore](../spi/src/main/java/org/projectnessie/versioned/VersionStore.java)
that has all the logic plus a variety of non-transactional and transactional database-adapters that
only perform reads and writes and do not implement any logic.

## Implemented database adapters

* Non-transactional
  * In-Memory (testing and prototyping)
  * RocksDB
  * DynamoDB (planned)
* Transactional
  * H2
  * HSQL

Note: not all database adapters are available via Nessie running via Quarkus!

## Nessie logic vs database-adapters

The whole logic around commits, merges, transplants, fetching keys and values resides in
[AbstractDatabaseAdapter](adapter/src/main/java/org/projectnessie/versioned/tiered/adapter/AbstractDatabaseAdapter.java)
and is shared across all kinds of database adapters.

Database adapters, for both transactional and non-transactional databases, have the database
specific implementations around the CAS-loop for non-transactional, catching
integrity-constraint-violations for transactional, the concrete physical data model, concrete read &
write implementations.

## Logical Data model

The [DatabaseAdapter](adapter/src/main/java/org/projectnessie/versioned/tiered/adapter/DatabaseAdapter.java)
interface defines the functions needed by the tiered-version-store implementation to access the
data.

Implementations of `DatabaseAdapter` are free to implement their own optimizations.

### Non-transactional

The logical data model shared by all database adapters consists of three entities:

* _Commit-log_ contains all commit-log-entries, identified by a deterministic hash. This is the same
  as for transactional databases.
* _Global-state-log_ contains all changes to Iceberg/Delta/Hive tables and views, identified by a
  rather random ID.
* _Global-pointer_ a single "table row" that points to the current _global-state-log_ and all HEADs
  for the named references. Consistent updates are guaranteed via a CAS operation on this entity
  comparing the HEAD of the global-state-log.

### Transactional

The data for transactional is a classic relational data model that consists of three tables:

* _Commit-log_ contains all commit-log-entries, identified by a deterministic hash. This is the same
  as for non-transactional databases.
* _Named-references_ contains all named-references and their current HEAD, the latter is used to
  guarantee consistent updates.
* _Global-state_ contains the current global state for a contents-key. Consistent changes are
  guaranteed by tracking the hash of the contents of the global-state-value.

## TODOs

* Global-state vs reference-conflict-handling:
  Multiple clients committing to the same table, no matter whether on the same branch or not,
  should receive information about the keys that failed and also receive the contents and/or
  global-contents along with the NessieConflictException / ReferenceConflictException. Clients
  can then check whether the contents/global-contents are "compatible" and "just" retry the commit
  with updated expected-global-states after potentially rewriting/merging for example the
  Iceberg table-metadata. The advantage is that a client does not have to retry the whole operation
  but just the commit.
  NOTE: Iceberg already has sophisticated conflict-resolution.
* Guardrails
  * maximum number of reads allowed to find an "expected hash"
  * maximum number of reads allowed to find a common-ancestor for a merge operation
  * maximum number of attempts / maximum duration / better retry-logic for the
    global-pointer-CAS-loop
* Allow an arbitrary number of keys. The
  current [EmbeddedKeyList](adapter/src/main/java/org/projectnessie/versioned/tiered/adapter/EmbeddedKeyList.java)
  is just serialized into a `CommitLogEntry`. This means, that a large-ish total number of
  content-keys (aka tables+views) causes probably too big serialized representations
  of `CommitLogEntry`.
* Allow an arbitrary number of named references. The
  current [GlobalStatePointer](adapter/src/main/java/org/projectnessie/versioned/tiered/adapter/GlobalStatePointer.java)
  holds all HEADs for all named-references. Keeping the least-recently-used named-references inside
  `GlobalStatePointer` makes a lot of sense, but externalizing rarely used named-references-HEADs to
  separate rows could be an option.
* GC
  * Can we just rely on the global-state-log instead of traversing all commit-log entries? Or at
    least for the "biggest" max-age value (think: depending on the GC policy)?
  * Should the 'dt' field persisted as a separate attribute (think: column)?
  * Identify unreferenced entries in the commit-log and global-state-log (and just delete those).
  * Let GC run at a "low pace" (think: low I/O + CPU overhead, don't impact production performance),
    so the `DatabaseAdapter` can natively filter entries that are "old enough" (if possible and
    needed).
  * The GC implementation must not rely on Spark.
  * Nessie-GC is a tiered and multi-phase operation:
    1. Nessie can natively (think: without looking inside contents objects) identify the most recent
       and GC-able global and per-commit contents.
    2. Nessie must then return tuples of "contents-type + contents-key + most-recent-global-states +
       newest-GC-able-states".
    3. Distinct implementations per content-type then have to find out which actual contents can be
       removed from object/file storage. (This is the step that identifies the no-longer-needed huge
       data-files.)

    * Nessie-GC (not Nessie "itself" like a Nessie-server or Nessie-client) needs access to the
      contents of the data lake.
    * Per-content-type implementations can leverage public APIs and code from for example Iceberg to
      read metadata/manifests/snapshots to identify actual data files. In case of Iceberg, Nessie-GC
      needs to identify the table snapshots that are no longer needed (GC-able) and after that it
      needs to

    4. Once a "per-content-type" implementation has deleted files from an object/file storage, it
       should modify the most-recent global-state and remove the information from
       metadata/manifests/snapshots/etc.
  * Nessie-GC also takes care of deleting stale named-references, if configured/enabled to do so.
  * It probably makes sense to somehow store already GC'd data (for example Iceberg-snapshots), so
    it does not need to consider an already GC'd Iceberg snapshot. Most Simple way is probably to
    check whether an Iceberg snapshot is still referenced in the current global-state.
* Use a more compact serialization format than Ion, something that has a schema.
