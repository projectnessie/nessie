# Nessie Specification

This page documents the complete Nessie specification. This includes:

* API and its constraints
* Contract for value objects

## API contract

The Nessie API is used by Nessie integrations within for example Apache Iceberg and
user facing applications like Web UIs.

Nessie defines a [REST API](rest.md) (OpenAPI) and implementations for [Java](java.md)
and [Python](python.md).

## Content managed by Nessie

### General Contract

_Content Objects_ describe the state of a data lake object like a table or view.
Nessie currently provides types for Iceberg tables views. Nessie uses
two identifiers for a single Content object:

1. The [Content Id](#content-id) is used to identify a content object across all branches even
   if the content object is being referred to using different table or view names.
2. The [Content Key](#content-key) is used to look up a content object by name, like a table name
   or view name. The Content Key changes when the associated table or view is renamed.

#### Content Key

The Content Key consists of multiple strings and is used to resolve a symbolic name, like a table
name or a view name used in SQL statements, to a Content object.

When a table or view is renamed using for example an SQL `ALTER TABLE RENAME` operation, Nessie
will record this operation using a _remove_ operation on the old key plus a _put_ operation on the
new key ([see below](#operations-in-a-nessie-commit)).

### On Reference State vs Global State

Nessie is designed to support multiple table formats like Apache Iceberg. 
Since different Nessie commits, think: on different branches in Nessie, can refer to the
same physical table but with different state of the data and potentially different schema, some
table formats require Nessie to refer to a single _Global State_.

IDs of the _Iceberg snapshot_, _Iceberg schema_, _Iceberg partition spec_, _Iceberg sort order_ 
within the Iceberg _table metadata_ are also stored per Nessie named reference (branch or tag),
as the so-called _on-reference-state_.

!!! note
    The term _all information in all Nessie commits_ used above precisely means all information
    in all Nessie commits _that are considered "live"_, have not been garbage-collected by Nessie.
    See also [Management Services](../guides/management.md).

#### Content Id

All contents object must have an `id` field. This field is unique to the object and immutable once
created. By convention, it is a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier)
though this is not enforced by this Specification. There are several expectations on this field:

1. Content Ids are immutable. Once created the object will keep the same `id` for its entire
   lifetime.
2. If the object is moved (e.g. stored under a different `Key`) it will keep the id.
3. The same content object, i.e. the same content-id, can be referred to using different keys
   on different branches.

There is no API to look up an object by `id` and the intention of an `id` is not to serve in that
capacity. An example usage of the `id` field might be storing auxiliary data on an object in a
local cache and using `id` to look up that auxiliary data.

!!! note
    A note about caching: The `Content` objects or the values of the referred information
    (e.g. schema, partitions etc.) might be cached locally by services using Nessie.

    For content types that do **not** track [Global State](#on-reference-state-vs-global-state),
    the hash of the contents object does uniquely reference an object in the Nessie
    history and is a suitable key to identify an object at a particular point in its history.

    Evolution of the _Global State_ is performed in a way that keeps old contents resp. contents
    on different branches (and tags) available. This is the case for Apache Iceberg.

    Content types that do track [Global State](#on-reference-state-vs-global-state),
    the Content Id must be included in the cache key.

    For simplicity, it is recommeded to always include the Content Id.

    Since the Content object is immutable, the hash is stable and since it is disconnected from
    Nessie's version store properties it exists across commits/branches and survives GC and other
    table maintenance operations.

    The commit hash on the other hand makes a poor cache key because multiple commits can refer
    to the same state of a Content object, e.g. a merge or transplant will change the commit hash
    but not the state of the Content object.

### Content Types

Nessie is designed to support various table formats, and currently supports the following types.
See also [Tables & Views](../guides/tables-views.md).

#### Iceberg Table

Apache Iceberg describes any table using the so called _table metadata_, see
[Iceberg Table Spec](https://iceberg.apache.org/spec/). Each Iceberg operation that modifies data,
for example an _append_ or _rewrite_ operation or more generally each Iceberg transaction, creates
a new Iceberg snapshot. Any Nessie commit refers to a particular Iceberg snapshot for an Iceberg
table, which translates to the state of an Iceberg table for a particular Nessie commit.

The Nessie `IcebergTable` object passed to Nessie in a [_Put operation_](#put-operation) therefore
consists of

1. the pointer to the Iceberg _table metadata_ and
2. the IDs of the _Iceberg snapshot_, _Iceberg schema_, _Iceberg partition spec_, _Iceberg sort order_ 
within the Iceberg _table metadata_.  (so-called _On Reference State_)

!!! note
    This model puts a strong restriction on the Iceberg table. All metadata JSON documents must be
    stored and none of the built-in iceberg maintenance procedures can be used. There are
    potentially serious issues regarding schema migrations in this model as well. Therefore, the
    Iceberg table spec should be considered subject to change in the near future.

#### Iceberg View

!!! note
    Iceberg Views are experimental and subject to change!

The state of an Iceberg view is represented using the attributes `versionId`, `schemaId`, `sqlText`
and `dialect`.

Iceberg views are handled similar to [Iceberg Tables](#iceberg-table).

## Operations in a Nessie commit

Each Nessie commit carries one or more operations. Each operation contains the
[Content Key](#content-key) and is either a [Put](#put-operation), [Delete](#delete-operation) or
[Unmodified](#unmodified-operation) operation.

A [Content Key](#content-key) must only occur once in a Nessie commit.

Operations present in a commit are passed into Nessie as a list of operations.

### Mapping SQL DDL to Nessie commit operations

A `CREATE TABLE` is mapped to one _Put operation_.

An `ALTER TABLE RENAME` is mapped to a _Delete operation_ using the Content Key for the table
being renamed plus at least one _Put operation_ using the Content Key of the table's new name,
using the Content Id of the table being renamed.

A `DROP TABLE` is represented as a Nessie _Delete operation_ (without a _Put operation_ for the
same Content Id).

A `DROP TABLE` + `CREATE TABLE` using the same table name (Content Key) in a single commit are
mapped to one _Put operation_ with a different Content Id.

### Put operation

A _Put operation_ modifies the state of the included Content object. It must contain the Content
object and, if the _Put operation_ modifies an existing content object, also the
the _expected contents_. The _expected contents_ attribute can be omitted, if the Content object
refers to a new Content Id, e.g. a newly created table or view. See also
[Conflict Resolution](#conflict-resolution).

A Nessie _Put operation_ is created for everything that modifies a table or a view, either its
definition (think: SQL DDL) or data (think: SQL DML).

### Delete operation

A _Delete operation_ does not carry any Content object and is used to indicate that a Content
object is no longer referenced using the Content Key of the _Delete operation_.

### Unmodified operation

An _Unmodified operation_ does not represent any change of the data, but can be included in a
Nessie commit operation to enforce strict serializable transactions. The presence of an
_Unmodified operation_ means that the Content object referred to via the operation's Content Key
must not have been modified since the Nessie commit's `expectedHash`.

The _Unmodified operation_ is not persisted.

## Version Store

See [Commit Kernel](kernel.md) for details.

### Conflict Resolution

The API passes an `expectedHash` parameter with a Nessie commit operation. This is the commit that
the client thinks is the most up to date (its HEAD). The Nessie backend will check to see if the
key has been modified since that `expectedHash` and if so, it will reject the requested modification
with a `NessieConflictException`. This is basically an optimistic lock that accounts for the fact
that the commit hash is global and nessie branch could have moved on from `expectedHash` without
modifying the key in question.

A Nessie [_Put operation_](#put-operation) that updates an existing content object must
pass the so-called _expected state_, which might be used to compare the current recorded state
of a content object with the state in the _expected state_ in the _Put operation_. If both
values differ, Nessie will reject the operation with a `NessieConflictException`.

The reason for these conditions is to behave like a ‘real’ database. You shouldn’t have to update
your reference before transacting on table `A` because it just happened to update table `B` whilst
you were preparing your transaction.
