# Nessie Specification

This page documents the complete nessie specification. This includes:

* API and its constraints
* Contract for value objects
* requirements for backend storage implementations

## API contract

!!! warning
    todo

## Contract for Value Objects

### General Contract

All contents object must have an `id` field. This field is unique to the object and immutable once created. By convention,
it is a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) though this is not enforced by the Specification.
There are several expectations on this field:

1. They are immutable. Once created the object will keep the same `id` for its entire lifetime
1. If the object is moved (eg stored under a different `Key`) it will keep the id
1. Two objects with the same `key` (eg on different branches) will only have the same `id` if the object is the same (eg the same iceberg table or view)

There is no API to look up an object by `id` and the intention of an `id` is not to serve in that capacity. An example usage
of the `id` field might be storing auxiliary data on an object in a local cache and using `id` to look up that auxiliary data.

### Iceberg Table

An Iceberg table object is exceedingly simple. The Iceberg table object only stores the path to the metadata JSON document that
should be read to describe the Iceberg table at this version.

This model puts a strong restriction on the Iceberg table. All metadata JSON documents must be stored and none of the built-in
iceberg maintenance procedures can be used. There are potentially serious issues regarding schema migrations in this model as well.
Therefore, the Iceberg table spec should be considered subject to change in the near future.

### View

The View definition provides the minimum viable set of features that a SQL view requires. The SQL View object is defined as follows:
1. Dialect: This is the specific SQL Dialect that this View stores (eg Dremio, Trino, Hive etc). The Dialect object can also have
   engine specific settings stored in the object. As of today these containers are empty. The dialect is a marker only to identify
   the store this View _should_ be used for.
1. SQL Text: This is clearly the important part of the view. This contains the SQL expression that describes the view in the given
   dialect. **Note** Nessie does not do any verification or checks to ensure that a SQL statement is valid for a given dialect.
   The SQL text should not contain DDL, eg the statement should be a `SELECT` and can omit the `CREATE VIEW` portion.
1. SQL Context: the base from which tables should be resolved. eg which database the view belongs to
1. Schema: Optional field. This is the expected schema of the SQL view, but is not validated. It is expected that the engine will
   be able to derive the schema on its own as this field may be missing or not up to date. The format of this field is detailed below.

#### Schema object
The schema object is a limited version of the Arrow schema definition. See eg
[Arrow Data Types and Schemas](https://arrow.apache.org/docs/python/api/datatypes.html). It supports the same fields and types
as the Arrow schema and is a 1:1 translation of the Arrow objects. The reason we have created our own schema definition is to avoid
couplign arrow or another format to our client libraries. The internal Schema definition has been created such that it isn't easily
accessible outside of Nessie's `org.projectnessie` namespace in Java. This highlights that these objects are not created or modified
by users directly. Nessie provides several converter modules (see [Schema Converters](https://github.com/projectnessie/nessie/tree/main/schema-converters)).
Here translation to and from known Schemas are provided and engines should only import the appropriate module.

Note that the default on disk format for the schema object is the arrow flatbuffer format.
### Delta Lake Table

### Hive Table & Database

## Contract for backing database

!!! warning
    todo

## Version Store

!!! warning
    todo

## Tiered Version Store

!!! warning
    todo
