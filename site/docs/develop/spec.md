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

#### Content Id

All contents object must have an `id` field. This field is unique to the object and immutable once created. By convention,
it is a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) though this is not enforced by the Specification.
There are several expectations on this field:

1. They are immutable. Once created the object will keep the same `id` for its entire lifetime
1. If the object is moved (eg stored under a different `Key`) it will keep the id
1. Two objects with the same `key` (eg on different branches) will only have the same `id` if the object is the same (eg the same iceberg table or view)

There is no API to look up an object by `id` and the intention of an `id` is not to serve in that capacity. An example usage
of the `id` field might be storing auxiliary data on an object in a local cache and using `id` to look up that auxiliary data.

#### PermanentId

All contents object must have a `permanentId` field. This field is set at commit time and is unique for the commit. All objects in
the commit will have the same `permanentId`. This field would not be updated on merges or transplants therefore the id is unique for
that commit permanently. By convention it is a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) though
this is not enforced by the Specification. There are several expectations on this field:

1. `permanentId`  will always be updated at commit time.
1. `permanentId` will not be updated during any transplant or merge operations.
1. All objects in the same original commit will have the same `permanentId`
1. `permanentId` is *not* related or derived from the commit-hash.

There is no API to look up an object by `permanentId` and the intention of an `permanentId` is not to serve in that capacity.
An example usage of the `permanentId` field is storing auxiliary data on an object in a local cache and using
`permanentId` to look up that auxiliary data.

!!! note
    A note about caching. The `Contents` objects are likely to be cached locally by services using Nessie. There is also likely
    to be auxiliary data (eg schema, partitions etc) stored by services which refer to a specific `Contents` at a specific
    commit or time. The tuple `(id, permanentId)` uniquely reference an object in the Nessie history and is a suitable key
    to identify an object at a particular point in its history.

    The commit hash makes a poor cache key as the commit hash could be garbage collected or the object could be merged/transplanted
    to a different branch resulting in a different commit hash. **Do not** use the commit hash for any other purpose than as
     a consistency check in the API.

    The object `id` alone is a good cache key for data that isn't tied to a particular commit such as ACLs. This will always match
    the same contents regardless of what `Key` it is stored at or what branch it is on.

### Iceberg Table

An Iceberg table object is exceedingly simple. The Iceberg table object only stores the path to the metadata JSON document that
should be read to describe the Iceberg table at this version.

This model puts a strong restriction on the Iceberg table. All metadata JSON documents must be stored and none of the built-in
iceberg maintenance procedures can be used. There are potentially serious issues regarding schema migrations in this model as well.
Therefore, the Iceberg table spec should be considered subject to change in the near future.

### View

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
