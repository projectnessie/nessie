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

All contents object must have an `id` field. This field is unique to the object and immutable once created. By convention
it is a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) though this is not enforced by the Specification.
There are several expectations on this field:

1. They are immutable. Once created the object will keep the same `id` for its entire lifetime
1. If the object is moved (eg stored under a different `Key`) it will keep the id
1. Two objects with the same `key` (eg on different branches) will only have the same `id` if the object is the same (eg the same iceberg table or view)

There is no API to look up an object by `id` and the intention of an `id` is not to serve in that capacity. An example usage
of the `id` field might be storing auxiliary data on an object in a local cache and using `id` to look up that auxiliary data.

### Iceberg Table

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
