# Nessie content types

Nessie comes with built-in content types. Every content-type relates to a corresponding
content-type ID (Java enum like string), a payload ID and an implementation of the
`org.projectnessie.model.Content` interface.

Nessie clients and servers need to know about the used content types via instances of
`org.projectnessie.model.types.ContentTypeBundle`. Instances of this interface are loaded via
the standard Java services mechanism.

Nessie servers, and tools that directly access the Nessie repository, need the `ContentTypeBundle`
and instances of `org.projectnessie.versioned.store.ContentSerializerBundle`, also provided via
the Java services mechanism, that provide serialization code via instances of
`org.projectnessie.versioned.store.ContentSerializer`.

## Known and assigned content types

| Payload ID | Content Type       | Model class                              | Description                                      | Implementor    |
|------------|--------------------|------------------------------------------|--------------------------------------------------|----------------|
| 0          | n/a                | n/a                                      | Legacy fallback value if the payload is unknown. | n/a            |
| 1          | `ICEBERG_TABLE`    | `org.projectnessie.model.IcebergTable`   | Iceberg tables.                                  | Project Nessie |
| 2          | `DELTA_LAKE_TABLE` | `org.projectnessie.model.DeltaLakeTable` | Delta Lake tables.                               | Project Nessie |
| 3          | `ICEBERG_VIEW`     | `org.projectnessie.model.IcebergView`    | Iceberg views.                                   | Project Nessie |
| 4          | `NAMESPACE`        | `org.projectnessie.model.Namespace`      | Namespaces.                                      | Project Nessie |

Since the ID values for payloads and the namespace for content types must be globally unique,
please register your Payload ID and Content Type via an
[issue](https://github.com/projectnessie/nessie/issues/new/choose).

## Implementing your own content types

TBD

### Content type bundle

`ContentTypeBundle`s make content types available to Nessie clients and servers.

Needs a resource file `META-INF/services/org.projectnessie.model.types.ContentTypeBundle`,
which contains the class name(s) that implement the
`org.projectnessie.model.types.ContentTypeBundle` interface.

The `ContentTypeBundle.register(ContentTypeRegistry contentTypeRegistry)` implementation
must call the given `Registrar` with name of each content type and the model interface type that
extends `org.projectnessie.model.Content`.

### Serializer bundle

`ContentSerializer`s provide (de)serialization functionality using a space efficient binary
representation for Nessie servers.

Needs a resource file `META-INF/services/org.projectnessie.versioned.store.ContentSerializerBundle`,
which contains the class name(s) that implement the
`org.projectnessie.versioned.store.ContentSerializerBundle` interface.

The `ContentSerializerBundle.register(ContentSerializerRegistry registry)` implementation
must call the given `ContentTypeRegistry` with the `ContentSerializer` implementation for each
content type serializer.

### Distributing content type and serializer bundles

TBD

* One jar for Nessie clients
* One jar for Nessie servers/tools

Open questions:

* Dependencies??
