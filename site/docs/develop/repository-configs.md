# Nessie repository configurations

Nessie allows to retrieve and persist configuration objects via its API. Read and/or write access to all or some
types of repository configuration types can be restricted.

The Nessie server side must know the schema of each repository configuration type via an interface that extends
`org.projectnessie.model.RepositoryConfig` and its type must be registered. This ensures that all repository
configuration objects comply with the implicitly defined schema.

Creating and/or updating repository configurations is supposed to be a rare operation.

Note: Nessie repository configurations are *not* supported with the old, legacy Nessie data model.  

Nessie clients and servers need to know about the used repository config types via instances of
`org.projectnessie.model.types.RepositoryConfigTypeBundle`. Instances of this interface are loaded via
the standard Java services mechanism.

## Known and assigned repository config  types

| Repository Config Type | Model class                                      | Description                                      | Implementor    |
|------------------------|--------------------------------------------------|--------------------------------------------------|----------------|
| `GARBAGE_COLLECTOR`    | `org.projectnessie.model.GarbageCollectorConfig` | Configuration for Nessie GC.                     | Project Nessie |

Since the ID values for repository config types must be globally unique, please register your Repository config 
type via an [issue](https://github.com/projectnessie/nessie/issues/new/choose).

In case the (Java) client reads a repository configuration object for which it does not have the corresponding
`RepositoryConfigTypeBundle`, it will provide an instance of `GenericRepositoryConfig`, which provides a `Map`
representing the JSON attributes (Nessie uses Jackson for JSON (de)serialization). This means, that clients are
always able to deserialize all repository configuration types, even if the matching `RepositoryConfigTypeBundle`
is not available to the Nessie Java client. In case you are using types of repository configs that *might* not
be available to the Nessie Java client, for example if the Nessie classes are relocated, as in Apache Iceberg,
be prepared to get an instance of `GenericRepositoryConfig` instead of the "right" Java type. 

## Implementing your own content types

TBD

### Repository config type bundle

`RepositoryConfigTypeBundle`s make repository config types available to Nessie clients
and servers.

Needs a resource file `META-INF/services/org.projectnessie.model.types.RepositoryConfigTypeBundle`,
which contains the class name(s) that implement the
`org.projectnessie.model.types.RepositoryConfigTypeBundle` interface.

The `RepositoryConfigTypeBundle.register(RepositoryConfigTypeRegistry repositoryConfigTypeRegistry)`
implementation must call the given `Registrar` with name of each repository config type and the model
interface type that  extends `org.projectnessie.model.RepositoryConfig`.
