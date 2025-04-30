# Nessie Specification

The Nessie `ConfigApi` (v2) allows the caller to retrieve the server's 'specVersion', which is a semver string
defining the behaviour of the Nessie Server.

The currently defined Nessie Specification version is [2.2.0](NESSIE-SPEC-2-0.md).

# Nessie Model and API

The `model` module contains java class that define the user-visible model for data exchanging data between Nessie
clients and servers, as well as corresponding APIs.

Note: not all classes in this module are meant to be referenced by external client-side code. See the following
sections for details.

# Nessie Model

The java classes in the `org.projectnessie.model` and `org.projectnessie.error` packages are meant to be used
by java clients to communicate with Nessie Servers. These classes can be used to compile custom client-side code.

The utilities provided by the `client` module can help building Nessie clients in `java`.

# Nessie API

The java classes in the `org.projectnessie.api` packages represent the Nessie API conceptually. These classes are
used to generate the Nessie OpenAPI specification.

However, `org.projectnessie.api` classes are not expected to be used for compiling client-side code outside the scope
of this repository.

Clients built in `java` should build against the API of the `client` module.

Clients build in other languages can generate language-specific bindings from the published OpenAPI specification of
the Nessie API.

The current development version is available from a running Nessie server at http://localhost:9000/q/swagger-ui.

# Migrating from API v1 to v2

Nessie API v2 was introduced with the following goal in mind. Some of these points do not hold in API v1.

* Adhering to [RESTful API guidelines](https://restfulapi.net/).
  * Specifically, using hyphens (`-`) to separate words.
* Providing pagination for potentially large responses.
* Maintaining the same functionality that was available in API v1 where possible.
  * One notable exception is that the `namepaceDepth` parameter of the "get entries" endpoint is not available in v2.
    The main reason for dropping it was to allow proper pagination of the response stream.
  * While functionally v2 is mostly equivalent to v1, most of the REST paths and parameter names are different.
* Maintaining source compatibility with older java clients.
  * Java clients compiled against v1 can be recompiled against v2 by changing only the API qualifier class from 
    `NessieApiV1` to `NessieApiV2`.
  * Some functionality that used to be imlemented on the server side in v1 is implemented on the java client side in v2
    (e.g. helper methods for `Namespace` manipulation).
* Providing ways to access future server-side functionality improvements (e.g. listing entries by a key range). 

## Notable changes between API v2 and v1

The following REST endpoint were removed:
* `/namespaces`
* `/reflogs`

`Namespace` objects can be manipulated via the "commit" endpoint or via helper methods that are still available in
the java client.

Reflog has been deprecated in v1 and is not supported at all in v2.

New URLs for maintained endpoints:
* Config: `/config`
* Commit log: `/trees/{ref}/history`
  * Create new commit: `/trees/{ref}/history/commit`
  * Merge: `/trees/{ref}/history/merge`
  * Transplant: `/trees/{ref}/history/transplant`
* Get content: `/trees/{ref}/contents`
* List keys: `/trees/{ref}/entries`
* Diff: `/trees/{ref-from}/diff/{ref-to}`

The `{ref}` path element in v2 is a rich reference to a particular version of the content tree. It is not limited to
just a branch or tag name, but can include a commit hash,
e.g. `main@2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d`. Please refer to the Nessie OpenAPI 
specification for the complete description of how and where the `{ref}` parameter may be used.

The default branch information can be retrieved at `/trees/-`.
