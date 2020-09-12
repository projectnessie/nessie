# Java Client Development

There are currently three options for using Nessie from Java:

1. `NessieClient` direct interaction with the REST API
2. Through Iceberg. Either the `NessieCatalog`, which follows the [Catalog](http://iceberg.apache.org/custom-catalog/)
   interface from Iceberg. Or via the Iceberg spark writer
3. Through Delta Lake. Nessie is used as a custom [LogStore](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/storage/LogStore.scala)

!!! note
    Typically interactions with Nessie will happen directly via the Iceberg or Delta Lake clients rather than using the
    direct API.

## Direct Java API

The `NessieClient` object wraps a Jersey Client and exposes interactions with the Nessie rest api. To use it simply

```java
client = NessieClient.basic(path, username, password);
Iterable<String> branches = client.getBranches();
StreamSupport.stream(branches.spliterator(), false)
             .map(Branch::getName)
             .forEach(System.out::println);
```

The client API has the full set of methods required to interact with Nessie at this level. The above example
authenticates with basic auth using a username and password. If using the AWS client and authenticating with IAM roles
use `NessieClient.aws(path)` to instantiate a client. The `path` argument is the full url for the nessie endpoint (eg
http://localhost:19120/api/v1).

!!! note
    TODO the exact functions and their definitions will be added here when the API is more stable

## Iceberg client

see [Iceberg](iceberg.md)

## Delta Lake client

see [Delta Lake](delta.md)
