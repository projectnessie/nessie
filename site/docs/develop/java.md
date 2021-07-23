# Java

## Java Client

Nessie has a thin client designed to be incorporated into existing projects with minimum 
difficulty. The client is a thin layer over Nessie's [openapi Rest APIs](rest.md).

To use the Nessie client, you can add it as a dependency to your Java project using 
Maven. The coordinates are:

```
<dependency>
  <groupId>org.projectnessie</groupId>
  <artifactId>nessie-client</artifactId>
  <version>{{ versions.java }}</version>
</dependency> 
```

For ease of integration with tools that carry many dependencies, the Nessie client's 
dependencies are declared optionaly. It is designed to work with 
any recent version JAX-RS client (Jersey and Resteasy are both tested inside Nessie's 
tests) + Jackson's DataBinding and JAX-RS modules (any version from the last ~3+ years).


### API

The `NessieClient` object wraps a Jersey Client and exposes interactions with the Nessie Rest API. To use it simply

```java
client = NessieClient.basic(path, username, password);
List<Reference> references = client.getTreeApi().getAllReferences();
references.stream()
  .map(Reference::getName)
  .forEach(System.out::println);
```

The client API has the full set of methods required to interact with Nessie at this level. The above example
authenticates with basic auth using a username and password. If using the AWS client and authenticating with IAM roles
use `NessieClient.aws(path)` to instantiate a client. The `path` argument is the full url for the nessie endpoint (eg
http://localhost:19120/api/v1).

## Server
The Nessie server is based on the Quarkus microkernel appserver and can run in a traditional 
JVM or be precompiled into a native image via GraalVM. We develop all code using JDK11 and then compile for release on JDK8.

### Rest API 
The Rest API is composed primarily of the [Contents](https://github.com/projectnessie/nessie/blob/main/model/src/main/java/org/projectnessie/api/ContentsApi.java), [Tree](https://github.com/projectnessie/nessie/blob/main/model/src/main/java/org/projectnessie/api/TreeApi.java) and [Config](https://github.com/projectnessie/nessie/blob/main/model/src/main/java/org/projectnessie/api/ConfigApi.java) APIs.

### Versioning Kernel
Deeper in the server, the core commit kernel is built on top of the [VersionStore SPI](https://github.com/projectnessie/nessie/blob/main/versioned/spi/src/main/java/org/projectnessie/versioned/VersionStore.java)  

### New Backing Store

Nessie comes pre-packaged with three storage engines: RocksDB, In-Memory and DynamoDB. 
For production deployments, DynamoDB is the recommended storage engine. The Nessie 
team is always evaluating new potential backing stores. If you're looking to implement 
something new, shout out to us via one of our [communication channels](index.md) to 
discuss what you're thinking so we can figure out the best way to move forward. 
