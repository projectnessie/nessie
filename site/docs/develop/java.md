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
dependencies are declared as `optional`. It is designed to work with 
any recent version of JAX-RS client (Jersey and Resteasy are both tested inside Nessie's 
tests) + Jackson's DataBinding and JAX-RS modules (any version from the last ~3+ years).


## API

The `NessieClientBuilder` and concrete builder implementations (such as `HttpClientBuilder`) provide an easy way of configuring and building a `NessieApi`. The currently stable API that should be used
is `NessieApiV1`, which can be instantiated as shown below:


```java

import java.net.URI;
import java.util.List;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.model.Reference;

NessieApiV1 api = HttpClientBuilder.builder()
  .withUri(URI.create("http://localhost:19121/api/v1"))
  .build(NessieApiV1.class);

List<Reference> references = api.getAllReferences().get();
references.stream()
  .map(Reference::getName)
  .forEach(System.out::println);
```

The following subsections will outline how different actions can be done via that Nessie API.

### Fetching details about a particular Reference

Fetches the `Reference` object of the `main` branch and then gets its hash
```java
api.getReference().refName("main").get().getHash();
```

### Creating a Reference

Creates a new branch `dev` that points to the `main` branch
```java
Reference main = api.getReference().refName("main").get();
Reference branch =
    api.createReference()
        .sourceRefName(main.getName())
        .reference(Branch.of("dev", main.getHash()))
        .create();
```

Creates a new tag `dev-tag` that points to the `main` branch
```java
Reference main = api.getReference().refName("main").get();
Reference tag =
    api.createReference()
        .sourceRefName(main.getName())
        .reference(Tag.of("dev-tag", main.getHash()))
        .create();
```

### Assigning a Reference

Assigns a previously created `devBranch2` to the `dev` branch
```java
Reference dev = api.getReference().refName("dev").get();
api.assignBranch()
    .branchName("devBranch2")
    .hash(dev.getHash())
    .assignTo(dev)
    .assign();
```

Assigns a previously created `dev-tag` to the `dev` branch
```java
Reference dev = api.getReference().refName("dev").get();
api.assignTag()
    .tagName("dev-tag")
    .hash(dev.getHash())
    .assignTo(dev)
    .assign();
```

### Deleting a Reference

Deletes a previously created branch
```java
api.deleteBranch()
    .branchName(dev.getName())
    .hash(dev.getHash())
    .delete();
```

Deletes a previously created tag
```java
api.deleteTag()
    .tagName(devTag.getName())
    .hash(devTag.getHash())
    .delete();
```


### Fetching the Server Configuration

```java
NessieConfiguration config = api.getConfig();
config.getDefaultBranch();
config.getVersion();
```

### Committing

Creates a new commit by adding metadata for an `IcebergTable` under the specified `ContentKey` instance represented by `key` and deletes content represented by `key2`

```java
ContentKey key = ContentKey.of("table.name.space", "name");
ContentKey key2 = ContentKey.of("other.name.space", "name2");
IcebergTable icebergTable = IcebergTable.of("path1", 42L);
api.commitMultipleOperations()
    .branchName(branch)
    .hash(main.getHash())
    .operation(Put.of(key, icebergTable))
    .operation(Delete.of(key2))
    .commitMeta(CommitMeta.fromMessage("commit 1"))
    .commit();
```

### Fetching Content

Fetches the content for a single `ContentKey`
```java
ContentKey key = ContentKey.of("table.name.space", "name");
Map<ContentKey, Content> map = api.getContent().key(key).refName("dev").get();
```

Fetches the content for multiple `ContentKey` instances
```java
List<ContentKey> keys =
  Arrays.asList(
  ContentKey.of("table.name.space", "name1"),
  ContentKey.of("table.name.space", "name2"),
  ContentKey.of("table.name.space", "name3"));
Map<ContentKey, Content> allContent = api.getContent().keys(keys).refName("dev").get();
```


### Fetching the Commit Log

Fetches the commit log for the `dev` reference
```java
LogResponse log = api.getCommitLog().refName("dev").get();
```

### Fetching Entries

Fetches the entries for the `dev` reference
```java
EntriesResponse entries = api.getEntries().refName("dev").get();
```

### Merging

This merges `fromBranch` into the given `intoBranch`
```java
api.mergeRefIntoBranch()
  .branchName("intoBranch")
  .hash(intoBranchHash)
  .fromRefName("fromBranch")
  .fromHash(fromHash)
  .merge();
```

### Transplanting

Transplant/cherry-pick a bunch of commits from `main` into the `dev` branch
```java
Branch dev = ...
api.transplantCommitsIntoBranch()
    .branchName(dev.getName())
    .hash(dev.getHash())
    .fromRefName("main")
    .hashesToTransplant(Collections.singletonList(api.getReference().refName("main").get().getHash()))
    .transplant()
```


## Authentication

Nessie has multiple `NessieAuthenticationProvider` implementations that allow different client authentication mechanisms as can be seen below.
The documentation for how to configure Nessie server authentication can be found [here](../try/authentication.md).

The `BasicAuthenticationProvider` allows connecting to a Nessie server that has `BASIC` authentication enabled.
Note that `BASIC` is not supported in production and should only be used for development/testing.
```java
NessieApiV1 api =
  HttpClientBuilder.builder()
  .withUri(URI.create("http://localhost:19121/api/v1"))
  .withAuthentication(BasicAuthenticationProvider.create("my_username", "very_secret"))
  .build(NessieApiV1.class);
```

The `BearerAuthenticationProvider` allows connecting to a Nessie server that has `BEARER` authentication enabled.
```java
NessieApiV1 api =
  HttpClientBuilder.builder()
  .withUri(URI.create("http://localhost:19121/api/v1"))
  .withAuthentication(BearerAuthenticationProvider.create("bearerToken"))
  .build(NessieApiV1.class);
```
