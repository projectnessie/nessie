# Java

## Java Client

Nessie has a thin client designed to be incorporated into existing projects with minimum 
difficulty. The client is a thin layer over Nessie's [openapi Rest APIs](rest.md).

To use the Nessie client, you can add it as a dependency to your Java project using 
Maven. The coordinates are:

```
<dependency>
  <groupId>org.projectnessie.nessie</groupId>
  <artifactId>nessie-client</artifactId>
  <version>{{ versions.nessie }}</version>
</dependency> 
```

For ease of integration with tools that carry many dependencies, the Nessie client's 
dependencies are declared as `optional`. It is designed to work with 
any recent version of JAX-RS client (Jersey and Resteasy are both tested inside Nessie's 
tests) + Jackson's DataBinding and JAX-RS modules (any version from the last ~3+ years).


## API

The `NessieClientBuilder` and concrete builder implementations (such as `HttpClientBuilder`) provide an easy way of configuring and building a `NessieApi`. The currently stable API that should be used
is `NessieApiV2`, which can be instantiated as shown below:


```java

import java.net.URI;
import java.util.List;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.model.Reference;

NessieApiV2 api = NessieClientBuilder.createClientBuilder(null, null)
  .withUri(URI.create("http://localhost:19120/api/v2"))
  .build(NessieApiV2.class);

api.getAllReferences()
  .stream()
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
ContentKey key = ContentKey.of("your-namespace", "your-table-name");
ContentKey key2 = ContentKey.of("your-namespace2", "your-table-name2");
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
ContentKey key = ContentKey.of("your-namespace", "your-table-name");
Map<ContentKey, Content> map = api.getContent().key(key).refName("dev").get();
```

Fetches the content for multiple `ContentKey` instances
```java
List<ContentKey> keys =
  Arrays.asList(
  ContentKey.of("your-namespace1", "your-table-name1"),
  ContentKey.of("your-namespace1", "your-table-name2"),
  ContentKey.of("your-namespace2", "your-table-name3"));
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
The documentation for how to configure Nessie server authentication can be found [here](../nessie-latest/authentication.md).

When configured with authentication enabled, a Nessie server expects every HTTP request to contain a 
valid Bearer token in an `Authorization` header. Two authentication providers allow a Nessie client
to automatically add the required token to the HTTP requests:

1. The `BearerAuthenticationProvider` is the simplest one and directly takes the Bearer token as a 
parameter; _the token must be valid for the entire duration of the client's lifetime_:

    ```java
    NessieApiV2 api =
      NessieClientBuilder.createClientBuilder(null, null)
      .withUri(URI.create("http://localhost:19120/api/v2"))
      .withAuthentication(BearerAuthenticationProvider.create("bearerToken"))
      .build(NessieApiV2.class);
    ```

2. The `Oauth2AuthenticationProvider` is more elaborate; at a minimum, it takes an OAuth2 token 
endpoint URI, a Client ID and a Client Secret, and uses them to obtain an access token from the 
token endpoint, which is then used as a Bearer token to authenticate against Nessie:

    ```java
    Map<String, String> authConfig =
        Map.of(
            CONF_NESSIE_AUTH_TYPE, "OAUTH2",
            CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT,
                "https://<oidc-server>/realms/<realm-name>/protocol/openid-connect/token",
            CONF_NESSIE_OAUTH2_CLIENT_ID, "my_client_id",
            CONF_NESSIE_OAUTH2_CLIENT_SECRET, "very_secret");
    NessieApiV2 api =
        NessieClientBuilder.createClientBuilder(null, null)
            .withUri(URI.create("http://localhost:19120/api/v2"))
            .withAuthenticationFromConfig(authConfig::get)
            .build(NessieApiV2.class);
    ```
    Since Nessie 0.75.1, the `Oauth2AuthenticationProvider` can also be configured programmatically;
    this can be convenient if it's necessary to supply a custom SSL context, a custom executor or 
    custom Jackson object mapper:
    ```java
    URI tokenEndpointUri = ...;
    SSLContext sslContext = ...;
    ExecutorService executor = ...;
    ObjectMapper objectMapper = ...;
    OAuth2AuthenticatorConfig authConfig =
        OAuth2AuthenticatorConfig.builder()
            .tokenEndpoint(tokenEndpointUri)
            .clientId("my_client_id")
            .clientSecret("very_secret")
            .sslContext(sslContext)
            .executor(executor) 
            .objectMapper(objectMapper)
            .build();
    NessieApiV2 api =
        NessieClientBuilder.createClientBuilder(null, null)
            .withUri(URI.create("http://localhost:19120/api/v2"))
            .withAuthentication(OAuth2AuthenticationProvider.create(authConfig))
            .build(NessieApiV2.class);
    ```
   
The main advantage of the `Oauth2AuthenticationProvider` over `BearerAuthenticationProvider` is 
that the token is automatically refreshed when it expires. It has more configuration options, 
which are documented in the [Tools Configuration](../nessie-latest/client_config.md) section.
