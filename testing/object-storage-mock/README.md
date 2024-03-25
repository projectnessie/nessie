# Nessie Object Storage Mock

This project provides an object storage mock implementing a rudimentary subset of functionalities needed to
write/read/list objects via S3, ADLS Gen2, GCS/HTTP.

**DISCLAIMER** Having your code work against this object storage does **not** mean that it will work against
real production S3/ADLS/GCS instance!! **Make sure to test your code properly!!***

The original intent of this project was to _simulate_ S3 object store having thousands to millions of objects,
implemented by generating listings and objects on the fly. Nowadays this project can also simulate an object
store by keeping objects on the Java heap. If you need more, then use "real implementations", like MinIO or the GCS
emulator. Presently, there is still no working ADLS Gen2 support in Azurite, so no official emulator, which is not
nice for a commercial product.

Since all activity on buckets and objects is routed via configurable instances of
the `org.projectnessie.objectstoragemock.Bucket` interface, it can be used to mock object store accesses.

Although the code does its job, be aware of completely or mostly unimplemented functionality:

* Security and access control
* Resumable uploads beyond tiny objects
* Precondition checks
* Proper error handling and HTTP result (code) matching

## Hint when you use Azure clients

Be aware that Azure clients _cannot_ be closed - there is just no `.close()` or `.dispose()` method. This means that
HTTP connection pools, threads and other resources will effectively never be released.

## Example: basic setup

Example:
```java
void demo() {
  try (MockServer server = ObjectStorageMock.builder()
    .putBuckets("mybucket", Bucket.builder()
      .lister(customObjectLister)
      .updater(customObjectUpdater)
      .deleter(customObjectDeleter)
      .object(customObjectRetriever)
      .build())
    .build()
    .start()) {

    System.out.println("S3 endpoint:   " + server.getS3BaseUri());
    System.out.println("GCS endpoint:  " + server.getGcsBaseUri());
    System.out.println("ADLS endpoint: " + server.getAdlsGen2BaseUri());
  }
}
```

## Example: basic S3 setup

Example:
```java
void demo() {
  try (MockServer server = ObjectStorageMock.builder()
    // configure the bucket, see above
    .build()
    .start()) {


    try (S3Client s3 = S3Client.builder()
      .httpClientBuilder(UrlConnectionHttpClient.builder())
      .applyMutation(builder -> builder.endpointOverride(server.getS3BaseUri()))
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("accessKey", "secretKey")))
      .build()) {
    
        // Use the S3Client ...
    }
  }
}
```

## Example: basic GCS setup

Example:
```java
void demo() {
  try (MockServer server = ObjectStorageMock.builder()
    // configure the bucket, see above
    .build()
    .start()) {


    try (Storage service = StorageOptions.http()
      .setProjectId("project-id")
      .setHost(serverInstance.getGcsBaseUri().toString())
      .setCredentials(NoCredentials.getInstance())
      .build()
      .getService()) {
   
        // Use the GCS Storage service ...
    }
  }
}
```

## Example: basic ADLS setup

Example:
```java

static HttpClient sharedHttpClient;

static createSharedhttpClient() {
  // Use a single, shared HttpClient to reduce the risk of leaking system resource, threads, memory, CPU time.
  // Yes, there is an environment variable AZURE_ENABLE_HTTP_CLIENT_SHARING that you can set to "true", but
  // that does not work with custom configurations... Another design bug in the Azure client.
  sharedHttpClient =
    HttpClient.createDefault(
      new HttpClientOptions()
        .setConfiguration(
          new ConfigurationBuilder()
              // configure what's needed
              .build()));
}

void demo() {
  try (MockServer server = ObjectStorageMock.builder()
    // configure the bucket, see above
    .build()
    .start()) {


    DataLakeFileSystemClient client = new DataLakeFileSystemClientBuilder()
      .httpClient(sharedHttpClient)
      // for posterity...
      .configuration(new ConfigurationBuilder().build())
      .endpoint(serverInstance.getAdlsGen2BaseUri().toString())
      .credential(new StorageSharedKeyCredential("accessName", "accountKey"))
      .fileSystemName(BUCKET)
      .build();
   
    // Use the ADLS Gen2 Storage service ...
  }
}
```
