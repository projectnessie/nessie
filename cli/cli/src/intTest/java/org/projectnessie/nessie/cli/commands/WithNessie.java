/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.nessie.cli.commands;

import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.generateMetadataWithManifestList;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;
import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;
import static org.projectnessie.client.config.NessieClientConfigSources.mapConfigSource;
import static org.projectnessie.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3Clients;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.catalog.files.s3.S3Sessions;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.http.SdkHttpClient;

public abstract class WithNessie {

  public static final String S3_BUCKET = "s3://bucket/";
  public static final String GCS_BUCKET = "gs://bucket/";
  public static final String ADLS_BUCKET = "abfs://bucket/";
  public static final String S3_WAREHOUSE = "warehouse";
  public static final String GCS_WAREHOUSE = "gcs_warehouse";
  public static final String ADLS_WAREHOUSE = "adls_warehouse";
  static String nessieApiUri;
  static String icebergUri;

  static ObjectStorageMock.MockServer objectStorage;
  static HeapStorageBucket bucket;

  static Branch defaultBranch;
  static Branch testBranch;

  static String tableOneBase;
  static String tableTwoBase;

  static ContentKey tableOneKey = ContentKey.of("tables", "one");
  static ContentKey tableTwoKey = ContentKey.of("tables", "two");
  static String tableOneId;
  static String tableTwoId;
  static IcebergTableMetadata tableOneMetadata;
  static IcebergTableMetadata tableTwoMetadata;
  static String tableOneMetadataLocation;
  static String tableTwoMetadataLocation;

  protected static void setupObjectStoreAndNessie(
      Map<String, String> serverConfig, Map<String, String> clientConfig) throws Exception {
    bucket = newHeapStorageBucket();
    objectStorage =
        ObjectStorageMock.builder().putBuckets("bucket", bucket.bucket()).build().start();

    Map<String, String> nessieProperties = new HashMap<>();
    nessieProperties.put("nessie.catalog.default-warehouse", S3_WAREHOUSE);
    nessieProperties.put("nessie.catalog.warehouses." + S3_WAREHOUSE + ".location", S3_BUCKET);
    nessieProperties.put("nessie.catalog.warehouses." + GCS_WAREHOUSE + ".location", GCS_BUCKET);
    nessieProperties.put("nessie.catalog.warehouses." + ADLS_WAREHOUSE + ".location", ADLS_BUCKET);

    nessieProperties.put(
        "nessie.catalog.service.s3.default-options.endpoint",
        objectStorage.getS3BaseUri().toString());
    nessieProperties.put("nessie.catalog.service.s3.default-options.path-style-access", "true");
    nessieProperties.put("nessie.catalog.service.s3.default-options.region", "eu-central-1");
    nessieProperties.put(
        "nessie.catalog.service.s3.default-options.access-key",
        "urn:nessie-secret:quarkus:with-nessie-access-key");
    nessieProperties.put("with-nessie-access-key.name", "accessKey");
    nessieProperties.put("with-nessie-access-key.secret", "secretKey");

    nessieProperties.put(
        "nessie.catalog.service.gcs.default-options.host",
        objectStorage.getGcsBaseUri().toString());
    nessieProperties.put("nessie.catalog.service.gcs.default-options.project-id", "nessie");
    nessieProperties.put("nessie.catalog.service.gcs.default-options.auth-type", "access_token");
    nessieProperties.put(
        "nessie.catalog.service.gcs.default-options.oauth2-token",
        "urn:nessie-secret:quarkus:my-secrets.gcs-default");
    nessieProperties.put("my-secrets.gcs-default.token", "tokenRef");

    nessieProperties.put(
        "nessie.catalog.service.adls.default-options.endpoint",
        objectStorage.getAdlsGen2BaseUri().toString());
    nessieProperties.put("nessie.catalog.service.adls.default-options.auth-type", "none");
    nessieProperties.put(
        "nessie.catalog.service.adls.default-options.account",
        "urn:nessie-secret:quarkus:my-secrets.adls-default");
    nessieProperties.put("my-secrets.adls-default.name", "account");
    nessieProperties.put("my-secrets.adls-default.secret", "secret");

    nessieProperties.putAll(serverConfig);

    NessieProcess.start(nessieProperties);

    tableOneBase = S3_BUCKET + "/" + UUID.randomUUID() + "/";
    tableTwoBase = S3_BUCKET + "/" + UUID.randomUUID() + "/";

    nessieApiUri = NessieProcess.baseUri + "api/v2";
    icebergUri = NessieProcess.baseUri + "iceberg";

    String s3accessKeyName = "s3-access-key";
    SecretsProvider secretsProvider =
        ResolvingSecretsProvider.builder()
            .putSecretsManager(
                "plain",
                unsafePlainTextSecretsProvider(
                    Map.of(s3accessKeyName, basicCredentials("foo", "bar").asMap())))
            .build();

    S3Config s3config = S3Config.builder().build();
    SdkHttpClient httpClient = S3Clients.apacheHttpClient(s3config, secretsProvider);

    S3Options s3options =
        ImmutableS3Options.builder()
            .defaultOptions(
                ImmutableS3NamedBucketOptions.builder()
                    .accessKey(URI.create("urn:nessie-secret:plain:" + s3accessKeyName))
                    .region("eu-central-1")
                    .endpoint(objectStorage.getS3BaseUri())
                    .pathStyleAccess(true)
                    .build())
            .build();

    S3Sessions sessions = new S3Sessions("foo", null);

    S3ClientSupplier clientSupplier =
        new S3ClientSupplier(httpClient, s3options, sessions, secretsProvider);

    ObjectIO objectIO = new S3ObjectIO(clientSupplier, null);

    tableOneMetadataLocation =
        generateMetadataWithManifestList(
            tableOneBase, objectWriter(objectIO, tableOneBase), m -> tableOneMetadata = m);
    tableTwoMetadataLocation =
        generateMetadataWithManifestList(
            tableTwoBase, objectWriter(objectIO, tableTwoBase), m -> tableTwoMetadata = m);

    try (NessieApiV2 api =
        createClientBuilderFromSystemSettings(mapConfigSource(clientConfig))
            .withUri(nessieApiUri)
            .build(NessieApiV2.class)) {
      defaultBranch = api.getDefaultBranch();

      Namespace namespace = tableOneKey.getNamespace();

      defaultBranch =
          api.commitMultipleOperations()
              .branch(defaultBranch)
              .operation(Operation.Put.of(namespace.toContentKey(), namespace))
              .commitMeta(CommitMeta.fromMessage("namespace"))
              .commit();

      testBranch = Branch.of("testBranch", defaultBranch.getHash());
      api.createReference().reference(testBranch).sourceRefName(defaultBranch.getName()).create();

      CommitResponse committed =
          api.commitMultipleOperations()
              .branch(defaultBranch)
              .operation(
                  Operation.Put.of(
                      tableOneKey,
                      IcebergTable.of(
                          tableOneMetadataLocation,
                          tableOneMetadata.currentSnapshotIdAsLong(),
                          tableOneMetadata.currentSchemaId(),
                          tableOneMetadata.defaultSpecId(),
                          tableOneMetadata.defaultSortOrderId())))
              .commitMeta(CommitMeta.fromMessage("table one"))
              .commitWithResponse();
      defaultBranch = committed.getTargetBranch();
      tableOneId = committed.getAddedContents().get(0).contentId();

      committed =
          api.commitMultipleOperations()
              .branch(testBranch)
              .operation(
                  Operation.Put.of(
                      tableTwoKey,
                      IcebergTable.of(
                          tableTwoMetadataLocation,
                          tableTwoMetadata.currentSnapshotIdAsLong(),
                          tableTwoMetadata.currentSchemaId(),
                          tableTwoMetadata.defaultSpecId(),
                          tableTwoMetadata.defaultSortOrderId())))
              .commitMeta(CommitMeta.fromMessage("table two"))
              .commitWithResponse();
      testBranch = committed.getTargetBranch();
      tableTwoId = committed.getAddedContents().get(0).contentId();
    }
  }

  @AfterAll
  public static void stop() throws Exception {
    objectStorage.close();
    NessieProcess.stop();
  }

  protected static IcebergGenerateFixtures.ObjectWriter objectWriter(
      ObjectIO objectIO, String currentBase) {
    return (name, data) -> {
      StorageUri location =
          name.isAbsolute()
              ? StorageUri.of(name)
              : StorageUri.of(currentBase + "/" + name.getPath());
      try (OutputStream output = objectIO.writeObject(location)) {
        output.write(data);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return location.toString();
    };
  }
}
