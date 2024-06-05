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
import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;
import static org.projectnessie.client.config.NessieClientConfigSources.mapConfigSource;
import static org.projectnessie.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3Clients;
import org.projectnessie.catalog.files.s3.S3Config;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.files.s3.S3ProgrammaticOptions;
import org.projectnessie.catalog.files.s3.S3Sessions;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
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
    nessieProperties.put("nessie.catalog.default-warehouse", "warehouse");
    nessieProperties.put("nessie.catalog.warehouses.warehouse.location", S3_BUCKET);
    nessieProperties.put(
        "nessie.catalog.service.s3.endpoint", objectStorage.getS3BaseUri().toString());
    nessieProperties.put("nessie.catalog.service.s3.path-style-access", "true");
    nessieProperties.put("nessie.catalog.service.s3.region", "eu-central-1");
    nessieProperties.put("nessie.catalog.service.s3.access-key.name", "accessKey");
    nessieProperties.put("nessie.catalog.service.s3.access-key.secret", "secretKey");
    nessieProperties.putAll(serverConfig);

    NessieProcess.start(nessieProperties);

    tableOneBase = S3_BUCKET + "/" + UUID.randomUUID() + "/";
    tableTwoBase = S3_BUCKET + "/" + UUID.randomUUID() + "/";

    nessieApiUri = NessieProcess.baseUri + "api/v2";
    icebergUri = NessieProcess.baseUri + "iceberg";

    S3Config s3config = S3Config.builder().build();
    SdkHttpClient httpClient = S3Clients.apacheHttpClient(s3config);

    S3Options<S3BucketOptions> s3options =
        S3ProgrammaticOptions.builder()
            .accessKey(basicCredentials("foo", "bar"))
            .region("eu-central-1")
            .endpoint(objectStorage.getS3BaseUri())
            .pathStyleAccess(true)
            .build();

    S3Sessions sessions = new S3Sessions("foo", null);

    S3ClientSupplier clientSupplier =
        new S3ClientSupplier(
            httpClient,
            s3config,
            s3options,
            new SecretsProvider(
                (names) ->
                    names.stream()
                        .collect(Collectors.toMap(k -> k, k -> Map.of("secret", "secret")))),
            sessions);

    ObjectIO objectIO = new S3ObjectIO(clientSupplier, Clock.systemUTC());

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
                          tableOneMetadata.currentSnapshotId(),
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
                          tableTwoMetadata.currentSnapshotId(),
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
