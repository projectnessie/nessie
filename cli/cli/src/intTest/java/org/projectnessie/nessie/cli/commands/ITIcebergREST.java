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

import static java.lang.String.format;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.generateMetadataWithManifestList;
import static org.projectnessie.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.NotConnectedException;
import org.projectnessie.nessie.cli.cmdspec.ImmutableConnectCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableListContentsCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableShowContentCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableUseReferenceCommandSpec;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.http.SdkHttpClient;

@ExtendWith(SoftAssertionsExtension.class)
public class ITIcebergREST {

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

  @BeforeAll
  public static void start() throws Exception {
    bucket = newHeapStorageBucket();
    objectStorage =
        ObjectStorageMock.builder().putBuckets("bucket", bucket.bucket()).build().start();

    NessieProcess.start(
        "-Dnessie.catalog.default-warehouse=warehouse",
        "-Dnessie.catalog.warehouses.warehouse.location=" + S3_BUCKET,
        "-Dnessie.catalog.service.s3.cloud=private",
        "-Dnessie.catalog.service.s3.endpoint=" + objectStorage.getS3BaseUri().toString(),
        "-Dnessie.catalog.service.s3.path-style-access=true",
        "-Dnessie.catalog.service.s3.region=eu-central-1",
        "-Dnessie.catalog.service.s3.access-key-id=accessKey",
        "-Dnessie.catalog.service.s3.secret-access-key=secretKey");

    tableOneBase = S3_BUCKET + "/" + UUID.randomUUID() + "/";
    tableTwoBase = S3_BUCKET + "/" + UUID.randomUUID() + "/";

    nessieApiUri = NessieProcess.baseUri + "api/v2";
    icebergUri = NessieProcess.baseUri + "iceberg";

    S3Config s3config = S3Config.builder().build();
    SdkHttpClient httpClient = S3Clients.apacheHttpClient(s3config);

    S3Options<S3BucketOptions> s3options =
        S3ProgrammaticOptions.builder()
            .accessKeyId("foo")
            .secretAccessKey("bar")
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
            (names) -> names.stream().collect(Collectors.toMap(k -> k, k -> "secret")),
            sessions);

    ObjectIO objectIO = new S3ObjectIO(clientSupplier, Clock.systemUTC());

    tableOneMetadataLocation =
        generateMetadataWithManifestList(
            tableOneBase, objectWriter(objectIO, tableOneBase), m -> tableOneMetadata = m);
    tableTwoMetadataLocation =
        generateMetadataWithManifestList(
            tableTwoBase, objectWriter(objectIO, tableTwoBase), m -> tableTwoMetadata = m);

    try (NessieApiV2 api =
        NessieClientBuilder.createClientBuilderFromSystemSettings()
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

  @AfterAll
  public static void stop() throws Exception {
    objectStorage.close();
    NessieProcess.stop();
  }

  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void connectAndUse(
      String initialUri,
      String icebergConnectedUri,
      String initialReference,
      Reference expectedReference)
      throws Exception {
    ImmutableConnectCommandSpec spec =
        ImmutableConnectCommandSpec.builder()
            .uri(initialUri)
            .initialReference(initialReference)
            .build();

    try (NessieCliTester cli = new NessieCliTester()) {
      soft.assertThatThrownBy(cli::mandatoryNessieApi).isInstanceOf(NotConnectedException.class);

      soft.assertThatCode(() -> cli.execute(spec)).doesNotThrowAnyException();

      soft.assertThat(cli.capturedOutput())
          .containsExactly(
              format("Connecting to %s ...", initialUri),
              format("Successfully connected to Iceberg REST at %s", icebergConnectedUri),
              format("Connecting to Nessie REST at %s/ ...", nessieApiUri),
              format(
                  "Successfully connected to Nessie REST at %s/ - Nessie API version 2, spec version 2.1.0",
                  nessieApiUri));

      soft.assertThatCode(cli::mandatoryNessieApi).doesNotThrowAnyException();

      soft.assertThat(cli.getCurrentReference()).isEqualTo(expectedReference);

      // "USE main" & check contents to verify that "USE" works for Nessie + Iceberg REST

      cli.execute(ImmutableUseReferenceCommandSpec.of(null, "BRANCH", "main"));
      soft.assertThat(cli.getCurrentReference()).isEqualTo(defaultBranch);

      cli.execute(ImmutableListContentsCommandSpec.of(null, null, null, null, null, null));
      soft.assertThat(cli.capturedOutput())
          .containsExactly("      NAMESPACE tables", "  ICEBERG_TABLE " + tableOneKey);
      cli.execute(
          ImmutableShowContentCommandSpec.of(null, "TABLE", null, null, tableOneKey.toString()));
      soft.assertThat(cli.capturedOutput())
          .map(s -> s.endsWith(",") ? s.substring(0, s.length() - 1) : s)
          .contains(
              "        JSON: ",
              "Content type: ICEBERG_TABLE",
              " Content Key: " + tableOneKey,
              "          ID: " + tableOneId,
              //
              "Nessie metadata:",
              "  \"id\": \"" + tableOneId + "\"",
              "  \"metadataLocation\": \"" + tableOneMetadataLocation + "\"",
              //
              "Iceberg metadata:",
              "    \"nessie.catalog.content-id\": \"" + tableOneId + "\"",
              "    \"nessie.commit.id\": \"" + defaultBranch.getHash() + "\"",
              "    \"nessie.commit.ref\": \""
                  + defaultBranch.getName()
                  + "@"
                  + defaultBranch.getHash()
                  + "\"");

      // "USE testBranch" & check contents to verify that "USE" works for Nessie + Iceberg REST

      cli.execute(ImmutableUseReferenceCommandSpec.of(null, "BRANCH", testBranch.getName()));
      soft.assertThat(cli.getCurrentReference()).isEqualTo(testBranch);

      cli.execute(ImmutableListContentsCommandSpec.of(null, null, null, null, null, null));
      soft.assertThat(cli.capturedOutput())
          .containsExactly("      NAMESPACE tables", "  ICEBERG_TABLE " + tableTwoKey);
      cli.execute(
          ImmutableShowContentCommandSpec.of(null, "TABLE", null, null, tableTwoKey.toString()));
      soft.assertThat(cli.capturedOutput())
          .map(s -> s.endsWith(",") ? s.substring(0, s.length() - 1) : s)
          .contains(
              "        JSON: ",
              "Content type: ICEBERG_TABLE",
              " Content Key: " + tableTwoKey,
              "          ID: " + tableTwoId,
              //
              "Nessie metadata:",
              "  \"id\": \"" + tableTwoId + "\"",
              "  \"metadataLocation\": \"" + tableTwoMetadataLocation + "\"",
              //
              "Iceberg metadata:",
              "    \"nessie.catalog.content-id\": \"" + tableTwoId + "\"",
              "    \"nessie.commit.id\": \"" + testBranch.getHash() + "\"",
              "    \"nessie.commit.ref\": \""
                  + testBranch.getName()
                  + "@"
                  + testBranch.getHash()
                  + "\"");
    }
  }

  static Stream<Arguments> connectAndUse() {
    return Stream.of(
        // CONNECT TO with initial reference to "testBranch"
        arguments(icebergUri, icebergUri, testBranch.getName(), testBranch),
        // CONNECT TO with a "prefixed" Iceberg REST URI, but a different initial reference
        arguments(
            icebergUri + "/" + testBranch.getName() + "/",
            icebergUri + "/" + testBranch.getName() + "/",
            defaultBranch.getName(),
            defaultBranch),
        // CONNECT TO with a "prefixed" Iceberg REST URI
        arguments(
            icebergUri + "/" + testBranch.getName() + "/",
            icebergUri + "/" + testBranch.getName() + "/",
            null,
            testBranch),
        arguments(
            icebergUri + "/" + testBranch.getName(),
            icebergUri + "/" + testBranch.getName(),
            null,
            testBranch),
        // CONNECT TO with a Nessie API URI
        arguments(nessieApiUri, icebergUri + "/", null, defaultBranch),
        arguments(nessieApiUri + "/", icebergUri + "/", null, defaultBranch),
        // CONNECT TO with an Iceberg REST URI
        arguments(icebergUri, icebergUri, null, defaultBranch),
        arguments(icebergUri + "/", icebergUri + "/", null, defaultBranch));
  }
}
