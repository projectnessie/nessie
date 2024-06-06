/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.server.catalog;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.InstanceOfAssertFactories.optional;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.generateMetadataWithManifestList;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.generateSimpleMetadata;
import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;
import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.S3_WAREHOUSE_LOCATION;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.storage.uri.StorageUri;

@QuarkusTest
@TestProfile(value = S3UnitTestProfiles.S3UnitTestProfile.class)
public class TestNessieCore {
  protected SoftAssertions soft;
  private static HttpClient httpClient;
  private NessieApiV2 api;
  private URI baseUri;

  @Inject ObjectIO objectIO;

  String currentBase;

  HeapStorageBucket heapStorageBucket;

  @BeforeAll
  static void setupHttpClient() {
    httpClient =
        Vertx.vertx()
            .createHttpClient(
                new HttpClientOptions().setMaxPoolSize(1000).setHttp2MaxPoolSize(1000));
  }

  @AfterAll
  static void closeHttpClient() throws Exception {
    httpClient.close().toCompletionStage().toCompletableFuture().get(100, SECONDS);
  }

  @BeforeEach
  void createNessieClient() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    api =
        createClientBuilderFromSystemSettings()
            .withUri(format("http://127.0.0.1:%d/api/v2", catalogServerPort))
            .build(NessieApiV2.class);

    @SuppressWarnings("resource")
    var baseUri =
        api.unwrapClient(org.projectnessie.client.http.HttpClient.class).orElseThrow().getBaseUri();
    if (baseUri.getPath().endsWith("/")) {
      baseUri = baseUri.resolve("..");
    }
    this.baseUri = baseUri.resolve("../catalog/v1/");
  }

  @AfterEach
  void closeNessieClient() {
    api.close();
  }

  @BeforeEach
  void setupSoftAssertions() {
    soft = new SoftAssertions();
  }

  @AfterEach
  void assertSoftAssertions() {
    soft.assertAll();
  }

  @BeforeEach
  public void clearBucket() {
    heapStorageBucket.clear();
  }

  @BeforeEach
  protected void setup() {
    currentBase = S3_WAREHOUSE_LOCATION + "/" + UUID.randomUUID() + "/";
  }

  protected IcebergGenerateFixtures.ObjectWriter objectWriter() {
    return (name, data) -> {
      URI location =
          name.isAbsolute() ? name : URI.create(currentBase + "/" + name.getPath()).normalize();
      try (OutputStream output = objectIO.writeObject(StorageUri.of(location))) {
        output.write(data);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return location.toString();
    };
  }

  @Test
  public void nessieApiWorks() {
    soft.assertThat(api.getConfig().getDefaultBranch()).isEqualTo("main");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void concurrentImports(boolean useIcebergREST) throws Exception {
    var tableMetadataLocation = generateMetadataWithManifestList(currentBase, objectWriter());

    var namespace = "stuff-" + useIcebergREST;
    var tableName = "concurrentImports-" + useIcebergREST;

    api.commitMultipleOperations()
        .commitMeta(fromMessage("a table named " + tableName))
        .operation(Operation.Put.of(ContentKey.of(namespace), Namespace.of(namespace)))
        .operation(
            Operation.Put.of(
                ContentKey.of(namespace, tableName),
                IcebergTable.of(tableMetadataLocation, 1, 0, 0, 0)))
        .branch(api.getDefaultBranch())
        .commitWithResponse();

    var snapshotUri =
        useIcebergREST
            ? baseUri.resolve(
                format(
                    "../../iceberg/v1/main%%7C%s/namespaces/%s/tables/%s",
                    WAREHOUSE_NAME, namespace, tableName))
            : baseUri.resolve("trees/main/snapshot/" + namespace + "." + tableName);

    var requests = new ArrayList<Future<String>>();
    for (var i = 0; i < 10; i++) {
      var req = httpRequest(snapshotUri).map(Buffer::toString);
      requests.add(req);
    }
    Future.all(requests).toCompletionStage().toCompletableFuture().get(1000, SECONDS);

    var firstBody = requests.get(0).result();
    soft.assertThat(requests).extracting(Future::result).allMatch(firstBody::equals);
  }

  @Test
  public void getMultipleSnapshots() throws Exception {
    var tableMetadataLocation = generateSimpleMetadata(objectWriter(), 2);

    var tableNames =
        IntStream.rangeClosed(1, 5)
            .mapToObj(i -> "getMultipleSnapshots" + i)
            .map(ContentKey::of)
            .toList();
    api.commitMultipleOperations()
        .commitMeta(fromMessage("some tables"))
        .operations(
            tableNames.stream()
                .map(t -> Operation.Put.of(t, IcebergTable.of(tableMetadataLocation, 1, 0, 0, 0)))
                .collect(Collectors.toUnmodifiableList()))
        .branch(api.getDefaultBranch())
        .commitWithResponse();

    var snapshotsUri =
        baseUri.resolve(
            "trees/main/snapshots?format=iceberg&"
                + tableNames.stream()
                    .map(ContentKey::toPathString)
                    .collect(Collectors.joining("&key=", "key=", "")));

    var tableMetadata = httpRequestString(snapshotsUri);

    System.err.println(
        IcebergJson.objectMapper().readValue(tableMetadata, JsonNode.class).toPrettyString());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void tableMetadata(int specVersion) throws Exception {
    var tableMetadataLocation = generateSimpleMetadata(objectWriter(), specVersion);

    var tableName = "tableMetadataWithoutManifests" + specVersion;

    api.commitMultipleOperations()
        .commitMeta(fromMessage("a table named " + tableName))
        .operation(
            Operation.Put.of(
                ContentKey.of(tableName), IcebergTable.of(tableMetadataLocation, 1, 0, 0, 0)))
        .branch(api.getDefaultBranch())
        .commitWithResponse();

    var snapshotUri = baseUri.resolve("trees/main/snapshot/" + tableName + "?format=iceberg");

    var tableMetadata = httpRequestString(snapshotUri);

    IcebergTableMetadata icebergTableMetadata =
        IcebergJson.objectMapper().readValue(tableMetadata, IcebergTableMetadata.class);

    soft.assertThat(icebergTableMetadata)
        .isNotNull()
        .extracting(IcebergTableMetadata::currentSnapshot, optional(IcebergSnapshot.class))
        .isNotEmpty()
        .get()
        .extracting(IcebergSnapshot::manifestList, IcebergSnapshot::manifests)
        .containsExactly(null, emptyList());
  }

  private static String httpRequestString(URI uri) throws Exception {
    return httpRequest(uri)
        .map(Buffer::toString)
        .toCompletionStage()
        .toCompletableFuture()
        .get(10, SECONDS);
  }

  private static Future<Buffer> httpRequest(URI uri) {
    return httpResponse(uri).compose(HttpClientResponse::body);
  }

  private static Future<HttpClientResponse> httpResponse(URI uri) {
    return httpClient
        .request(
            HttpMethod.GET,
            uri.getPort(),
            uri.getHost(),
            uri.getRawPath() + (uri.getRawQuery() != null ? "?" + uri.getRawQuery() : ""))
        .compose(HttpClientRequest::send)
        .map(
            r -> {
              if (r.statusCode() != 200) {
                throw new RuntimeException(
                    "Failed request to "
                        + uri
                        + " : HTTP/"
                        + r.statusCode()
                        + " "
                        + r.statusMessage());
              }
              return r;
            });
  }
}
