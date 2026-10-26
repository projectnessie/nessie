/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.server.catalog.s3;

import static io.restassured.RestAssured.given;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;
import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.BUCKET;
import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.S3_WAREHOUSE_LOCATION;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.vertx.http.HttpServer;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateTableResponse;
import org.projectnessie.objectstoragemock.sts.ImmutableAssumeRoleResult;
import org.projectnessie.objectstoragemock.sts.ImmutableCredentials;
import org.projectnessie.objectstoragemock.sts.ImmutableRoleUser;
import org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.AssumeRoleHandlerHolder;
import org.projectnessie.server.catalog.S3UnitTestProfiles;
import org.projectnessie.storage.uri.StorageUri;

@QuarkusTest
@TestProfile(TestDefaultTableLocations.Profile.class)
public class TestDefaultTableLocations {
  @Inject HttpServer httpServer;
  @Inject ObjectIO objectIO;

  @SuppressWarnings("unused") // Injected by the object-storage test resource
  private AssumeRoleHandlerHolder assumeRoleHandler;

  static Stream<Arguments> tableLocations() {
    return Stream.of(
            new String[] {"table.name", "table.name"},
            new String[] {"part/name", "part%2Fname"},
            new String[] {"/name", "%2Fname"},
            new String[] {"s3://example/path", "s3%3A%2F%2Fexample%2Fpath"},
            new String[] {"part/../name", "part%2F..%2Fname"},
            new String[] {"..", "%2E."},
            new String[] {"part%2Fname", "part%252Fname"})
        .flatMap(
            element ->
                Stream.of(false, true)
                    .flatMap(
                        staged ->
                            Stream.of("remote-signing", "vended-credentials")
                                .map(
                                    delegation ->
                                        Arguments.of(element[0], element[1], staged, delegation))));
  }

  @ParameterizedTest
  @MethodSource("tableLocations")
  void defaultTableLocation(String element, String encoded, boolean staged, String delegation)
      throws Exception {
    ConcurrentLinkedQueue<String> policies = new ConcurrentLinkedQueue<>();
    assumeRoleHandler.set(
        (action, version, roleArn, roleSessionName, policy, duration, externalId, serialNumber) -> {
          policies.add(policy);
          return ImmutableAssumeRoleResult.builder()
              .sourceIdentity("test")
              .assumedRoleUser(
                  ImmutableRoleUser.builder().arn("arn").assumedRoleId("test-role").build())
              .credentials(
                  ImmutableCredentials.builder()
                      .accessKeyId("test-access-key")
                      .secretAccessKey("test-secret-key")
                      .sessionToken("test-session-token")
                      .expiration(Instant.now().plus(1, ChronoUnit.HOURS))
                      .build())
              .build();
        });

    String group = "locations_" + UUID.randomUUID();
    String namespace = group + '\u001f' + element;
    String base = httpServer.getLocalBaseUri().resolve("/iceberg/v1/main/").toString();
    for (List<String> levels : List.of(List.of(group), List.of(group, element))) {
      given()
          .contentType(ContentType.JSON)
          .body(IcebergJson.objectMapper().writeValueAsString(Map.of("namespace", levels)))
          .post(base + "namespaces")
          .then()
          .statusCode(200);
    }

    String unrelated = "s3://" + BUCKET + "/unrelated/" + group + "/data.txt";
    byte[] data = "table data".getBytes(UTF_8);
    try (OutputStream output = objectIO.writeObject(StorageUri.of(unrelated))) {
      output.write(data);
    }

    String json =
        given()
            .contentType(ContentType.JSON)
            .header("X-Iceberg-Access-Delegation", delegation)
            .body(
                IcebergJson.objectMapper()
                    .writeValueAsString(
                        Map.of(
                            "name", element,
                            "stage-create", staged,
                            "schema",
                                Map.of(
                                    "type",
                                    "struct",
                                    "schema-id",
                                    0,
                                    "fields",
                                    List.of(
                                        Map.of(
                                            "id",
                                            1,
                                            "name",
                                            "id",
                                            "required",
                                            true,
                                            "type",
                                            "long"))))))
            .post(base + "namespaces/{namespace}/tables", namespace)
            .then()
            .statusCode(200)
            .extract()
            .asString();
    IcebergCreateTableResponse response =
        IcebergJson.objectMapper().readValue(json, IcebergCreateTableResponse.class);
    String location = response.metadata().location();
    String expected = S3_WAREHOUSE_LOCATION + "/" + group + "/" + encoded + "/" + encoded + "_";
    assertThat(location).startsWith(expected);
    assertThat(UUID.fromString(location.substring(expected.length()))).isNotNull();
    if (staged) {
      assertThat(response.metadataLocation()).isNull();
    } else {
      assertThat(response.metadataLocation()).startsWith(location + "/metadata/");
      try (InputStream input = objectIO.readObject(StorageUri.of(response.metadataLocation()))) {
        assertThat(IcebergJson.objectMapper().readTree(input).get("location").asString())
            .isEqualTo(location);
      }
    }

    // Exercise the returned client configuration, including its literal object-key spelling.
    String dataLocation = location + "/data/file.txt";
    try (S3FileIO fileIO = new S3FileIO()) {
      Map<String, String> clientConfig = new HashMap<>(response.config());
      clientConfig.put("uri", httpServer.getLocalBaseUri().resolve("/iceberg/").toString());
      fileIO.initialize(clientConfig);
      try (OutputStream output = fileIO.newOutputFile(dataLocation).createOrOverwrite()) {
        output.write(data);
      }
      try (InputStream input = fileIO.newInputFile(dataLocation).newStream()) {
        assertThat(input.readAllBytes()).isEqualTo(data);
      }
    }
    try (InputStream input = objectIO.readObject(StorageUri.of(dataLocation))) {
      assertThat(input.readAllBytes()).isEqualTo(data);
    }

    if (delegation.equals("remote-signing")) {
      assertThat(policies).isEmpty();
      String endpoint =
          httpServer
              .getLocalBaseUri()
              .resolve("/iceberg/")
              .resolve(response.config().get("s3.signer.endpoint"))
              .toString();
      String unrelatedHttpUri =
          URI.create(response.config().get("s3.endpoint"))
              .resolve("/" + BUCKET + "/unrelated/" + group + "/data.txt")
              .toString();
      given()
          .contentType(ContentType.JSON)
          .body(
              IcebergJson.objectMapper()
                  .writeValueAsString(
                      Map.of(
                          "method",
                          "GET",
                          "region",
                          "us-east-1",
                          "uri",
                          unrelatedHttpUri,
                          "headers",
                          Map.of(),
                          "properties",
                          Map.of())))
          .post(endpoint)
          .then()
          .statusCode(403);
    } else {
      assertThat(response.config()).containsEntry("s3.session-token", "test-session-token");
      String key = StorageUri.of(location).pathWithoutLeadingTrailingSlash();
      assertThat(policies)
          .isNotEmpty()
          .allSatisfy(
              policy ->
                  assertThat(policy)
                      .contains("arn:aws:s3:::" + BUCKET + "/" + key + "/*")
                      .doesNotContain("/unrelated/"));
      assertThat(response.storageCredentials())
          .anySatisfy(credential -> assertThat(credential.prefix()).isEqualTo(location));
    }
  }

  public static class Profile extends S3UnitTestProfiles.S3UnitTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("nessie.catalog.default-warehouse", WAREHOUSE_NAME)
          .put("nessie.catalog.service.s3.default-options.client-iam.enabled", "true")
          .put("nessie.catalog.service.s3.default-options.client-iam.assume-role", "test-role")
          .build();
    }
  }
}
