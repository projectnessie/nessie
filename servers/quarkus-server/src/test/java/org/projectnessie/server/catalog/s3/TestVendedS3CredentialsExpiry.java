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
package org.projectnessie.server.catalog.s3;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.objectstoragemock.sts.ImmutableAssumeRoleResult;
import org.projectnessie.objectstoragemock.sts.ImmutableCredentials;
import org.projectnessie.objectstoragemock.sts.ImmutableRoleUser;
import org.projectnessie.server.catalog.IcebergResourceLifecycleManager;
import org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager;
import org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.AssumeRoleHandlerHolder;
import org.projectnessie.server.catalog.S3UnitTestProfiles;

@WithTestResource(ObjectStorageMockTestResourceLifecycleManager.class)
@WithTestResource(IcebergResourceLifecycleManager.ForUnitTests.class)
@QuarkusTest
@TestProfile(TestVendedS3CredentialsExpiry.Profile.class)
public class TestVendedS3CredentialsExpiry {

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  private final SoftAssertions soft = new SoftAssertions();

  @SuppressWarnings("unused") // Injected by ObjectStorageMockTestResourceLifecycleManager
  private AssumeRoleHandlerHolder assumeRoleHandler;

  @AfterEach
  void assertAll() {
    soft.assertAll();
  }

  @Test
  void testExpiredVendedCredentials() throws IOException {
    assumeRoleHandler.set(
        (action,
            version,
            roleArn,
            roleSessionName,
            policy,
            durationSeconds,
            externalId,
            serialNumber) -> {
          soft.assertThat(durationSeconds).isEqualTo(Duration.ofHours(5).toSeconds());
          soft.assertThat(externalId).isEqualTo("test-external-id");
          soft.assertThat(roleArn).isEqualTo("test-role");
          soft.assertThat(roleSessionName).isEqualTo("test-session-name");

          return ImmutableAssumeRoleResult.builder()
              .sourceIdentity("test")
              .assumedRoleUser(
                  ImmutableRoleUser.builder().arn("arn").assumedRoleId("test-role").build())
              .credentials(
                  ImmutableCredentials.builder()
                      .accessKeyId("test-access-key-111")
                      .secretAccessKey("test-secret-key-111")
                      .sessionToken("test-token-111")
                      .expiration(Instant.ofEpochSecond(10))
                      .build())
              .build();
        });

    int catalogServerPort = Integer.getInteger("quarkus.http.port");

    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.setConf(new Configuration());
      soft.assertThatCode(
              () ->
                  catalog.initialize(
                      "nessie-s3-iceberg-api",
                      Map.of(
                          CatalogProperties.URI,
                          String.format("http://127.0.0.1:%d/iceberg/", catalogServerPort))))
          .doesNotThrowAnyException();
      soft.assertThatCode(() -> catalog.createNamespace(Namespace.of("foo")))
          .doesNotThrowAnyException();
      soft.assertThatThrownBy(
              () ->
                  catalog.createTable(
                      TableIdentifier.of("foo", "bar"),
                      new Schema(required(1, "id", Types.IntegerType.get()))))
          .hasMessageMatching(
              "Provided credentials expire \\(1970-01-01T00:00:10Z\\) before the expected session end "
                  + "\\(now: .+, duration: PT5H\\)");
    }
  }

  public static class Profile extends S3UnitTestProfiles.S3UnitTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("nessie.catalog.default-warehouse", WAREHOUSE_NAME)
          .put("nessie.catalog.service.s3.default-options.region", "us-west-2")
          .put(
              "nessie.catalog.service.s3.default-options.access-key",
              "urn:nessie-secret:quarkus:test-access-key-secret")
          .put("test-access-key-secret.name", "test-secret-key")
          .put("test-access-key-secret.secret", "test-secret-key")
          .put("nessie.catalog.service.s3.default-options.request-signing-enabled", "false")
          .put("nessie.catalog.service.s3.default-options.client-iam.enabled", "true")
          .put("nessie.catalog.service.s3.default-options.client-iam.session-duration", "PT5H")
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.role-session-name",
              "test-session-name")
          .put("nessie.catalog.service.s3.default-options.client-iam.assume-role", "test-role")
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.external-id",
              "test-external-id")
          .build();
    }
  }
}
