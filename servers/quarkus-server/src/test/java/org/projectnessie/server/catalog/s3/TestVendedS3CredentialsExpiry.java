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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.files.s3.Cloud;
import org.projectnessie.catalog.files.s3.S3ClientAuthenticationMode;
import org.projectnessie.objectstoragemock.sts.ImmutableAssumeRoleResult;
import org.projectnessie.objectstoragemock.sts.ImmutableCredentials;
import org.projectnessie.objectstoragemock.sts.ImmutableRoleUser;
import org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager;
import org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.AssumeRoleHandlerHolder;

@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    value = ObjectStorageMockTestResourceLifecycleManager.class)
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
      soft.assertThatThrownBy(
              () ->
                  catalog.initialize(
                      "nessie-s3-iceberg-api",
                      Map.of(
                          CatalogProperties.URI,
                          String.format("http://127.0.0.1:%d/iceberg/", catalogServerPort))))
          .hasMessageMatching(
              "Provided credentials expire \\(1970-01-01T00:00:10Z\\) before the expected session end "
                  + "\\(now: .+, duration: PT5H\\)");
    }
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("nessie.catalog.default-warehouse.name", "warehouse")
          .put("nessie.catalog.default-warehouse.location", "s3://test-bucket")
          .put("nessie.catalog.service.s3.cloud", Cloud.PRIVATE.name())
          .put("nessie.catalog.service.s3.region", "us-west-2")
          .put("nessie.catalog.service.s3.auth-mode", S3ClientAuthenticationMode.ASSUME_ROLE.name())
          .put("nessie.catalog.service.s3.client-session-duration", "PT5H")
          .put("nessie.catalog.service.s3.role-session-name", "test-session-name")
          .put("nessie.catalog.service.s3.assumed-role", "test-role")
          .put("nessie.catalog.service.s3.external-id", "test-external-id")
          .put("nessie.catalog.service.s3.access-key-id-ref", "awsAccessKeyId")
          .put("nessie.catalog.service.s3.secret-access-key-ref", "awsSecretAccessKey")
          .put("nessie.catalog.secrets.awsAccessKeyId", "test-access-key")
          .put("nessie.catalog.secrets.awsSecretAccessKey", "test-secret-key")
          .build();
    }
  }
}
