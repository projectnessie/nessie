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

import static java.util.Collections.singletonMap;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.intellij.lang.annotations.Language;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadCredentialsResponse;
import org.projectnessie.server.catalog.AbstractIcebergCatalogIntTests;
import org.projectnessie.server.catalog.FlociS3TestResourceLifecycleManager;
import org.projectnessie.testing.floci.s3.FlociS3Container;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@WithTestResource(FlociS3TestResourceLifecycleManager.class)
@QuarkusIntegrationTest
@TestProfile(ITS3AssumeRoleIcebergCatalog.Profile.class)
public class ITS3AssumeRoleIcebergCatalog extends AbstractIcebergCatalogIntTests {

  @Language("JSON")
  public static final String IAM_ALLOW_TEMP =
      """
      {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*/temp/*"}
      """;

  @SuppressWarnings("unused")
  // Injected by FlociS3TestResourceLifecycleManager
  private FlociS3Container flociS3;

  @Override
  protected Map<String, String> catalogOptions() {
    return singletonMap(
        CatalogProperties.WAREHOUSE_LOCATION, flociS3.s3BucketUri(scheme(), "").toString());
  }

  @Override
  protected String temporaryLocation() {
    return flociS3.s3BucketUri(scheme(), "/temp/" + UUID.randomUUID()).toString();
  }

  @Override
  protected FileIO temporaryFileIO(RESTCatalog catalog, FileIO io) {
    return io;
  }

  @Override
  protected String scheme() {
    return "s3";
  }

  @Override
  protected void verifyLoadCredentialsResponse(
      Table table, IcebergLoadCredentialsResponse response) {
    soft.assertThat(response.storageCredentials())
        .anySatisfy(
            credential -> {
              soft.assertThat(credential.prefix()).startsWith(table.location());
              soft.assertThat(credential.config())
                  .containsKeys(
                      "s3.access-key-id",
                      "s3.secret-access-key",
                      "s3.session-token",
                      "s3.session-token-expires-at-ms");
            });
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("nessie.catalog.service.s3.default-options.request-signing-enabled", "false")
          .put("nessie.catalog.service.s3.default-options.client-iam.enabled", "true")
          .put("nessie.catalog.service.s3.default-options.client-iam.statements[0]", IAM_ALLOW_TEMP)
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.assume-role",
              "arn:aws:iam::123456789012:role/client-role")
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.external-id",
              "test-external-id")
          .build();
    }
  }
}
