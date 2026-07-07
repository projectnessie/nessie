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
package org.projectnessie.catalog.service.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions.AzureAuthType;
import org.projectnessie.catalog.files.config.GcsBucketOptions.GcsAuthType;
import org.projectnessie.catalog.files.config.ImmutableAdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsNamedFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsNamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.files.config.ImmutableS3BucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TestJackson3CatalogServiceConfigCompatibility {
  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  @Test
  void serviceConfigRoundTrip() throws Exception {
    ServiceConfig config =
        ImmutableServiceConfig.builder()
            .objectStoresHealthCheck(false)
            .retryAfterThrottled(Duration.ofSeconds(30))
            .build();

    String json = MAPPER.writeValueAsString(config);
    assertThat(MAPPER.readValue(json, ServiceConfig.class)).isEqualTo(config);
  }

  @Test
  void catalogConfigRoundTrip() throws Exception {
    CatalogConfig config =
        ImmutableCatalogConfig.builder()
            .defaultWarehouse("warehouse")
            .putIcebergConfigDefault("default-key", "default-value")
            .putIcebergConfigOverride("override-key", "override-value")
            .putWarehouse(
                "warehouse",
                ImmutableWarehouseConfig.builder()
                    .location("s3://bucket/warehouse/")
                    .putIcebergConfigDefault("warehouse-default", "warehouse-value")
                    .putIcebergConfigOverride("warehouse-override", "warehouse-override-value")
                    .build())
            .build();

    String json = MAPPER.writeValueAsString(config);
    CatalogConfig deserialized = MAPPER.readValue(json, CatalogConfig.class);
    assertThat(deserialized).isEqualTo(config);
    assertThat(deserialized.getWarehouse("s3://bucket/warehouse/"))
        .isEqualTo(config.warehouses().get("warehouse"));
  }

  @Test
  void lakehouseConfigRoundTrip() throws Exception {
    LakehouseConfig config =
        ImmutableLakehouseConfig.builder()
            .catalog(
                ImmutableCatalogConfig.builder()
                    .defaultWarehouse("warehouse")
                    .putWarehouse(
                        "warehouse",
                        ImmutableWarehouseConfig.builder()
                            .location("s3://bucket/warehouse")
                            .build())
                    .build())
            .s3(
                ImmutableS3Options.builder()
                    .defaultOptions(
                        ImmutableS3BucketOptions.builder()
                            .endpoint(URI.create("https://s3.example.com"))
                            .region("us-east-1")
                            .build())
                    .putBucket(
                        "bucket",
                        ImmutableS3NamedBucketOptions.builder()
                            .authority("bucket")
                            .pathPrefix("warehouse")
                            .build())
                    .build())
            .gcs(
                ImmutableGcsOptions.builder()
                    .defaultOptions(
                        ImmutableGcsBucketOptions.builder()
                            .host(URI.create("https://storage.googleapis.com"))
                            .projectId("project")
                            .authType(GcsAuthType.ACCESS_TOKEN)
                            .build())
                    .putBucket(
                        "bucket",
                        ImmutableGcsNamedBucketOptions.builder()
                            .authority("bucket")
                            .pathPrefix("warehouse")
                            .build())
                    .build())
            .adls(
                ImmutableAdlsOptions.builder()
                    .defaultOptions(
                        ImmutableAdlsFileSystemOptions.builder()
                            .endpoint("https://account.dfs.core.windows.net")
                            .authType(AzureAuthType.NONE)
                            .build())
                    .putFileSystem(
                        "filesystem",
                        ImmutableAdlsNamedFileSystemOptions.builder()
                            .authority("account")
                            .pathPrefix("warehouse")
                            .build())
                    .build())
            .build();

    String json = MAPPER.writeValueAsString(config);
    assertThat(MAPPER.readValue(json, LakehouseConfig.class)).isEqualTo(config);
  }
}
