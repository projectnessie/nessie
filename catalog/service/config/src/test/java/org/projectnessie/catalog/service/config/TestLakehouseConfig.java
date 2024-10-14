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
package org.projectnessie.catalog.service.config;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.AdlsOptions;
import org.projectnessie.catalog.files.config.GcsBucketOptions;
import org.projectnessie.catalog.files.config.GcsOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsNamedFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsUserDelegation;
import org.projectnessie.catalog.files.config.ImmutableGcsBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsDownscopedCredentials;
import org.projectnessie.catalog.files.config.ImmutableGcsNamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.files.config.ImmutableS3BucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3ClientIam;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.files.config.ImmutableS3ServerIam;
import org.projectnessie.catalog.files.config.S3AuthType;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith(SoftAssertionsExtension.class)
public class TestLakehouseConfig {
  @InjectSoftAssertions protected SoftAssertions soft;

  private static ObjectMapper mapper;

  @BeforeAll
  static void setupMapper() {
    mapper = new ObjectMapper().findAndRegisterModules();
  }

  @Test
  public void lakehouseLocationsEmpty() {
    var lakehouseConfig =
        ImmutableLakehouseConfig.builder()
            .catalog(ImmutableCatalogConfig.builder().build())
            .s3(ImmutableS3Options.builder().build())
            .gcs(ImmutableGcsOptions.builder().build())
            .adls(ImmutableAdlsOptions.builder().build())
            .build();
    var lakehouseLocations = LakehouseLocations.buildLakehouseLocations(lakehouseConfig);

    soft.assertThat(lakehouseLocations.locationsBySchema().get("s3"))
        .isSameAs(lakehouseLocations.s3Locations());
    soft.assertThat(lakehouseLocations.locationsBySchema().get("gs"))
        .isSameAs(lakehouseLocations.gcsLocations());
    soft.assertThat(lakehouseLocations.locationsBySchema().get("abfs"))
        .isSameAs(lakehouseLocations.adlsLocations());

    soft.assertThat(lakehouseLocations.s3Locations().defaultBucketOptions()).isEmpty();
    soft.assertThat(lakehouseLocations.gcsLocations().defaultBucketOptions()).isEmpty();
    soft.assertThat(lakehouseLocations.adlsLocations().defaultBucketOptions()).isEmpty();

    soft.assertThat(lakehouseLocations.locationsBySchema()).containsOnlyKeys("s3", "gs", "abfs");
  }

  @Test
  public void lakehouseLocations() {
    var s3Defaults = ImmutableS3BucketOptions.builder().build();
    var gcsDefaults = ImmutableGcsBucketOptions.builder().build();
    var adlsDefaults = ImmutableAdlsFileSystemOptions.builder().build();

    var s3bucket1 =
        ImmutableS3NamedBucketOptions.builder()
            .authority("authority-1")
            .pathPrefix("path-1")
            .build();
    var s3bucket2 = ImmutableS3NamedBucketOptions.builder().build();
    var s3bucket3 =
        ImmutableS3NamedBucketOptions.builder().name("name-3").pathPrefix("path-3").build();

    var gcsBucket1 =
        ImmutableGcsNamedBucketOptions.builder()
            .authority("authority-1")
            .pathPrefix("path-1")
            .build();
    var gcsBucket2 = ImmutableGcsNamedBucketOptions.builder().build();
    var gcsBucket3 =
        ImmutableGcsNamedBucketOptions.builder().name("name-3").pathPrefix("path-3").build();

    var adlsBucket1 =
        ImmutableAdlsNamedFileSystemOptions.builder()
            .authority("authority-1")
            .pathPrefix("path-1")
            .build();
    var adlsBucket2 = ImmutableAdlsNamedFileSystemOptions.builder().build();
    var adlsBucket3 =
        ImmutableAdlsNamedFileSystemOptions.builder().name("name-3").pathPrefix("path-3").build();

    var lakehouseConfig =
        ImmutableLakehouseConfig.builder()
            .catalog(ImmutableCatalogConfig.builder().build())
            .s3(
                ImmutableS3Options.builder()
                    .defaultOptions(s3Defaults)
                    .putBucket("bucket-1", s3bucket1)
                    .putBucket("bucket-2", s3bucket2)
                    .putBucket("bucket-3", s3bucket3)
                    .build())
            .gcs(
                ImmutableGcsOptions.builder()
                    .defaultOptions(gcsDefaults)
                    .putBucket("bucket-1", gcsBucket1)
                    .putBucket("bucket-2", gcsBucket2)
                    .putBucket("bucket-3", gcsBucket3)
                    .build())
            .adls(
                ImmutableAdlsOptions.builder()
                    .defaultOptions(adlsDefaults)
                    .putFileSystem("bucket-1", adlsBucket1)
                    .putFileSystem("bucket-2", adlsBucket2)
                    .putFileSystem("bucket-3", adlsBucket3)
                    .build())
            .build();
    var lakehouseLocations = LakehouseLocations.buildLakehouseLocations(lakehouseConfig);

    soft.assertThat(lakehouseLocations.s3Locations().defaultBucketOptions())
        .get()
        .isSameAs(s3Defaults);
    soft.assertThat(lakehouseLocations.gcsLocations().defaultBucketOptions())
        .get()
        .isSameAs(gcsDefaults);
    soft.assertThat(lakehouseLocations.adlsLocations().defaultBucketOptions())
        .get()
        .isSameAs(adlsDefaults);

    soft.assertThat(lakehouseLocations.s3Locations().storageLocations())
        .containsEntry(StorageUri.of("s3://authority-1/path-1"), s3bucket1)
        .containsEntry(StorageUri.of("s3://bucket-2/"), s3bucket2)
        .containsEntry(StorageUri.of("s3://name-3/path-3"), s3bucket3)
        .hasSize(3);
    soft.assertThat(lakehouseLocations.gcsLocations().storageLocations())
        .containsEntry(StorageUri.of("gs://authority-1/path-1"), gcsBucket1)
        .containsEntry(StorageUri.of("gs://bucket-2/"), gcsBucket2)
        .containsEntry(StorageUri.of("gs://name-3/path-3"), gcsBucket3)
        .hasSize(3);
    soft.assertThat(lakehouseLocations.adlsLocations().storageLocations())
        .containsEntry(StorageUri.of("abfs://authority-1/path-1"), adlsBucket1)
        .containsEntry(StorageUri.of("abfs://bucket-2/"), adlsBucket2)
        .containsEntry(StorageUri.of("abfs://name-3/path-3"), adlsBucket3)
        .hasSize(3);

    soft.assertThat(lakehouseLocations.locationsBySchema().get("s3"))
        .isSameAs(lakehouseLocations.s3Locations());
    soft.assertThat(lakehouseLocations.locationsBySchema().get("gs"))
        .isSameAs(lakehouseLocations.gcsLocations());
    soft.assertThat(lakehouseLocations.locationsBySchema().get("abfs"))
        .isSameAs(lakehouseLocations.adlsLocations());
    soft.assertThat(lakehouseLocations.locationsBySchema()).containsOnlyKeys("s3", "gs", "abfs");
  }

  @ParameterizedTest
  @MethodSource
  public void fromSmallryeToJackson(Map<String, String> smallrye, LakehouseConfig expected)
      throws Exception {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .setAddDefaultSources(false)
            .setAddDiscoveredSources(false)
            .withMapping(SmallryeConfigs.class)
            .withSources(new PropertiesConfigSource(smallrye, "configSource", 100))
            .build();

    SmallryeConfigs sc = config.getConfigMapping(SmallryeConfigs.class);
    CatalogConfig catalog = sc.catalog();
    S3Options s3 = sc.s3();
    GcsOptions gcs = sc.gcs();
    AdlsOptions adls = sc.adls();

    CatalogConfig catalogCopy = catalog.deepClone();
    S3Options s3Copy = s3.deepClone();
    GcsOptions gcsCopy = gcs.deepClone();
    AdlsOptions adlsCopy = adls.deepClone();

    LakehouseConfig lakehouseConfig =
        ImmutableLakehouseConfig.builder()
            .catalog(catalog)
            .s3(s3)
            .gcs(gcs)
            .adls(adls)
            .build()
            .deepClone();

    String string = mapper.writeValueAsString(lakehouseConfig);

    LakehouseConfig deserialized = mapper.readValue(string, LakehouseConfig.class);

    soft.assertThat(deserialized.catalog().warehouses())
        .containsExactlyInAnyOrderEntriesOf(expected.catalog().warehouses());
    soft.assertThat(deserialized.catalog()).isEqualTo(expected.catalog());
    soft.assertThat(deserialized.s3()).isEqualTo(expected.s3());
    soft.assertThat(deserialized.gcs()).isEqualTo(expected.gcs());
    soft.assertThat(deserialized.adls()).isEqualTo(expected.adls());

    soft.assertThat(deserialized)
        .isEqualTo(expected)
        .isEqualTo(lakehouseConfig)
        .extracting(
            LakehouseConfig::catalog,
            LakehouseConfig::s3,
            LakehouseConfig::gcs,
            LakehouseConfig::adls)
        .containsExactly(catalogCopy, s3Copy, gcsCopy, adlsCopy);
  }

  static Stream<Arguments> fromSmallryeToJackson() {
    return Stream.of(
        // empty
        arguments(
            ImmutableMap.<String, String>builder().build(),
            ImmutableLakehouseConfig.builder()
                .catalog(ImmutableCatalogConfig.builder().build())
                .s3(ImmutableS3Options.builder().build())
                .gcs(ImmutableGcsOptions.builder().build())
                .adls(ImmutableAdlsOptions.builder().build())
                .build()),
        // some things
        arguments(
            ImmutableMap.<String, String>builder()
                // Iceberg default config (can be overridden per warehouse)
                .put("nessie.catalog.iceberg-config-defaults.default-key-1", "default-value-1")
                .put("nessie.catalog.iceberg-config-defaults.default-key-2", "default-value-2")
                .put("nessie.catalog.iceberg-config-overrides.override1", "override1-value")
                .put("nessie.catalog.iceberg-config-overrides.override2", "override2-value")
                // Warehouses
                // default warehouse
                .put("nessie.catalog.default-warehouse", "warehouse")
                .put("nessie.catalog.warehouses.warehouse.location", "s3://my-bucket/")
                .put(
                    "nessie.catalog.warehouses.warehouse.iceberg-config-defaults.bucket-config1",
                    "bucket-value1")
                .put(
                    "nessie.catalog.warehouses.warehouse.iceberg-config-overrides.bucket-override1",
                    "override1")
                // additional warehouses
                .put("nessie.catalog.warehouses.gcs-warehouse.location", "gs://bucket-foo/")
                .put("nessie.catalog.warehouses.adls-warehouse.location", "abfs://bucket-foo/")
                // S3 settings
                // default S3 settings
                .put("nessie.catalog.service.s3.default-options.endpoint", "http://localhost:9000")
                .put("nessie.catalog.service.s3.default-options.path-style-access", "true")
                .put("nessie.catalog.service.s3.default-options.region", "us-west-2")
                .put(
                    "nessie.catalog.service.s3.default-options.access-key",
                    "urn:nessie-secret:quarkus:my-secrets.s3-default")
                .put("nessie.catalog.service.s3.default-options.auth-type", "APPLICATION_GLOBAL")
                // per-bucket S3 settings
                .put("nessie.catalog.service.s3.buckets.bucket1.endpoint", "s3a://bucket1")
                .put(
                    "nessie.catalog.service.s3.buckets.bucket1.access-key",
                    "urn:nessie-secret:quarkus:my-secrets.s3-bucket")
                .put("nessie.catalog.service.s3.buckets.bucket1.region", "us-east-1")
                .put(
                    "nessie.catalog.service.s3.buckets.bucket1.client-iam.assume-role",
                    "client-assume")
                .put("nessie.catalog.service.s3.buckets.bucket1.client-iam.enabled", "true")
                .put("nessie.catalog.service.s3.buckets.bucket1.client-iam.external-id", "ext-id")
                .put(
                    "nessie.catalog.service.s3.buckets.bucket1.client-iam.role-session-name",
                    "role-session-name")
                .put(
                    "nessie.catalog.service.s3.buckets.bucket1.client-iam.session-duration",
                    "PT123S")
                .put("nessie.catalog.service.s3.buckets.bucket1.client-iam.statements[0]", "stmt1")
                .put("nessie.catalog.service.s3.buckets.bucket1.client-iam.statements[1]", "stmt2")
                .put(
                    "nessie.catalog.service.s3.buckets.bucket1.server-iam.assume-role",
                    "server-role")
                .put("nessie.catalog.service.s3.buckets.bucket1.server-iam.enabled", "true")
                .put("nessie.catalog.service.s3.buckets.bucket1.server-iam.external-id", "ext-role")
                .put(
                    "nessie.catalog.service.s3.buckets.bucket1.server-iam.role-session-name",
                    "server-session-name")
                .put(
                    "nessie.catalog.service.s3.buckets.bucket1.server-iam.session-duration",
                    "PT666S")
                .put("nessie.catalog.service.s3.buckets.bucket1.server-iam.policy", "my policy")
                // GCS settings
                .put("nessie.catalog.service.gcs.default-options.host", "http://localhost:4443")
                .put("nessie.catalog.service.gcs.default-options.project-id", "nessie")
                .put("nessie.catalog.service.gcs.default-options.auth-type", "access_token")
                .put(
                    "nessie.catalog.service.gcs.default-options.oauth2-token",
                    "urn:nessie-secret:quarkus:my-secrets.gcs-default")
                // per-bucket GCS settings
                .put("nessie.catalog.service.gcs.buckets.bucket1.host", "http://localhost:4443")
                .put("nessie.catalog.service.gcs.buckets.bucket1.project-id", "nessie")
                .put("nessie.catalog.service.gcs.buckets.bucket1.auth-type", "access_token")
                .put(
                    "nessie.catalog.service.gcs.buckets.bucket1.oauth2-token",
                    "urn:nessie-secret:quarkus:my-secrets.gcs-bucket")
                .put(
                    "nessie.catalog.service.gcs.buckets.bucket1.downscoped-credentials.enable",
                    "true")
                .put(
                    "nessie.catalog.service.gcs.buckets.bucket1.downscoped-credentials.expiration-margin",
                    "PT123S")
                .put(
                    "nessie.catalog.service.gcs.buckets.bucket1.downscoped-credentials.refresh-margin",
                    "PT42S")
                // ADLS settings
                .put(
                    "nessie.catalog.service.adls.default-options.endpoint",
                    "http://localhost/adlsgen2/bucke")
                .put("nessie.catalog.service.adls.default-options.auth-type", "none")
                .put(
                    "nessie.catalog.service.adls.default-options.account",
                    "urn:nessie-secret:quarkus:my-secrets.adls-default")
                // per-file-system ADLS settings
                .put(
                    "nessie.catalog.service.adls.file-systems.bucket1.endpoint",
                    "http://localhost/adlsgen2/bucket")
                .put("nessie.catalog.service.adls.file-systems.bucket1.auth-type", "none")
                .put(
                    "nessie.catalog.service.adls.file-systems.bucket1.account",
                    "urn:nessie-secret:quarkus:my-secrets.adls-fs")
                .put(
                    "nessie.catalog.service.adls.file-systems.bucket1.user-delegation.enable",
                    "true")
                .put(
                    "nessie.catalog.service.adls.file-systems.bucket1.user-delegation.key-expiry",
                    "PT123S")
                .put(
                    "nessie.catalog.service.adls.file-systems.bucket1.user-delegation.sas-expiry",
                    "PT42S")
                .build(),
            ImmutableLakehouseConfig.builder()
                .catalog(
                    ImmutableCatalogConfig.builder()
                        // Iceberg default config (can be overridden per warehouse)
                        .putIcebergConfigDefault("default-key-1", "default-value-1")
                        .putIcebergConfigDefault("default-key-2", "default-value-2")
                        .putIcebergConfigOverride("override1", "override1-value")
                        .putIcebergConfigOverride("override2", "override2-value")
                        // Warehouses
                        // default warehouse
                        .defaultWarehouse("warehouse")
                        .putWarehouse(
                            "warehouse",
                            ImmutableWarehouseConfig.builder()
                                .location("s3://my-bucket/")
                                .putIcebergConfigDefault("bucket-config1", "bucket-value1")
                                .putIcebergConfigOverride("bucket-override1", "override1")
                                .build())
                        // additional warehouses
                        .putWarehouse(
                            "gcs-warehouse",
                            ImmutableWarehouseConfig.builder().location("gs://bucket-foo/").build())
                        .putWarehouse(
                            "adls-warehouse",
                            ImmutableWarehouseConfig.builder()
                                .location("abfs://bucket-foo/")
                                .build())
                        .build())

                // S3 settings
                // default S3 settings
                .s3(
                    ImmutableS3Options.builder()
                        .defaultOptions(
                            ImmutableS3BucketOptions.builder()
                                .endpoint(URI.create("http://localhost:9000"))
                                .pathStyleAccess(true)
                                .region("us-west-2")
                                .accessKey(
                                    URI.create("urn:nessie-secret:quarkus:my-secrets.s3-default"))
                                .authType(S3AuthType.APPLICATION_GLOBAL)
                                .build())
                        // per-bucket S3 settings
                        .putBucket(
                            "bucket1",
                            ImmutableS3NamedBucketOptions.builder()
                                .endpoint(URI.create("s3a://bucket1"))
                                .accessKey(
                                    URI.create("urn:nessie-secret:quarkus:my-secrets.s3-bucket"))
                                .region("us-east-1")
                                .clientIam(
                                    ImmutableS3ClientIam.builder()
                                        .assumeRole("client-assume")
                                        .enabled(true)
                                        .externalId("ext-id")
                                        .roleSessionName("role-session-name")
                                        .sessionDuration(Duration.ofSeconds(123))
                                        .statements(List.of("stmt1", "stmt2"))
                                        .build())
                                .serverIam(
                                    ImmutableS3ServerIam.builder()
                                        .assumeRole("server-role")
                                        .enabled(true)
                                        .externalId("ext-role")
                                        .roleSessionName("server-session-name")
                                        .sessionDuration(Duration.ofSeconds(666))
                                        .policy("my policy")
                                        .build())
                                .build())
                        .build())
                // GCS settings
                .gcs(
                    ImmutableGcsOptions.builder()
                        .defaultOptions(
                            ImmutableGcsBucketOptions.builder()
                                .host(URI.create("http://localhost:4443"))
                                .projectId("nessie")
                                .authType(GcsBucketOptions.GcsAuthType.ACCESS_TOKEN)
                                .oauth2Token(
                                    URI.create("urn:nessie-secret:quarkus:my-secrets.gcs-default"))
                                .build())
                        // per-bucket GCS settings
                        .putBucket(
                            "bucket1",
                            ImmutableGcsNamedBucketOptions.builder()
                                .host(URI.create("http://localhost:4443"))
                                .projectId("nessie")
                                .authType(GcsBucketOptions.GcsAuthType.ACCESS_TOKEN)
                                .oauth2Token(
                                    URI.create("urn:nessie-secret:quarkus:my-secrets.gcs-bucket"))
                                .downscopedCredentials(
                                    ImmutableGcsDownscopedCredentials.builder()
                                        .enable(true)
                                        .expirationMargin(Duration.ofSeconds(123))
                                        .refreshMargin(Duration.ofSeconds(42))
                                        .build())
                                .build())
                        .build())
                // ADLS settings
                .adls(
                    ImmutableAdlsOptions.builder()
                        .defaultOptions(
                            ImmutableAdlsFileSystemOptions.builder()
                                .endpoint("http://localhost/adlsgen2/bucke")
                                .authType(AdlsFileSystemOptions.AzureAuthType.NONE)
                                .account(
                                    URI.create("urn:nessie-secret:quarkus:my-secrets.adls-default"))
                                .build())
                        // per-file-system ADLS settings
                        .putFileSystem(
                            "bucket1",
                            ImmutableAdlsNamedFileSystemOptions.builder()
                                .endpoint("http://localhost/adlsgen2/bucket")
                                .authType(AdlsFileSystemOptions.AzureAuthType.NONE)
                                .account(URI.create("urn:nessie-secret:quarkus:my-secrets.adls-fs"))
                                .userDelegation(
                                    ImmutableAdlsUserDelegation.builder()
                                        .enable(true)
                                        .keyExpiry(Duration.ofSeconds(123))
                                        .sasExpiry(Duration.ofSeconds(42))
                                        .build())
                                .build())
                        .build())
                // done w/ lakehouse config
                .build()
            //
            ));
  }
}
