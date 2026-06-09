/*
 * Copyright (C) 2025 Dremio
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
package org.projectnessie.catalog.files;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.catalog.files.api.ObjectIO.PYICEBERG_FILE_IO_IMPL;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.adls.AdlsClientSupplier;
import org.projectnessie.catalog.files.adls.AdlsObjectIO;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.BucketOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsConfig;
import org.projectnessie.catalog.files.config.ImmutableAdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsConfig;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.files.config.ImmutableS3BucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3ClientIam;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.files.gcs.GcsObjectIO;
import org.projectnessie.catalog.files.gcs.GcsStorageSupplier;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3Credentials;
import org.projectnessie.catalog.files.s3.S3CredentialsResolver;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

@ExtendWith(SoftAssertionsExtension.class)
class TestObjectIO {
  private static final SecretsProvider secretsProvider =
      ResolvingSecretsProvider.builder()
          .putSecretsManager("plain", unsafePlainTextSecretsProvider(Map.of()))
          .build();

  @InjectSoftAssertions protected SoftAssertions soft;

  private static S3ObjectIO s3(BucketOptions options) {
    return s3(options, null);
  }

  private static S3ObjectIO s3(BucketOptions options, S3CredentialsResolver credentialsResolver) {
    S3Options s3Options =
        ImmutableS3Options.builder()
            .defaultOptions(ImmutableS3BucketOptions.builder().from(options).build())
            .build();
    return new S3ObjectIO(
        new S3ClientSupplier(null, s3Options, null, secretsProvider), credentialsResolver);
  }

  private static GcsObjectIO gcs(BucketOptions options) {
    GcsStorageSupplier supplier =
        new GcsStorageSupplier(
            null,
            ImmutableGcsConfig.builder().build(),
            ImmutableGcsOptions.builder()
                .defaultOptions(ImmutableGcsBucketOptions.builder().from(options).build())
                .build(),
            secretsProvider);
    return new GcsObjectIO(supplier);
  }

  private static AdlsObjectIO adls(BucketOptions options) {
    AdlsClientSupplier supplier =
        new AdlsClientSupplier(
            null,
            ImmutableAdlsConfig.builder().build(),
            ImmutableAdlsOptions.builder()
                .defaultOptions(ImmutableAdlsFileSystemOptions.builder().from(options).build())
                .build(),
            secretsProvider);
    return new AdlsObjectIO(supplier);
  }

  public static Stream<Arguments> tableConfigWithOverrides() {
    BucketOptions defaults = ImmutableS3BucketOptions.builder().build();
    BucketOptions overrides =
        ImmutableS3BucketOptions.builder()
            .putTableConfigOverrides(PYICEBERG_FILE_IO_IMPL, "FileIOOverride1")
            .putTableConfigOverrides("prop1", "test-value-1")
            .build();
    return Stream.of(
        Arguments.of(
            s3(defaults), "s3://wh", PYICEBERG_FILE_IO_IMPL, "pyiceberg.io.fsspec.FsspecFileIO"),
        Arguments.of(s3(overrides), "s3://wh", PYICEBERG_FILE_IO_IMPL, "FileIOOverride1"),
        Arguments.of(s3(overrides), "s3://wh", "prop1", "test-value-1"),
        Arguments.of(
            gcs(defaults), "gs://wh", PYICEBERG_FILE_IO_IMPL, "pyiceberg.io.fsspec.FsspecFileIO"),
        Arguments.of(gcs(overrides), "gs://wh", PYICEBERG_FILE_IO_IMPL, "FileIOOverride1"),
        Arguments.of(gcs(overrides), "gs://wh", "prop1", "test-value-1"),
        Arguments.of(
            adls(defaults),
            "abfss://wh",
            PYICEBERG_FILE_IO_IMPL,
            "pyiceberg.io.fsspec.FsspecFileIO"),
        Arguments.of(adls(overrides), "abfss://wh", PYICEBERG_FILE_IO_IMPL, "FileIOOverride1"),
        Arguments.of(adls(overrides), "abfss://wh", "prop1", "test-value-1"));
  }

  @ParameterizedTest
  @MethodSource
  void tableConfigWithOverrides(
      ObjectIO objectIO, String warehouseLocation, String propName, String expectedValue) {
    StorageUri whUri = StorageUri.of(warehouseLocation);
    StorageLocations storageLocations =
        StorageLocations.storageLocations(whUri, List.of(whUri), List.of());
    Map<String, String> config = new HashMap<>();

    objectIO.configureIcebergTable(storageLocations, config::put, (p, c) -> {}, x -> false, false);
    soft.assertThat(config).containsEntry(propName, expectedValue);

    config.clear();
    objectIO.configureIcebergTable(storageLocations, config::put, (p, c) -> {}, x -> true, true);
    soft.assertThat(config).containsEntry(propName, expectedValue);
  }

  @ParameterizedTest
  @MethodSource
  void s3VendedCredentials(String warehouseLocation, String tableLocation) {
    S3CredentialsResolver resolver = mock(S3CredentialsResolver.class);
    S3Credentials credentials =
        new S3Credentials(
            AwsSessionCredentials.builder()
                .accessKeyId("key")
                .secretAccessKey("secret")
                .sessionToken("token")
                .expirationTime(Instant.ofEpochMilli(12345))
                .build());

    StorageUri whUri = StorageUri.of(warehouseLocation);
    StorageUri tableUri = StorageUri.of(tableLocation);
    StorageLocations storageLocations =
        StorageLocations.storageLocations(whUri, List.of(tableUri), List.of());

    var bucketOptions =
        ImmutableS3BucketOptions.builder()
            .region("us-east-1")
            .clientIam(ImmutableS3ClientIam.builder().enabled(true).assumeRole("role").build())
            .build();
    when(resolver.resolveSessionCredentials(any(), eq(storageLocations))).thenReturn(credentials);

    Map<String, String> config = new HashMap<>();
    Map<String, Map<String, String>> storageCredentials = new HashMap<>();

    s3(bucketOptions, resolver)
        .configureIcebergTable(
            storageLocations, config::put, storageCredentials::put, x -> false, true);

    soft.assertThat(config)
        .containsEntry("s3.access-key-id", "key")
        .containsEntry("s3.secret-access-key", "secret")
        .containsEntry("s3.session-token", "token")
        .containsEntry("s3.session-token-expires-at-ms", "12345");
    soft.assertThat(storageCredentials)
        .containsKeys("s3://bucket/warehouse", "s3://bucket/warehouse/table");
    soft.assertThat(storageCredentials.values())
        .allSatisfy(
            credential ->
                soft.assertThat(credential)
                    .containsEntry("s3.access-key-id", "key")
                    .containsEntry("s3.secret-access-key", "secret")
                    .containsEntry("s3.session-token", "token")
                    .containsEntry("s3.session-token-expires-at-ms", "12345"));
  }

  static Stream<Arguments> s3VendedCredentials() {
    return Stream.of(Arguments.of("s3a://bucket/warehouse", "s3a://bucket/warehouse/table"));
  }
}
