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
package org.projectnessie.quarkus.config;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;
import static org.projectnessie.catalog.secrets.TokenSecret.tokenSecret;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.TokenSecret;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCatalogSecretsConfig {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void adlsOptions(
      Map<String, String> configs,
      BasicCredentials top,
      KeySecret sas,
      BasicCredentials bucket1,
      KeySecret sas1,
      BasicCredentials bucket2,
      KeySecret sas2) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .setAddDefaultSources(false)
            .setAddDiscoveredSources(false)
            .withMapping(CatalogAdlsConfig.class)
            .withSources(new PropertiesConfigSource(configs, "configSource", 100))
            .build();

    CatalogAdlsConfig catalogConfig = config.getConfigMapping(CatalogAdlsConfig.class);
    Optional<CatalogAdlsFileSystemOptions> b1 =
        Optional.ofNullable(catalogConfig.fileSystems().get("bucket1"));
    Optional<CatalogAdlsFileSystemOptions> b2 =
        Optional.ofNullable(catalogConfig.fileSystems().get("bucket2"));

    soft.assertThat(basicMap(catalogConfig.account()))
        .containsExactlyInAnyOrderEntriesOf(basicMap(top));
    soft.assertThat(keyMap(catalogConfig.sasToken())).isEqualTo(keyMap(sas));
    soft.assertThat(basicMap(b1.flatMap(CatalogAdlsFileSystemOptions::account)))
        .containsExactlyInAnyOrderEntriesOf(basicMap(bucket1));
    soft.assertThat(keyMap(b1.flatMap(CatalogAdlsFileSystemOptions::sasToken)))
        .isEqualTo(keyMap(sas1));
    soft.assertThat(basicMap(b2.flatMap(CatalogAdlsFileSystemOptions::account)))
        .containsExactlyInAnyOrderEntriesOf(basicMap(bucket2));
    soft.assertThat(keyMap(b2.flatMap(CatalogAdlsFileSystemOptions::sasToken)))
        .isEqualTo(keyMap(sas2));
  }

  static Stream<Arguments> adlsOptions() {
    return Stream.of(
        arguments(
            Map.of(
                "nessie.catalog.service.adls.account.name", "id",
                "nessie.catalog.service.adls.account.secret", "secret",
                "nessie.catalog.service.adls.sas-token", "sas",
                "nessie.catalog.service.adls.file-systems.bucket1.account.name", "bucket-name",
                "nessie.catalog.service.adls.file-systems.bucket1.account.secret", "bucket-secret",
                "nessie.catalog.service.adls.file-systems.bucket2.sas-token", "bucket2-sas"),
            basicCredentials("id", "secret"),
            keySecret("sas"),
            basicCredentials("bucket-name", "bucket-secret"),
            null,
            null,
            keySecret("bucket2-sas")),
        arguments(
            Map.of(
                "nessie.catalog.service.adls.sas-token", "sas",
                "nessie.catalog.service.adls.file-systems.bucket1.account.name", "bucket-name",
                "nessie.catalog.service.adls.file-systems.bucket1.account.secret", "bucket-secret",
                "nessie.catalog.service.adls.file-systems.bucket2.account.name", "id",
                "nessie.catalog.service.adls.file-systems.bucket2.account.secret", "secret",
                "nessie.catalog.service.adls.file-systems.bucket2.sas-token", "bucket2-sas"),
            null,
            keySecret("sas"),
            basicCredentials("bucket-name", "bucket-secret"),
            null,
            basicCredentials("id", "secret"),
            keySecret("bucket2-sas"))
        //
        );
  }

  @ParameterizedTest
  @MethodSource
  public void gcsOptions(
      Map<String, String> configs,
      KeySecret ac,
      TokenSecret o2,
      KeySecret ek,
      KeySecret dk,
      KeySecret b1ac,
      TokenSecret b1o2,
      KeySecret b1ek,
      KeySecret b1dk,
      KeySecret b2ac,
      TokenSecret b2o2,
      KeySecret b2ek,
      KeySecret b2dk) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .setAddDefaultSources(false)
            .setAddDiscoveredSources(false)
            .withMapping(CatalogGcsConfig.class)
            .withSources(new PropertiesConfigSource(configs, "configSource", 100))
            .build();

    CatalogGcsConfig catalogConfig = config.getConfigMapping(CatalogGcsConfig.class);
    Optional<CatalogGcsBucketConfig> b1 =
        Optional.ofNullable(catalogConfig.buckets().get("bucket1"));
    Optional<CatalogGcsBucketConfig> b2 =
        Optional.ofNullable(catalogConfig.buckets().get("bucket2"));

    soft.assertThat(keyMap(catalogConfig.authCredentialsJson())).isEqualTo(keyMap(ac));
    soft.assertThat(keyMap(catalogConfig.decryptionKey())).isEqualTo(keyMap(dk));
    soft.assertThat(keyMap(catalogConfig.encryptionKey())).isEqualTo(keyMap(ek));
    soft.assertThat(expiringTokenMap(catalogConfig.oauth2Token()))
        .containsExactlyInAnyOrderEntriesOf(expiringTokenMap(o2));

    soft.assertThat(keyMap(b1.flatMap(CatalogGcsBucketConfig::authCredentialsJson)))
        .isEqualTo(keyMap(b1ac));
    soft.assertThat(keyMap(b1.flatMap(CatalogGcsBucketConfig::decryptionKey)))
        .isEqualTo(keyMap(b1dk));
    soft.assertThat(keyMap(b1.flatMap(CatalogGcsBucketConfig::encryptionKey)))
        .isEqualTo(keyMap(b1ek));
    soft.assertThat(expiringTokenMap(b1.flatMap(CatalogGcsBucketConfig::oauth2Token)))
        .containsExactlyInAnyOrderEntriesOf(expiringTokenMap(b1o2));

    soft.assertThat(keyMap(b2.flatMap(CatalogGcsBucketConfig::authCredentialsJson)))
        .isEqualTo(keyMap(b2ac));
    soft.assertThat(keyMap(b2.flatMap(CatalogGcsBucketConfig::decryptionKey)))
        .isEqualTo(keyMap(b2dk));
    soft.assertThat(keyMap(b2.flatMap(CatalogGcsBucketConfig::encryptionKey)))
        .isEqualTo(keyMap(b2ek));
    soft.assertThat(expiringTokenMap(b2.flatMap(CatalogGcsBucketConfig::oauth2Token)))
        .containsExactlyInAnyOrderEntriesOf(expiringTokenMap(b2o2));
  }

  static Stream<Arguments> gcsOptions() {
    return Stream.of(
        arguments(
            Map.of(
                "nessie.catalog.service.gcs.auth-credentials-json",
                "auth-cred",
                "nessie.catalog.service.gcs.oauth2-token.token",
                "oauth2",
                "nessie.catalog.service.gcs.oauth2-token.expires-at",
                "2024-12-24T12:12:12Z",
                "nessie.catalog.service.gcs.encryption-key",
                "enc-key",
                "nessie.catalog.service.gcs.decryption-key",
                "dec-key",
                "nessie.catalog.service.gcs.buckets.bucket1.oauth2-token.token",
                "b1-oauth2",
                "nessie.catalog.service.gcs.buckets.bucket1.decryption-key",
                "b1-dec",
                "nessie.catalog.service.gcs.buckets.bucket2.oauth2-token.token",
                "b2-oauth2",
                "nessie.catalog.service.gcs.buckets.bucket2.oauth2-token.expires-at",
                "2025-01-01T12:12:12Z",
                "nessie.catalog.service.gcs.buckets.bucket2.decryption-key",
                "b2-dec-key"),
            KeySecret.keySecret("auth-cred"),
            TokenSecret.tokenSecret("oauth2", Instant.parse("2024-12-24T12:12:12Z")),
            KeySecret.keySecret("enc-key"),
            KeySecret.keySecret("dec-key"),
            null,
            TokenSecret.tokenSecret("b1-oauth2", null),
            null,
            KeySecret.keySecret("b1-dec"),
            null,
            TokenSecret.tokenSecret("b2-oauth2", Instant.parse("2025-01-01T12:12:12Z")),
            null,
            KeySecret.keySecret("b2-dec-key"))
        //
        );
  }

  @ParameterizedTest
  @MethodSource
  public void s3Options(
      Map<String, String> configs,
      BasicCredentials top,
      BasicCredentials bucket1,
      BasicCredentials bucket2) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .setAddDefaultSources(false)
            .setAddDiscoveredSources(false)
            .withMapping(CatalogS3Config.class)
            .withSources(new PropertiesConfigSource(configs, "configSource", 100))
            .build();

    CatalogS3Config catalogConfig = config.getConfigMapping(CatalogS3Config.class);
    Optional<CatalogS3BucketConfig> b1 =
        Optional.ofNullable(catalogConfig.buckets().get("bucket1"));
    Optional<CatalogS3BucketConfig> b2 =
        Optional.ofNullable(catalogConfig.buckets().get("bucket2"));

    soft.assertThat(basicMap(catalogConfig.accessKey()))
        .containsExactlyInAnyOrderEntriesOf(basicMap(top));
    soft.assertThat(basicMap(b1.flatMap(CatalogS3BucketConfig::accessKey)))
        .containsExactlyInAnyOrderEntriesOf(basicMap(bucket1));
    soft.assertThat(basicMap(b2.flatMap(CatalogS3BucketConfig::accessKey)))
        .containsExactlyInAnyOrderEntriesOf(basicMap(bucket2));
  }

  static Stream<Arguments> s3Options() {
    return Stream.of(
        arguments(
            Map.of(
                "nessie.catalog.service.s3.access-key.name", "id",
                "nessie.catalog.service.s3.access-key.secret", "secret",
                "nessie.catalog.service.s3.buckets.bucket1.access-key.name", "bucket-name",
                "nessie.catalog.service.s3.buckets.bucket1.access-key.secret", "bucket-secret",
                "nessie.catalog.service.s3.buckets.bucket2.session-iam-policy", "bucket2-policy"),
            basicCredentials("id", "secret"),
            basicCredentials("bucket-name", "bucket-secret"),
            null),
        arguments(
            Map.of(
                "nessie.catalog.service.s3.buckets.bucket1.access-key.name",
                "bucket-name",
                "nessie.catalog.service.s3.buckets.bucket1.access-key.secret",
                "bucket-secret",
                "nessie.catalog.service.s3.buckets.bucket2.access-key.name",
                "bucket2-name",
                "nessie.catalog.service.s3.buckets.bucket2.access-key.secret",
                "bucket2-secret"),
            null,
            basicCredentials("bucket-name", "bucket-secret"),
            basicCredentials("bucket2-name", "bucket2-secret"))
        //
        );
  }

  static Map<String, String> basicMap(BasicCredentials basicCredentials) {
    return basicMap(Optional.ofNullable(basicCredentials));
  }

  static Map<String, String> basicMap(Optional<BasicCredentials> basicCredentials) {
    return basicCredentials
        .map(c -> Map.of("name", c.name(), "secret", c.secret()))
        .orElse(Map.of());
  }

  static Map<String, String> expiringTokenMap(TokenSecret tokenSecret) {
    return expiringTokenMap(Optional.ofNullable(tokenSecret));
  }

  static Map<String, String> expiringTokenMap(Optional<TokenSecret> expiringTokenCredentials) {
    return expiringTokenCredentials
        .map(
            c ->
                Map.of(
                    "token", c.token(), "secret", c.expiresAt().map(Instant::toString).orElse("")))
        .orElse(Map.of());
  }

  static String keyMap(KeySecret keySecret) {
    return keyMap(Optional.ofNullable(keySecret));
  }

  static String keyMap(Optional<KeySecret> keyCredentials) {
    return keyCredentials.map(KeySecret::key).orElse(null);
  }
}
