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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.service.config.SecretsValidation.FailureCategory.NOT_FOUND;
import static org.projectnessie.catalog.service.config.SecretsValidation.FailureCategory.TECHNICAL_ERROR;

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.config.AdlsConfig;
import org.projectnessie.catalog.files.config.ImmutableAdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsNamedFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsConfig;
import org.projectnessie.catalog.files.config.ImmutableGcsNamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.files.config.ImmutableS3BucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.files.config.ImmutableSecretStore;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsManager;
import org.projectnessie.catalog.service.config.SecretsValidation.SecretValidationFailure;

public class TestSecretsValidation {

  private static final RuntimeException FAILURE = new RuntimeException("failed");

  @ParameterizedTest
  @MethodSource
  public void smallryeConfigs(
      SmallryeConfigs configs, Collection<SecretValidationFailure> expectedFailures) {
    var secretsValidation =
        SecretsValidation.builder()
            .secretsProvider(
                ResolvingSecretsProvider.builder()
                    .putSecretsManager(
                        "plain",
                        new SecretsManager() {
                          @Override
                          public <S extends Secret> Optional<S> getSecret(
                              @Nonnull String name,
                              @Nonnull SecretType secretType,
                              @Nonnull Class<S> secretJavaType) {
                            if (name.equals("fail")) {
                              throw FAILURE;
                            }
                            return Optional.empty();
                          }
                        })
                    .build())
            .build();

    assertThat(secretsValidation.validateSmallryeConfigs(configs))
        .containsExactlyInAnyOrderElementsOf(expectedFailures);
  }

  @ParameterizedTest
  @MethodSource
  public void lakehouseConfig(
      LakehouseConfig config, Collection<SecretValidationFailure> expectedFailures) {
    var secretsValidation =
        SecretsValidation.builder()
            .secretsProvider(
                ResolvingSecretsProvider.builder()
                    .putSecretsManager(
                        "plain",
                        new SecretsManager() {
                          @Override
                          public <S extends Secret> Optional<S> getSecret(
                              @Nonnull String name,
                              @Nonnull SecretType secretType,
                              @Nonnull Class<S> secretJavaType) {
                            if (name.equals("fail")) {
                              throw FAILURE;
                            }
                            return Optional.empty();
                          }
                        })
                    .build())
            .build();

    assertThat(secretsValidation.validateLakehouseConfig(config))
        .containsExactlyInAnyOrderElementsOf(expectedFailures);
  }

  static Stream<Arguments> lakehouseConfig() {
    URI noManager = URI.create("urn:nessie-secret:foo:bar");
    URI notFound = URI.create("urn:nessie-secret:plain:bar");
    URI fail = URI.create("urn:nessie-secret:plain:fail");

    Supplier<ImmutableLakehouseConfig.Builder> builder =
        () ->
            ImmutableLakehouseConfig.builder()
                .adls(ImmutableAdlsOptions.builder().build())
                .gcs(ImmutableGcsOptions.builder().build())
                .s3(ImmutableS3Options.builder().build())
                .catalog(ImmutableCatalogConfig.builder().build());

    return Stream.of(
        arguments(builder.get().build(), List.of()),
        // 1
        arguments(
            builder
                .get()
                .s3(
                    ImmutableS3Options.builder()
                        .defaultOptions(
                            ImmutableS3BucketOptions.builder().accessKey(notFound).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("s3", "<default>", "accessKey"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        // 2
        arguments(
            builder
                .get()
                .s3(
                    ImmutableS3Options.builder()
                        .putBucket(
                            "foo",
                            ImmutableS3NamedBucketOptions.builder().accessKey(noManager).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("s3", "foo", "accessKey"),
                    noManager,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        // 3
        arguments(
            builder
                .get()
                .gcs(
                    ImmutableGcsOptions.builder()
                        .defaultOptions(
                            ImmutableGcsBucketOptions.builder()
                                .authCredentialsJson(notFound)
                                .build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("gcs", "<default>", "authCredentialsJson"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        // 4
        arguments(
            builder
                .get()
                .gcs(
                    ImmutableGcsOptions.builder()
                        .defaultOptions(
                            ImmutableGcsBucketOptions.builder().decryptionKey(notFound).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("gcs", "<default>", "decryptionKey"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        // 5
        arguments(
            builder
                .get()
                .gcs(
                    ImmutableGcsOptions.builder()
                        .defaultOptions(
                            ImmutableGcsBucketOptions.builder().encryptionKey(notFound).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("gcs", "<default>", "encryptionKey"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        // 6
        arguments(
            builder
                .get()
                .gcs(
                    ImmutableGcsOptions.builder()
                        .putBucket(
                            "foo",
                            ImmutableGcsNamedBucketOptions.builder()
                                .authCredentialsJson(noManager)
                                .build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("gcs", "foo", "authCredentialsJson"),
                    noManager,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        // 7
        arguments(
            builder
                .get()
                .adls(
                    ImmutableAdlsOptions.builder()
                        .defaultOptions(
                            ImmutableAdlsFileSystemOptions.builder().account(notFound).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("adls", "<default>", "account"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        arguments(
            builder
                .get()
                .adls(
                    ImmutableAdlsOptions.builder()
                        .defaultOptions(
                            ImmutableAdlsFileSystemOptions.builder().sasToken(notFound).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("adls", "<default>", "sasToken"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        arguments(
            builder
                .get()
                .adls(
                    ImmutableAdlsOptions.builder()
                        .putFileSystem(
                            "foo",
                            ImmutableAdlsNamedFileSystemOptions.builder()
                                .account(noManager)
                                .build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("adls", "foo", "account"),
                    noManager,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        //
        arguments(
            builder
                .get()
                .s3(
                    ImmutableS3Options.builder()
                        .defaultOptions(
                            ImmutableS3BucketOptions.builder().accessKey(notFound).build())
                        .build())
                .gcs(
                    ImmutableGcsOptions.builder()
                        .defaultOptions(
                            ImmutableGcsBucketOptions.builder()
                                .authCredentialsJson(notFound)
                                .decryptionKey(notFound)
                                .encryptionKey(notFound)
                                .build())
                        .build())
                .adls(
                    ImmutableAdlsOptions.builder()
                        .defaultOptions(
                            ImmutableAdlsFileSystemOptions.builder()
                                .account(notFound)
                                .sasToken(notFound)
                                .build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("s3", "<default>", "accessKey"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"),
                ImmutableSecretValidationFailure.of(
                    List.of("gcs", "<default>", "authCredentialsJson"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"),
                ImmutableSecretValidationFailure.of(
                    List.of("gcs", "<default>", "decryptionKey"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"),
                ImmutableSecretValidationFailure.of(
                    List.of("gcs", "<default>", "encryptionKey"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"),
                ImmutableSecretValidationFailure.of(
                    List.of("adls", "<default>", "account"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"),
                ImmutableSecretValidationFailure.of(
                    List.of("adls", "<default>", "sasToken"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))));
  }

  static Stream<Arguments> smallryeConfigs() {
    URI noManager = URI.create("urn:nessie-secret:foo:bar");
    URI notFound = URI.create("urn:nessie-secret:plain:bar");
    URI fail = URI.create("urn:nessie-secret:plain:fail");

    Supplier<ImmutableSmallryeConfigs.Builder> forS3config =
        () ->
            ImmutableSmallryeConfigs.builder()
                .usePersistedLakehouseConfig(false) // doesn't have any effect in this test
                .validateSecrets(true) // doesn't have any effect in this test
                .s3(ImmutableS3Options.builder().build())
                .gcs(ImmutableGcsOptions.builder().build())
                .gcsConfig(ImmutableGcsConfig.builder().build())
                .adls(ImmutableAdlsOptions.builder().build())
                .adlsconfig(AdlsConfig.builder().build())
                .serviceConfig(
                    ImmutableServiceConfig.builder().objectStoresHealthCheck(false).build())
                .catalog(ImmutableCatalogConfig.builder().build());

    return Stream.of(
        arguments(forS3config.get().s3config(S3Config.builder().build()).build(), List.of()),
        arguments(
            forS3config
                .get()
                .s3config(
                    S3Config.builder()
                        .keyStore(ImmutableSecretStore.builder().password(notFound).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("s3", "keyStore"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        arguments(
            forS3config
                .get()
                .s3config(
                    S3Config.builder()
                        .trustStore(ImmutableSecretStore.builder().password(notFound).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("s3", "trustStore"),
                    notFound,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        arguments(
            forS3config
                .get()
                .s3config(
                    S3Config.builder()
                        .trustStore(ImmutableSecretStore.builder().password(noManager).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("s3", "trustStore"),
                    noManager,
                    NOT_FOUND,
                    Optional.empty(),
                    "secret does not exist"))),
        arguments(
            forS3config
                .get()
                .s3config(
                    S3Config.builder()
                        .trustStore(ImmutableSecretStore.builder().password(fail).build())
                        .build())
                .build(),
            List.of(
                ImmutableSecretValidationFailure.of(
                    List.of("s3", "trustStore"),
                    fail,
                    TECHNICAL_ERROR,
                    Optional.of(FAILURE),
                    "java.lang.RuntimeException: failed"))));
  }
}
