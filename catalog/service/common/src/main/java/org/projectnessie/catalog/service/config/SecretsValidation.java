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

import static org.projectnessie.catalog.secrets.SecretType.BASIC;
import static org.projectnessie.catalog.secrets.SecretType.EXPIRING_TOKEN;
import static org.projectnessie.catalog.secrets.SecretType.KEY;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.AdlsOptions;
import org.projectnessie.catalog.files.config.GcsBucketOptions;
import org.projectnessie.catalog.files.config.GcsOptions;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.files.config.SecretStore;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.secrets.TokenSecret;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public abstract class SecretsValidation {
  public abstract SecretsProvider secretsProvider();

  public static ImmutableSecretsValidation.Builder builder() {
    return ImmutableSecretsValidation.builder();
  }

  public Collection<SecretValidationFailure> validateLakehouseConfig(
      LakehouseConfig lakehouseConfig) {
    var failures = new ArrayList<SecretValidationFailure>();
    failures.addAll(validateS3Options(lakehouseConfig.s3()));
    failures.addAll(validateGcsOptions(lakehouseConfig.gcs()));
    failures.addAll(validateAdlsOptions(lakehouseConfig.adls()));
    return failures;
  }

  public Collection<SecretValidationFailure> validateSmallryeConfigs(
      SmallryeConfigs smallryeConfigs) {
    return validateS3Config(smallryeConfigs.s3config());
  }

  public Collection<SecretValidationFailure> validateS3Options(S3Options s3) {
    var failures = new ArrayList<SecretValidationFailure>();
    s3.defaultOptions().map(b -> validateS3Bucket(b, "<default>")).ifPresent(failures::addAll);
    s3.buckets()
        .forEach(
            (key, value) -> failures.addAll(validateS3Bucket(value, value.name().orElse(key))));
    return failures;
  }

  public Collection<SecretValidationFailure> validateS3Config(S3Config s3) {
    var failures = new ArrayList<SecretValidationFailure>();
    s3.keyStore().map(b -> validateS3SecretStore(b, "keyStore")).ifPresent(failures::addAll);
    s3.trustStore().map(b -> validateS3SecretStore(b, "trustStore")).ifPresent(failures::addAll);
    return failures;
  }

  public Collection<SecretValidationFailure> validateS3SecretStore(
      SecretStore secretStore, String store) {
    var failures = new ArrayList<SecretValidationFailure>();
    validateSecret(secretStore.password(), KEY, List.of("s3", store)).ifPresent(failures::add);
    return failures;
  }

  public Collection<SecretValidationFailure> validateGcsOptions(GcsOptions gcs) {
    var failures = new ArrayList<SecretValidationFailure>();
    gcs.defaultOptions().map(b -> validateGcsBucket(b, "<default>")).ifPresent(failures::addAll);
    gcs.buckets()
        .forEach(
            (key, value) -> failures.addAll(validateGcsBucket(value, value.name().orElse(key))));
    return failures;
  }

  public Collection<SecretValidationFailure> validateAdlsOptions(AdlsOptions adls) {
    var failures = new ArrayList<SecretValidationFailure>();
    adls.defaultOptions()
        .map(fs -> validateAdlsFileSystem(fs, "<default>"))
        .ifPresent(failures::addAll);
    adls.fileSystems()
        .forEach(
            (key, value) ->
                failures.addAll(validateAdlsFileSystem(value, value.name().orElse(key))));
    return failures;
  }

  public Collection<SecretValidationFailure> validateS3Bucket(S3BucketOptions b, String name) {
    var failures = new ArrayList<SecretValidationFailure>();
    validateSecret(b.accessKey(), BASIC, List.of("s3", name, "accessKey")).ifPresent(failures::add);
    return failures;
  }

  public Collection<SecretValidationFailure> validateGcsBucket(GcsBucketOptions b, String name) {
    var failures = new ArrayList<SecretValidationFailure>();
    validateSecret(b.authCredentialsJson(), KEY, List.of("gcs", name, "authCredentialsJson"))
        .ifPresent(failures::add);
    validateSecret(b.oauth2Token(), EXPIRING_TOKEN, List.of("gcs", name, "oauth2Token"))
        .ifPresent(failures::add);
    validateSecret(b.decryptionKey(), KEY, List.of("gcs", name, "decryptionKey"))
        .ifPresent(failures::add);
    validateSecret(b.encryptionKey(), KEY, List.of("gcs", name, "encryptionKey"))
        .ifPresent(failures::add);
    return failures;
  }

  private Collection<SecretValidationFailure> validateAdlsFileSystem(
      AdlsFileSystemOptions fs, String name) {
    var failures = new ArrayList<SecretValidationFailure>();
    validateSecret(fs.account(), BASIC, List.of("adls", name, "account")).ifPresent(failures::add);
    validateSecret(fs.sasToken(), KEY, List.of("adls", name, "sasToken")).ifPresent(failures::add);
    return failures;
  }

  private Optional<SecretValidationFailure> validateSecret(
      Optional<URI> uri, SecretType secretType, List<String> propertyPath) {
    if (uri.isEmpty()) {
      return Optional.empty();
    }

    Class<? extends Secret> javaType;
    switch (secretType) {
      case BASIC:
        javaType = BasicCredentials.class;
        break;
      case KEY:
        javaType = KeySecret.class;
        break;
      case EXPIRING_TOKEN:
        javaType = TokenSecret.class;
        break;
      default:
        throw new IllegalArgumentException("Invalid secret type: " + secretType);
    }

    try {
      if (secretsProvider().getSecret(uri.get(), secretType, javaType).isPresent()) {
        return Optional.empty();
      }
      return Optional.of(
          ImmutableSecretValidationFailure.of(
              propertyPath,
              uri.get(),
              FailureCategory.NOT_FOUND,
              Optional.empty(),
              "secret does not exist"));
    } catch (Exception e) {
      return Optional.of(
          ImmutableSecretValidationFailure.of(
              propertyPath,
              uri.get(),
              FailureCategory.TECHNICAL_ERROR,
              Optional.of(e),
              e.toString()));
    }
  }

  @NessieImmutable
  public interface SecretValidationFailure {

    List<String> propertyPath();

    URI uri();

    FailureCategory failureCategory();

    Optional<Throwable> failure();

    String message();

    static ImmutableSecretValidationFailure.Builder builder() {
      return ImmutableSecretValidationFailure.builder();
    }
  }

  public enum FailureCategory {
    NOT_FOUND,
    TECHNICAL_ERROR,
  }
}
