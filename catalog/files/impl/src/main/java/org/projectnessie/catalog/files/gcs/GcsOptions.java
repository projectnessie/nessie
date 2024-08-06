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
package org.projectnessie.catalog.files.gcs;

import static org.projectnessie.catalog.secrets.SecretAttribute.secretAttribute;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.projectnessie.catalog.files.gcs.ImmutableGcsNamedBucketOptions.Builder;
import org.projectnessie.catalog.secrets.SecretAttribute;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public interface GcsOptions {

  /**
   * Default bucket configuration, default/fallback values for all buckets are taken from this one.
   */
  @ConfigItem(section = "default-options", firstIsSectionDoc = true)
  Optional<? extends GcsBucketOptions> defaultOptions();

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the defaults from the top-level GCS
   * settings.
   */
  @ConfigItem(section = "buckets", firstIsSectionDoc = true)
  @ConfigPropertyName("bucket-name")
  Map<String, ? extends GcsNamedBucketOptions> buckets();

  /** Override the default read timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> readTimeout();

  /** Override the default connection timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> connectTimeout();

  /** Override the default maximum number of attempts. */
  @ConfigItem(section = "transport")
  OptionalInt maxAttempts();

  /** Override the default logical request timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> logicalTimeout();

  /** Override the default total timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> totalTimeout();

  /** Override the default initial retry delay. */
  @ConfigItem(section = "transport")
  Optional<Duration> initialRetryDelay();

  /** Override the default maximum retry delay. */
  @ConfigItem(section = "transport")
  Optional<Duration> maxRetryDelay();

  /** Override the default retry delay multiplier. */
  @ConfigItem(section = "transport")
  OptionalDouble retryDelayMultiplier();

  /** Override the default initial RPC timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> initialRpcTimeout();

  /** Override the default maximum RPC timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> maxRpcTimeout();

  /** Override the default RPC timeout multiplier. */
  @ConfigItem(section = "transport")
  OptionalDouble rpcTimeoutMultiplier();

  List<SecretAttribute<GcsBucketOptions, Builder, ?>> SECRET_ATTRIBUTES =
      ImmutableList.of(
          secretAttribute(
              "authCredentialsJson",
              SecretType.KEY,
              GcsBucketOptions::authCredentialsJson,
              ImmutableGcsNamedBucketOptions.Builder::authCredentialsJson),
          secretAttribute(
              "oauth2Token",
              SecretType.EXPIRING_TOKEN,
              GcsBucketOptions::oauth2Token,
              ImmutableGcsNamedBucketOptions.Builder::oauth2Token),
          secretAttribute(
              "encryptionKey",
              SecretType.KEY,
              GcsBucketOptions::encryptionKey,
              ImmutableGcsNamedBucketOptions.Builder::encryptionKey),
          secretAttribute(
              "decryptionKey",
              SecretType.KEY,
              GcsBucketOptions::decryptionKey,
              ImmutableGcsNamedBucketOptions.Builder::decryptionKey));

  default GcsBucketOptions resolveSecrets(
      String filesystemName, GcsBucketOptions specific, SecretsProvider secretsProvider) {
    GcsBucketOptions defaultOptions =
        defaultOptions().map(GcsBucketOptions.class::cast).orElse(GcsNamedBucketOptions.FALLBACK);

    ImmutableGcsNamedBucketOptions.Builder builder =
        ImmutableGcsNamedBucketOptions.builder().from(defaultOptions);
    if (specific != null) {
      builder.from(specific);
    }

    return secretsProvider
        .applySecrets(
            builder,
            "object-stores.gcs",
            defaultOptions,
            filesystemName,
            specific,
            SECRET_ATTRIBUTES)
        .build();
  }

  default GcsBucketOptions effectiveOptionsForBucket(
      Optional<String> bucketName, SecretsProvider secretsProvider) {
    if (bucketName.isEmpty()) {
      return resolveSecrets(null, null, secretsProvider);
    }
    String name = bucketName.get();
    GcsBucketOptions perBucket = buckets().get(name);
    return resolveSecrets(name, perBucket, secretsProvider);
  }

  default void validate() {
    // nothing to validate
  }
}
