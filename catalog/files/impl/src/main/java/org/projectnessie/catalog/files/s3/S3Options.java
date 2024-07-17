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
package org.projectnessie.catalog.files.s3;

import static org.projectnessie.catalog.secrets.SecretAttribute.secretAttribute;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.catalog.secrets.SecretAttribute;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public interface S3Options {

  /** Default value for {@link #sessionCredentialCacheMaxEntries()}. */
  int DEFAULT_MAX_SESSION_CREDENTIAL_CACHE_ENTRIES = 1000;

  /** Default value for {@link #stsClientsCacheMaxEntries()}. */
  int DEFAULT_MAX_STS_CLIENT_CACHE_ENTRIES = 50;

  /** Default value for {@link #sessionCredentialRefreshGracePeriod()}. */
  Duration DEFAULT_SESSION_REFRESH_GRACE_PERIOD = Duration.ofMinutes(5);

  /**
   * The time period to subtract from the S3 session credentials (assumed role credentials) expiry
   * time to define the time when those credentials become eligible for refreshing.
   */
  @ConfigItem(section = "sts")
  Optional<Duration> sessionCredentialRefreshGracePeriod();

  default Duration effectiveSessionCredentialRefreshGracePeriod() {
    return sessionCredentialRefreshGracePeriod().orElse(DEFAULT_SESSION_REFRESH_GRACE_PERIOD);
  }

  /**
   * Maximum number of entries to keep in the session credentials cache (assumed role credentials).
   */
  @ConfigItem(section = "sts")
  OptionalInt sessionCredentialCacheMaxEntries();

  default int effectiveSessionCredentialCacheMaxEntries() {
    return sessionCredentialCacheMaxEntries().orElse(DEFAULT_MAX_SESSION_CREDENTIAL_CACHE_ENTRIES);
  }

  /** Maximum number of entries to keep in the STS clients cache. */
  @ConfigItem(section = "sts")
  OptionalInt stsClientsCacheMaxEntries();

  default int effectiveStsClientsCacheMaxEntries() {
    return stsClientsCacheMaxEntries().orElse(DEFAULT_MAX_STS_CLIENT_CACHE_ENTRIES);
  }

  /**
   * Default bucket configuration, default/fallback values for all buckets are taken from this one.
   */
  @ConfigItem(section = "default-options", firstIsSectionDoc = true)
  Optional<? extends S3BucketOptions> defaultOptions();

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the values from top-level S3 settings.
   */
  @ConfigItem(section = "buckets", firstIsSectionDoc = true)
  @ConfigPropertyName("bucket-name")
  Map<String, ? extends S3NamedBucketOptions> buckets();

  List<SecretAttribute<S3BucketOptions, ImmutableS3NamedBucketOptions.Builder, ?>>
      SECRET_ATTRIBUTES =
          ImmutableList.of(
              secretAttribute(
                  "accessKey",
                  SecretType.BASIC,
                  S3BucketOptions::accessKey,
                  ImmutableS3NamedBucketOptions.Builder::accessKey));

  default S3BucketOptions resolveSecrets(
      String filesystemName, S3BucketOptions specific, SecretsProvider secretsProvider) {
    S3BucketOptions defaultOptions =
        defaultOptions().map(S3BucketOptions.class::cast).orElse(S3NamedBucketOptions.FALLBACK);

    ImmutableS3NamedBucketOptions.Builder builder =
        ImmutableS3NamedBucketOptions.builder().from(defaultOptions);
    if (specific != null) {
      builder.from(specific);
    }

    return secretsProvider
        .applySecrets(
            builder,
            "object-stores.s3",
            defaultOptions,
            filesystemName,
            specific,
            SECRET_ATTRIBUTES)
        .build();
  }

  default S3BucketOptions effectiveOptionsForBucket(
      Optional<String> bucketName, SecretsProvider secretsProvider) {
    if (bucketName.isEmpty()) {
      return resolveSecrets(null, null, secretsProvider);
    }
    String name = bucketName.get();
    S3BucketOptions perBucket = buckets().get(name);
    return resolveSecrets(name, perBucket, secretsProvider);
  }
}
