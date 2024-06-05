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
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.catalog.files.s3.S3ProgrammaticOptions.S3PerBucketOptions;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.SecretAttribute;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public interface S3Options<PER_BUCKET extends S3BucketOptions> extends S3BucketOptions {

  /** Default value for {@link #sessionCredentialCacheMaxEntries()}. */
  int DEFAULT_MAX_SESSION_CREDENTIAL_CACHE_ENTRIES = 1000;

  /** Default value for {@link #stsClientsCacheMaxEntries()}. */
  int DEFAULT_MAX_STS_CLIENT_CACHE_ENTRIES = 50;

  /** Default value for {@link #sessionCredentialRefreshGracePeriod()}. */
  Duration DEFAULT_SESSION_REFRESH_GRACE_PERIOD = Duration.ofMinutes(5);

  /**
   * The default endpoint override to use, if not configured per bucket (see {@code buckets}). The
   * endpoint must be specified for private (non-AWS) clouds, either per bucket or here.
   *
   * <p>If the endpoint URIs for the Nessie server and clients differ, this one defines the endpoint
   * used for the Nessie server.
   */
  @Override
  Optional<URI> endpoint();

  /**
   * When using a specific {@code endpoint} and the endpoint URIs for the Nessie server differ, you
   * can specify the URI passed down to clients using this setting. Otherwise, clients will receive
   * the value from the {@code endpoint} setting.
   */
  @Override
  Optional<URI> externalEndpoint();

  /**
   * Whether to use path-style access. If true, path-style access will be used, as in: {@code
   * https://<domain>/<bucket>}. If false, a virtual-hosted style will be used instead, as in:
   * {@code https://<bucket>.<domain>}. If unspecified, the default will depend on the cloud
   * provider.
   */
  @Override
  Optional<Boolean> pathStyleAccess();

  /**
   * The default DNS name of the region to use, if not configured per bucket. The region must be
   * specified for AWS, either per bucket or here.
   */
  @Override
  Optional<String> region();

  /**
   * The default access-key-id and secret-access-key to use, if not configured per bucket. An
   * access-key-id must be configured, either per bucket or here.
   */
  @Override
  Optional<BasicCredentials> accessKey();

  /**
   * The time period to subtract from the S3 session credentials (assumed role credentials) expiry
   * time to define the time when those credentials become eligible for refreshing.
   */
  Optional<Duration> sessionCredentialRefreshGracePeriod();

  default Duration effectiveSessionCredentialRefreshGracePeriod() {
    return sessionCredentialRefreshGracePeriod().orElse(DEFAULT_SESSION_REFRESH_GRACE_PERIOD);
  }

  /**
   * Maximum number of entries to keep in the session credentials cache (assumed role credentials).
   */
  OptionalInt sessionCredentialCacheMaxEntries();

  default int effectiveSessionCredentialCacheMaxEntries() {
    return sessionCredentialCacheMaxEntries().orElse(DEFAULT_MAX_SESSION_CREDENTIAL_CACHE_ENTRIES);
  }

  /** Maximum number of entries to keep in the STS clients cache. */
  OptionalInt stsClientsCacheMaxEntries();

  default int effectiveStsClientsCacheMaxEntries() {
    return stsClientsCacheMaxEntries().orElse(DEFAULT_MAX_STS_CLIENT_CACHE_ENTRIES);
  }

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the values from top-level S3 settings.
   */
  @ConfigPropertyName("bucket-name")
  Map<String, PER_BUCKET> buckets();

  default S3BucketOptions effectiveOptionsForBucket(
      Optional<String> bucketName, SecretsProvider secretsProvider) {
    if (bucketName.isEmpty()) {
      return resolveSecrets(null, null, secretsProvider);
    }
    String name = bucketName.get();
    S3BucketOptions perBucket = buckets().get(name);
    if (perBucket == null) {
      return resolveSecrets(name, null, secretsProvider);
    }

    return resolveSecrets(name, perBucket, secretsProvider);
  }

  List<SecretAttribute<S3BucketOptions, S3PerBucketOptions.Builder, ?>> SECRET_ATTRIBUTES =
      ImmutableList.of(
          secretAttribute(
              "accessKey",
              SecretType.BASIC,
              S3BucketOptions::accessKey,
              S3PerBucketOptions.Builder::accessKey));

  default S3BucketOptions resolveSecrets(
      String filesystemName, S3BucketOptions specific, SecretsProvider secretsProvider) {
    S3PerBucketOptions.Builder builder = S3PerBucketOptions.builder().from(this);
    if (specific != null) {
      builder.from(specific);
    }

    return secretsProvider
        .applySecrets(
            builder, "object-stores.s3", this, filesystemName, specific, SECRET_ATTRIBUTES)
        .build();
  }
}
