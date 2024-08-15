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
package org.projectnessie.catalog.files.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

@ConfigMapping(prefix = "nessie.catalog.service.s3")
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
  @WithName("sts.session-grace-period")
  Optional<Duration> sessionCredentialRefreshGracePeriod();

  default Duration effectiveSessionCredentialRefreshGracePeriod() {
    return sessionCredentialRefreshGracePeriod().orElse(DEFAULT_SESSION_REFRESH_GRACE_PERIOD);
  }

  /**
   * Maximum number of entries to keep in the session credentials cache (assumed role credentials).
   */
  @ConfigItem(section = "sts")
  @WithName("sts.session-cache-max-size")
  OptionalInt sessionCredentialCacheMaxEntries();

  default int effectiveSessionCredentialCacheMaxEntries() {
    return sessionCredentialCacheMaxEntries().orElse(DEFAULT_MAX_SESSION_CREDENTIAL_CACHE_ENTRIES);
  }

  /** Maximum number of entries to keep in the STS clients cache. */
  @ConfigItem(section = "sts")
  @WithName("sts.clients-cache-max-size")
  OptionalInt stsClientsCacheMaxEntries();

  default int effectiveStsClientsCacheMaxEntries() {
    return stsClientsCacheMaxEntries().orElse(DEFAULT_MAX_STS_CLIENT_CACHE_ENTRIES);
  }

  /**
   * Default bucket configuration, default/fallback values for all buckets are taken from this one.
   */
  @ConfigItem(section = "default-options")
  Optional<S3BucketOptions> defaultOptions();

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the values from top-level S3 settings.
   */
  @ConfigItem(section = "buckets")
  @ConfigPropertyName("bucket-name")
  Map<String, S3NamedBucketOptions> buckets();

  default S3BucketOptions effectiveOptionsForBucket(Optional<String> bucketName) {
    S3BucketOptions defaultOptions =
        defaultOptions().map(S3BucketOptions.class::cast).orElse(S3NamedBucketOptions.FALLBACK);

    if (bucketName.isEmpty()) {
      return defaultOptions;
    }

    S3BucketOptions specific = buckets().get(bucketName.orElse(null));
    if (specific == null) {
      return defaultOptions;
    }

    ImmutableS3NamedBucketOptions.Builder builder =
        ImmutableS3NamedBucketOptions.builder().from(defaultOptions).from(specific);
    ImmutableS3ServerIam.Builder serverIam = ImmutableS3ServerIam.builder();
    ImmutableS3ClientIam.Builder clientIam = ImmutableS3ClientIam.builder();
    defaultOptions.serverIam().ifPresent(serverIam::from);
    defaultOptions.clientIam().ifPresent(clientIam::from);
    specific.serverIam().ifPresent(serverIam::from);
    specific.clientIam().ifPresent(clientIam::from);
    builder.serverIam(serverIam.build());
    builder.clientIam(clientIam.build());

    return builder.build();
  }

  default void validate() {
    defaultOptions().ifPresent(options -> options.validate("<default>"));
    buckets().forEach((key, opts) -> opts.validate(opts.name().orElse(key)));
  }
}
