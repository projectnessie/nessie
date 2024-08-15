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
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

/**
 * Configuration for Google Cloud Storage (GCS) object stores.
 *
 * <p>Default settings to be applied to all buckets can be set in the {@code default-options} group.
 * Specific settings for each bucket can be specified via the {@code buckets} map.
 *
 * <p>All settings are optional. The defaults of these settings are defined by the Google Java SDK
 * client.
 */
@ConfigMapping(prefix = "nessie.catalog.service.gcs")
public interface GcsOptions {

  /**
   * Default bucket configuration, default/fallback values for all buckets are taken from this one.
   */
  @ConfigItem(section = "default-options")
  Optional<GcsBucketOptions> defaultOptions();

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the defaults from the top-level GCS
   * settings.
   */
  @ConfigItem(section = "buckets")
  @ConfigPropertyName("bucket-name")
  Map<String, GcsNamedBucketOptions> buckets();

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

  default GcsBucketOptions effectiveOptionsForBucket(Optional<String> bucketName) {
    GcsBucketOptions defaultOptions =
        defaultOptions().map(GcsBucketOptions.class::cast).orElse(GcsNamedBucketOptions.FALLBACK);

    if (bucketName.isEmpty()) {
      return defaultOptions;
    }

    GcsBucketOptions specific = buckets().get(bucketName.get());
    if (specific == null) {
      return defaultOptions;
    }

    return ImmutableGcsNamedBucketOptions.builder().from(defaultOptions).from(specific).build();
  }

  default void validate() {
    // nothing to validate
  }
}
