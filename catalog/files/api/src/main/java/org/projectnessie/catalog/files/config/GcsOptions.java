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

import static org.projectnessie.catalog.files.config.OptionsUtil.resolveSpecializedBucket;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.storage.uri.StorageUri;

/**
 * Configuration for Google Cloud Storage (GCS) object stores.
 *
 * <p>Default settings to be applied to all buckets can be set in the {@code default-options} group.
 * Specific settings for each bucket can be specified via the {@code buckets} map.
 *
 * <p>All settings are optional. The defaults of these settings are defined by the Google Java SDK
 * client.
 */
@NessieImmutable
@JsonSerialize(as = ImmutableGcsOptions.class)
@JsonDeserialize(as = ImmutableGcsOptions.class)
public interface GcsOptions {

  /**
   * Default bucket configuration, default/fallback values for all buckets are taken from this one.
   */
  @ConfigItem(section = "default-options")
  Optional<GcsBucketOptions> defaultOptions();

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the defaults from the top-level GCS settings
   * in {@code default-options}.
   */
  @ConfigItem(section = "buckets")
  @ConfigPropertyName("key")
  Map<String, GcsNamedBucketOptions> buckets();

  default GcsNamedBucketOptions resolveOptionsForUri(StorageUri uri) {
    Optional<GcsNamedBucketOptions> specific = resolveSpecializedBucket(uri, buckets());

    ImmutableGcsNamedBucketOptions.Builder builder = ImmutableGcsNamedBucketOptions.builder();
    defaultOptions().ifPresent(builder::from);
    specific.ifPresent(builder::from);
    return builder.build();
  }

  default GcsOptions validate() {
    // nothing to validate
    return this;
  }

  @Value.NonAttribute
  @JsonIgnore
  default GcsOptions deepClone() {
    ImmutableGcsOptions.Builder b = ImmutableGcsOptions.builder().from(this).buckets(Map.of());
    defaultOptions().ifPresent(v -> b.defaultOptions(v.deepClone()));
    buckets().forEach((n, v) -> b.putBucket(n, v.deepClone()));
    return b.build();
  }
}
