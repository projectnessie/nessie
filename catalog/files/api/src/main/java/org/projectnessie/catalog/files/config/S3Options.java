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

@NessieImmutable
@JsonSerialize(as = ImmutableS3Options.class)
@JsonDeserialize(as = ImmutableS3Options.class)
public interface S3Options {

  /**
   * Default bucket configuration, default/fallback values for all buckets are taken from this one.
   */
  @ConfigItem(section = "default-options")
  Optional<S3BucketOptions> defaultOptions();

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the defaults from the top-level S3 settings
   * in {@code default-options}.
   */
  @ConfigItem(section = "buckets")
  @ConfigPropertyName("key")
  Map<String, S3NamedBucketOptions> buckets();

  default S3NamedBucketOptions resolveOptionsForUri(StorageUri uri) {
    Optional<S3NamedBucketOptions> specific = resolveSpecializedBucket(uri, buckets());

    ImmutableS3NamedBucketOptions.Builder builder = ImmutableS3NamedBucketOptions.builder();
    defaultOptions().ifPresent(builder::from);
    specific.ifPresent(builder::from);
    ImmutableS3ServerIam.Builder serverIam = ImmutableS3ServerIam.builder();
    ImmutableS3ClientIam.Builder clientIam = ImmutableS3ClientIam.builder();
    defaultOptions()
        .ifPresent(
            d -> {
              d.serverIam().ifPresent(serverIam::from);
              d.clientIam().ifPresent(clientIam::from);
            });
    specific.ifPresent(
        d -> {
          d.serverIam().ifPresent(serverIam::from);
          d.clientIam().ifPresent(clientIam::from);
        });
    builder.serverIam(serverIam.build());
    builder.clientIam(clientIam.build());

    return builder.build();
  }

  default S3Options validate() {
    defaultOptions().ifPresent(options -> options.validate("<default>"));
    buckets().forEach((key, opts) -> opts.validate(opts.name().orElse(key)));
    return this;
  }

  @Value.NonAttribute
  @JsonIgnore
  default S3Options deepClone() {
    ImmutableS3Options.Builder b = ImmutableS3Options.builder().from(this).buckets(Map.of());
    defaultOptions().ifPresent(v -> b.defaultOptions(v.deepClone()));
    buckets().forEach((n, v) -> b.putBucket(n, v.deepClone()));
    return b.build();
  }
}
