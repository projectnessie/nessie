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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableS3Options.class)
@JsonDeserialize(as = ImmutableS3Options.class)
public interface S3Options {

  @ConfigItem(section = "sts")
  Optional<S3Sts> sts();

  @Value.NonAttribute
  @JsonIgnore
  default S3Sts effectiveSts() {
    return sts().orElse(ImmutableS3Sts.builder().build());
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

  default S3Options validate() {
    defaultOptions().ifPresent(options -> options.validate("<default>"));
    buckets().forEach((key, opts) -> opts.validate(opts.name().orElse(key)));
    return this;
  }

  @Value.Check
  default S3Options normalizeBuckets() {
    Map<String, S3NamedBucketOptions> buckets = new HashMap<>();
    boolean changed = false;
    for (String bucketName : buckets().keySet()) {
      S3NamedBucketOptions options = buckets().get(bucketName);
      if (options.name().isPresent()) {
        String explicitName = options.name().get();
        changed |= !explicitName.equals(bucketName);
        bucketName = options.name().get();
      } else {
        changed = true;
        options = ImmutableS3NamedBucketOptions.builder().from(options).name(bucketName).build();
      }
      if (buckets.put(bucketName, options) != null) {
        throw new IllegalArgumentException(
            "Duplicate S3 bucket name '" + bucketName + "', check your S3 bucket configurations");
      }
    }

    return changed
        ? ImmutableS3Options.builder()
            .from(this)
            .defaultOptions(defaultOptions())
            .buckets(buckets)
            .build()
        : this;
  }

  @Value.NonAttribute
  @JsonIgnore
  default S3Options deepClone() {
    ImmutableS3Options.Builder b = ImmutableS3Options.builder().from(this).buckets(Map.of());
    sts().ifPresent(v -> b.sts(ImmutableS3Sts.copyOf(v)));
    defaultOptions().ifPresent(v -> b.defaultOptions(v.deepClone()));
    buckets().forEach((n, v) -> b.putBucket(n, v.deepClone()));
    return b.build();
  }
}
