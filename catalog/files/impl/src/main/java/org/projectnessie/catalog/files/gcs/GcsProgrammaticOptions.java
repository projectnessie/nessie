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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@Value.Style(allParameters = false)
@SuppressWarnings("immutables:from") // defaultOptions + buckets are not copied
public abstract class GcsProgrammaticOptions implements GcsOptions {

  @Override
  public abstract Optional<GcsBucketOptions> defaultOptions();

  @Override
  public abstract Map<String, GcsNamedBucketOptions> buckets();

  public static GcsOptions normalize(GcsOptions gcsOptions) {
    ImmutableGcsProgrammaticOptions.Builder builder =
        ImmutableGcsProgrammaticOptions.builder().from(gcsOptions);
    // not copied by from() because of different type parameters in method return types
    builder.defaultOptions(gcsOptions.defaultOptions());
    builder.buckets(gcsOptions.buckets());
    return builder.build();
  }

  @Value.Check
  protected GcsProgrammaticOptions normalizeBuckets() {
    Map<String, GcsNamedBucketOptions> buckets = new HashMap<>();
    for (String bucketName : buckets().keySet()) {
      GcsNamedBucketOptions options = buckets().get(bucketName);
      if (options.name().isPresent()) {
        bucketName = options.name().get();
      } else {
        options = ImmutableGcsNamedBucketOptions.builder().from(options).name(bucketName).build();
      }
      if (buckets.put(bucketName, options) != null) {
        throw new IllegalArgumentException(
            "Duplicate GCS bucket name '" + bucketName + "', check your GCS bucket configurations");
      }
    }
    if (buckets.equals(buckets())) {
      return this;
    }
    return ImmutableGcsProgrammaticOptions.builder()
        .from(this)
        .defaultOptions(defaultOptions())
        .buckets(buckets)
        .build();
  }
}
