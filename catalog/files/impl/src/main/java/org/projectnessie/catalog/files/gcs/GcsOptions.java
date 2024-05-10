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

import java.util.Map;
import java.util.Optional;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public interface GcsOptions<PER_BUCKET extends GcsBucketOptions> extends GcsBucketOptions {

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the values from {@link GcsOptions}.
   */
  @ConfigPropertyName("bucket-name")
  Map<String, PER_BUCKET> buckets();

  default GcsBucketOptions effectiveOptionsForBucket(Optional<String> bucketName) {
    if (bucketName.isEmpty()) {
      return this;
    }
    GcsBucketOptions perBucket = buckets().get(bucketName.get());
    if (perBucket == null) {
      return this;
    }

    return GcsProgrammaticOptions.GcsPerBucketOptions.builder().from(this).from(perBucket).build();
  }
}
