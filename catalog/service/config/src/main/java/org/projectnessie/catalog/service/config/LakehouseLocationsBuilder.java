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
package org.projectnessie.catalog.service.config;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.AdlsNamedFileSystemOptions;
import org.projectnessie.catalog.files.config.BucketOptions;
import org.projectnessie.catalog.files.config.GcsBucketOptions;
import org.projectnessie.catalog.files.config.GcsNamedBucketOptions;
import org.projectnessie.catalog.files.config.PerBucket;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3NamedBucketOptions;
import org.projectnessie.storage.uri.StorageUri;

final class LakehouseLocationsBuilder {

  public static LakehouseLocations build(LakehouseConfig config) {
    LakehouseStorageLocations<S3NamedBucketOptions, S3BucketOptions> s3 =
        buildLakehouseStorageLocations("s3", config.s3().buckets(), config.s3().defaultOptions());
    LakehouseStorageLocations<GcsNamedBucketOptions, GcsBucketOptions> gcs =
        buildLakehouseStorageLocations("gs", config.gcs().buckets(), config.gcs().defaultOptions());
    LakehouseStorageLocations<AdlsNamedFileSystemOptions, AdlsFileSystemOptions> adls =
        buildLakehouseStorageLocations(
            "abfs", config.adls().fileSystems(), config.adls().defaultOptions());

    Map<String, LakehouseStorageLocations<?, ?>> locationsBySchema =
        Map.of("s3", s3, "gs", gcs, "abfs", adls);

    return ImmutableLakehouseLocations.of(locationsBySchema, s3, gcs, adls);
  }

  static <N extends PerBucket, D extends BucketOptions>
      LakehouseStorageLocations<N, D> buildLakehouseStorageLocations(
          String schema, Map<String, N> buckets, Optional<D> defaultOptions) {
    Map<StorageUri, N> map =
        buckets.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> {
                      String key = e.getKey();
                      N bucket = e.getValue();
                      String authority =
                          bucket.authority().isPresent()
                              ? bucket.authority().get()
                              : bucket.name().orElse(key);
                      String bucketPathPrefix = bucket.pathPrefix().orElse("");
                      return StorageUri.of(
                          String.format("%s://%s/%s", schema, authority, bucketPathPrefix));
                    },
                    Map.Entry::getValue));
    return ImmutableLakehouseStorageLocations.of(map, defaultOptions);
  }
}
