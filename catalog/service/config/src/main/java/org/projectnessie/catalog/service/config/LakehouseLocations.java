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
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.AdlsNamedFileSystemOptions;
import org.projectnessie.catalog.files.config.GcsBucketOptions;
import org.projectnessie.catalog.files.config.GcsNamedBucketOptions;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3NamedBucketOptions;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface LakehouseLocations {
  Map<String, LakehouseStorageLocations<?, ?>> locationsBySchema();

  /** Typed convenience for {@link #locationsBySchema()}{@code .get("s3")}. */
  LakehouseStorageLocations<S3NamedBucketOptions, S3BucketOptions> s3Locations();

  /** Typed convenience for {@link #locationsBySchema()}{@code .get("gs")}. */
  LakehouseStorageLocations<GcsNamedBucketOptions, GcsBucketOptions> gcsLocations();

  /** Typed convenience for {@link #locationsBySchema()}{@code .get("abfs")}. */
  LakehouseStorageLocations<AdlsNamedFileSystemOptions, AdlsFileSystemOptions> adlsLocations();

  static LakehouseLocations buildLakehouseLocations(LakehouseConfig config) {
    return LakehouseLocationsBuilder.build(config);
  }
}
