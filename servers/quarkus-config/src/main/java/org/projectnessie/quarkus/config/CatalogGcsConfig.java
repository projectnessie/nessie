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
package org.projectnessie.quarkus.config;

import io.smallrye.config.ConfigMapping;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.catalog.files.config.GcsOptions;
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
public interface CatalogGcsConfig extends GcsOptions {

  @Override
  Optional<CatalogGcsBucketConfig> defaultOptions();

  @Override
  @ConfigPropertyName("bucket-name")
  Map<String, CatalogGcsNamedBucketConfig> buckets();
}
