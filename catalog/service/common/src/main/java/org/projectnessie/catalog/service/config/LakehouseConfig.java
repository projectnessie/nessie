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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.catalog.files.config.AdlsOptions;
import org.projectnessie.catalog.files.config.GcsOptions;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Describes the configuration of the whole catalog, including all warehouses and object storage
 * settings. System level settings, for example HTTP or service runtime behavior configurations, are
 * not included.
 */
@NessieImmutable
@JsonSerialize(as = ImmutableLakehouseConfig.class)
@JsonDeserialize(as = ImmutableLakehouseConfig.class)
public interface LakehouseConfig {
  // "nessie.catalog"
  CatalogConfig catalog();

  // "nessie.catalog.service.s3"
  S3Options s3();

  // "nessie.catalog.service.gcs"
  GcsOptions gcs();

  // "nessie.catalog.service.adls"
  AdlsOptions adls();

  @Value.NonAttribute
  @JsonIgnore
  default LakehouseConfig deepClone() {
    return ImmutableLakehouseConfig.builder()
        .catalog(catalog().deepClone())
        .s3(s3().deepClone())
        .gcs(gcs().deepClone())
        .adls(adls().deepClone())
        .build();
  }
}
