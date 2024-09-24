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
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.catalog.files.config.AdlsConfig;
import org.projectnessie.catalog.files.config.AdlsOptions;
import org.projectnessie.catalog.files.config.GcsConfig;
import org.projectnessie.catalog.files.config.GcsOptions;
import org.projectnessie.catalog.files.config.ImmutableS3StsCache;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.files.config.S3StsCache;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.immutables.NessieImmutable;

/* (This is not a javadoc to let it not appear in the generated markdown on the website!)
 *
 * Container for all options/config types retrieved via smallrye-config.
 *
 * <p>CDI injection targets use the "base" types like {@link CatalogConfig} or {@link S3Options}.
 * Annotating those types with {@link ConfigMapping @ConfigMapping} would clash with the immutable
 * variants that Nessie wants to expose from {@link LakehouseConfig} objects. Adding CDI qualifiers
 * to for example subtypes that are annotated with {@link ConfigMapping @ConfigMapping} and extend
 * the "base" types doesn't work for some reason.
 */
@ConfigMapping(prefix = "nessie.catalog")
@NessieImmutable
public interface SmallryeConfigs {
  /**
   * Flag whether the lakehouse config is managed (persisted in Nessie) or static (retrieved from
   * Quarkus configs).
   *
   * @hidden hidden from docs on projectnessie website
   */
  @WithName("advanced.persist-config")
  @WithDefault("false")
  boolean usePersistedLakehouseConfig();

  @WithName("service.s3")
  @ConfigItem(section = "s3", sectionDocFromType = true)
  S3Options s3();

  @WithName("service.gcs")
  @ConfigItem(section = "gcs", sectionDocFromType = true)
  GcsOptions gcs();

  @WithName("service.adls")
  @ConfigItem(section = "adls", sectionDocFromType = true)
  AdlsOptions adls();

  @WithParentName
  CatalogConfig catalog();

  @WithParentName
  @ConfigItem(section = "service_config", sectionDocFromType = true)
  ServiceConfig serviceConfig();

  @WithName("service.s3")
  @ConfigItem(section = "s3_config", sectionDocFromType = true)
  S3Config s3config();

  @WithName("service.gcs")
  @ConfigItem(section = "gcs_config", sectionDocFromType = true)
  GcsConfig gcsConfig();

  @WithName("service.s3.sts")
  @ConfigItem(section = "s3_config_sts", sectionDocFromType = true)
  Optional<S3StsCache> s3StsCache();

  @Value.NonAttribute
  @JsonIgnore
  default S3StsCache effectiveS3StsCache() {
    return s3StsCache().orElse(ImmutableS3StsCache.builder().build());
  }

  @WithName("service.adls")
  @ConfigItem(section = "adls_config", sectionDocFromType = true)
  AdlsConfig adlsconfig();

  /**
   * Optional: validate at server startup that all referenced secrets can be resolved. Startup will
   * fail, it one or more secrets cannot be resolved at startup time, hence the default is `false`.
   */
  @WithName("validate-secrets")
  @WithDefault("false")
  boolean validateSecrets();
}
