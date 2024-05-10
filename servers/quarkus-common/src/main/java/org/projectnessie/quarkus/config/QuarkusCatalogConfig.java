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
import io.smallrye.config.WithName;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

@ConfigMapping(prefix = "nessie.catalog")
public interface QuarkusCatalogConfig extends CatalogConfig {
  @Override
  @WithName("default-warehouse")
  Optional<QuarkusWarehouseConfig> defaultWarehouse();

  @Override
  @WithName("warehouses")
  Map<String, QuarkusWarehouseConfig> warehouses();

  @Override
  @WithName("iceberg-config-defaults")
  Map<String, String> icebergConfigDefaults();

  @Override
  @WithName("iceberg-config-overrides")
  Map<String, String> icebergConfigOverrides();

  /**
   * Secrets must be provided in the configuration as key-value pairs of the form: {@code
   * nessie.catalog.secrets.<secret-ref>=<value>}, where _`secret-ref`_ is the name used where it is
   * referenced, for example via `nessie.catalog.service.s3.access-key-id-ref`.
   */
  @ConfigPropertyName("secret-ref")
  Map<String, String> secrets();
}
