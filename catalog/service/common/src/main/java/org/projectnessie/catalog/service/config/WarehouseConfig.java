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
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public interface WarehouseConfig {

  /**
   * Iceberg config defaults specific to this warehouse. They override any defaults specified in
   * {@link CatalogConfig#icebergConfigDefaults()}.
   */
  @ConfigPropertyName("iceberg-property")
  Map<String, String> icebergConfigDefaults();

  /**
   * Iceberg config overrides specific to this warehouse. They override any overrides specified in
   * {@link CatalogConfig#icebergConfigOverrides()}.
   */
  @ConfigPropertyName("iceberg-property")
  Map<String, String> icebergConfigOverrides();

  /** Location of the warehouse. Used to determine the base location of a table. */
  String location();

  /**
   * Whether the warehouse should use the token endpoint proxied through the Rest Catalog server, or
   * the real one directly. The default is false (use the real one directly).
   *
   * <p>Using the proxied endpoint is considered insecure and should be avoided.
   */
  Optional<Boolean> allowAuthProxy();
}
