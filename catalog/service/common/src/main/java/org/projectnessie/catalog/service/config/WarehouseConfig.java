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

import static org.projectnessie.catalog.service.config.CatalogConfig.removeTrailingSlash;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithName;
import java.util.Map;
import org.immutables.value.Value;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableWarehouseConfig.class)
@JsonDeserialize(as = ImmutableWarehouseConfig.class)
public interface WarehouseConfig {

  /**
   * Iceberg config defaults specific to this warehouse, potentially overriding any defaults
   * specified in {@code iceberg-config-defaults} in <a href="#warehouse-defaults">Warehouse
   * defaults</a>.
   */
  @ConfigPropertyName("iceberg-property")
  @WithName("iceberg-config-defaults")
  Map<String, String> icebergConfigDefaults();

  /**
   * Iceberg config overrides specific to this warehouse. They override any overrides specified in
   * {@code iceberg-config-overrides} in <a href="#warehouse-defaults">Warehouse defaults</a>.
   */
  @ConfigPropertyName("iceberg-property")
  @WithName("iceberg-config-overrides")
  Map<String, String> icebergConfigOverrides();

  /** Location of the warehouse. Used to determine the base location of a table. */
  @WithConverter(TrimTrailingSlash.class)
  @WithName("location")
  String location();

  @Value.Check
  default WarehouseConfig normalize() {
    String removed = removeTrailingSlash(location());
    return removed.equals(location())
        ? this
        : ImmutableWarehouseConfig.builder().from(this).location(removed).build();
  }
}
