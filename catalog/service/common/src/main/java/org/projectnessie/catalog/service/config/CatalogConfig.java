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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.WithName;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableCatalogConfig.class)
@JsonDeserialize(as = ImmutableCatalogConfig.class)
public interface CatalogConfig {

  /**
   * Name of the default warehouse. This one is used when a warehouse is not specified in a query.
   * If no default warehouse is configured and a request does not specify a warehouse, the request
   * will fail.
   */
  @ConfigItem(section = "warehouseDefaults")
  @WithName("default-warehouse")
  Optional<String> defaultWarehouse();

  /** Map of warehouse names to warehouse configurations. */
  @ConfigPropertyName("warehouse-name")
  @ConfigItem(section = "warehouses")
  @WithName("warehouses")
  Map<String, WarehouseConfig> warehouses();

  /**
   * Iceberg config defaults applicable to all clients and warehouses. Any properties that are
   * common to all iceberg clients should be included here. They will be passed to all clients on
   * all warehouses as config defaults. These defaults can be overridden on a per-warehouse basis,
   * see {@code iceberg-config-defaults} in <a href="#warehouses">Warehouses</a>.
   */
  @ConfigPropertyName("iceberg-property")
  @ConfigItem(section = "warehouseDefaults")
  @WithName("iceberg-config-defaults")
  Map<String, String> icebergConfigDefaults();

  /**
   * Iceberg config overrides applicable to all clients and warehouses. Any properties that are
   * common to all iceberg clients should be included here. They will be passed to all clients on
   * all warehouses as config overrides. These overrides can be overridden on a per-warehouse basis,
   * see {@code iceberg-config-overrides} in <a href="#warehouses">Warehouses</a>.
   */
  @ConfigPropertyName("iceberg-property")
  @ConfigItem(section = "warehouseDefaults")
  @WithName("iceberg-config-overrides")
  Map<String, String> icebergConfigOverrides();

  @Value.NonAttribute
  @JsonIgnore
  default CatalogConfig deepClone() {
    ImmutableCatalogConfig.Builder b =
        ImmutableCatalogConfig.builder().from(this).warehouses(Map.of());
    warehouses().forEach((n, w) -> b.putWarehouse(n, ImmutableWarehouseConfig.copyOf(w)));
    return b.build();
  }

  /**
   * Returns the given {@code warehouse} if not-empty or the {@link #defaultWarehouse() default
   * warehouse}. Throws an {@link IllegalStateException} if neither is given/present.
   */
  default String resolveWarehouseName(String warehouse) {
    boolean hasWarehouse = warehouse != null && !warehouse.isEmpty();
    if (hasWarehouse) {
      return warehouse;
    }
    Optional<String> def = defaultWarehouse();
    if (def.isEmpty()) {
      throw new IllegalStateException("No default-warehouse configured");
    }
    return def.get();
  }

  /**
   * Attempts to match a warehouse by name or location. If no warehouse is found, the default
   * warehouse is returned.
   *
   * <p>Matching by location is required because the Iceberg REST API allows using either the name
   * or the location to identify a warehouse.
   */
  default WarehouseConfig getWarehouse(String warehouse) {
    String resolvedWarehouse = resolveWarehouseName(warehouse);
    WarehouseConfig w = warehouses().get(resolvedWarehouse);
    if (w != null) {
      return w;
    }

    // Lookup the warehouse by location (but not by the default warehouse name).
    boolean hasWarehouse = warehouse != null && !warehouse.isEmpty();
    if (hasWarehouse) {
      String warehouseLocation = removeTrailingSlash(warehouse);
      for (WarehouseConfig wc : warehouses().values()) {
        if (wc.location().equals(warehouseLocation)) {
          return wc;
        }
      }
    }

    throw new IllegalStateException("Warehouse '" + warehouse + "' is not known");
  }

  default CatalogConfig validate() {
    defaultWarehouse()
        .ifPresent(
            name -> {
              if (!warehouses().containsKey(name)) {
                throw new IllegalStateException("Default warehouse '" + name + "' is not defined.");
              }
            });
    return this;
  }

  static String removeTrailingSlash(String s) {
    return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }
}
