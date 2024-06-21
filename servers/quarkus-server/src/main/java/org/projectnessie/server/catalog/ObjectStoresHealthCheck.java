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
package org.projectnessie.server.catalog;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.quarkus.config.QuarkusCatalogConfig;
import org.projectnessie.quarkus.config.QuarkusWarehouseConfig;
import org.projectnessie.storage.uri.StorageUri;

@Readiness
@ApplicationScoped
public class ObjectStoresHealthCheck implements HealthCheck {
  public static final String NAME = "Warehouses Object Stores";

  @Inject QuarkusCatalogConfig catalogConfig;
  @Inject ObjectIO objectIO;

  @Override
  public HealthCheckResponse call() {
    HealthCheckResponseBuilder healthCheckResponse = HealthCheckResponse.builder().name(NAME);
    boolean up = true;
    if (catalogConfig.objectStoresHealthCheck()) {
      for (Map.Entry<String, QuarkusWarehouseConfig> warehouse :
          catalogConfig.warehouses().entrySet()) {
        String name = warehouse.getKey();
        QuarkusWarehouseConfig warehouseConfig = warehouse.getValue();

        String warehouseLocation = warehouseConfig.location();
        try {
          objectIO.ping(StorageUri.of(warehouseLocation));
          healthCheckResponse.withData("warehouse." + name + ".status", "UP");
        } catch (Exception ex) {
          healthCheckResponse.withData("warehouse." + name + ".status", "DOWN");
          healthCheckResponse.withData("warehouse." + name + ".error", ex.toString());
          up = false;
        }
      }
    }
    return healthCheckResponse.status(up).build();
  }
}
