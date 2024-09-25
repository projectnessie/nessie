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

import static com.google.common.base.Throwables.getStackTraceAsString;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.UUID;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.config.ServiceConfig;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.storage.uri.StorageUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Readiness
@ApplicationScoped
public class ObjectStoresHealthCheck implements HealthCheck {
  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStoresHealthCheck.class);

  public static final String NAME = "Warehouses Object Stores";

  @Inject ServiceConfig serviceConfig;
  @Inject LakehouseConfig lakehouseConfig;
  @Inject ObjectIO objectIO;
  @Inject ServerConfig serverConfig;

  @Override
  public HealthCheckResponse call() {
    HealthCheckResponseBuilder healthCheckResponse = HealthCheckResponse.builder().name(NAME);
    boolean up = true;
    if (serviceConfig.objectStoresHealthCheck()) {
      for (Map.Entry<String, WarehouseConfig> warehouse :
          lakehouseConfig.catalog().warehouses().entrySet()) {
        String name = warehouse.getKey();
        WarehouseConfig warehouseConfig = warehouse.getValue();

        String warehouseLocation = warehouseConfig.location();
        try {
          objectIO.ping(StorageUri.of(warehouseLocation));
          healthCheckResponse.withData("warehouse." + name + ".status", "UP");
        } catch (Exception ex) {
          String errorId = UUID.randomUUID().toString();
          LOGGER.error("Failed to ping warehouse '{}', error ID {}", name, errorId, ex);
          healthCheckResponse.withData("warehouse." + name + ".status", "DOWN");
          healthCheckResponse.withData("warehouse." + name + ".error-id", errorId);
          healthCheckResponse.withData(
              "warehouse." + name + ".error",
              serverConfig.sendStacktraceToClient() ? getStackTraceAsString(ex) : ex.toString());
          up = false;
        }
      }
    }
    return healthCheckResponse.status(up).build();
  }
}
