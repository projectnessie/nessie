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
package org.projectnessie.server.catalog.gcs;

import static java.util.UUID.randomUUID;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.projectnessie.server.catalog.AbstractIcebergCatalogIntTests;
import org.projectnessie.server.catalog.GcsEmulatorTestResourceLifecycleManager;
import org.projectnessie.server.catalog.GcsToken;
import org.projectnessie.server.catalog.WarehouseLocation;

@WithTestResource(GcsEmulatorTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
public class ITGcsIcebergCatalog extends AbstractIcebergCatalogIntTests {

  @WarehouseLocation URI warehouseLocation;
  @GcsToken String gcsToken;

  @Override
  protected Map<String, String> catalogOptions() {
    return Map.of(
        CatalogProperties.WAREHOUSE_LOCATION,
        warehouseLocation.toString(),
        "gcs.oauth2.token",
        gcsToken);
  }

  @Override
  protected String temporaryLocation() {
    return warehouseLocation.toString() + "/temp/" + randomUUID();
  }

  @Override
  protected String scheme() {
    return warehouseLocation.getScheme();
  }
}
