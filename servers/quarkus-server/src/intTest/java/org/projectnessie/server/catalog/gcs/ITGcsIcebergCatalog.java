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

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.net.URI;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.projectnessie.server.catalog.AbstractIcebergCatalogTests;
import org.projectnessie.server.catalog.GcsEmulatorTestResourceLifecycleManager;
import org.projectnessie.server.catalog.GcsEmulatorTestResourceLifecycleManager.WarehouseLocation;

@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    value = GcsEmulatorTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
public class ITGcsIcebergCatalog extends AbstractIcebergCatalogTests {

  @WarehouseLocation URI warehouseLocation;

  @Override
  protected RESTCatalog catalog() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "nessie-s3-iceberg-api",
        Map.of(
            CatalogProperties.URI,
            String.format("http://127.0.0.1:%d/iceberg/", catalogServerPort),
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation.toString()));
    catalogs.add(catalog);
    return catalog;
  }

  @Override
  protected String temporaryLocation() {
    return warehouseLocation.resolve("/temp").toString();
  }
}
