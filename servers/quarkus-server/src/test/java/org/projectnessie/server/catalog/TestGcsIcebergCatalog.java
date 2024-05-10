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

import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.GCS_WAREHOUSE_LOCATION;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;

@QuarkusTest
@TestProfile(GcsUnitTestProfile.class)
public class TestGcsIcebergCatalog extends AbstractIcebergCatalogUnitTests {

  @Override
  protected RESTCatalog catalog() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "nessie-gcs-iceberg-api",
        Map.of(
            CatalogProperties.URI,
            String.format("http://127.0.0.1:%d/iceberg/", catalogServerPort),
            CatalogProperties.WAREHOUSE_LOCATION,
            GCS_WAREHOUSE_LOCATION));
    catalogs.add(catalog);
    return catalog;
  }

  @Override
  protected String temporaryLocation() {
    return GCS_WAREHOUSE_LOCATION + "/temp/" + UUID.randomUUID();
  }
}
