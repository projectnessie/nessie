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
package org.projectnessie.server.catalog.s3;

import static java.util.Collections.singletonMap;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.projectnessie.minio.MinioContainer;
import org.projectnessie.server.catalog.AbstractIcebergCatalogTests;
import org.projectnessie.server.catalog.MinioTestResourceLifecycleManager;

@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    value = MinioTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
public class ITS3IcebergCatalog extends AbstractIcebergCatalogTests {

  @SuppressWarnings("unused")
  // Injected by MinioTestResourceLifecycleManager
  private MinioContainer minio;

  @Override
  protected Map<String, String> catalogOptions() {
    return singletonMap(CatalogProperties.WAREHOUSE_LOCATION, minio.s3BucketUri("").toString());
  }

  @Override
  protected String temporaryLocation() {
    return minio.s3BucketUri("") + "/temp/" + UUID.randomUUID();
  }
}
