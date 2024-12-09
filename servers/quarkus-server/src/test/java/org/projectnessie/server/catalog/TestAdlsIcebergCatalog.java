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

import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.ADLS_WAREHOUSE_LOCATION;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import java.util.UUID;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@QuarkusTest
@TestProfile(AdlsUnitTestProfile.class)
public class TestAdlsIcebergCatalog extends AbstractIcebergCatalogUnitTests {

  @Override
  protected String temporaryLocation() {
    return ADLS_WAREHOUSE_LOCATION + "/temp/" + UUID.randomUUID();
  }

  @Override
  protected String scheme() {
    return "adls";
  }

  @Override
  protected Map<String, String> catalogOptions() {
    return ImmutableMap.<String, String>builder()
        .putAll(super.catalogOptions())
        .put("adls.auth.shared-key.account.name", "account@account.dfs.core.windows.net")
        .put("adls.auth.shared-key.account.key", "key")
        .build();
  }
}
