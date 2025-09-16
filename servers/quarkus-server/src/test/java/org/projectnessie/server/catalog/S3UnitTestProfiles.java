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

import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;
import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.INIT_ADDRESS;
import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.bucketWarehouseLocation;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.List;
import java.util.Map;

public abstract class S3UnitTestProfiles implements QuarkusTestProfile {

  public static class S3UnitTestProfile extends S3UnitTestProfiles {
    @Override
    protected String scheme() {
      return "s3";
    }
  }

  public static class S3AUnitTestProfile extends S3UnitTestProfiles {
    @Override
    protected String scheme() {
      return "s3a";
    }
  }

  public static class S3NUnitTestProfile extends S3UnitTestProfiles {
    @Override
    protected String scheme() {
      return "s3n";
    }
  }

  @Override
  public List<TestResourceEntry> testResources() {
    return List.of(
        new TestResourceEntry(
            ObjectStorageMockTestResourceLifecycleManager.class,
            ImmutableMap.of(INIT_ADDRESS, "localhost")),
        new TestResourceEntry(IcebergResourceLifecycleManager.ForUnitTests.class));
  }

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .put(
            "nessie.catalog.warehouses." + WAREHOUSE_NAME + ".location",
            bucketWarehouseLocation(scheme()))
        .build();
  }

  protected abstract String scheme();
}
