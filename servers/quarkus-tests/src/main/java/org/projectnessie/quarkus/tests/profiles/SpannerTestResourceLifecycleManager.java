/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.quarkus.tests.profiles;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceId;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.versioned.storage.spanner.SpannerBackendTestFactory;

public class SpannerTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  private SpannerBackendTestFactory spanner;

  private Optional<String> containerNetworkId;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    containerNetworkId = context.containerNetworkId();
  }

  @Override
  public Map<String, String> start() {
    spanner = new SpannerBackendTestFactory();
    spanner.startSpanner(containerNetworkId);

    DatabaseId databaseId = spanner.databaseId();
    InstanceId instanceId = databaseId.getInstanceId();
    return ImmutableMap.of(
        "quarkus.google.cloud.project-id",
        instanceId.getProject(),
        "nessie.version.store.persist.spanner.instance-id",
        instanceId.getInstance(),
        "nessie.version.store.persist.spanner.database-id",
        databaseId.getDatabase(),
        "nessie.version.store.persist.spanner.emulator-host",
        spanner.emulatorHost());
  }

  @Override
  public void stop() {
    if (spanner != null) {
      try {
        spanner.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        spanner = null;
      }
    }
  }
}
