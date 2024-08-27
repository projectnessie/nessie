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
package org.projectnessie.quarkus.tests.profiles;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.versioned.storage.jdbc2tests.MariaDBBackendTestFactory;

public class MariaDBTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  private MariaDBBackendTestFactory mariadb;

  private Optional<String> containerNetworkId;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    containerNetworkId = context.containerNetworkId();
  }

  @Override
  public Map<String, String> start() {
    mariadb = new MariaDBBackendTestFactory();

    try {
      mariadb.start(containerNetworkId);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return mariadb.getQuarkusConfig();
  }

  @Override
  public void stop() {
    if (mariadb != null) {
      try {
        mariadb.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mariadb = null;
      }
    }
  }
}
