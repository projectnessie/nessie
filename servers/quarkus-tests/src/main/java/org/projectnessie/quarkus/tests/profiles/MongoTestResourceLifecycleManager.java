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

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.versioned.storage.mongodbtests2.MongoDB2BackendTestFactory;

public class MongoTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  private MongoDB2BackendTestFactory mongo;

  private Optional<String> containerNetworkId;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    containerNetworkId = context.containerNetworkId();
  }

  @Override
  public Map<String, String> start() {
    mongo = new MongoDB2BackendTestFactory();

    try {
      mongo.start(containerNetworkId);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return mongo.getQuarkusConfig();
  }

  @Override
  public void stop() {
    if (mongo != null) {
      try {
        mongo.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        mongo = null;
      }
    }
  }
}
