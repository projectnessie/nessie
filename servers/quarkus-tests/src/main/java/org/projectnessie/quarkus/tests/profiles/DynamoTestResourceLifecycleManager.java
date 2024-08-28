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
import org.projectnessie.versioned.storage.dynamodbtests2.DynamoDB2BackendTestFactory;

public class DynamoTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  private DynamoDB2BackendTestFactory dynamo;

  private Optional<String> containerNetworkId = Optional.empty();

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    containerNetworkId = context.containerNetworkId();
  }

  @Override
  public Map<String, String> start() {
    dynamo = new DynamoDB2BackendTestFactory();

    try {
      // Only start the Docker container (local Dynamo-compatible).
      dynamo.start(containerNetworkId);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return dynamo.getQuarkusConfig();
  }

  @Override
  public void stop() {
    if (dynamo != null) {
      try {
        dynamo.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        dynamo = null;
      }
    }
  }
}
