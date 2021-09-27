/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.server.profiles;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Collections;
import java.util.Map;
import org.projectnessie.versioned.persist.dynamodb.LocalDynamoTestConnectionProviderSource;

public class DynamoTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

  private LocalDynamoTestConnectionProviderSource dynamo;

  @Override
  public Map<String, String> start() {
    dynamo = new LocalDynamoTestConnectionProviderSource();

    try {
      // Only start the Docker container (local Dynamo-compatible). The DynamoDatabaseClient will
      // be configured via Quarkus -> Quarkus-Dynamo / DynamoVersionStoreFactory.
      dynamo.startDynamo();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return Collections.singletonMap("quarkus.dynamodb.endpoint-override", dynamo.getEndpointURI());
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
