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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.Objects;
import org.projectnessie.versioned.persist.mongodb.FlapdoodleMongoTestConnectionProviderSource;
import org.projectnessie.versioned.persist.mongodb.MongoClientConfig;

public class MongoTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
  private final FlapdoodleMongoTestConnectionProviderSource mongo =
      new FlapdoodleMongoTestConnectionProviderSource();

  @Override
  public Map<String, String> start() {
    try {
      mongo.start();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    MongoClientConfig config = mongo.getConnectionProviderConfig();

    return ImmutableMap.of(
        "quarkus.mongodb.connection-string", Objects.requireNonNull(config.getConnectionString()),
        "quarkus.mongodb.database", Objects.requireNonNull(config.getDatabaseName()));
  }

  @Override
  public void stop() {
    try {
      mongo.stop();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
