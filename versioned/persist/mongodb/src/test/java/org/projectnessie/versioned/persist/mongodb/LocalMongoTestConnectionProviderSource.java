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
package org.projectnessie.versioned.persist.mongodb;

import java.util.Optional;

/** MongoDB test connection-provider source using MongoDB in a Docker container. */
public class LocalMongoTestConnectionProviderSource extends MongoTestConnectionProviderSource {

  private final LocalMongo localMongo = new LocalMongo();

  public String getConnectionString() {
    return localMongo.getConnectionString();
  }

  public String getDatabaseName() {
    return localMongo.getDatabaseName();
  }

  @Override
  public void start() throws Exception {
    startMongo(Optional.empty(), false);

    configureConnectionProviderConfigFromDefaults(
        c -> c.withConnectionString(getConnectionString()).withDatabaseName(getDatabaseName()));

    super.start();
  }

  /**
   * Starts MongoDB with an optional Docker network ID and a flag to turn off all output to stdout
   * and stderr.
   */
  public void startMongo(Optional<String> containerNetworkId, boolean quiet) {
    localMongo.startMongo(containerNetworkId, quiet);
  }

  @Override
  public void stop() throws Exception {
    try {
      super.stop();
    } finally {
      localMongo.stop();
    }
  }
}
