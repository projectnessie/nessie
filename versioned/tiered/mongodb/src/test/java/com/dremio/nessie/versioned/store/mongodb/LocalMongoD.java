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
package com.dremio.nessie.versioned.store.mongodb;

import java.io.IOException;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

/**
 * Creates and configures a non-sharded flapdoodle MongoDB instance.
 */
class LocalMongoD extends LocalMongoBase {
  private static final String MONGODB_INFO = "mongodbD-local-info";

  private static class MongoDHolder implements ServerHolder {
    private MongodExecutable mongoExec;
    private String connectionString;

    MongoDHolder() {
    }

    @Override
    public String getConnectionString() {
      return connectionString;
    }

    @Override
    public void start() throws IOException {
      final int port = Network.getFreeServerPort();
      final MongodConfig config = MongodConfig.builder()
          .version(Version.Main.PRODUCTION)
          .net(new Net(port, Network.localhostIsIPv6()))
          .build();

      mongoExec = MONGOD_STARTER.prepare(config);
      mongoExec.start();
      connectionString = "mongodb://localhost:" + port;
    }

    @Override
    public void stop() {
      mongoExec.stop();
    }
  }

  @Override
  protected String getIdentifier() {
    return MONGODB_INFO;
  }

  @Override
  protected ServerHolder createHolder() {
    return new MongoDHolder();
  }
}
