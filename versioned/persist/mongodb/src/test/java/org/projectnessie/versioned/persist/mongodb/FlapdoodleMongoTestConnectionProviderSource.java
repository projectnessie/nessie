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

import static org.assertj.core.api.Assertions.assertThat;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.Defaults;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import java.io.IOException;
import java.net.ServerSocket;

/**
 * MongoDB test connection-provider source using a locally spawned MongoDB instance via
 * "flapdoodle".
 */
public class FlapdoodleMongoTestConnectionProviderSource extends MongoTestConnectionProviderSource {

  private MongodExecutable mongo;

  @Override
  public void start() throws Exception {
    if (mongo != null) {
      throw new IllegalStateException("Already started");
    }

    MongodStarter starter =
        MongodStarter.getInstance(Defaults.runtimeConfigFor(Command.MongoD).build());

    int port = findRandomOpenPortOnAllLocalInterfaces();
    assertThat(port).isGreaterThan(0);

    MongodConfig mongodConfig =
        MongodConfig.builder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(port, Network.localhostIsIPv6()))
            .build();

    mongo = starter.prepare(mongodConfig);
    mongo.start();

    String connectionString = String.format("mongodb://localhost:%d", port);
    String databaseName = "test";

    configureConnectionProviderConfigFromDefaults(
        c -> c.withConnectionString(connectionString).withDatabaseName(databaseName));

    super.start();
  }

  @Override
  public void stop() throws Exception {
    try {
      super.stop();
    } finally {
      try {
        if (mongo != null) {
          mongo.stop();
        }
      } finally {
        mongo = null;
      }
    }
  }

  private Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
