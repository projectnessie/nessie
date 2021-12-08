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
import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

/**
 * MongoDB test connection-provider source using a locally spawned MongoDB instance via
 * "flapdoodle".
 */
public class FlapdoodleMongoTestConnectionProviderSource extends MongoTestConnectionProviderSource {
  private MongodExecutable mongo;
  private File sockFile;

  @Override
  public void start() throws Exception {
    if (mongo != null) {
      throw new IllegalStateException("Already started");
    }

    MongodStarter starter =
        MongodStarter.getInstance(Defaults.runtimeConfigFor(Command.MongoD).build());

    sockFile =
        File.createTempFile("flapdoodle-mongod-" + ThreadLocalRandom.current().nextLong(), ".sock");
    assertThat(sockFile.delete()).isTrue();
    String unixSocket = sockFile.getAbsolutePath();

    MongodConfig mongodConfig =
        MongodConfig.builder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(0, Network.localhostIsIPv6()))
            .putArgs("--bind_ip", unixSocket)
            .build();

    mongo = starter.prepare(mongodConfig);
    mongo.start();

    assertThat(Paths.get(unixSocket)).exists();

    String connectionString = String.format("mongodb://%s", unixSocket.replaceAll("\\/", "%2f"));
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

        if (!sockFile.delete()) {
          sockFile.deleteOnExit();
        }
      }
    }
  }
}
