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
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

/**
 * MongoDB test connection-provider source using a locally spawned MongoDB instance via
 * "flapdoodle".
 */
public class FlapdoodleMongoTestConnectionProviderSource extends MongoTestConnectionProviderSource {
  private MongodExecutable mongo;

  private File storageDir;

  @Override
  public void start() throws Exception {
    if (mongo != null) {
      throw new IllegalStateException("Already started");
    }

    MongodStarter starter =
        MongodStarter.getInstance(Defaults.runtimeConfigFor(Command.MongoD).build());

    long timeoutAt = System.currentTimeMillis() + 600_000;
    int port;
    while (true) {
      assertThat(timeoutAt - System.currentTimeMillis())
          .describedAs("Could not start mongod")
          .isGreaterThan(0L);

      port = ThreadLocalRandom.current().nextInt(1024, 32768);

      try {
        new ServerSocket(port).close();
      } catch (IOException e) {
        // probably: port already in use
        continue;
      }

      // Need to use "our own" storage directory, because flapdoodle doesn't clean up temporary
      // files
      // then the process failed to start.
      if (storageDir == null) {
        storageDir =
            File.createTempFile("mongo-flapdoodle-" + ThreadLocalRandom.current().nextLong(), "");
      }
      deleteStorageDir();
      assertThat(storageDir.mkdirs()).isTrue();

      MongodConfig mongodConfig =
          MongodConfig.builder()
              .version(Version.Main.PRODUCTION)
              .replication(new Storage(storageDir.getAbsolutePath(), null, 0))
              .net(new Net(port, Network.localhostIsIPv6()))
              .build();

      mongo = starter.prepare(mongodConfig);
      try {
        mongo.start();
      } catch (Exception e) {
        // Current flapdoodle version throws a RuntimeException, if the mongod process could not
        // be started. Reason has already been logged.
        mongo.stop();
        continue;
      }

      break;
    }

    String connectionString = String.format("mongodb://localhost:%d", port);
    String databaseName = "test";

    configureConnectionProviderConfigFromDefaults(
        c -> c.withConnectionString(connectionString).withDatabaseName(databaseName));

    super.start();
  }

  private void deleteStorageDir() {
    if (storageDir == null) {
      return;
    }
    delTree(storageDir);
  }

  private void delTree(File f) {
    if (f == null) {
      return;
    }
    if (f.isDirectory()) {
      File[] files = f.listFiles();
      if (files != null) {
        for (File file : files) {
          delTree(file);
        }
      }
    }
    f.delete();
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
        deleteStorageDir();
      }
    }
  }
}
