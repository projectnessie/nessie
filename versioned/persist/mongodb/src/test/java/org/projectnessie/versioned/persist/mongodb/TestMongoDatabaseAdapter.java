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

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.versioned.persist.tests.AbstractTieredCommitsTest;

public class TestMongoDatabaseAdapter extends AbstractTieredCommitsTest {
  private static MongodExecutable mongo;
  private static int port;

  @BeforeAll
  static void startMongo() throws IOException {
    MongodStarter starter = MongodStarter.getDefaultInstance();

    port = Network.getFreeServerPort();

    MongodConfig mongodConfig =
        MongodConfig.builder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(port, Network.localhostIsIPv6()))
            .build();

    mongo = starter.prepare(mongodConfig);
    mongo.start();

    createAdapter(
        "MongoDB",
        config ->
            ImmutableMongoDatabaseAdapterConfig.builder()
                .from(config)
                .connectionString(String.format("mongodb://localhost:%d", port))
                .databaseName("test")
                .build());
  }

  @AfterAll
  static void closeMongoDb() {
    if (mongo != null) {
      mongo.stop();
    }
  }
}
