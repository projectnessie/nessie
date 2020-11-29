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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.MongoStoreConfig;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

class TestMongoDbStore extends TestStore<MongoDbStore> {
  private static MongodExecutable mongoExec;
  private static String connectionString;
  private static final String testDatabaseName = "mydb";

  /**
   * Set up the embedded flapdoodle MongoDB server for unit tests.
   * @throws IOException if there's an issue grabbing the port or determining IP version.
   */
  @BeforeAll
  public static void setupServer() throws IOException {
    final int port = Network.getFreeServerPort();
    final MongodConfig config = MongodConfig.builder()
        .version(Version.Main.PRODUCTION)
        .net(new Net(port, Network.localhostIsIPv6()))
        .build();

    mongoExec = MongodStarter.getDefaultInstance().prepare(config);
    mongoExec.start();
    connectionString = "mongodb://localhost:" + port;
  }

  /**
   * Shut down the embedded flapdoodle MongoDB server.
   */
  @AfterAll
  public static void teardownServer() {
    if (null != mongoExec) {
      mongoExec.stop();
    }
  }

  @Override
  protected MongoDbStore createStore() {
    return new MongoDbStore(new MongoStoreConfig() {
      @Override
      public String getConnectionString() {
        return connectionString;
      }

      @Override
      public String getDatabaseName() {
        return testDatabaseName;
      }
    });
  }

  @Override
  protected void resetStoreState() {
    store.getCollections().forEach((k, v) -> v.deleteMany(Filters.ne("_id", "s")));
  }

  @Test
  public void testDatabaseName() {
    final MongoDatabase mongoDatabase = store.getDatabase();
    assertEquals(testDatabaseName, mongoDatabase.getName());
  }
}
