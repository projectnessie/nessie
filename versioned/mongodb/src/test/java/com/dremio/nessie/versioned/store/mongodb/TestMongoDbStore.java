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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.dremio.nessie.versioned.tests.AbstractTestStore;

/**
 * A test class that contains MongoDB specific tests.
 */
class TestMongoDbStore extends AbstractTestStore<MongoDbStore> {
  private MongoStoreConfig mongoStoreConfig;
  private MongoDbStore mongoDbStore;
  private static final String testDatabaseName = "mydb";
  private static LocalMongo localMongo = LocalMongo.getInstance();

  /**
   * Set up the embedded flapdoodle MongoDB server for unit tests.
   * @throws IOException if there's an issue grabbing the port or determining IP version.
   */
  @BeforeAll
  public static void setupServer() throws IOException {
    localMongo.setupServer();
  }

  /**
   * Shut down the embedded flapdoodle MongoDB server.
   */
  @AfterAll
  public static void teardownServer() {
    localMongo.teardownServer();
  }

  /**
   * Creates an instance of MongoDBStore on which tests are executed.
   * @return the instance
   */
  @Override
  protected MongoDbStore createStore() {
    mongoStoreConfig = new MongoStoreConfig() {
      @Override
      public String getConnectionString() {
        return localMongo.getConnectionString();
      }

      @Override
      public String getDatabaseName() {
        return testDatabaseName;
      }
    };

    mongoDbStore = new MongoDbStore(mongoStoreConfig);

    return mongoDbStore;
  }

  @Override
  protected void resetStoreState() {
    store.resetCollections();
  }
}
