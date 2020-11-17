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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoDatabase;

public class TestMongoDbStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestMongoDbStore.class);

  final String testDatabaseName = "mydb";
  final String connectionString = "mongodb://localhost:27017";

  @Test
  public void closeMongoDBClientThatDoesNotExist() {
    MongoDbStore mongoDbStore = new MongoDbStore(connectionString, testDatabaseName);
    assertNull(mongoDbStore.mongoClient);
    mongoDbStore.close();
  }

  /**
   * A test to open and close a connection to the database via the Mongo Client.
   * The test ensures that the database name set up can be retrieved.
   */
  @Test
  public void createAndCloseClient() {
    MongoDbStore mongoDbStore = new MongoDbStore(connectionString, testDatabaseName);
    assertNull(mongoDbStore.mongoClient);
    mongoDbStore.start();
    assertNotNull(mongoDbStore.mongoClient);
    MongoDatabase mongoDatabase = mongoDbStore.mongoClient.getDatabase(testDatabaseName);
    assertTrue(mongoDatabase.getName().equals(testDatabaseName));
    mongoDbStore.close();
    assertNull(mongoDbStore.mongoClient);
  }
}
