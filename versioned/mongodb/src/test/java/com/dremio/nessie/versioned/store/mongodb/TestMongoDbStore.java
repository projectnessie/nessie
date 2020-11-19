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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.store.ValueType;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

public class TestMongoDbStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestMongoDbStore.class);

  final String testDatabaseName = "mydb";
  final String connectionString = "localhost";
  MongoDbStore mongoDbStore;
  MongoClientSettings mongoClientSettings;

  @BeforeEach
  public void setUp() {
    mongoDbStore = new MongoDbStore(connectionString, testDatabaseName);
    mongoClientSettings = MongoClientSettings.builder()
      .applyToClusterSettings(builder ->
        builder.hosts(Arrays.asList(new ServerAddress(connectionString, mongoDbStore.mongoPort))))
      .codecRegistry(mongoDbStore.pojoCodecRegistry)
      .build();  }

  @AfterEach
  public void teardown() {
    mongoDbStore.close();
  }

  /**
   * A test to open and close a connection to the database via the Mongo Client.
   * The test ensures that the database name set up can be retrieved.
   */
  @Test
  public void createClient() {
    mongoDbStore.start();
    MongoDatabase mongoDatabase = mongoDbStore.getMongoDatabase();
    assertTrue(mongoDatabase.getName().equals(testDatabaseName));
  }

  @Disabled
  @Test
  public void putValue() {
    L2 l2 = TestValueTypeUtility.getSampleL2();
    mongoDbStore.start();
    MongoDatabase mongoDatabase = mongoDbStore.getMongoDatabase();
    assertTrue(mongoDatabase.getName().equals(testDatabaseName));
    mongoDbStore.put(ValueType.L2, l2, null);
    // TODO verify the ValueType was successfully stored.
    //    Consumer<L1> printConsumer = new Consumer<L1>() {
    //      @Override
    //      public void accept(final L1 l1) {
    //        LOGGER.info(l1.toString());
    //      }
    //    };
    //    mongoDbStore.l1MongoCollection.find().forEach(printConsumer);
    //
  }
}
