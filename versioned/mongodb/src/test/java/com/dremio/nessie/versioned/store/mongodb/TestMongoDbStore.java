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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.store.ValueType;
import com.mongodb.client.MongoDatabase;

public class TestMongoDbStore {
  final String testDatabaseName = "mydb";
  final String connectionString = "localhost";
  MongoDbStore mongoDbStore;

  /**
   * Set up the objects necessary for the tests.
   */
  @BeforeEach
  public void setUp() {
    mongoDbStore = new MongoDbStore(connectionString, testDatabaseName);
  }

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
    assertEquals(mongoDatabase.getName(), testDatabaseName);
  }

  @Test
  public void putValue() {
    L1 l1 = TestSamples.getSampleL1();
    L2 l2 = TestSamples.getSampleL2();
    L3 l3 = TestSamples.getSampleL3();
    mongoDbStore.start();
    MongoDatabase mongoDatabase = mongoDbStore.getMongoDatabase();
    assertEquals(mongoDatabase.getName(), testDatabaseName);
    mongoDbStore.put(ValueType.L2, l2, null);
    // TODO verify the ValueType was successfully stored. This requires the decode to work.
    //    Consumer<L2> printConsumer = new Consumer<L2>() {
    //      @Override
    //      public void accept(final L2 l2) {
    //        LOGGER.info(l2.toString());
    //      }
    //    };
    //    mongoDbStore.l2MongoCollection.find().forEach(printConsumer);

  }
}
