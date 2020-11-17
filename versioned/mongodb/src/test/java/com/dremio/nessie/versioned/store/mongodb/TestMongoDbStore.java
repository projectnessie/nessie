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

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.ValueType;
import com.mongodb.client.MongoDatabase;

public class TestMongoDbStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestMongoDbStore.class);

  final String testDatabaseName = "mydb";
  final String connectionString = "localhost";
  MongoDbStore mongoDbStore;

  @BeforeEach
  public void setUp() {
    mongoDbStore = new MongoDbStore(connectionString, testDatabaseName);
    assertNull(mongoDbStore.mongoClient);
  }

  @AfterEach
  public void teardown() {
    mongoDbStore.close();
    assertNull(mongoDbStore.mongoClient);
  }

  /**
   * A test to open and close a connection to the database via the Mongo Client.
   * The test ensures that the database name set up can be retrieved.
   */
  @Test
  public void createClient() {
    mongoDbStore.start();
    assertNotNull(mongoDbStore.mongoClient);
    MongoDatabase mongoDatabase = mongoDbStore.mongoClient.getDatabase(testDatabaseName);
    assertTrue(mongoDatabase.getName().equals(testDatabaseName));
  }

  @Disabled
  @Test
  public void putValue() {
    Map<String, Entity> attributeMap = new HashMap<>();
    L2 l2 = L2.SCHEMA.mapToItem(attributeMap);

    //    mongoDbStore.start();
    //    assertNotNull(mongoDbStore.mongoClient);
    MongoDatabase mongoDatabase = mongoDbStore.mongoClient.getDatabase(testDatabaseName);
    assertTrue(mongoDatabase.getName().equals(testDatabaseName));
    //
    //    // mongoDbStore.put(ValueType.VALUE, InternalValue.of(ByteString.copyFromUtf8(new String("This is a test"))), null);
    //    int SIZE = 151;
    //    L1 l1Sample = new L1(Id.EMPTY, new IdMap(SIZE, L2.EMPTY_ID), null, KeyList.EMPTY, ParentList.EMPTY);
    //
    mongoDbStore.put(ValueType.L2, l2, null);
    //    Consumer<L1> printBlock = new Consumer<L1>() {
    //      @Override
    //      public void accept(final L1 l1) {
    //        LOGGER.info(l1.toString());
    //      }
    //    };
    //    mongoDbStore.l1MongoCollection.find().forEach(printBlock);
    //
  }
}
