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

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.Iterables;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TestMongoDbStore {
  private final ConnectionString connectionString = new ConnectionString("mongodb://localhost:27017");
  private final String testDatabaseName = "mydb";
  private MongoDbStore mongoDbStore;

  /**
   * Set up the objects necessary for the tests.
   */
  @BeforeEach
  public void setUp() {
    mongoDbStore = new MongoDbStore(connectionString, testDatabaseName);
    mongoDbStore.start();
  }

  /**
   * Close the store and clean up written data.
   */
  @AfterEach
  public void teardown() {
    // Clean up the collections.
    final BasicDBObject blank = new BasicDBObject();
    mongoDbStore.collections.forEach((k, v) -> v.deleteMany(blank));
    mongoDbStore.close();
  }

  @Test
  public void testDatabaseName() {
    final MongoDatabase mongoDatabase = mongoDbStore.getMongoDatabase();
    assertEquals(mongoDatabase.getName(), testDatabaseName);
  }

  @Test
  public void putL1Value() {
    final L1 sampleL1 = TestSamples.getSampleL1();
    mongoDbStore.put(ValueType.L1, sampleL1, Optional.empty());
    final L1 readL1 = (L1)Iterables.get(mongoDbStore.collections.get(ValueType.L1).find(), 0);
    assertEquals(L1.SCHEMA.itemToMap(sampleL1, true), L1.SCHEMA.itemToMap(readL1, true));
  }

  @Test
  public void putL2Value() {
    final L2 sampleL2 = TestSamples.getSampleL2();
    mongoDbStore.put(ValueType.L2, sampleL2, Optional.empty());
    final L2 readL2 = (L2)Iterables.get(mongoDbStore.collections.get(ValueType.L2).find(), 0);
    assertEquals(L2.SCHEMA.itemToMap(sampleL2, true), L2.SCHEMA.itemToMap(readL2, true));
  }

  @Test
  public void putL3Value() {
    final L3 sampleL3 = TestSamples.getSampleL3();
    mongoDbStore.put(ValueType.L3, sampleL3, Optional.empty());
    final L3 readL3 = (L3)Iterables.get(mongoDbStore.collections.get(ValueType.L3).find(), 0);
    assertEquals(L3.SCHEMA.itemToMap(sampleL3, true), L3.SCHEMA.itemToMap(readL3, true));
  }

  @Test
  public void loadSingleL1Value() {
    final L1 sampleL1 = TestSamples.getSampleL1();
    mongoDbStore.put(ValueType.L1, sampleL1, Optional.empty());
    L1 readL1 = (L1)mongoDbStore.loadSingle(ValueType.L1, sampleL1.getId());
    assertEquals(L1.SCHEMA.itemToMap(sampleL1, true), L1.SCHEMA.itemToMap(readL1, true));
  }
}
