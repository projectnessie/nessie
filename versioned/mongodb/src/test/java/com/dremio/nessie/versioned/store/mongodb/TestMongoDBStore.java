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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.tests.AbstractTestStore;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

/**
 * A test class that contains MongoDB specific tests.
 */
@ExtendWith(LocalMongo.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMongoDBStore extends AbstractTestStore<MongoDBStore> {
  private static final String testDatabaseName = "mydb";
  private String connectionString;

  @BeforeAll
  void init(String connectionString) {
    this.connectionString = connectionString;
  }

  @AfterAll
  void close() {
    if (null != store) {
      store.close();
    }
  }

  /**
   * Creates an instance of MongoDBStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected MongoDBStore createStore() {
    final MongoStoreConfig config = new MongoStoreConfig() {
      @Override
      public String getConnectionString() {
        return connectionString;
      }

      @Override
      public String getDatabaseName() {
        return testDatabaseName;
      }
    };

    return new MongoDBStore(config);
  }

  @Override
  protected long getRandomSeed() {
    return 8612341233543L;
  }

  @Override
  protected void resetStoreState() {
    store.resetCollections();
  }

  @Test
  void loadPagination() throws ReferenceNotFoundException {
    final ImmutableMultimap.Builder<ValueType, HasId> builder = ImmutableMultimap.builder();
    for (int i = 0; i < (10 + MongoDBStore.LOAD_SIZE); ++i) {
      // Only create a single type as this is meant to test the pagination within Mongo, not the variety. Variety is
      // taken care of by a test in AbstractTestStore.
      builder.put(ValueType.REF, SampleEntities.createTag(random));
    }

    final Multimap<ValueType, HasId> objs = builder.build();
    objs.forEach(this::putThenLoad);

    testLoad(objs);
  }
}
