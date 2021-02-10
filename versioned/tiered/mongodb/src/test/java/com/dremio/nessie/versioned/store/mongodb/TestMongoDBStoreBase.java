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

import com.dremio.nessie.versioned.impl.AbstractTestStore;

/**
 * A base test class that contains MongoDB specific tests.
 */
abstract class TestMongoDBStoreBase extends AbstractTestStore<MongoDBStore> {
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
    };

    return new MongoDBStore(config);
  }

  @Override
  protected MongoDBStore createRawStore() {
    return createStore();
  }

  @Override
  protected void resetStoreState() {
    store.resetCollections();
  }

  @Override
  protected int loadSize() {
    return MongoDBStore.LOAD_SIZE;
  }
}
