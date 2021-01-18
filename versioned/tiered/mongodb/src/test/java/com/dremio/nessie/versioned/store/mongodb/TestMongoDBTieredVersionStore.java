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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import com.dremio.nessie.versioned.impl.AbstractITTieredVersionStore;

@ExtendWith(LocalMongo.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled("MongoDBStore not fully implemented")
class TestMongoDBTieredVersionStore extends AbstractITTieredVersionStore {
  private static final String testDatabaseName = "mydb";
  private String connectionString;

  @BeforeAll
  void init(String connectionString) {
    this.connectionString = connectionString;
  }

  @Override
  protected MongoStoreFixture createNewFixture() {
    return new MongoStoreFixture(connectionString, testDatabaseName);
  }
}
