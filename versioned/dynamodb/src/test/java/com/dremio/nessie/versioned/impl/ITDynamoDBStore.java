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
package com.dremio.nessie.versioned.impl;

import com.dremio.nessie.versioned.LocalDynamoDB;
import com.dremio.nessie.versioned.store.dynamo.DynamoStore;
import com.dremio.nessie.versioned.tests.AbstractTestStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A test class that contains MongoDB specific tests.
 */
@ExtendWith(LocalDynamoDB.class)
class ITDynamoDBStore extends AbstractTestStore<DynamoStore> {
  private DynamoStoreFixture fixture;

  @BeforeEach
  public void setup() {
    super.store = null;
    fixture = new DynamoStoreFixture();
    super.setup();
  }

  @AfterEach
  void deleteResources() {
    fixture.close();
  }

  /**
   * Creates an instance of MongoDBStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected DynamoStore createStore() {
    return fixture.getStore();
  }

  @Override
  protected long getRandomSeed() {
    return 8612341233543L;
  }

  @Override
  protected void resetStoreState() {
    // empty
  }
}
