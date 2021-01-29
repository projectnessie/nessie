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
package com.dremio.nessie.versioned.store.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;

import com.dremio.nessie.versioned.impl.AbstractTestStore;

/**
 * Runs basic store-tests against a database in a test-container.
 */
class ITJdbcStore extends AbstractTestStore<JdbcStore> {
  private JdbcFixture fixture;

  @AfterAll
  static void shutdown() {
    JdbcFixture.cleanup();
  }

  @AfterEach
  void deleteResources() {
    if (fixture != null) {
      fixture.close();
    }
  }

  protected JdbcFixture createFixture() {
    return new JdbcOssFixture();
  }

  /**
   * Creates an instance of MongoDBStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected JdbcStore createStore() {
    fixture = createFixture();
    return fixture.getStore();
  }

  @Override
  protected JdbcStore createRawStore() {
    return createFixture().getStore();
  }

  @Override
  protected int loadSize() {
    return 100;
  }

  @Override
  protected long getRandomSeed() {
    return 8612341233543L;
  }

  @Override
  protected void resetStoreState() {
    super.store = null;
  }
}
