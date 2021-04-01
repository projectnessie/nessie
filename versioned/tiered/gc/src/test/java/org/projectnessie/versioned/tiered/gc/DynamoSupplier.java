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
package org.projectnessie.versioned.tiered.gc;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.function.Supplier;

import org.projectnessie.versioned.dynamodb.DynamoStore;
import org.projectnessie.versioned.dynamodb.DynamoStoreConfig;
import org.projectnessie.versioned.store.Store;

public class DynamoSupplier implements Supplier<Store>, Serializable {

  private static final long serialVersionUID = 5030232198230089450L;

  static DynamoStoreConfig dynamoStoreConfig;

  public static void setDynamoStoreConfig(DynamoStoreConfig config) {
    dynamoStoreConfig = config;
  }

  static DynamoStore createStore() throws URISyntaxException {
    return new DynamoStore(dynamoStoreConfig);
  }

  @Override
  public Store get() {
    Store store;
    try {
      store = createStore();
      store.start();
      return store;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static void deleteAllTables() {
    ((DynamoStore) new DynamoSupplier().get()).deleteTables();
  }

}
