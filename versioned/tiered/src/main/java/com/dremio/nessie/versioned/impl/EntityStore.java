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

import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.StoreOperationException;
import com.dremio.nessie.versioned.store.ValueType;

/**
 * Wraps entity instances related method around a {@link Store}.
 */
class EntityStore {
  final Store store;

  EntityStore(Store store) {
    this.store = store;
  }

  /**
   * Retrieve a single value.
   *
   * @param <V> The value type.
   * @param type The {@link ValueType} to load.
   * @param id The id of the value.
   * @return The value at the given Id.
   * @throws NotFoundException If the value is not found.
   * @throws StoreOperationException Thrown if some kind of underlying storage operation fails.
   */
  <V extends HasId> V loadSingle(ValueType valueType, Id id) {
    return EntityTypeBridge.buildEntity(valueType, c -> {
      store.loadSingle(valueType, id, c);
    });
  }

}
