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

import java.util.function.Consumer;

import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.ValueType;

/**
 * Ugly bridge class to hide the private entities needed to initialize a store.
 */
public final class EntityStoreHelper {

  /**
   * Helper method for store implementations to create the initial entities.
   *
   * @param consumer consumer that receives the value-type and instance
   */
  public static void storeMinimumEntities(Consumer<SaveOp<?>> consumer) {
    consumer.accept(new EntitySaveOp<>(ValueType.L1, L1.EMPTY));
    consumer.accept(new EntitySaveOp<>(ValueType.L2, L2.EMPTY));
    consumer.accept(new EntitySaveOp<>(ValueType.L3, L3.EMPTY));
  }

}
