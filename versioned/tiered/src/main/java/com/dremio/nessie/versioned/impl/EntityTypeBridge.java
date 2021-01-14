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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.ValueType;

/**
 * Ugly bridge class to hide the private entities from the outside world.
 */
public class EntityTypeBridge {

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <C extends BaseConsumer<C>, E extends HasId> E buildEntity(ValueType valueType, Consumer<C> consumer) {
    EntityType et = EntityType.forType(valueType);
    return (E) et.buildEntity(consumer);
  }

  /**
   * Helper method for store implementations to create the initial entities.
   *
   * @param consumer consumer that receives the value-type and instance
   */
  public static void storeMinimumEntities(BiConsumer<ValueType, HasId> consumer) {
    consumer.accept(ValueType.L1, L1.EMPTY);
    consumer.accept(ValueType.L2, L2.EMPTY);
    consumer.accept(ValueType.L3, L3.EMPTY);
  }
}
