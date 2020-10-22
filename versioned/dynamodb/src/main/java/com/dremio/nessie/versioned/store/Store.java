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
package com.dremio.nessie.versioned.store;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;

public interface Store extends AutoCloseable {

  String KEY_NAME = "id";

  void start();

  @Override
  void close();

  /**
   * Load the collection of loadsteps in order.
   *
   * <p>This Will fail if any load within any step. Consumers are informed as the
   * records are loaded so this load may leave inputs in a partial state.
   *
   * @param loadstep The step to load
   */
  void load(LoadStep loadstep) throws ReferenceNotFoundException;

  <V> boolean putIfAbsent(ValueType type, V value);

  <V> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased);

  boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition);

  void save(List<SaveOp<?>> ops);

  <V> V loadSingle(ValueType valueType, Id id);

  /**
   * Do a conditional update. If the condition succeeds, return the values in the object. If it fails, return a Optional.empty().
   *
   * @param type The type of value the store is applied to.
   * @param id The id the update operation applies to
   * @param update The update expression to use.
   * @param condition The optional condition to consider before applying the update.
   * @return The complete value if the update was successful, otherwise Optional.empty()
   * @throws ReferenceNotFoundException Thrown if the underlying id doesn't have an object.
   */
  <V> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition)
      throws ReferenceNotFoundException;

  Stream<InternalRef> getRefs();
}
