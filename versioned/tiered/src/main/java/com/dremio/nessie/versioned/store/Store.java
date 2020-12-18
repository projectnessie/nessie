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

import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;

public interface Store extends AutoCloseable {

  String KEY_NAME = "id";

  /**
   * Start the store.
   * @throws StoreOperationException Thrown if Store fails to initialize.
   */
  void start();

  /**
   * Close the store and any underlying resources or connections.
   */
  @Override
  void close();

  /**
   * Load the collection of {@link LoadStep}s in order.
   *
   * <p>This Will fail if any load within any step. Consumers are informed as the
   * records are loaded so this load may leave inputs in a partial state.
   *
   * @param loadstep The first step of the chain to load.
   * @throws NotFoundException If one or more items are not found.
   * @throws StoreOperationException If the store fails to complete an operation.
   */
  void load(LoadStep loadstep);

  /**
   * Put a value if the record does not currently exist.
   *
   * @param <V> The value type.
   * @param type The {@link ValueType} to insert.
   * @param value The value to insert. Should match {@code ValueType.getObjectClass()}.
   * @return Returns true if the item was absent and is now inserted.
   * @throws StoreOperationException Thrown if some kind of underlying operation fails.
   */
  <V> boolean putIfAbsent(ValueType type, V value);

  /**
   * Put a value in the store.
   *
   * <p>This could be an insert or an update. A condition can be optionally provided that will be validated
   * before completing the put operation.
   *
   * @param <V> The value type.
   * @param type The {@link ValueType} to insert/update.
   * @param value The value to insert. Should match {@code ValueType.getObjectClass()}.
   * @param condition The optional condition to check before doing the put operation.
   * @throws ConditionFailedException If the condition provided didn't match the current state of the value.
   * @throws StoreOperationException Thrown if some kind of underlying storage operation fails.
   */
  <V> void put(ValueType type, V value, Optional<ConditionExpression> condition);

  /**
   * Delete a value.
   *
   * @param type The {@link ValueType} to delete.
   * @param id The id of the value to be deleted.
   * @param condition An optional condition that must be met before deleting the value.
   * @return true if the delete condition was matched and the value was deleted.
   * @throws NotFoundException If no value was found for the specified Id.
   * @throws StoreOperationException Thrown if some kind of underlying storage operation fails.
   */
  boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition);

  /**
   * Batch put/save operation.
   *
   * <p>Saves a large number of items. This has the same insert/update behavior as a single value
   * put.
   *
   * <p>Note that that this operation is not guaranteed to be atomic. It could fail with a portion
   * of the provided input saved and a portion left unsaved.
   *
   * @param ops The set of items to save.
   * @throws StoreOperationException Thrown if some kind of underlying storage operation fails.
   */
  void save(List<SaveOp<?>> ops);

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
  <V> V loadSingle(ValueType type, Id id);

  /**
   * Do a conditional update. If the condition succeeds, return the values in the object. If it fails, return a Optional.empty().
   *
   * @param type The type of value the store is applied to.
   * @param id The id the update operation applies to
   * @param update The update expression to use.
   * @param condition The optional condition to consider before applying the update.
   * @return The complete value if the update was successful, otherwise Optional.empty()
   * @throws NotFoundException Thrown if no value is found with the provided id.
   * @throws StoreOperationException Thrown if some kind of underlying storage operation fails.
   */
  <V> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition);

  /**
   * Get a list of all available refs.
   *
   * @return A stream of refs.
   * @throws StoreOperationException Thrown if some kind of underlying storage operation fails.
   */
  Stream<InternalRef> getRefs();
}
