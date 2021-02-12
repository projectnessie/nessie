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
package org.projectnessie.versioned.store;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.tiered.BaseValue;

public interface Store extends AutoCloseable {

  String KEY_NAME = "id";
  String DT_NAME = "dt";

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
   * @param <C> the consumer type
   * @param saveOp The {@link SaveOp} that describes the value to put.
   * @return Returns true if the item was absent and is now inserted.
   * @throws StoreOperationException Thrown if some kind of underlying operation fails.
   */
  <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp);

  /**
   * Put a value in the store.
   *
   * <p>This could be an insert or an update. A condition can be optionally provided that will be validated
   * before completing the put operation.
   *
   * @param <C> the consumer type
   * @param saveOp The {@link SaveOp} that describes the value to put.
   * @param condition The optional condition to check before doing the put operation.
   * @throws ConditionFailedException If the condition provided didn't match the current state of the value.
   * @throws StoreOperationException Thrown if some kind of underlying storage operation fails.
   */
  <C extends BaseValue<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> condition);

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
  <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition);

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
   * Retrieve a single value and pass its properties to the given {@code consumer}.
   *
   * @param type the type to load
   * @param id id to load
   * @param consumer consumer that will receive the properties
   * @param <C> type of the consumer
   */
  <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer);

  /**
   * Do a conditional update. If the condition succeeds, optionally return the values via a consumer.
   *
   * @param type The type of value the store is applied to.
   * @param id The id the update operation applies to
   * @param update The update expression to use.
   * @param condition The optional condition to consider before applying the update.
   * @param consumer The consumer, if present, that will receive the updated value.
   * @param <C> the consumer type
   * @return {@code true} if successful
   * @throws NotFoundException Thrown if no value is found with the provided id.
   * @throws StoreOperationException Thrown if some kind of underlying storage operation fails.
   */
  <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer) throws NotFoundException;

  interface Acceptor<C extends BaseValue<C>> {
    void applyValue(C consumer);
  }

  /**
   * Get a list of all available values of the requested value type.
   *
   * @param <C> the consumer type
   * @param type The {@link ValueType} to load.
   * @return stream with {@link Acceptor} instances that accept {@link BaseValue} instances to produce values
   */
  <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type);
}
