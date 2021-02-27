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
package org.projectnessie.versioned.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.EntityType;
import org.projectnessie.versioned.impl.PersistentBase;
import org.projectnessie.versioned.impl.PersistentBase.EntityBuilder;
import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.HasId;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.LoadStep;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

/**
 * Simple mock store that holds data in a map for simple tests.
 */
public class MockStore implements Store {

  private final Map<ItemKey, Object> items = new HashMap<>();

  @Override
  public void start() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void load(LoadStep loadstep) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <C extends BaseValue<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> condition) {
    if (condition.isPresent()) {
      throw new UnsupportedOperationException();
    }
    EntityType<C, ?, ?> et = EntityType.forType(saveOp.getType());
    EntityBuilder<?, ?> eb = et.newEntityProducer();
    saveOp.serialize((C) eb);
    HasId h = eb.build();
    items.put(new ItemKey(et, h.getId()), h);
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer) throws NotFoundException {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    return items.entrySet()
        .stream()
        .filter(e -> e.getKey().type.valueType.equals(type))
        .map(
          e ->
              c -> ((PersistentBase<C>) e.getValue()).applyToConsumer(c)
          );
  }


  private static class ItemKey {

    private final EntityType<?, ?, ?> type;
    private final Id id;

    public ItemKey(EntityType<?, ?, ?> type, Id id) {
      super();
      this.type = type;
      this.id = id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, type);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ItemKey)) {
        return false;
      }
      ItemKey other = (ItemKey) obj;
      return Objects.equals(id, other.id) && type == other.type;
    }

  }
}
