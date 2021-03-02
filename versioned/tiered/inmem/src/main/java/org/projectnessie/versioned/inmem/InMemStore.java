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
package org.projectnessie.versioned.inmem;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.EntityStoreHelper;
import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.inmem.BaseObj.BaseObjProducer;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.LoadOp;
import org.projectnessie.versioned.store.LoadStep;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * A <em>non-production</em> {@link Store} implementation for testing and benchmarking purposes.
 * <p>Based on one {@link ConcurrentMap} for each value-type.</p>
 */
public class InMemStore implements Store {

  private static final InMemValueVisitor VALUE_VISITOR = new InMemValueVisitor();

  private final Map<ValueType<?>, ConcurrentMap<Id, BaseObj<?>>> store;

  private final Map<ValueType<?>, Supplier<? extends BaseObjProducer<?>>> typeSupplierMap;

  /**
   * Construct the in-memory {@link Store} implementation.
   */
  public InMemStore() {
    Builder<ValueType<?>, ConcurrentMap<Id, BaseObj<?>>> storeBuilder = ImmutableMap.builder();
    Builder<ValueType<?>, Supplier<? extends BaseObjProducer<?>>> typeSupplierBuilder = ImmutableMap.builder();

    for (ValueType<?> value : ValueType.values()) {
      storeBuilder.put(value, new ConcurrentHashMap<>());
    }

    typeSupplierBuilder.put(ValueType.L1, L1Obj.L1Producer::new);
    typeSupplierBuilder.put(ValueType.L2, L2Obj.L2Producer::new);
    typeSupplierBuilder.put(ValueType.L3, L3Obj.L3Producer::new);
    typeSupplierBuilder.put(ValueType.COMMIT_METADATA, CommitMetadataObj.CommitMetadataProducer::new);
    typeSupplierBuilder.put(ValueType.KEY_FRAGMENT, FragmentObj.FragmentProducer::new);
    typeSupplierBuilder.put(ValueType.REF, RefObj.RefProducer::new);
    typeSupplierBuilder.put(ValueType.VALUE, ValueObj.ValueProducer::new);

    this.store = storeBuilder.build();
    this.typeSupplierMap = typeSupplierBuilder.build();
  }

  @Override
  public void start() {
    // make sure we have an empty l1 (ignore result, doesn't matter)
    EntityStoreHelper.storeMinimumEntities(this::putIfAbsent);
  }

  @Override
  public void close() {
    store.values().forEach(Map::clear);
  }

  @Override
  public void load(LoadStep loadstep) {
    while (true) {
      Map<ValueType<?>, List<Id>> missing = new HashMap<>();
      loadstep.getOps().forEach(op -> loadSingle(op, missing));

      if (!missing.isEmpty()) {
        throw new NotFoundException(missing.toString());
      }

      Optional<LoadStep> next = loadstep.getNext();
      if (!next.isPresent()) {
        break;
      }
      loadstep = next.get();
    }
  }

  private <C extends BaseValue<C>> void loadSingle(LoadOp<C> op, Map<ValueType<?>, List<Id>> missing) {
    BaseObj<C> obj = store(op.getValueType()).get(op.getId());
    if (obj == null) {
      missing.computeIfAbsent(op.getValueType(), x -> new ArrayList<>()).add(op.getId());
    } else {
      obj.consume(op.getReceiver());
      op.done();
    }
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer) {
    BaseObj<C> obj = store(type).get(id);
    if (obj == null) {
      throw new NotFoundException(String.format("Unable to load item %s:%s.", type, id));
    }
    obj.consume(consumer);
  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    return store(saveOp.getType()).putIfAbsent(saveOp.getId(), produce(saveOp)) == null;
  }

  @SuppressWarnings("unchecked")
  private <C extends BaseValue<C>> BaseObj<C> produce(SaveOp<C> saveOp) {
    BaseObjProducer<C> objProducer = newProducer(saveOp.getType());
    saveOp.serialize((C) objProducer);
    return objProducer.build();
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  <C extends BaseValue<C>> BaseObjProducer<C> newProducer(ValueType<C> type) {
    BaseObjProducer<C> objProducer = (BaseObjProducer<C>) typeSupplierMap.get(type).get();
    return objProducer;
  }

  @Override
  public <C extends BaseValue<C>> void put(SaveOp<C> saveOp,
      Optional<ConditionExpression> condition) {
    ConcurrentMap<Id, BaseObj<C>> map = store(saveOp.getType());
    if (condition.isPresent()) {
      map.compute(saveOp.getId(), (id, v) -> {
        if (v != null) {
          v.evaluate(translate(condition.get()));
        } else {
          throw new ConditionFailedException("foo");
        }
        return produce(saveOp);
      });
    } else {
      map.put(saveOp.getId(), produce(saveOp));
    }
  }

  static List<Function> translate(ConditionExpression conditionExpression) {
    return conditionExpression.getFunctions().stream()
        .map(f -> f.accept(VALUE_VISITOR))
        .collect(Collectors.toList());
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id,
      Optional<ConditionExpression> condition) {
    ConcurrentMap<Id, BaseObj<C>> map = store(type);
    if (condition.isPresent()) {
      BaseObj<C> value = map.get(id);
      if (value == null) {
        return false;
      }
      try {
        value.evaluate(translate(condition.get()));
      } catch (ConditionFailedException failed) {
        return false;
      }
      return map.remove(id, value);
    } else {
      return map.remove(id) != null;
    }
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    ops.forEach(op -> store.get(op.getType()).put(op.getId(), produce(op)));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer)
      throws NotFoundException {
    ConcurrentMap<Id, BaseObj<C>> map = store(type);
    while (true) {
      BaseObj<C> value = map.get(id);
      if (value == null) {
        throw new NotFoundException(String.format("Not found: %s:%s", type, id));
      }
      if (condition.isPresent()) {
        try {
          value.evaluate(translate(condition.get()));
        } catch (ConditionFailedException failed) {
          return false;
        }
      }
      try {
        BaseObj<C> updated = value.update(update);
        map.compute(id, (i, current) -> {
          if (current != value) {
            throw new ConcurrentModificationException();
          }
          return updated;
        });
        consumer.ifPresent(c -> updated.consume((C) c));
        return true;
      } catch (ConcurrentModificationException modified) {
        // ignore, just retry
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    return store(type).values().stream().map(obj -> obj::consume);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private <C extends BaseValue<C>> ConcurrentMap<Id, BaseObj<C>> store(ValueType<C> type) {
    ConcurrentMap m = store.get(type);
    return (ConcurrentMap<Id, BaseObj<C>>) m;
  }
}
