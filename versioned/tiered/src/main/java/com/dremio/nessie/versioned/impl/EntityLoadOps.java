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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.ValueType;

/**
 * Builder to create {@link LoadOp}s in a {@link LoadStep}.
 */
final class EntityLoadOps {
  private final Map<EntityType<?, ?>, List<Deferred>> deferred = new HashMap<>();
  private final Map<LoadOpKey, EntityLoadOp<?>> direct = new HashMap<>();

  EntityLoadOps() {
    // empty
  }

  /**
   * Type-safe variant of {@link #load(EntityType, Id, Consumer)}.
   *
   * @param valueType type to load
   * @param entityType entity class
   * @param id id to load
   * @param consumer consumer that will receive the loaded entity
   * @param <E> entity type
   */
  @SuppressWarnings("unused")
  <C extends BaseConsumer<C>, E extends PersistentBase<C>> void load(
      EntityType<C, E> valueType, Class<E> entityType, Id id, Consumer<E> consumer) {
    load(valueType, id, consumer);
  }

  /**
   * Registers a {@link Consumer} to receive the materialized instance of an entity to load.
   *
   * @param valueType type to load
   * @param id id to load
   * @param consumer consumer that will receive the loaded entity
   * @param <E> entity type
   */
  <C extends BaseConsumer<C>, E extends PersistentBase<C>> void load(
      EntityType<C, E> valueType, Id id, Consumer<E> consumer) {
    EntityLoadOp<?> loadOp = direct
        .computeIfAbsent(new LoadOpKey(valueType, id), k -> new EntityLoadOp<>(valueType.valueType, k.id));
    @SuppressWarnings({"unchecked", "rawtypes"}) Consumer<HasId> c = (Consumer) consumer;
    loadOp.consumers.add(c);
  }

  /**
   * Type-safe variant of {@link #loadDeferred(EntityType, Supplier, Consumer)}.
   *
   * @param valueType type to load
   * @param entityType entity class
   * @param id supplier of the id to load, which will be called when one of the {@code build()} methods is executed
   * @param consumer consumer that will receive the loaded entity
   * @param <E> entity type
   */
  @SuppressWarnings("unused")
  <C extends BaseConsumer<C>, E extends PersistentBase<C>> void loadDeferred(
      EntityType<C, E> valueType, Class<E> entityType, Supplier<Id> id, Consumer<E> consumer) {
    loadDeferred(valueType, id, consumer);
  }

  /**
   * Registers a {@link Consumer} to receive the materialized instance of an entity to load, the
   * {@link Id} to load is evaluated when one of the {@code build()} methods is executed.
   *
   * @param valueType type to load
   * @param id supplier of the id to load, which will be called when one of the {@code build()} methods is executed
   * @param consumer consumer that will receive the loaded entity
   * @param <E> entity type
   */
  <C extends BaseConsumer<C>, E extends PersistentBase<C>> void loadDeferred(
      EntityType<C, E> valueType, Supplier<Id> id, Consumer<E> consumer) {
    List<Deferred> consumers = deferred.computeIfAbsent(valueType, k -> new ArrayList<>());
    @SuppressWarnings({"unchecked", "rawtypes"}) Consumer<HasId> c = (Consumer) consumer;
    consumers.add(new Deferred(id, c));
  }

  /**
   * Checks whether any load-operation has been registered.
   *
   * @return {@code true}, if any load-operation has been registered.
   */
  boolean isEmpty() {
    return direct.isEmpty() && deferred.isEmpty();
  }

  /**
   * Creates an optional with a load-step.
   *
   * @return an {@link Optional} with the built {@link LoadStep} or empty, if no load-ops are registered.
   */
  Optional<LoadStep> buildOptional() {
    return isEmpty() ? Optional.empty() : Optional.of(build());
  }

  /**
   * Shortcut for {@code build(Optional::empty)}.
   *
   * @return created load-step
   */
  LoadStep build() {
    return build(Optional::empty);
  }

  /**
   * Evaluates all {@link Id}s from the registered
   * {@link #loadDeferred(EntityType, Class, Supplier, Consumer) deferred loads} and
   * produces one {@link LoadOp} per pair of {@link ValueType}+{@link Id}.
   *
   * @param next the optional next load-step for the created load-step
   * @return load-step
   */
  LoadStep build(Supplier<Optional<LoadStep>> next) {
    deferred.forEach((type, l) -> l.forEach(d -> buildDeferred(type, d)));

    return new EntityLoadStep(direct, next);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void buildDeferred(EntityType<?, ?> type, Deferred d) {
    Consumer c = d.consumer;
    load(type, d.idSupplier.get(), c);
  }

  private static final class Deferred {
    private final Supplier<Id> idSupplier;
    private final Consumer<HasId> consumer;

    Deferred(Supplier<Id> idSupplier, Consumer<HasId> consumer) {
      this.idSupplier = idSupplier;
      this.consumer = consumer;
    }
  }

  private static final class LoadOpKey {
    private final EntityType<?, ?> type;
    private final Id id;

    LoadOpKey(EntityType<?, ?> type, Id id) {
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
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      LoadOpKey other = (LoadOpKey) obj;
      return Objects.equals(id, other.id) && type.equals(other.type);
    }

    @Override
    public String toString() {
      return "LoadOpKey{" + "type=" + type
          + ", id=" + id
          + '}';
    }
  }

  @SuppressWarnings("rawtypes")
  private static class EntityLoadOp<C extends BaseConsumer<C>> extends LoadOp<C> {
    private final List<Consumer<HasId>> consumers = new ArrayList<>();
    private C receiver;

    EntityLoadOp(ValueType<C> type, Id id) {
      super(type, id);
    }

    @Override
    public C getReceiver() {
      receiver = EntityType.forType(getValueType()).newEntityProducer();
      return receiver;
    }

    @Override
    public void done() {
      PersistentBase entity = EntityType.forType(getValueType()).buildFromProducer(receiver);
      consumers.forEach(c -> c.accept(entity));
    }

    EntityLoadOp<C> combine(EntityLoadOp<C> other) {
      consumers.addAll(other.consumers);
      return this;
    }

    @Override
    public String toString() {
      return "EntityLoadOp{" + getValueType() + ":" + getId() + ", " + consumers.size()
          + " consumers}";
    }
  }

  private static class EntityLoadStep implements LoadStep {

    private final Supplier<Optional<LoadStep>> next;
    private final Map<LoadOpKey, EntityLoadOp<?>> ops;

    EntityLoadStep(Map<LoadOpKey, EntityLoadOp<?>> ops, Supplier<Optional<LoadStep>> next) {
      this.next = next;
      this.ops = ops;
    }

    @Override
    public Optional<LoadStep> getNext() {
      return next.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Stream<LoadOp<?>> getOps() {
      return ops.values().stream().map(LoadOp.class::cast);
    }

    @SuppressWarnings("OptionalIsPresent")
    @Override
    public LoadStep combine(LoadStep other) {
      EntityLoadStep o = (EntityLoadStep) other;

      o.ops.forEach((id, op) -> this.ops.compute(id, (k, old) -> old != null ? old.combine((EntityLoadOp) op) : op));

      return new EntityLoadStep(ops, () -> {
        Optional<LoadStep> nextA = this.next.get();
        Optional<LoadStep> nextB = o.next.get();
        if (nextA.isPresent()) {
          if (nextB.isPresent()) {
            return Optional.of(nextA.get().combine(nextB.get()));
          }

          return nextA;
        }

        return nextB;
      });
    }
  }
}
