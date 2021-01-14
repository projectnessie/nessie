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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.ValueType;

/**
 * Builder to create {@link LoadOp}s in a {@link LoadStep}.
 */
public final class EntityLoadOps {
  private final Map<ValueType, List<Deferred>> deferred = new EnumMap<>(ValueType.class);
  private final Map<LoadOpKey, List<Consumer<HasId>>> direct = new HashMap<>();

  public EntityLoadOps() {
    // empty
  }

  /**
   * Type-safe variant of {@link #load(ValueType, Id, Consumer)}.
   *
   * @param valueType type to load
   * @param entityType entity class
   * @param id id to load
   * @param consumer consumer that will receive the loaded entity
   * @param <E> entity type
   */
  @SuppressWarnings("unused")
  public <E extends HasId> void load(ValueType valueType, Class<E> entityType, Id id, Consumer<E> consumer) {
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
  public <E extends HasId> void load(ValueType valueType, Id id, Consumer<E> consumer) {
    List<Consumer<HasId>> consumers = direct
        .computeIfAbsent(new LoadOpKey(valueType, id), k -> new ArrayList<>());
    @SuppressWarnings("unchecked") Consumer<HasId> c = (Consumer<HasId>) consumer;
    consumers.add(c);
  }

  /**
   * Type-safe variant of {@link #loadDeferred(ValueType, Supplier, Consumer)}.
   *
   * @param valueType type to load
   * @param entityType entity class
   * @param id supplier of the id to load, which will be called when one of the {@code build()} methods is executed
   * @param consumer consumer that will receive the loaded entity
   * @param <E> entity type
   */
  @SuppressWarnings("unused")
  public <E extends HasId> void loadDeferred(ValueType valueType, Class<E> entityType, Supplier<Id> id, Consumer<E> consumer) {
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
  public <E extends HasId> void loadDeferred(ValueType valueType, Supplier<Id> id, Consumer<E> consumer) {
    List<Deferred> consumers = deferred.computeIfAbsent(valueType, k -> new ArrayList<>());
    @SuppressWarnings("unchecked") Consumer<HasId> c = (Consumer<HasId>) consumer;
    consumers.add(new Deferred(id, c));
  }

  /**
   * Checks whether any load-operation has been registered.
   *
   * @return {@code true}, if any load-operation has been registered.
   */
  public boolean isEmpty() {
    return direct.isEmpty() && deferred.isEmpty();
  }

  /**
   * Creates an optional with a load-step.
   *
   * @return an {@link Optional} with the built {@link LoadStep} or empty, if no load-ops are registered.
   */
  public Optional<LoadStep> buildOptional() {
    return isEmpty() ? Optional.empty() : Optional.of(build());
  }

  /**
   * Shortcut for {@code build(Optional::empty)}.
   *
   * @return created load-step
   */
  public LoadStep build() {
    return build(Optional::empty);
  }

  /**
   * Evaluates all {@link Id}s from the registered
   * {@link #loadDeferred(ValueType, Class, Supplier, Consumer) deferred loads} and
   * produces one {@link LoadOp} per pair of {@link ValueType}+{@link Id}.
   *
   * @param next the optional next load-step for the created load-step
   * @return load-step
   */
  public LoadStep build(Supplier<Optional<LoadStep>> next) {
    deferred.forEach((t, l) -> l.forEach(d -> load(t, d.idSupplier.get(), d.consumer)));

    List<LoadOp<?>> ops = direct.entrySet().stream()
        .map(e -> e.getKey().createLoadOp(v -> e.getValue().forEach(c -> c.accept(v))))
        .collect(Collectors.toList());

    return new LoadStep(ops, next);
  }

  private static final class Deferred {
    private final Supplier<Id> idSupplier;
    private final Consumer<HasId> consumer;

    public Deferred(Supplier<Id> idSupplier,
        Consumer<HasId> consumer) {
      this.idSupplier = idSupplier;
      this.consumer = consumer;
    }
  }

  private static final class LoadOpKey {
    private final ValueType type;
    private final Id id;

    LoadOpKey(ValueType type, Id id) {
      super();
      this.type = type;
      this.id = id;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    LoadOp<?> createLoadOp(Consumer<HasId> finished) {
      BaseConsumer producer = type.newEntityProducer();
      return type.createLoadOp(id, producer, prod -> finished.accept(type.buildFromProducer(prod)));
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
      return Objects.equals(id, other.id) && type == other.type;
    }
  }
}
