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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.tiered.BaseValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.util.GlobalTracer;

public class TracingStore implements Store {

  private static final Logger LOGGER = LoggerFactory.getLogger(TracingStore.class);

  private final Store store;

  public TracingStore(Store store) {
    this.store = store;
  }

  private Tracer getTracer() {
    return GlobalTracer.get();
  }

  private SpanBuilder createSpan(String name) {
    Tracer tracer = getTracer();
    return tracer.buildSpan(name).asChildOf(tracer.activeSpan());
  }

  @Override
  public void start() {
    try (Scope ignore = createSpan("Store.start").startActive(true)) {
      store.start();
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to start", e);
      throw e;
    }
  }

  @Override
  public void close() {
    try (Scope ignore = createSpan("Store.close").startActive(true)) {
      store.close();
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to close", e);
      throw e;
    }
  }

  @Override
  public void load(LoadStep loadstep) {
    try (Scope ignore = createSpan("Store.load")
        .withTag("nessie.store.operation", "LoadSingle")
        .startActive(true)) {
      store.load(loadstep);
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to load {}", loadstep, e);
      throw e;
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(
      SaveOp<C> saveOp) {
    try (Scope ignore = createSpan("Store.putIfAbsent")
        .withTag("nessie.store.operation", "LoadSingle")
        .withTag("nessie.store.value-type", safeOpTypeToString(saveOp))
        .withTag("nessie.store.id", safeOpIdToString(saveOp))
        .startActive(true)) {
      return store.putIfAbsent(saveOp);
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to put-if-absent {}", saveOp, e);
      throw e;
    }
  }

  @Override
  public <C extends BaseValue<C>> void put(
      SaveOp<C> saveOp,
      Optional<ConditionExpression> condition) {
    try (Scope ignore = createSpan("Store.put")
        .withTag("nessie.store.operation", "Put")
        .withTag("nessie.store.value-type", safeOpTypeToString(saveOp))
        .withTag("nessie.store.id", safeOpIdToString(saveOp))
        .startActive(true)) {
      store.put(saveOp, condition);
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to put {} {}", saveOp, condition, e);
      throw e;
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(
      ValueType<C> type, Id id,
      Optional<ConditionExpression> condition) {
    try (Scope ignore = createSpan("Store.delete")
        .withTag("nessie.store.operation", "Delete")
        .withTag("nessie.store.value-type", safeName(type))
        .withTag("nessie.store.id", safeToString(id))
        .startActive(true)) {
      return store.delete(type, id, condition);
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to delete {} {}", type, id, e);
      throw e;
    }
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    try (Scope scope = createSpan("Store.save")
        .withTag("nessie.store.operation", "Save")
        .withTag("nessie.store.num-ops", safeSize(ops))
        .startActive(true)) {

      scope.span().log(ops.stream().collect(Collectors.groupingBy(
          op -> String.format("nessie.store.save.%s.ids", op.getType().name()),
          Collectors.mapping(op -> op.getId().toString(), Collectors.joining(", "))
      )));

      store.save(ops);
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to save {}", ops, e);
      throw e;
    }
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(
      ValueType<C> type, Id id, C consumer) {
    try (Scope ignore = createSpan("Store.loadSingle")
        .withTag("nessie.store.operation", "LoadSingle")
        .withTag("nessie.store.value-type", safeName(type))
        .withTag("nessie.store.id", safeToString(id))
        .startActive(true)) {
      store.loadSingle(type, id, consumer);
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to load-single {} {} {}", type, id, consumer, e);
      throw e;
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean update(
      ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition,
      Optional<BaseValue<C>> consumer) throws NotFoundException {
    try (Scope ignore = createSpan("Store.update")
        .withTag("nessie.store.operation", "Update")
        .withTag("nessie.store.value-type", safeName(type))
        .withTag("nessie.store.id", safeToString(id))
        .withTag("nessie.store.update", safeToString(update))
        .withTag("nessie.store.condition", safeToString(condition))
        .startActive(true)) {
      return store.update(type, id, update, condition, consumer);
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to update {} {} {} {} {}", type, id, update, condition, consumer, e);
      throw e;
    }
  }

  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(
      ValueType<C> type) {
    try (Scope ignore = createSpan("Store.getValues")
        .withTag("nessie.store.operation", "GetValues")
        .withTag("nessie.store.value-type", type.name())
        .startActive(true)) {
      return store.getValues(type);
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to get values {}", type, e);
      throw e;
    }
  }

  private static <C extends BaseValue<C>> String safeName(ValueType<C> type) {
    return type != null ? type.name() : null;
  }

  private static String safeToString(Object o) {
    return o != null ? o.toString() : "<null>";
  }

  private static int safeSize(Collection<?> collection) {
    return collection != null ? collection.size() : -1;
  }

  private static <C extends BaseValue<C>> String safeOpIdToString(SaveOp<C> saveOp) {
    return saveOp != null ? safeToString(saveOp.getId()) : "<null>";
  }

  private static <C extends BaseValue<C>> String safeOpTypeToString(SaveOp<C> saveOp) {
    return saveOp != null ? safeName(saveOp.getType()) : "<null>";
  }
}
