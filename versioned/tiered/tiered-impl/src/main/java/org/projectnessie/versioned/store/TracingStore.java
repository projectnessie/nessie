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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.tiered.BaseValue;

import com.google.common.collect.ImmutableMap;

import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;

public class TracingStore implements Store {

  private final Store store;
  private final Supplier<Tracer> tracerSupplier;

  public TracingStore(Store store) {
    this(store, GlobalTracer::get);
  }

  TracingStore(Store store, Supplier<Tracer> tracerSupplier) {
    this.store = store;
    this.tracerSupplier = tracerSupplier;
  }

  private Tracer getTracer() {
    return tracerSupplier.get();
  }

  private SpanBuilder createSpan(String name) {
    Tracer tracer = getTracer();
    return tracer.buildSpan(name).asChildOf(tracer.activeSpan());
  }

  @Override
  public void start() {
    try (Scope scope = createSpan("Store.start")
        .withTag("nessie.store.operation", "Start")
        .startActive(true)) {
      try {
        store.start();
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @Override
  public void close() {
    try (Scope scope = createSpan("Store.close")
        .withTag("nessie.store.operation", "Close")
        .startActive(true)) {
      try {
        store.close();
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @Override
  public void load(LoadStep loadstep) {
    try (Scope scope = createSpan("Store.load")
        .withTag("nessie.store.operation", "Load")
        .startActive(true)) {
      try {
        store.load(loadstep);
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(
      SaveOp<C> saveOp) {
    try (Scope scope = createSpan("Store.putIfAbsent")
        .withTag("nessie.store.operation", "PutIfAbsent")
        .withTag("nessie.store.value-type", safeOpTypeToString(saveOp))
        .withTag("nessie.store.id", safeOpIdToString(saveOp))
        .startActive(true)) {
      try {
        return store.putIfAbsent(saveOp);
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> void put(
      SaveOp<C> saveOp,
      Optional<ConditionExpression> condition) {
    try (Scope scope = createSpan("Store.put")
        .withTag("nessie.store.operation", "Put")
        .withTag("nessie.store.value-type", safeOpTypeToString(saveOp))
        .withTag("nessie.store.id", safeOpIdToString(saveOp))
        .startActive(true)) {
      try {
        store.put(saveOp, condition);
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(
      ValueType<C> type, Id id,
      Optional<ConditionExpression> condition) {
    try (Scope scope = createSpan("Store.delete")
        .withTag("nessie.store.operation", "Delete")
        .withTag("nessie.store.value-type", safeName(type))
        .withTag("nessie.store.id", safeToString(id))
        .startActive(true)) {
      try {
        return store.delete(type, id, condition);
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    try (Scope scope = createSpan("Store.save")
        .withTag("nessie.store.operation", "Save")
        .withTag("nessie.store.num-ops", safeSize(ops))
        .startActive(true)) {
      try {
        scope.span().log(ops.stream().collect(Collectors.groupingBy(
            op -> String.format("nessie.store.save.%s.ids", op.getType().name()),
            Collectors.mapping(op -> op.getId().toString(), Collectors.joining(", "))
        )));

        store.save(ops);
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(
      ValueType<C> type, Id id, C consumer) {
    try (Scope scope = createSpan("Store.loadSingle")
        .withTag("nessie.store.operation", "LoadSingle")
        .withTag("nessie.store.value-type", safeName(type))
        .withTag("nessie.store.id", safeToString(id))
        .startActive(true)) {
      try {
        store.loadSingle(type, id, consumer);
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean update(
      ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition,
      Optional<BaseValue<C>> consumer) throws NotFoundException {
    try (Scope scope = createSpan("Store.update")
        .withTag("nessie.store.operation", "Update")
        .withTag("nessie.store.value-type", safeName(type))
        .withTag("nessie.store.id", safeToString(id))
        .withTag("nessie.store.update", safeToString(update))
        .withTag("nessie.store.condition", safeToString(condition))
        .startActive(true)) {
      try {
        return store.update(type, id, update, condition, consumer);
      } catch (RuntimeException e) {
        throw traceRuntimeException(scope, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(
      ValueType<C> type) {
    Scope scope = createSpan("Store.getValues")
        .withTag("nessie.store.operation", "GetValues")
        .withTag("nessie.store.value-type", type.name())
        .startActive(true);
    try {
      return store.getValues(type).onClose(scope::close);
    } catch (RuntimeException e) {
      e = traceRuntimeException(scope, e);
      scope.close();
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

  private static RuntimeException traceRuntimeException(Scope scope, RuntimeException e) {
    if (!(e instanceof StoreException) || e instanceof StoreOperationException) {
      Tags.ERROR.set(scope.span().log(ImmutableMap.of(Fields.EVENT, Tags.ERROR.getKey(),
          Fields.ERROR_OBJECT, e.toString())), true);
    }
    return e;
  }
}
