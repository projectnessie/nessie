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

import static org.projectnessie.versioned.TracingUtil.safeSize;
import static org.projectnessie.versioned.TracingUtil.safeToString;
import static org.projectnessie.versioned.TracingUtil.traceError;

import com.google.common.annotations.VisibleForTesting;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.util.GlobalTracer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.tiered.BaseValue;

public class TracingStore implements Store {

  static final String TAG_OPERATION = "nessie.store.operation";
  static final String TAG_VALUE_TYPE = "nessie.store.value-type";
  static final String TAG_ID = "nessie.store.id";
  static final String TAG_NUM_OPS = "nessie.store.num-ops";
  static final String TAG_UPDATE = "nessie.store.update";
  static final String TAG_CONDITION = "nessie.store.condition";

  private final Store store;

  public TracingStore(Store store) {
    this.store = store;
  }

  private SpanBuilder createSpan(String name) {
    Tracer tracer = GlobalTracer.get();
    String opName = makeSpanName(name);
    return tracer.buildSpan(opName).withTag(TAG_OPERATION, name).asChildOf(tracer.activeSpan());
  }

  @VisibleForTesting
  static String makeSpanName(String name) {
    return "Store." + Character.toLowerCase(name.charAt(0)) + name.substring(1);
  }

  @Override
  public void start() {
    Span span = createSpan("Start").start();
    try (Scope scope = GlobalTracer.get().activateSpan(span)) {
      try {
        store.start();
      } catch (RuntimeException e) {
        throw traceRuntimeException(span, e);
      }
    }
  }

  @Override
  public void close() {
    Span span = createSpan("Close").start();
    try (Scope scope = GlobalTracer.get().activateSpan(span)) {
      try {
        store.close();
      } catch (RuntimeException e) {
        throw traceRuntimeException(span, e);
      }
    }
  }

  @Override
  public void load(LoadStep loadstep) {
    Span span = createSpan("Load").start();
    try (Scope scope = GlobalTracer.get().activateSpan(span)) {
      try {
        store.load(loadstep);
      } catch (RuntimeException e) {
        throw traceRuntimeException(span, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    Span span =
        createSpan("PutIfAbsent")
            .withTag(TAG_VALUE_TYPE, safeOpTypeToString(saveOp))
            .withTag(TAG_ID, safeOpIdToString(saveOp))
            .start();
    try (Scope scope = GlobalTracer.get().activateSpan(span)) {
      try {
        return store.putIfAbsent(saveOp);
      } catch (RuntimeException e) {
        throw traceRuntimeException(span, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> void put(
      SaveOp<C> saveOp, Optional<ConditionExpression> condition) {
    Span span =
        createSpan("Put")
            .withTag(TAG_VALUE_TYPE, safeOpTypeToString(saveOp))
            .withTag(TAG_ID, safeOpIdToString(saveOp))
            .start();
    try (Scope scope = GlobalTracer.get().activateSpan(span)) {
      try {
        store.put(saveOp, condition);
      } catch (RuntimeException e) {
        throw traceRuntimeException(span, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(
      ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    Span span =
        createSpan("Delete")
            .withTag(TAG_VALUE_TYPE, safeName(type))
            .withTag(TAG_ID, safeToString(id))
            .start();
    try (Scope scope = GlobalTracer.get().activateSpan(span)) {
      try {
        return store.delete(type, id, condition);
      } catch (RuntimeException e) {
        throw traceRuntimeException(span, e);
      }
    }
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    Span span = createSpan("Save").withTag(TAG_NUM_OPS, safeSize(ops)).start();
    try (Scope scope = GlobalTracer.get().activateSpan(span)) {
      try {
        span.log(
            ops.stream()
                .collect(
                    Collectors.groupingBy(
                        op -> String.format("nessie.store.save.%s.ids", op.getType().name()),
                        Collectors.mapping(
                            op -> op.getId().toString(), Collectors.joining(", ")))));

        store.save(ops);
      } catch (RuntimeException e) {
        throw traceRuntimeException(span, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer) {
    Span span =
        createSpan("LoadSingle")
            .withTag(TAG_VALUE_TYPE, safeName(type))
            .withTag(TAG_ID, safeToString(id))
            .start();
    try (Scope scope = GlobalTracer.get().activateSpan(span)) {
      try {
        store.loadSingle(type, id, consumer);
      } catch (RuntimeException e) {
        throw traceRuntimeException(span, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean update(
      ValueType<C> type,
      Id id,
      UpdateExpression update,
      Optional<ConditionExpression> condition,
      Optional<BaseValue<C>> consumer)
      throws NotFoundException {
    Span span =
        createSpan("Update")
            .withTag(TAG_VALUE_TYPE, safeName(type))
            .withTag(TAG_ID, safeToString(id))
            .withTag(TAG_UPDATE, safeToString(update))
            .withTag(TAG_CONDITION, safeToString(condition))
            .start();
    try (Scope scope = GlobalTracer.get().activateSpan(span)) {
      try {
        return store.update(type, id, update, condition, consumer);
      } catch (RuntimeException e) {
        throw traceRuntimeException(span, e);
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    Span span = createSpan("GetValues").withTag(TAG_VALUE_TYPE, type.name()).start();
    Scope scope = GlobalTracer.get().activateSpan(span);
    try {
      return store.getValues(type).onClose(scope::close);
    } catch (RuntimeException e) {
      e = traceRuntimeException(span, e);
      scope.close();
      throw e;
    }
  }

  private static <C extends BaseValue<C>> String safeName(ValueType<C> type) {
    return type != null ? type.name() : null;
  }

  private static <C extends BaseValue<C>> String safeOpIdToString(SaveOp<C> saveOp) {
    return saveOp != null ? safeToString(saveOp.getId()) : "<null>";
  }

  private static <C extends BaseValue<C>> String safeOpTypeToString(SaveOp<C> saveOp) {
    return saveOp != null ? safeName(saveOp.getType()) : "<null>";
  }

  private static RuntimeException traceRuntimeException(Span span, RuntimeException e) {
    if (!(e instanceof StoreException) || e instanceof StoreOperationException) {
      return traceError(span, e);
    }
    return e;
  }
}
