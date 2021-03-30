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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Stubber;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.tiered.L3;

import com.google.common.collect.ImmutableMap;

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;

class TestTracingStore {

  private static Stream<Arguments> storeInvocations() {
    // Exception-throws to be tested, one list per distinct throws clause
    List<Exception> storeThrows = Arrays.asList(
      new IllegalArgumentException("illegal-arg"),
      new NullPointerException("NPE"),
      new RuntimeException("NPE"),
      new StoreOperationException("NPE"),
      new StoreException("general store exception"),
      new NotFoundException("store not-found-exception")
    );

    Id someId = Id.of(new byte[20]);
    LoadStep someLoadStep = mock(LoadStep.class);
    L3 someConsumer = mock(L3.class);
    @SuppressWarnings("unchecked") SaveOp<L3> someSaveOp = mock(SaveOp.class);
    when(someSaveOp.getId()).thenReturn(someId);
    when(someSaveOp.getType()).thenReturn(ValueType.L3);

    // "Declare" test-invocations for all Store functions with their respective outcomes
    // and exceptions.
    Stream<StoreInvocation<?>> storeFunctions = Stream.of(
        new StoreInvocation<>("start",
            ImmutableMap.of(),
            Collections.emptyList(),
            Store::start,
            storeThrows),
        new StoreInvocation<>("close",
            ImmutableMap.of(),
            Collections.emptyList(),
            Store::close,
            storeThrows),
        new StoreInvocation<>("load",
            ImmutableMap.of(),
            Collections.emptyList(),
            s -> s.load(someLoadStep),
            storeThrows),
        new StoreInvocation<>("putIfAbsent",
            ImmutableMap.of("nessie.store.id", "0000000000000000000000000000000000000000",
                "nessie.store.value-type", "l3"),
            Collections.emptyList(),
            s -> s.putIfAbsent(someSaveOp),
            true, storeThrows),
        new StoreInvocation<>("put",
            ImmutableMap.of("nessie.store.id", "0000000000000000000000000000000000000000",
                "nessie.store.value-type", "l3"),
            Collections.emptyList(),
            s -> s.put(someSaveOp, Optional.empty()),
            storeThrows),
        new StoreInvocation<>("delete",
            ImmutableMap.of("nessie.store.id", "0000000000000000000000000000000000000000",
                "nessie.store.value-type", "l3"),
            Collections.emptyList(),
            s -> s.delete(ValueType.L3, someId, Optional.empty()),
            true, storeThrows),
        new StoreInvocation<>("save",
            ImmutableMap.of("nessie.store.num-ops", 1),
            Collections.singletonList(Collections.singletonMap("nessie.store.save.l3.ids", "0000000000000000000000000000000000000000")),
            s -> s.save(Collections.singletonList(someSaveOp)),
            storeThrows),
        new StoreInvocation<>("loadSingle",
            ImmutableMap.of("nessie.store.id", "0000000000000000000000000000000000000000",
                "nessie.store.value-type", "l3"),
            Collections.emptyList(),
            s -> s.loadSingle(ValueType.L3, someId, someConsumer),
            storeThrows),
        new StoreInvocation<>("update",
            ImmutableMap.of("nessie.store.id", "0000000000000000000000000000000000000000",
                "nessie.store.value-type", "l3",
                "nessie.store.update", "UpdateExpression{clauses=[]}",
                "nessie.store.condition", "Optional.empty"),
            Collections.emptyList(),
            s -> s.update(ValueType.L3, someId, UpdateExpression.initial(), Optional.empty(), Optional.empty()),
            true, storeThrows)
    );

    // flatten all "normal executions" + "throws XYZ"
    return storeFunctions.flatMap(invocation -> {
          // Construct a stream of arguments, both "normal" results and exceptional results.
          Stream<Arguments> normalExecs = Stream.of(
              Arguments.of(invocation.opName, null, invocation.tags, invocation.logs, invocation.result, invocation.function)
          );
          Stream<Arguments> exceptionalExecs = invocation.failures.stream().map(
              ex -> Arguments.of(invocation.opName, ex, invocation.tags, invocation.logs, null, invocation.function)
          );
          return Stream.concat(normalExecs, exceptionalExecs);
        }
    );
  }

  @ParameterizedTest
  @MethodSource("storeInvocations")
  void storeInvocation(String opName, Exception expectedThrow, Map<String, ?> tags, List<Map<String, String>> logs,
      Object result, ThrowingFunction<?, Store> storeFunction) throws Throwable {
    Stubber stubber;
    if (expectedThrow != null) {
      // The invocation expects an exception to be thrown
      stubber = doThrow(expectedThrow);
    } else if (result != null) {
      // Non-void method
      stubber = doReturn(result);
    } else {
      // void method
      stubber = doNothing();
    }

    TestTracer tracer = new TestTracer();
    Store mockedStore = mock(Store.class);
    storeFunction.accept(stubber.when(mockedStore));
    Store store = new TracingStore(mockedStore, () -> tracer);

    ThrowingConsumer<Store> storeExec = s -> {
      Object r = storeFunction.accept(s);
      if (result != null) {
        // non-void methods must return something
        assertNotNull(r);
      } else {
        // void methods return nothing
        assertNull(r);
      }
      if (result instanceof Stream) {
        // Stream-results shall be closed to indicate the "end" of an invocation
        Stream<?> stream = (Stream<?>) r;
        stream.forEach(ignore -> {});
        stream.close();
      }
    };

    if (expectedThrow == null) {
      // No exception expected, just invoke the Store function
      storeExec.accept(store);
    } else {
      // Surround the Store function with an 'assertThrows'
      assertEquals(expectedThrow.getMessage(),
          assertThrows(expectedThrow.getClass(),
              () -> storeExec.accept(store)).getMessage());
    }

    String uppercase = Character.toUpperCase(opName.charAt(0)) + opName.substring(1);

    boolean isServerError = expectedThrow != null
        && (!(expectedThrow instanceof StoreException) || expectedThrow instanceof StoreOperationException);

    List<Map<String, String>> expectedLogs = new ArrayList<>(logs);
    if (isServerError) {
      expectedLogs.add(ImmutableMap.of("event", "error", "error.object", expectedThrow.toString()));
    }

    ImmutableMap.Builder<Object, Object> tagsBuilder = ImmutableMap.builder().putAll(tags);
    if (isServerError) {
      tagsBuilder.put("error", true);
    }
    Map<Object, Object> expectedTags = tagsBuilder.put("nessie.store.operation", uppercase)
        .build();

    assertAll(
        () -> assertEquals("Store." + opName, tracer.opName),
        () -> assertEquals(expectedLogs, tracer.activeSpan.logs, "expected logs don't match"),
        () -> assertEquals(new HashMap<>(expectedTags), tracer.activeSpan.tags, "expected tags don't match"),
        () -> assertTrue(tracer.parentSet, "Span-parent not set"),
        () -> assertTrue(tracer.closed, "Scope not closed"));
  }

  static class StoreInvocation<R> {
    final String opName;
    final Map<String, ?> tags;
    final List<Map<String, String>> logs;
    final ThrowingFunction<?, Store> function;
    final R result;
    final List<Exception> failures;

    StoreInvocation(String opName, Map<String, ?> tags, List<Map<String, String>> logs,
        ThrowingFunction<?, Store> function, R result, List<Exception> failures) {
      this.opName = opName;
      this.tags = tags;
      this.logs = logs;
      this.function = function;
      this.result = result;
      this.failures = failures;
    }

    StoreInvocation(String opName, Map<String, ?> tags, List<Map<String, String>> logs,
        ThrowingConsumer<Store> function, List<Exception> failures) {
      this.opName = opName;
      this.tags = tags;
      this.logs = logs;
      this.function = store -> {
        function.accept(store);
        return null;
      };
      this.result = null;
      this.failures = failures;
    }
  }

  @FunctionalInterface
  interface ThrowingFunction<R, A> {
    R accept(A arg) throws Throwable;
  }

  static class TestTracer implements Tracer {

    TestSpan activeSpan;
    boolean closed;
    boolean parentSet;
    String opName;

    @Override
    public ScopeManager scopeManager() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Span activeSpan() {
      return activeSpan;
    }

    @Override
    public SpanBuilder buildSpan(String operationName) {
      opName = operationName;
      return new SpanBuilder() {

        final Map<String, Object> tags = new HashMap<>();

        @Override
        public SpanBuilder withTag(String key, String value) {
          tags.put(key, value);
          return this;
        }

        @Override
        public SpanBuilder withTag(String key, boolean value) {
          tags.put(key, value);
          return this;
        }

        @Override
        public SpanBuilder withTag(String key, Number value) {
          tags.put(key, value);
          return this;
        }

        @Override
        public Scope startActive(boolean finishSpanOnClose) {
          activeSpan = new TestSpan(tags);

          return new Scope() {
            @Override
            public void close() {
              assertFalse(closed);
              closed = true;
            }

            @Override
            public Span span() {
              return activeSpan;
            }
          };
        }

        @Override
        public SpanBuilder asChildOf(SpanContext parent) {
          throw new UnsupportedOperationException();
        }

        @Override
        public SpanBuilder asChildOf(Span parent) {
          assertFalse(parentSet);
          assertNull(parent);
          parentSet = true;
          return this;
        }

        @Override
        public SpanBuilder addReference(String referenceType, SpanContext referencedContext) {
          throw new UnsupportedOperationException();
        }

        @Override
        public SpanBuilder ignoreActiveSpan() {
          throw new UnsupportedOperationException();
        }

        @Override
        public SpanBuilder withStartTimestamp(long microseconds) {
          throw new UnsupportedOperationException();
        }

        @Override
        public Span startManual() {
          throw new UnsupportedOperationException();
        }

        @Override
        public Span start() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <C> SpanContext extract(Format<C> format, C carrier) {
      throw new UnsupportedOperationException();
    }

    private class TestSpan implements Span {

      Map<String, Object> tags = new HashMap<>();
      List<Map<String, ?>> logs = new ArrayList<>();

      TestSpan(Map<String, Object> tags) {
        this.tags.putAll(tags);
      }

      @Override
      public SpanContext context() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Span setTag(String key, String value) {
        tags.put(key, value);
        return this;
      }

      @Override
      public Span setTag(String key, boolean value) {
        tags.put(key, value);
        return this;
      }

      @Override
      public Span setTag(String key, Number value) {
        tags.put(key, value);
        return this;
      }

      @Override
      public Span log(Map<String, ?> fields) {
        logs.add(fields);
        return this;
      }

      @Override
      public Span log(long timestampMicroseconds, Map<String, ?> fields) {
        return log(fields);
      }

      @Override
      public Span log(String event) {
        return log(Collections.singletonMap("event", event));
      }

      @Override
      public Span log(long timestampMicroseconds, String event) {
        return log(event);
      }

      @Override
      public Span setBaggageItem(String key, String value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getBaggageItem(String key) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Span setOperationName(String operationName) {
        opName = operationName;
        return this;
      }

      @Override
      public void finish() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void finish(long finishMicros) {
        throw new UnsupportedOperationException();
      }
    }
  }
}
