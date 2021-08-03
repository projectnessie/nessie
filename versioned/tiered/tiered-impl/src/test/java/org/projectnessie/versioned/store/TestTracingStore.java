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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Stubber;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.test.tracing.TestTracer;
import org.projectnessie.versioned.test.tracing.TestedTraceingStoreInvocation;
import org.projectnessie.versioned.tiered.L3;

class TestTracingStore {

  private static Stream<Arguments> storeInvocations() {
    // Exception-throws to be tested, one list per distinct throws clause
    List<Exception> storeThrows =
        Arrays.asList(
            new IllegalArgumentException("illegal-arg"),
            new NullPointerException("NPE"),
            new RuntimeException("NPE"),
            new StoreOperationException("NPE"),
            new StoreException("general store exception"),
            new NotFoundException("store not-found-exception"));

    Id someId = Id.of(new byte[20]);
    LoadStep someLoadStep = mock(LoadStep.class);
    L3 someConsumer = mock(L3.class);
    @SuppressWarnings("unchecked")
    SaveOp<L3> someSaveOp = mock(SaveOp.class);
    when(someSaveOp.getId()).thenReturn(someId);
    when(someSaveOp.getType()).thenReturn(ValueType.L3);

    // "Declare" test-invocations for all Store functions with their respective outcomes
    // and exceptions.
    Stream<TestedTraceingStoreInvocation<Store>> storeFunctions =
        Stream.of(
            new TestedTraceingStoreInvocation<Store>("Start", storeThrows).method(Store::start),
            new TestedTraceingStoreInvocation<Store>("Close", storeThrows).method(Store::close),
            new TestedTraceingStoreInvocation<Store>("Load", storeThrows)
                .method(s -> s.load(someLoadStep)),
            new TestedTraceingStoreInvocation<Store>("PutIfAbsent", storeThrows)
                .tag(TracingStore.TAG_ID, "0000000000000000000000000000000000000000")
                .tag(TracingStore.TAG_VALUE_TYPE, "l3")
                .function(s -> s.putIfAbsent(someSaveOp), () -> true),
            new TestedTraceingStoreInvocation<Store>("Put", storeThrows)
                .tag(TracingStore.TAG_ID, "0000000000000000000000000000000000000000")
                .tag(TracingStore.TAG_VALUE_TYPE, "l3")
                .method(s -> s.put(someSaveOp, Optional.empty())),
            new TestedTraceingStoreInvocation<Store>("Delete", storeThrows)
                .tag(TracingStore.TAG_ID, "0000000000000000000000000000000000000000")
                .tag(TracingStore.TAG_VALUE_TYPE, "l3")
                .function(s -> s.delete(ValueType.L3, someId, Optional.empty()), () -> true),
            new TestedTraceingStoreInvocation<Store>("Save", storeThrows)
                .tag(TracingStore.TAG_NUM_OPS, 1)
                .log(
                    Collections.singletonMap(
                        "nessie.store.save.l3.ids", "0000000000000000000000000000000000000000"))
                .method(s -> s.save(Collections.singletonList(someSaveOp))),
            new TestedTraceingStoreInvocation<Store>("LoadSingle", storeThrows)
                .tag(TracingStore.TAG_ID, "0000000000000000000000000000000000000000")
                .tag(TracingStore.TAG_VALUE_TYPE, "l3")
                .method(s -> s.loadSingle(ValueType.L3, someId, someConsumer)),
            new TestedTraceingStoreInvocation<Store>("Update", storeThrows)
                .tag(TracingStore.TAG_ID, "0000000000000000000000000000000000000000")
                .tag(TracingStore.TAG_VALUE_TYPE, "l3")
                .tag(TracingStore.TAG_UPDATE, "UpdateExpression{clauses=[]}")
                .tag(TracingStore.TAG_CONDITION, "<null>")
                .function(
                    s ->
                        s.update(
                            ValueType.L3,
                            someId,
                            UpdateExpression.initial(),
                            Optional.empty(),
                            Optional.empty()),
                    () -> true));

    return TestedTraceingStoreInvocation.toArguments(storeFunctions);
  }

  @BeforeAll
  static void setupGlobalTracer() {
    TestTracer.registerGlobal();
  }

  @ParameterizedTest
  @MethodSource("storeInvocations")
  void storeInvocation(TestedTraceingStoreInvocation<Store> invocation, Exception expectedThrow)
      throws Throwable {

    boolean isServerError =
        expectedThrow != null
            && (!(expectedThrow instanceof StoreException)
                || expectedThrow instanceof StoreOperationException);
    String opNameTag = "nessie.store.operation";

    TestTracer tracer = new TestTracer();
    tracer.registerForCurrentTest();

    Object result = invocation.getResult() != null ? invocation.getResult().get() : null;

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

    Store mockedStore = mock(Store.class);
    invocation.getFunction().accept(stubber.when(mockedStore));
    Store store = new TracingStore(mockedStore);

    ThrowingConsumer<Store> storeExec =
        s -> {
          Object r = invocation.getFunction().accept(s);
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
      assertEquals(
          expectedThrow.getMessage(),
          assertThrows(expectedThrow.getClass(), () -> storeExec.accept(store)).getMessage());
    }

    List<Map<String, String>> expectedLogs = new ArrayList<>(invocation.getLogs());
    if (isServerError) {
      expectedLogs.add(ImmutableMap.of("event", "error", "error.object", expectedThrow.toString()));
    }

    ImmutableMap.Builder<Object, Object> tagsBuilder =
        ImmutableMap.builder().putAll(invocation.getTags());
    if (isServerError) {
      tagsBuilder.put("error", true);
    }
    Map<Object, Object> expectedTags = tagsBuilder.put(opNameTag, invocation.getOpName()).build();

    assertAll(
        () -> assertEquals(TracingStore.makeSpanName(invocation.getOpName()), tracer.getOpName()),
        () ->
            assertEquals(
                expectedLogs, tracer.getActiveSpan().getLogs(), "expected logs don't match"),
        () ->
            assertEquals(
                new HashMap<>(expectedTags),
                tracer.getActiveSpan().getTags(),
                "expected tags don't match"),
        () -> assertTrue(tracer.isParentSet(), "Span-parent not set"),
        () -> assertTrue(tracer.isClosed(), "Scope not closed"));
  }

  @Test
  void spanNames() {
    assertAll(
        () -> assertEquals("Store.foo", TracingStore.makeSpanName("Foo")),
        () -> assertEquals("Store.fooBar", TracingStore.makeSpanName("FooBar")),
        () -> assertEquals("Store.fBar", TracingStore.makeSpanName("FBar")));
  }
}
