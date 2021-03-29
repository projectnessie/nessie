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
package org.projectnessie.versioned;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Stubber;
import org.projectnessie.versioned.VersionStore.Collector;

import com.google.common.collect.ImmutableMap;

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;

class TestTracingVersionStore {

  // This test implementation shall exercise all functions on VersionStore and cover all exception
  // variants, which are all declared exceptions plus IllegalArgumentException (parameter error)
  // plus a runtime-exception (server error).

  // Implemented as a parameterized-tests with each set of arguments representing one version-store
  // invocation.

  private static Stream<Arguments> versionStoreInvocations() {
    // Exception-throws to be tested, one list per distinct throws clause
    List<Exception> runtimeThrows = Arrays.asList(
        new IllegalArgumentException("illegal-arg"),
        new NullPointerException("NPE")
    );
    List<Exception> refNotFoundThrows = Arrays.asList(
        new IllegalArgumentException("illegal-arg"),
        new NullPointerException("NPE"),
        new ReferenceNotFoundException("not-found")
    );
    List<Exception> refNotFoundAndRefConflictThrows = Arrays.asList(
        new IllegalArgumentException("illegal-arg"),
        new NullPointerException("NPE"),
        new ReferenceNotFoundException("not-found"),
        new ReferenceConflictException("some conflict")
    );
    List<Exception> refNotFoundAndRefAlreadyExistsThrows = Arrays.asList(
        new IllegalArgumentException("illegal-arg"),
        new NullPointerException("NPE"),
        new ReferenceNotFoundException("not-found"),
        new ReferenceAlreadyExistsException("already exists")
    );

    // "Declare" test-invocations for all VersionStore functions with their respective outcomes
    // and exceptions.
    Stream<VersionStoreInvocation<?>> versionStoreFunctions = Stream.of(
        new VersionStoreInvocation<>("toHash",
            ImmutableMap.of("nessie.version-store.ref", "mock-branch"),
            vs -> vs.toHash(BranchName.of("mock-branch")),
            () -> Hash.of("cafebabe"), refNotFoundThrows),
        new VersionStoreInvocation<>("toRef",
            ImmutableMap.of("nessie.version-store.ref", "mock-branch"),
            vs -> vs.toRef("mock-branch"),
            () -> WithHash.of(Hash.of("deadbeefcafebabe"), BranchName.of("mock-branch")),
            refNotFoundThrows),
        new VersionStoreInvocation<>("commit",
            ImmutableMap.of("nessie.version-store.branch", "mock-branch",
                "nessie.version-store.num-ops", 0,
                "nessie.version-store.hash", "Optional.empty"),
            vs -> vs.commit(BranchName.of("mock-branch"), Optional.empty(), "metadata", Collections.emptyList()),
            refNotFoundAndRefConflictThrows),
        new VersionStoreInvocation<>("transplant",
            ImmutableMap.of("nessie.version-store.target-branch", "mock-branch",
                "nessie.version-store.transplants", 0,
                "nessie.version-store.hash", "Optional.empty"),
            vs -> vs.transplant(BranchName.of("mock-branch"), Optional.empty(), Collections.emptyList()),
            refNotFoundAndRefConflictThrows),
        new VersionStoreInvocation<>("merge",
            ImmutableMap.of("nessie.version-store.to-branch", "mock-branch",
                "nessie.version-store.from-hash", "Hash 42424242",
                "nessie.version-store.expected-hash", "Optional.empty"),
            vs -> vs.merge(Hash.of("42424242"), BranchName.of("mock-branch"), Optional.empty()),
            refNotFoundAndRefConflictThrows),
        new VersionStoreInvocation<>("assign",
            ImmutableMap.of("nessie.version-store.ref", "BranchName{name=mock-branch}",
                "nessie.version-store.target-hash", "Hash 12341234",
                "nessie.version-store.expected-hash", "Optional.empty"),
            vs -> vs.assign(BranchName.of("mock-branch"), Optional.empty(), Hash.of("12341234")),
            refNotFoundAndRefConflictThrows),
        new VersionStoreInvocation<>("create",
            ImmutableMap.of("nessie.version-store.target-hash", "Optional[Hash cafebabe]",
                "nessie.version-store.ref", "BranchName{name=mock-branch}"),
            vs -> vs.create(BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))),
            refNotFoundAndRefAlreadyExistsThrows),
        new VersionStoreInvocation<>("delete",
            ImmutableMap.of("nessie.version-store.ref", "BranchName{name=mock-branch}",
                "nessie.version-store.hash", "Optional[Hash cafebabe]"),
            vs -> vs.delete(BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))),
            refNotFoundAndRefConflictThrows),
        new VersionStoreInvocation<>("getCommits",
            ImmutableMap.of("nessie.version-store.ref", "BranchName{name=mock-branch}"),
            vs -> vs.getCommits(BranchName.of("mock-branch")),
            () -> Stream.of(
                WithHash.of(Hash.of("cafebabe"), "log#1"),
                WithHash.of(Hash.of("deadbeef"), "log#2")),
            refNotFoundThrows),
        new VersionStoreInvocation<>("getKeys",
            ImmutableMap.of("nessie.version-store.ref", "Hash cafe4242"),
            vs -> vs.getKeys(Hash.of("cafe4242")),
            () -> Stream.of(Key.of("hello", "world")),
            refNotFoundThrows),
        new VersionStoreInvocation<>("getNamedRefs",
            ImmutableMap.of(),
            VersionStore::getNamedRefs,
            () -> Stream.of(
                WithHash.of(Hash.of("cafebabe"), BranchName.of("foo")),
                WithHash.of(Hash.of("deadbeef"), BranchName.of("cow"))),
            runtimeThrows),
        new VersionStoreInvocation<>("getValue",
            ImmutableMap.of("nessie.version-store.ref", "BranchName{name=mock-branch}",
                "nessie.version-store.key", "some.key"),
            vs -> vs.getValue(BranchName.of("mock-branch"), Key.of("some", "key")),
            () -> "foo",
            refNotFoundThrows),
        new VersionStoreInvocation<>("getValues",
            ImmutableMap.of("nessie.version-store.ref", "BranchName{name=mock-branch}",
                "nessie.version-store.keys", "[some.key]"),
            vs -> vs.getValues(BranchName.of("mock-branch"), Collections.singletonList(Key.of("some", "key"))),
            () -> Collections.singletonList(Optional.empty()),
            refNotFoundThrows),
        new VersionStoreInvocation<>("getDiffs",
            ImmutableMap.of("nessie.version-store.from", "BranchName{name=mock-branch}",
                "nessie.version-store.to", "BranchName{name=foo-branch}"),
            vs -> vs.getDiffs(BranchName.of("mock-branch"), BranchName.of("foo-branch")),
            Stream::empty,
            refNotFoundThrows),
        new VersionStoreInvocation<>("collectGarbage",
            ImmutableMap.of(),
            VersionStore::collectGarbage,
            () -> mock(Collector.class),
            runtimeThrows)
    );

    // flatten all "normal executions" + "throws XYZ"
    return versionStoreFunctions.flatMap(invocation -> {
          // Construct a stream of arguments, both "normal" results and exceptional results.
          Stream<Arguments> normalExecs = Stream.of(
              Arguments.of(invocation.opName, null, invocation.tags, invocation.result, invocation.function)
          );
          Stream<Arguments> exceptionalExecs = invocation.failures.stream().map(
              ex -> Arguments.of(invocation.opName, ex, invocation.tags, null, invocation.function)
          );
          return Stream.concat(normalExecs, exceptionalExecs);
        }
    );
  }

  @ParameterizedTest
  @MethodSource("versionStoreInvocations")
  void versionStoreInvocation(String opName, Exception expectedThrow, Map<String, ?> tags, Supplier<?> resultSupplier,
      ThrowingFunction<?, VersionStore<String, String>> versionStoreFunction) throws Throwable {
    Object result = resultSupplier != null ? resultSupplier.get() : null;

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
    @SuppressWarnings("unchecked") VersionStore<String, String> mockedVersionStore = mock(VersionStore.class);
    versionStoreFunction.accept(stubber.when(mockedVersionStore));
    VersionStore<String, String> versionStore = new TracingVersionStore<>(mockedVersionStore, () -> tracer);

    ThrowingConsumer<VersionStore<String, String>> versionStoreExec = vs -> {
      Object r = versionStoreFunction.accept(vs);
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
      // No exception expected, just invoke the VersionStore function
      versionStoreExec.accept(versionStore);
    } else {
      // Surround the VersionStore function with an 'assertThrows'
      assertEquals(expectedThrow.getMessage(),
          assertThrows(expectedThrow.getClass(),
              () -> versionStoreExec.accept(versionStore)).getMessage());
    }

    String uppercase = Character.toUpperCase(opName.charAt(0)) + opName.substring(1);

    boolean isServerError = expectedThrow != null
        && !(expectedThrow instanceof VersionStoreException) && !(expectedThrow instanceof IllegalArgumentException);

    List<Map<String, String>> expectedLogs =
        isServerError
            ? Collections.singletonList(ImmutableMap.of("event", "error", "error.object", expectedThrow.toString()))
            : Collections.emptyList();

    ImmutableMap.Builder<Object, Object> tagsBuilder = ImmutableMap.builder().putAll(tags);
    if (isServerError) {
      tagsBuilder.put("error", true);
    }
    Map<Object, Object> expectedTags = tagsBuilder.put("nessie.version-store.operation", uppercase)
        .build();

    assertAll(
        () -> assertEquals("VersionStore." + opName, tracer.opName),
        () -> assertEquals(expectedLogs, tracer.activeSpan.logs, "expected logs don't match"),
        () -> assertEquals(new HashMap<>(expectedTags), tracer.activeSpan.tags, "expected tags don't match"),
        () -> assertTrue(tracer.parentSet, "Span-parent not set"),
        () -> assertTrue(tracer.closed, "Scope not closed"));
  }

  static class VersionStoreInvocation<R> {
    final String opName;
    final Map<String, ?> tags;
    final ThrowingFunction<?, VersionStore<String, String>> function;
    final Supplier<R> result;
    final List<Exception> failures;

    VersionStoreInvocation(String opName, Map<String, ?> tags,
        ThrowingFunction<?, VersionStore<String, String>> function,
        Supplier<R> result, List<Exception> failures) {
      this.opName = opName;
      this.tags = tags;
      this.function = function;
      this.result = result;
      this.failures = failures;
    }

    VersionStoreInvocation(String opName, Map<String, ?> tags,
        ThrowingConsumer<VersionStore<String, String>> function,
        List<Exception> failures) {
      this.opName = opName;
      this.tags = tags;
      this.function = vs -> {
        function.accept(vs);
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
