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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

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
import org.projectnessie.versioned.test.tracing.TestTracer;
import org.projectnessie.versioned.test.tracing.TestedTraceingStoreInvocation;

class TestTracingVersionStore {

  // This test implementation shall exercise all functions on VersionStore and cover all exception
  // variants, which are all declared exceptions plus IllegalArgumentException (parameter error)
  // plus a runtime-exception (server error).

  // Implemented as a parameterized-tests with each set of arguments representing one version-store
  // invocation.

  private static Stream<Arguments> versionStoreInvocations() {
    // Exception-throws to be tested, one list per distinct throws clause
    List<Exception> runtimeThrows =
        Arrays.asList(new IllegalArgumentException("illegal-arg"), new NullPointerException("NPE"));
    List<Exception> refNotFoundThrows =
        Arrays.asList(
            new IllegalArgumentException("illegal-arg"),
            new NullPointerException("NPE"),
            new ReferenceNotFoundException("not-found"));
    List<Exception> refNotFoundAndRefConflictThrows =
        Arrays.asList(
            new IllegalArgumentException("illegal-arg"),
            new NullPointerException("NPE"),
            new ReferenceNotFoundException("not-found"),
            new ReferenceConflictException("some conflict"));
    List<Exception> refNotFoundAndRefAlreadyExistsThrows =
        Arrays.asList(
            new IllegalArgumentException("illegal-arg"),
            new NullPointerException("NPE"),
            new ReferenceNotFoundException("not-found"),
            new ReferenceAlreadyExistsException("already exists"));

    // "Declare" test-invocations for all VersionStore functions with their respective outcomes
    // and exceptions.
    Stream<TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>>
        versionStoreFunctions =
            Stream.of(
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "ToHash", refNotFoundThrows)
                    .tag("nessie.version-store.ref", "mock-branch")
                    .function(
                        vs -> vs.toHash(BranchName.of("mock-branch")), () -> Hash.of("cafebabe")),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "ToRef", refNotFoundThrows)
                    .tag("nessie.version-store.ref", "mock-branch")
                    .function(
                        vs -> vs.toRef("mock-branch"),
                        () ->
                            WithHash.of(Hash.of("deadbeefcafebabe"), BranchName.of("mock-branch"))),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "Commit", refNotFoundAndRefConflictThrows)
                    .tag("nessie.version-store.branch", "mock-branch")
                    .tag("nessie.version-store.num-ops", 0)
                    .tag("nessie.version-store.hash", "Optional.empty")
                    .function(
                        vs ->
                            vs.commit(
                                BranchName.of("mock-branch"),
                                Optional.empty(),
                                "metadata",
                                Collections.emptyList()),
                        () -> Hash.of("deadbeefcafebabe")),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "Transplant", refNotFoundAndRefConflictThrows)
                    .tag("nessie.version-store.target-branch", "mock-branch")
                    .tag("nessie.version-store.transplants", 0)
                    .tag("nessie.version-store.hash", "Optional.empty")
                    .method(
                        vs ->
                            vs.transplant(
                                BranchName.of("mock-branch"),
                                Optional.empty(),
                                Collections.emptyList())),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "Merge", refNotFoundAndRefConflictThrows)
                    .tag("nessie.version-store.to-branch", "mock-branch")
                    .tag("nessie.version-store.from-hash", "Hash 42424242")
                    .tag("nessie.version-store.expected-hash", "Optional.empty")
                    .method(
                        vs ->
                            vs.merge(
                                Hash.of("42424242"),
                                BranchName.of("mock-branch"),
                                Optional.empty())),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "Assign", refNotFoundAndRefConflictThrows)
                    .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                    .tag("nessie.version-store.target-hash", "Hash 12341234")
                    .tag("nessie.version-store.expected-hash", "Optional.empty")
                    .method(
                        vs ->
                            vs.assign(
                                BranchName.of("mock-branch"),
                                Optional.empty(),
                                Hash.of("12341234"))),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "Create", refNotFoundAndRefAlreadyExistsThrows)
                    .tag("nessie.version-store.target-hash", "Optional[Hash cafebabe]")
                    .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                    .function(
                        vs ->
                            vs.create(
                                BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))),
                        () -> Hash.of("deadbeefcafebabe")),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "Delete", refNotFoundAndRefConflictThrows)
                    .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                    .tag("nessie.version-store.hash", "Optional[Hash cafebabe]")
                    .method(
                        vs ->
                            vs.delete(
                                BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe")))),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "GetCommits", refNotFoundThrows)
                    .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                    .function(
                        vs -> vs.getCommits(BranchName.of("mock-branch")),
                        () ->
                            Stream.of(
                                WithHash.of(Hash.of("cafebabe"), "log#1"),
                                WithHash.of(Hash.of("deadbeef"), "log#2"))),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "GetKeys", refNotFoundThrows)
                    .tag("nessie.version-store.ref", "Hash cafe4242")
                    .function(
                        vs -> vs.getKeys(Hash.of("cafe4242")),
                        () -> Stream.of(Key.of("hello", "world"))),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "GetNamedRefs", runtimeThrows)
                    .function(
                        VersionStore::getNamedRefs,
                        () ->
                            Stream.of(
                                WithHash.of(Hash.of("cafebabe"), BranchName.of("foo")),
                                WithHash.of(Hash.of("deadbeef"), BranchName.of("cow")))),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "GetValue", refNotFoundThrows)
                    .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                    .tag("nessie.version-store.key", "some.key")
                    .function(
                        vs -> vs.getValue(BranchName.of("mock-branch"), Key.of("some", "key")),
                        () -> "foo"),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "GetValues", refNotFoundThrows)
                    .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                    .tag("nessie.version-store.keys", "[some.key]")
                    .function(
                        vs ->
                            vs.getValues(
                                BranchName.of("mock-branch"),
                                Collections.singletonList(Key.of("some", "key"))),
                        () -> Collections.singletonList(Optional.empty())),
                new TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>>(
                        "GetDiffs", refNotFoundThrows)
                    .tag("nessie.version-store.from", "BranchName{name=mock-branch}")
                    .tag("nessie.version-store.to", "BranchName{name=foo-branch}")
                    .function(
                        vs ->
                            vs.getDiffs(BranchName.of("mock-branch"), BranchName.of("foo-branch")),
                        Stream::empty));

    return TestedTraceingStoreInvocation.toArguments(versionStoreFunctions);
  }

  @BeforeAll
  static void setupGlobalTracer() {
    TestTracer.registerGlobal();
  }

  @ParameterizedTest
  @MethodSource("versionStoreInvocations")
  void versionStoreInvocation(
      TestedTraceingStoreInvocation<VersionStore<String, String, DummyEnum>> invocation,
      Exception expectedThrow)
      throws Throwable {

    boolean isServerError =
        expectedThrow != null
            && !(expectedThrow instanceof VersionStoreException)
            && !(expectedThrow instanceof IllegalArgumentException);
    String opNameTag = "nessie.version-store.operation";

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

    @SuppressWarnings("unchecked")
    VersionStore<String, String, DummyEnum> mockedStore = mock(VersionStore.class);
    invocation.getFunction().accept(stubber.when(mockedStore));
    VersionStore<String, String, DummyEnum> store = new TracingVersionStore<>(mockedStore);

    ThrowingConsumer<VersionStore<String, String, DummyEnum>> storeExec =
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
        () ->
            assertEquals(
                TracingVersionStore.makeSpanName(invocation.getOpName()), tracer.getOpName()),
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
        () -> assertEquals("VersionStore.foo", TracingVersionStore.makeSpanName("Foo")),
        () -> assertEquals("VersionStore.fooBar", TracingVersionStore.makeSpanName("FooBar")),
        () -> assertEquals("VersionStore.fBar", TracingVersionStore.makeSpanName("FBar")));
  }

  enum DummyEnum {
    DUMMY
  }
}
