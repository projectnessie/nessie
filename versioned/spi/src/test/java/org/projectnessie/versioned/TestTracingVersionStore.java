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

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Stubber;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.test.tracing.TestedTraceingStoreInvocation;

class TestTracingVersionStore {

  // This test implementation shall exercise all functions on VersionStore and cover all exception
  // variants, which are all declared exceptions plus IllegalArgumentException (parameter error)
  // plus a runtime-exception (server error).

  // Implemented as a parameterized-tests with each set of arguments representing one version-store
  // invocation.

  @SuppressWarnings("MustBeClosedChecker")
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

    MetadataRewriter<CommitMeta> metadataRewriter =
        new MetadataRewriter<CommitMeta>() {
          @Override
          public CommitMeta rewriteSingle(CommitMeta metadata) {
            return metadata;
          }

          @Override
          public CommitMeta squash(List<CommitMeta> metadata) {
            return CommitMeta.fromMessage(
                metadata.stream().map(CommitMeta::getMessage).collect(Collectors.joining(", ")));
          }
        };

    MergeResult<Object> dummyMergeResult =
        MergeResult.builder()
            .effectiveTargetHash(Hash.of("123456"))
            .targetBranch(BranchName.of("foo"))
            .build();

    // "Declare" test-invocations for all VersionStore functions with their respective outcomes
    // and exceptions.
    @SuppressWarnings("unchecked")
    Stream<TestedTraceingStoreInvocation<VersionStore>> versionStoreFunctions =
        Stream.of(
            new TestedTraceingStoreInvocation<VersionStore>("GetNamedRef", refNotFoundThrows)
                .tag("nessie.version-store.ref", "mock-branch")
                .function(
                    vs -> vs.getNamedRef("mock-branch", GetNamedRefsParams.DEFAULT),
                    () -> ReferenceInfo.of(Hash.of("cafebabe"), BranchName.of("mock-branch"))),
            new TestedTraceingStoreInvocation<VersionStore>(
                    "Commit", refNotFoundAndRefConflictThrows)
                .tag("nessie.version-store.branch", "mock-branch")
                .tag("nessie.version-store.num-ops", 0)
                .tag("nessie.version-store.hash", "Optional.empty")
                .function(
                    vs ->
                        vs.commit(
                            BranchName.of("mock-branch"),
                            Optional.empty(),
                            CommitMeta.fromMessage("metadata"),
                            Collections.emptyList(),
                            () -> null,
                            (k, c) -> {}),
                    () -> Hash.of("deadbeefcafebabe")),
            new TestedTraceingStoreInvocation<VersionStore>(
                    "Transplant", refNotFoundAndRefConflictThrows)
                .tag("nessie.version-store.target-branch", "mock-branch")
                .tag("nessie.version-store.transplants", 0)
                .tag("nessie.version-store.hash", "Optional.empty")
                .function(
                    vs ->
                        vs.transplant(
                            BranchName.of("mock-branch"),
                            Optional.empty(),
                            Collections.emptyList(),
                            metadataRewriter,
                            true,
                            Collections.emptyMap(),
                            MergeBehavior.NORMAL,
                            false,
                            false),
                    () -> dummyMergeResult),
            new TestedTraceingStoreInvocation<VersionStore>(
                    "Merge", refNotFoundAndRefConflictThrows)
                .tag("nessie.version-store.to-branch", "mock-branch")
                .tag("nessie.version-store.from-hash", "Hash 42424242")
                .tag("nessie.version-store.expected-hash", "Optional.empty")
                .function(
                    vs ->
                        vs.merge(
                            Hash.of("42424242"),
                            BranchName.of("mock-branch"),
                            Optional.empty(),
                            metadataRewriter,
                            false,
                            null,
                            null,
                            false,
                            false),
                    () -> dummyMergeResult),
            new TestedTraceingStoreInvocation<VersionStore>(
                    "Assign", refNotFoundAndRefConflictThrows)
                .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                .tag("nessie.version-store.target-hash", "Hash 12341234")
                .tag("nessie.version-store.expected-hash", "Optional.empty")
                .method(
                    vs ->
                        vs.assign(
                            BranchName.of("mock-branch"), Optional.empty(), Hash.of("12341234"))),
            new TestedTraceingStoreInvocation<VersionStore>(
                    "Create", refNotFoundAndRefAlreadyExistsThrows)
                .tag("nessie.version-store.target-hash", "Optional[Hash cafebabe]")
                .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                .function(
                    vs -> vs.create(BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))),
                    () -> Hash.of("deadbeefcafebabe")),
            new TestedTraceingStoreInvocation<VersionStore>(
                    "Delete", refNotFoundAndRefConflictThrows)
                .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                .tag("nessie.version-store.hash", "Optional[Hash cafebabe]")
                .function(
                    vs -> vs.delete(BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))),
                    () -> Hash.of("deadbeefcafebabe")),
            new TestedTraceingStoreInvocation<VersionStore>("GetCommits.stream", refNotFoundThrows)
                .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                .function(
                    vs -> vs.getCommits(BranchName.of("mock-branch"), false),
                    () ->
                        PaginationIterator.of(
                            Commit.builder()
                                .hash(Hash.of("cafebabe"))
                                .commitMeta(CommitMeta.fromMessage("log#1"))
                                .build(),
                            Commit.builder()
                                .hash(Hash.of("deadbeef"))
                                .commitMeta(CommitMeta.fromMessage("log#2"))
                                .build())),
            new TestedTraceingStoreInvocation<VersionStore>("GetKeys.stream", refNotFoundThrows)
                .tag("nessie.version-store.ref", "Hash cafe4242")
                .function(
                    vs -> vs.getKeys(Hash.of("cafe4242"), null, false),
                    () -> PaginationIterator.of(ContentKey.of("hello", "world"))),
            new TestedTraceingStoreInvocation<VersionStore>("GetNamedRefs.stream", runtimeThrows)
                .function(
                    stringStringDummyEnumVersionStore ->
                        stringStringDummyEnumVersionStore.getNamedRefs(
                            GetNamedRefsParams.DEFAULT, null),
                    () ->
                        PaginationIterator.of(
                            WithHash.of(Hash.of("cafebabe"), BranchName.of("foo")),
                            WithHash.of(Hash.of("deadbeef"), BranchName.of("cow")))),
            new TestedTraceingStoreInvocation<VersionStore>("GetValue", refNotFoundThrows)
                .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                .tag("nessie.version-store.key", "some.key")
                .function(
                    vs -> vs.getValue(BranchName.of("mock-branch"), ContentKey.of("some", "key")),
                    () -> IcebergTable.of("meta", 42, 43, 44, 45)),
            new TestedTraceingStoreInvocation<VersionStore>("GetValues", refNotFoundThrows)
                .tag("nessie.version-store.ref", "BranchName{name=mock-branch}")
                .tag("nessie.version-store.keys", "[some.key]")
                .function(
                    vs ->
                        vs.getValues(
                            BranchName.of("mock-branch"),
                            Collections.singletonList(ContentKey.of("some", "key"))),
                    Collections::emptyMap),
            new TestedTraceingStoreInvocation<VersionStore>("GetDiffs.stream", refNotFoundThrows)
                .tag("nessie.version-store.from", "BranchName{name=mock-branch}")
                .tag("nessie.version-store.to", "BranchName{name=foo-branch}")
                .function(
                    vs ->
                        vs.getDiffs(
                            BranchName.of("mock-branch"), BranchName.of("foo-branch"), null),
                    PaginationIterator::empty));

    return TestedTraceingStoreInvocation.toArguments(versionStoreFunctions);
  }

  @ParameterizedTest
  @MethodSource("versionStoreInvocations")
  void versionStoreInvocation(
      TestedTraceingStoreInvocation<VersionStore> invocation, Exception expectedThrow)
      throws Throwable {

    boolean isServerError =
        expectedThrow != null
            && !(expectedThrow instanceof VersionStoreException)
            && !(expectedThrow instanceof IllegalArgumentException);
    String opNameTag = "nessie.version-store.operation";

    List<SpanData> spanData = new ArrayList<>();

    SpanExporter spanExporter =
        new SpanExporter() {
          @Override
          public CompletableResultCode export(Collection<SpanData> spans) {
            spanData.addAll(spans);
            return CompletableResultCode.ofSuccess();
          }

          @Override
          public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
          }

          @Override
          public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
          }
        };
    try (OpenTelemetrySdk openTelemetrySdk =
        OpenTelemetrySdk.builder()
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                    .build())
            .build()) {
      Tracer tracer = openTelemetrySdk.getTracer("testing");

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

      VersionStore mockedStore = mock(VersionStore.class);
      invocation.getFunction().accept(stubber.when(mockedStore));
      VersionStore store = new TracingVersionStore(tracer, mockedStore);

      ThrowingConsumer<VersionStore> storeExec =
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
            if (result instanceof PaginationIterator) {
              // Stream-results shall be closed to indicate the "end" of an invocation
              PaginationIterator<?> iter = (PaginationIterator<?>) r;
              iter.forEachRemaining(ignore -> {});
              iter.close();
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

      Map<AttributeKey<?>, Object> expectedAttributes = new HashMap<>();
      expectedAttributes.put(stringKey(opNameTag), invocation.getOpName());

      assertThat(spanData)
          .hasSize(1)
          .first()
          .extracting(SpanData::getName, SpanData::getParentSpanId, SpanData::getStatus)
          .containsExactly(
              TracingVersionStore.makeSpanName(invocation.getOpName()),
              "0000000000000000",
              isServerError ? StatusData.error() : StatusData.unset());
      assertThat(spanData.get(0).getAttributes().asMap()).containsAllEntriesOf(expectedAttributes);
      if (isServerError) {
        assertThat(spanData.get(0).getEvents()).hasSize(1);
        Attributes eventData = spanData.get(0).getEvents().get(0).getAttributes();
        assertThat(eventData.get(stringKey("exception.message")))
            .isEqualTo(expectedThrow.getMessage());
        assertThat(eventData.get(stringKey("exception.stacktrace")))
            .startsWith(expectedThrow.toString());
      }
    }
  }

  @Test
  void spanNames() {
    assertAll(
        () -> assertEquals("VersionStore.foo", TracingVersionStore.makeSpanName("Foo")),
        () -> assertEquals("VersionStore.fooBar", TracingVersionStore.makeSpanName("FooBar")),
        () -> assertEquals("VersionStore.fBar", TracingVersionStore.makeSpanName("FBar")));
  }
}
