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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;

import com.google.common.collect.ImmutableMap;

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;

class TestTracingVersionStore {

  private void verify(Exception exception, Mocker mocker, String opName,
      ThrowingConsumer<VersionStore<String, String>> executable, Map<String, ?> tags) throws Throwable {
    TestTracer tracer = new TestTracer();

    VersionStore<String, String> versionStore = mockedVersionStore(tracer, mocker);

    if (exception == null) {
      executable.accept(versionStore);
    } else {
      assertEquals(exception.getMessage(),
          assertThrows(exception.getClass(),
              () -> executable.accept(versionStore)).getMessage());
    }

    String uppercase = Character.toUpperCase(opName.charAt(0)) + opName.substring(1);

    List<Map<String, String>> expectedLogs =
        (exception instanceof RuntimeException)
            ? Collections.singletonList(ImmutableMap.of("event", "error", "error.object", exception.toString()))
            : Collections.emptyList();

    ImmutableMap.Builder<Object, Object> tagsBuilder = ImmutableMap.builder().putAll(tags);
    if (exception instanceof RuntimeException) {
      tagsBuilder.put("error", true);
    }
    Map<Object, Object> expectedTags = tagsBuilder.put("nessie.version-store.operation", uppercase)
        .build();

    assertAll(
        () -> assertEquals("VersionStore." + opName, tracer.opName),
        () -> assertEquals(expectedLogs, tracer.activeSpan.logs, "expected logs don't match"),
        () -> assertEquals(expectedTags, tracer.activeSpan.tags, "expected tags don't match"),
        () -> assertTrue(tracer.parentSet, "Span-parent not set"),
        () -> assertTrue(tracer.closed, "Scope not closed"));
  }

  @Test
  void toHash() throws Throwable {
    verify(null,
        vs -> when(vs.toHash(BranchName.of("mock-branch"))).thenReturn(Hash.of("cafebabe")),
        "toHash",
        vs -> vs.toHash(BranchName.of("mock-branch")),
        ImmutableMap.of(
            "nessie.version-store.ref", "mock-branch"));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  void toHashNPE() throws Throwable {
    Exception exception = new NullPointerException("mocked");
    verify(exception,
        vs -> when(vs.toHash(null)).thenThrow(exception),
        "toHash",
        vs -> vs.toHash(null),
        ImmutableMap.of(
            "nessie.version-store.ref", "<null>"));
  }

  @Test
  void toHashNPE2() throws Throwable {
    Exception exception = new NullPointerException("mocked");
    verify(exception,
        vs -> when(vs.toHash(BranchName.of("mock-branch"))).thenThrow(exception),
        "toHash",
        vs -> vs.toHash(BranchName.of("mock-branch")),
        ImmutableMap.of(
            "nessie.version-store.ref", "mock-branch"));
  }

  @Test
  void toHashRefNotFound() throws Throwable {
    ReferenceNotFoundException refNF = new ReferenceNotFoundException("mocked");
    verify(refNF,
        vs -> when(vs.toHash(BranchName.of("mock-branch"))).thenThrow(refNF),
        "toHash",
        vs -> vs.toHash(BranchName.of("mock-branch")),
        ImmutableMap.of(
            "nessie.version-store.ref", "mock-branch"));
  }

  @Test
  void toRef() {
    // TODO
  }

  @Test
  void commit() {
    // TODO
  }

  @Test
  void transplant() {
    // TODO
  }

  @Test
  void merge() {
    // TODO
  }

  @Test
  void assign() {
    // TODO
  }

  @Test
  void create() throws Throwable {
    verify(null,
        vs -> doNothing().when(vs).create(BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))),
        "create",
        vs -> vs.create(BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))),
        ImmutableMap.of("nessie.version-store.target-hash", "Optional[Hash cafebabe]",
            "nessie.version-store.ref", "BranchName{name=mock-branch}"));
  }

  @Test
  void createEmptyHash() throws Throwable {
    verify(null,
        vs -> doNothing().when(vs).create(BranchName.of("mock-branch"), Optional.empty()),
        "create",
        vs -> vs.create(BranchName.of("mock-branch"), Optional.empty()),
        ImmutableMap.of("nessie.version-store.target-hash", "Optional.empty",
            "nessie.version-store.ref", "BranchName{name=mock-branch}"));
  }

  @Test
  void delete() {
    // TODO
  }

  @Test
  void getCommits() throws Throwable {
    verify(null,
        vs -> when(vs.getCommits(BranchName.of("mock-branch"))).thenReturn(
            Stream.of(
                WithHash.of(Hash.of("cafebabe"), "log#1"),
                WithHash.of(Hash.of("deadbeef"), "log#2")
            )
        ),
        "getCommits",
        vs -> assertStream(
            vs.getCommits(BranchName.of("mock-branch")),
            WithHash.of(Hash.of("cafebabe"), "log#1"),
            WithHash.of(Hash.of("deadbeef"), "log#2")),
        ImmutableMap.of("nessie.version-store.ref", "BranchName{name=mock-branch}"));
  }

  @Test
  void getCommitsRefNF() throws Throwable {
    ReferenceNotFoundException refNF = new ReferenceNotFoundException("mocked");
    verify(refNF,
        vs -> when(vs.getCommits(BranchName.of("mock-branch"))).thenThrow(refNF),
        "getCommits",
        vs -> vs.getCommits(BranchName.of("mock-branch")),
        ImmutableMap.of("nessie.version-store.ref", "BranchName{name=mock-branch}"));
  }

  @Test
  void getKeys() {
    // TODO
  }

  @Test
  void getNamedRefsRE() throws Throwable {
    Exception exception = new RuntimeException("mocked");
    verify(exception,
        vs -> when(vs.getNamedRefs()).thenThrow(exception),
        "getNamedRefs",
        VersionStore::getNamedRefs,
        ImmutableMap.of());
  }

  @Test
  void getNamedRefs() throws Throwable {
    verify(null,
        vs -> when(vs.getNamedRefs()).thenReturn(Stream.of(
            WithHash.of(Hash.of("cafebabe"), BranchName.of("foo")),
            WithHash.of(Hash.of("deadbeef"), BranchName.of("cow"))
        )),
        "getNamedRefs",
        vs -> assertStream(
            vs.getNamedRefs(),
            WithHash.of(Hash.of("cafebabe"), BranchName.of("foo")),
            WithHash.of(Hash.of("deadbeef"), BranchName.of("cow"))),
        ImmutableMap.of());
  }

  @Test
  void getValue() {
    // TODO
  }

  @Test
  void getValues() {
    // TODO
  }

  @Test
  void getDiffs() {
    // TODO
  }

  @Test
  void collectGarbage() {
    // TODO
  }

  @FunctionalInterface
  interface Mocker {
    void mock(VersionStore<String, String> mockedVersionStore) throws Exception;
  }

  private static <R> void assertStream(Stream<R> stream, Object... expected) {
    List<R> result = stream.collect(Collectors.toList());
    assertEquals(Arrays.asList(expected), result);
    stream.close();
  }

  @SuppressWarnings("unchecked")
  static VersionStore<String, String> mockedVersionStore(Tracer tracer, Mocker mocker) throws Exception {
    VersionStore<String, String> versionStore = mock(VersionStore.class);
    mocker.mock(versionStore);
    return new TracingVersionStore<>(versionStore, () -> tracer);
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
