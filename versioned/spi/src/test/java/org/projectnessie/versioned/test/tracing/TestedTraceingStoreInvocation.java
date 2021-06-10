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
package org.projectnessie.versioned.test.tracing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Describes a store (either {@code Store} or {@code VersionStore}) function invocation (for {@code
 * TestTracingStore} and {@code TestTracingVersionStore}), which trace logs + tags it should emit,
 * the expected result and a list of expected exceptions.
 *
 * @param <S> store type
 */
public class TestedTraceingStoreInvocation<S> {
  final String opName;
  Map<String, Object> tags = new HashMap<>();
  List<Map<String, String>> logs = new ArrayList<>();
  ThrowingFunction<?, S> function;
  Supplier<?> result;
  final List<Exception> failures;

  public TestedTraceingStoreInvocation(String opName, List<Exception> failures) {
    this.opName = opName;
    this.failures = failures;
  }

  public TestedTraceingStoreInvocation<S> tag(String tag, Object value) {
    this.tags.put(tag, value);
    return this;
  }

  public TestedTraceingStoreInvocation<S> log(Map<String, String> log) {
    this.logs.add(log);
    return this;
  }

  /**
   * The function to be tested.
   *
   * @param function function to be tested
   * @param result supplier for the expected result
   * @param <R> result type
   * @return {@code this}
   */
  public <R> TestedTraceingStoreInvocation<S> function(
      ThrowingFunction<R, S> function, Supplier<R> result) {
    this.function = function;
    this.result = result;
    return this;
  }

  /**
   * The method (function returning {@code void}) to be tested.
   *
   * @param method method to be tested
   * @return {@code this}
   */
  public TestedTraceingStoreInvocation<S> method(ThrowingConsumer<S> method) {
    this.function =
        store -> {
          method.accept(store);
          return null;
        };
    return this;
  }

  /**
   * Convert a stream of {@link TestedTraceingStoreInvocation}s to a stream of {@link Arguments},
   * which are pairs of {@link TestedTraceingStoreInvocation} plus {@link Exception}, which are the
   * arguments for the parameterized tests.
   *
   * @param versionStoreFunctions stream of {@link TestedTraceingStoreInvocation}s
   * @param <S> store type, a {@code Store} or {@code VersionStore}
   * @return Stream of {@link Arguments} (pairs of {@link TestedTraceingStoreInvocation} plus {@link
   *     Exception});
   */
  public static <S> Stream<Arguments> toArguments(
      Stream<TestedTraceingStoreInvocation<S>> versionStoreFunctions) {
    // flatten all "normal executions" + "throws XYZ"
    return versionStoreFunctions.flatMap(
        invocation -> {
          // Construct a stream of arguments, both "normal" results and exceptional results.
          Stream<Arguments> normalExecs = Stream.of(Arguments.of(invocation, null));
          Stream<Arguments> exceptionalExecs =
              invocation.getFailures().stream().map(ex -> Arguments.of(invocation, ex));
          return Stream.concat(normalExecs, exceptionalExecs);
        });
  }

  public String getOpName() {
    return opName;
  }

  public Map<String, ?> getTags() {
    return tags;
  }

  public List<Map<String, String>> getLogs() {
    return logs;
  }

  /** The wrapped/delegated store function invocation. */
  public ThrowingFunction<?, S> getFunction() {
    return function;
  }

  /** Supplier for the expected result. */
  public Supplier<?> getResult() {
    return result;
  }

  /** List of exceptions to test. */
  public List<Exception> getFailures() {
    return failures;
  }

  @FunctionalInterface
  public interface ThrowingFunction<R, A> {
    R accept(A arg) throws Throwable;
  }
}
