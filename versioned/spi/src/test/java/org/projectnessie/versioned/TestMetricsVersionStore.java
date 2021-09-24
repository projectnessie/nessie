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
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.AbstractTimer;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Stubber;

class TestMetricsVersionStore {

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
    Stream<VersionStoreInvocation<?>> versionStoreFunctions =
        Stream.of(
            new VersionStoreInvocation<>(
                "tohash",
                vs -> vs.toHash(BranchName.of("mock-branch")),
                () -> Hash.of("cafebabe"),
                refNotFoundThrows),
            new VersionStoreInvocation<>(
                "toref",
                vs -> vs.toRef("mock-branch"),
                () -> WithHash.of(Hash.of("deadbeefcafebabe"), BranchName.of("mock-branch")),
                refNotFoundThrows),
            new VersionStoreInvocation<>(
                "commit",
                vs ->
                    vs.commit(
                        BranchName.of("mock-branch"),
                        Optional.empty(),
                        "metadata",
                        Collections.emptyList()),
                () -> Hash.of("cafebabedeadbeef"),
                refNotFoundAndRefConflictThrows),
            new VersionStoreInvocation<>(
                "transplant",
                vs ->
                    vs.transplant(
                        BranchName.of("mock-branch"), Optional.empty(), Collections.emptyList()),
                refNotFoundAndRefConflictThrows),
            new VersionStoreInvocation<>(
                "merge",
                vs -> vs.merge(Hash.of("42424242"), BranchName.of("mock-branch"), Optional.empty()),
                refNotFoundAndRefConflictThrows),
            new VersionStoreInvocation<>(
                "assign",
                vs ->
                    vs.assign(BranchName.of("mock-branch"), Optional.empty(), Hash.of("12341234")),
                refNotFoundAndRefConflictThrows),
            new VersionStoreInvocation<>(
                "create",
                vs -> vs.create(BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))),
                () -> Hash.of("cafebabedeadbeef"),
                refNotFoundAndRefAlreadyExistsThrows),
            new VersionStoreInvocation<>(
                "delete",
                vs -> vs.delete(BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))),
                refNotFoundAndRefConflictThrows),
            new VersionStoreInvocation<>(
                "getcommits",
                vs -> vs.getCommits(BranchName.of("mock-branch")),
                () ->
                    Stream.of(
                        WithHash.of(Hash.of("cafebabe"), "log#1"),
                        WithHash.of(Hash.of("deadbeef"), "log#2")),
                refNotFoundThrows),
            new VersionStoreInvocation<>(
                "getkeys",
                vs -> vs.getKeys(Hash.of("cafe4242")),
                () -> Stream.of(Key.of("hello", "world")),
                refNotFoundThrows),
            new VersionStoreInvocation<>(
                "getnamedrefs",
                VersionStore::getNamedRefs,
                () ->
                    Stream.of(
                        WithHash.of(Hash.of("cafebabe"), BranchName.of("foo")),
                        WithHash.of(Hash.of("deadbeef"), BranchName.of("cow"))),
                runtimeThrows),
            new VersionStoreInvocation<>(
                "getvalue",
                vs -> vs.getValue(BranchName.of("mock-branch"), Key.of("some", "key")),
                () -> "foo",
                refNotFoundThrows),
            new VersionStoreInvocation<>(
                "getvalues",
                vs ->
                    vs.getValues(
                        BranchName.of("mock-branch"),
                        Collections.singletonList(Key.of("some", "key"))),
                () -> Collections.singletonList(Optional.empty()),
                refNotFoundThrows),
            new VersionStoreInvocation<>(
                "getdiffs",
                vs -> vs.getDiffs(BranchName.of("mock-branch"), BranchName.of("foo-branch")),
                Stream::empty,
                refNotFoundThrows));

    // flatten all "normal executions" + "throws XYZ"
    return versionStoreFunctions.flatMap(
        invocation -> {
          // Construct a stream of arguments, both "normal" results and exceptional results.
          Stream<Arguments> normalExecs =
              Stream.of(
                  Arguments.of(invocation.opName, null, invocation.result, invocation.function));
          Stream<Arguments> exceptionalExecs =
              invocation.failures.stream()
                  .map(ex -> Arguments.of(invocation.opName, ex, null, invocation.function));
          return Stream.concat(normalExecs, exceptionalExecs);
        });
  }

  @ParameterizedTest
  @MethodSource("versionStoreInvocations")
  void versionStoreInvocation(
      String opName,
      Exception expectedThrow,
      Supplier<?> resultSupplier,
      ThrowingFunction<?, VersionStore<String, String, DummyEnum>> versionStoreFunction)
      throws Throwable {
    TestMeterRegistry registry = new TestMeterRegistry();

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

    @SuppressWarnings("unchecked")
    VersionStore<String, String, DummyEnum> mockedVersionStore = mock(VersionStore.class);
    versionStoreFunction.accept(stubber.when(mockedVersionStore));
    VersionStore<String, String, DummyEnum> versionStore =
        new MetricsVersionStore<>(mockedVersionStore, registry, registry.clock);

    Id timerId = timerId(opName, expectedThrow);

    ThrowingConsumer<VersionStore<String, String, DummyEnum>> versionStoreExec =
        vs -> {
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
            assertNull(registry.timers.get(timerId), "Timer " + timerId + " registered too early");
            stream.forEach(ignore -> {});
            stream.close();
          }
        };

    if (expectedThrow == null) {
      // No exception expected, just invoke the VersionStore function
      versionStoreExec.accept(versionStore);
    } else {
      // Surround the VersionStore function with an 'assertThrows'
      assertEquals(
          expectedThrow.getMessage(),
          assertThrows(expectedThrow.getClass(), () -> versionStoreExec.accept(versionStore))
              .getMessage());
    }

    TestTimer timer = registry.timers.get(timerId);

    // The timer must have been registered, when the VersionStore function has finished
    // and the returned Stream has been closed.
    assertNotNull(
        timer, "Timer " + timerId + " not registered, registered: " + registry.timers.keySet());

    // Assert some timings
    assertAll(
        () -> assertEquals(1L, timer.count()),
        () -> assertEquals(registry.clock.expectedDuration, timer.totalTime(TimeUnit.NANOSECONDS)));
  }

  static class VersionStoreInvocation<R> {
    final String opName;
    final ThrowingFunction<?, VersionStore<String, String, DummyEnum>> function;
    final Supplier<R> result;
    final List<Exception> failures;

    VersionStoreInvocation(
        String opName,
        ThrowingFunction<?, VersionStore<String, String, DummyEnum>> function,
        Supplier<R> result,
        List<Exception> failures) {
      this.opName = opName;
      this.function = function;
      this.result = result;
      this.failures = failures;
    }

    VersionStoreInvocation(
        String opName,
        ThrowingConsumer<VersionStore<String, String, DummyEnum>> function,
        List<Exception> failures) {
      this.opName = opName;
      this.function =
          vs -> {
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

  enum DummyEnum {
    DUMMY
  }

  private static Id timerId(String opName, Exception expectedThrow) {
    // All exceptions except instances of VersionStoreException and IllegalArgumentExceptions
    // are server-errors.
    boolean isErrorException =
        expectedThrow != null
            && (!(expectedThrow instanceof VersionStoreException))
            && (!(expectedThrow instanceof IllegalArgumentException));

    return new Id(
        "nessie.versionstore.request",
        Tags.of(
            "error",
            Boolean.toString(isErrorException),
            "request",
            opName,
            "application",
            "Nessie"),
        "nanoseconds",
        null,
        Type.TIMER);
  }

  static class TestTimer extends AbstractTimer {

    List<Long> recorded = new ArrayList<>();

    TestTimer(
        Id id,
        Clock clock,
        DistributionStatisticConfig distributionStatisticConfig,
        PauseDetector pauseDetector,
        TimeUnit baseTimeUnit,
        boolean supportsAggregablePercentiles) {
      super(
          id,
          clock,
          distributionStatisticConfig,
          pauseDetector,
          baseTimeUnit,
          supportsAggregablePercentiles);
    }

    @Override
    protected void recordNonNegative(long amount, TimeUnit unit) {
      recorded.add(unit.toNanos(amount));
    }

    @Override
    public long count() {
      return recorded.size();
    }

    @Override
    public double totalTime(TimeUnit unit) {
      return unit.convert(recorded.stream().mapToLong(Long::longValue).sum(), TimeUnit.NANOSECONDS);
    }

    @Override
    public double max(TimeUnit unit) {
      return recorded.stream().mapToLong(Long::longValue).max().orElse(0L);
    }
  }

  static class TestMeterRegistry extends MeterRegistry {
    final TestClock clock;

    final Map<Id, Gauge> gauges = new HashMap<>();
    final Map<Id, TestTimer> timers = new HashMap<>();

    static TestMeterRegistry currentRegistry;

    TestMeterRegistry() {
      this(new TestClock());
    }

    TestMeterRegistry(TestClock testClock) {
      super(testClock);
      this.clock = testClock;
      currentRegistry = this;
    }

    @Override
    protected <T> Gauge newGauge(Id id, T obj, ToDoubleFunction<T> valueFunction) {
      Gauge gauge =
          new Gauge() {
            @Override
            public double value() {
              return valueFunction.applyAsDouble(obj);
            }

            @Override
            public Id getId() {
              return id;
            }
          };
      assertNull(gauges.putIfAbsent(id, gauge), "duplicate gauge with id " + id);
      return gauge;
    }

    @Override
    protected Counter newCounter(Id id) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Timer newTimer(
        Id id,
        DistributionStatisticConfig distributionStatisticConfig,
        PauseDetector pauseDetector) {
      TestTimer timer =
          new TestTimer(
              id, clock, defaultHistogramConfig(), pauseDetector, getBaseTimeUnit(), false);
      assertNull(timers.putIfAbsent(id, timer), "duplicate timer with id " + id);
      return timer;
    }

    @Override
    protected DistributionSummary newDistributionSummary(
        Id id, DistributionStatisticConfig distributionStatisticConfig, double scale) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Meter newMeter(Id id, Type type, Iterable<Measurement> measurements) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected <T> FunctionTimer newFunctionTimer(
        Id id,
        T obj,
        ToLongFunction<T> countFunction,
        ToDoubleFunction<T> totalTimeFunction,
        TimeUnit totalTimeFunctionUnit) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected <T> FunctionCounter newFunctionCounter(
        Id id, T obj, ToDoubleFunction<T> countFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
      return TimeUnit.NANOSECONDS;
    }

    @Override
    protected DistributionStatisticConfig defaultHistogramConfig() {
      return DistributionStatisticConfig.DEFAULT;
    }
  }

  static class TestClock implements Clock {
    long offset = ThreadLocalRandom.current().nextLong(100_000_000L);
    long expectedDuration = ThreadLocalRandom.current().nextLong(100_000L) + 4242L;

    Iterator<Long> monotonicTime = Arrays.asList(offset, offset + expectedDuration).iterator();

    @Override
    public long wallTime() {
      fail("No wall-clock allowed for time-measurement");
      return 0L;
    }

    @Override
    public long monotonicTime() {
      return monotonicTime.next();
    }
  }
}
