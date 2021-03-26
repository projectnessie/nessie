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
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;

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

class TestMetricsVersionStore {

  private void verify(Exception exception, Mocker mocker, String opName,
      ThrowingConsumer<VersionStore<String, String>> executable) throws Throwable {
    TestMeterRegistry registry = new TestMeterRegistry();

    VersionStore<String, String> versionStore = mockedVersionStore(registry, mocker);

    if (exception == null) {
      executable.accept(versionStore);
    } else {
      assertEquals(exception.getMessage(),
          assertThrows(exception.getClass(),
              () -> executable.accept(versionStore)).getMessage());
    }

    Id timerId = new Id("nessie.version-store.request",
        Tags.of("error", exception instanceof RuntimeException ? "true" : "false",
            "request", opName,
            "test", "unit"),
        "nanoseconds",
        null,
        Type.TIMER);

    TestTimer timer = registry.timers.get(timerId);
    assertNotNull(timer, "Timer " + timerId + " not registered, registered: " + registry.timers.keySet());

    assertAll(
        () -> assertEquals(1L, timer.count()),
        () -> assertEquals(registry.clock.expectedDuration, timer.totalTime(TimeUnit.NANOSECONDS))
    );
  }

  @Test
  void toHash() throws Throwable {
    verify(null,
        vs -> when(vs.toHash(BranchName.of("mock-branch"))).thenReturn(Hash.of("cafebabe")),
        "to-hash",
        vs -> vs.toHash(BranchName.of("mock-branch")));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  void toHashNPE() throws Throwable {
    Exception exception = new NullPointerException("mocked");
    verify(exception,
        vs -> when(vs.toHash(null)).thenThrow(exception),
        "to-hash",
        vs -> vs.toHash(null));
  }

  @Test
  void toHashNPE2() throws Throwable {
    Exception exception = new NullPointerException("mocked");
    verify(exception,
        vs -> when(vs.toHash(BranchName.of("mock-branch"))).thenThrow(exception),
        "to-hash",
        vs -> vs.toHash(BranchName.of("mock-branch")));
  }

  @Test
  void toHashRefNotFound() throws Throwable {
    ReferenceNotFoundException refNF = new ReferenceNotFoundException("mocked");
    verify(refNF,
        vs -> when(vs.toHash(BranchName.of("mock-branch"))).thenThrow(refNF),
        "to-hash",
        vs -> vs.toHash(BranchName.of("mock-branch")));
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
        vs -> vs.create(BranchName.of("mock-branch"), Optional.of(Hash.of("cafebabe"))));
  }

  @Test
  void createEmptyHash() throws Throwable {
    verify(null,
        vs -> doNothing().when(vs).create(BranchName.of("mock-branch"), Optional.empty()),
        "create",
        vs -> vs.create(BranchName.of("mock-branch"), Optional.empty()));
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
        "get-commits",
        vs -> assertStream(
            vs.getCommits(BranchName.of("mock-branch")),
            WithHash.of(Hash.of("cafebabe"), "log#1"),
            WithHash.of(Hash.of("deadbeef"), "log#2")));
  }

  @Test
  void getCommitsRefNF() throws Throwable {
    ReferenceNotFoundException refNF = new ReferenceNotFoundException("mocked");
    verify(refNF,
        vs -> when(vs.getCommits(BranchName.of("mock-branch"))).thenThrow(refNF),
        "get-commits",
        vs -> vs.getCommits(BranchName.of("mock-branch")));
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
        "get-named-refs",
        VersionStore::getNamedRefs
    );
  }

  @Test
  void getNamedRefs() throws Throwable {
    verify(null,
        vs -> when(vs.getNamedRefs()).thenReturn(Stream.of(
            WithHash.of(Hash.of("cafebabe"), BranchName.of("foo")),
            WithHash.of(Hash.of("deadbeef"), BranchName.of("cow"))
        )),
        "get-named-refs",
        vs -> assertStream(
            vs.getNamedRefs(),
            WithHash.of(Hash.of("cafebabe"), BranchName.of("foo")),
            WithHash.of(Hash.of("deadbeef"), BranchName.of("cow")))
    );
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

    assertTrue(TestMeterRegistry.currentRegistry.timers.isEmpty(), "Must have no timers yet, "
        + "because no measurement should have been recorded, because the Stream's not closed");

    stream.close();
  }

  @SuppressWarnings("unchecked")
  static VersionStore<String, String> mockedVersionStore(TestMeterRegistry registry, Mocker mocker) throws Exception {
    VersionStore<String, String> versionStore = mock(VersionStore.class);
    mocker.mock(versionStore);
    return new MetricsVersionStore<>(versionStore, Collections.singletonMap("test", "unit"), registry, registry.clock);
  }

  static class TestTimer extends AbstractTimer {

    List<Long> recorded = new ArrayList<>();

    TestTimer(Id id, Clock clock,
        DistributionStatisticConfig distributionStatisticConfig,
        PauseDetector pauseDetector, TimeUnit baseTimeUnit, boolean supportsAggregablePercentiles) {
      super(id, clock, distributionStatisticConfig, pauseDetector, baseTimeUnit,
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
      Gauge gauge = new Gauge() {
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
    protected Timer newTimer(Id id, DistributionStatisticConfig distributionStatisticConfig,
        PauseDetector pauseDetector) {
      TestTimer timer = new TestTimer(id, clock, defaultHistogramConfig(), pauseDetector, getBaseTimeUnit(), false);
      assertNull(timers.putIfAbsent(id, timer), "duplicate timer with id " + id);
      return timer;
    }

    @Override
    protected DistributionSummary newDistributionSummary(Id id,
        DistributionStatisticConfig distributionStatisticConfig, double scale) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Meter newMeter(Id id, Type type, Iterable<Measurement> measurements) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected <T> FunctionTimer newFunctionTimer(Id id, T obj, ToLongFunction<T> countFunction,
        ToDoubleFunction<T> totalTimeFunction, TimeUnit totalTimeFunctionUnit) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected <T> FunctionCounter newFunctionCounter(Id id, T obj,
        ToDoubleFunction<T> countFunction) {
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
