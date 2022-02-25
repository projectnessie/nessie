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
package org.projectnessie.versioned.persist.adapter.spi;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.spi.TryLoopState.MonotonicClock;

class TestTryLoopState {

  @ParameterizedTest
  @ValueSource(longs = {1, 5, 50, 100, 200})
  void doesNotSleepLongerThanMax(long maxSleep) throws Exception {
    int retries = 50;
    Long[] times = new Long[retries];
    Arrays.fill(times, 0L);
    MonotonicClock clock = mockedClock(0L, times);

    long lower = 1;
    long upper = 2;
    TryLoopState tryLoopState =
        new TryLoopState(
            "test",
            this::retryErrorMessage,
            mockedConfig(retries, Long.MAX_VALUE, 1, upper, maxSleep),
            clock,
            (success, tls) -> {});

    verify(clock, times(1)).currentNanos();

    InOrder inOrderClock = inOrder(clock);

    for (int i = 0; i < retries; i++) {
      tryLoopState.retry();
      long finalLower = lower;
      long finalUpper = upper;
      ArgumentMatcher<Long> matcher =
          new ArgumentMatcher<Long>() {
            @Override
            public boolean matches(Long l) {
              return l >= finalLower && l < finalUpper && l <= maxSleep;
            }

            @Override
            public String toString() {
              return "lower = " + finalLower + ", upper = " + finalUpper + ", max = " + maxSleep;
            }
          };
      inOrderClock.verify(clock, times(1)).sleepMillis(longThat(matcher));

      if (upper * 2 <= maxSleep) {
        lower *= 2;
        upper *= 2;
      }
    }

    verify(clock, times(1 + retries)).currentNanos();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 50})
  void retriesWithinBounds(int retries) throws Exception {
    Long[] times = new Long[retries];
    Arrays.fill(times, 0L);
    MonotonicClock clock = mockedClock(0L, times);

    TryLoopState tryLoopState =
        new TryLoopState(
            "test",
            this::retryErrorMessage,
            mockedConfig(retries, 42L),
            clock,
            (success, tls) -> {});

    verify(clock, times(1)).currentNanos();

    for (int i = 0; i < retries; i++) {
      tryLoopState.retry();
    }

    verify(clock, times(1 + retries)).currentNanos();
    verify(clock, times(retries)).sleepMillis(anyLong());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 50})
  void retriesOutOfBounds(int retries) throws Exception {
    Long[] times = new Long[retries];
    Arrays.fill(times, 0L);
    MonotonicClock clock = mockedClock(0L, times);

    TryLoopState tryLoopState =
        new TryLoopState(
            "test",
            this::retryErrorMessage,
            mockedConfig(retries - 1, 42L),
            clock,
            (success, tls) -> {});

    verify(clock, times(1)).currentNanos();

    for (int i = 0; i < retries - 1; i++) {
      tryLoopState.retry();
    }

    verify(clock, times(retries)).currentNanos();
    verify(clock, times(retries - 1)).sleepMillis(anyLong());

    assertThatThrownBy(tryLoopState::retry)
        .isInstanceOf(ReferenceRetryFailureException.class)
        .hasMessage(retryErrorMessage(null));
  }

  @Test
  void sleepDurations() throws Exception {
    int retries = 10;

    Long[] times = new Long[retries];
    Arrays.fill(times, 0L);
    MonotonicClock clock = mockedClock(0L, times);

    // Must be "big" enough so that the upper/lower sleep-time-bounds doubling exceed this value
    long timeoutMillis = 42L;

    TryLoopState tryLoopState =
        new TryLoopState(
            "test",
            this::retryErrorMessage,
            mockedConfig(retries, timeoutMillis),
            clock,
            (success, tls) -> {});

    long lower = DatabaseAdapterConfig.DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER;
    long upper = DatabaseAdapterConfig.DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER;

    for (int i = 0; i < retries; i++) {
      tryLoopState.retry();

      long l = Math.min(lower, timeoutMillis);
      long u = Math.min(upper, timeoutMillis);

      verify(clock).sleepMillis(longThat(v -> v >= l && v <= u));
      clearInvocations(clock);

      lower *= 2;
      upper *= 2;
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 50})
  void retriesOutOfTime(int retries) throws Exception {
    Long[] times = new Long[retries];
    Arrays.fill(times, 0L);
    times[retries - 1] = TimeUnit.MILLISECONDS.toNanos(43L);
    MonotonicClock clock = mockedClock(0L, times);

    TryLoopState tryLoopState =
        new TryLoopState(
            "test",
            this::retryErrorMessage,
            mockedConfig(retries, 42L),
            clock,
            (success, tls) -> {});

    verify(clock, times(1)).currentNanos();

    for (int i = 0; i < retries - 1; i++) {
      tryLoopState.retry();
    }

    verify(clock, times(retries)).currentNanos();
    verify(clock, times(retries - 1)).sleepMillis(anyLong());

    assertThatThrownBy(tryLoopState::retry)
        .isInstanceOf(ReferenceRetryFailureException.class)
        .hasMessage(retryErrorMessage(null));
  }

  MonotonicClock mockedClock(Long t0, Long... times) {
    MonotonicClock mock = spy(MonotonicClock.class);
    when(mock.currentNanos()).thenReturn(t0, times);
    doNothing().when(mock).sleepMillis(anyLong());
    return mock;
  }

  DatabaseAdapterConfig mockedConfig(int retries, long commitTimeout) {
    return mockedConfig(retries, commitTimeout, 5L, 25L, Long.MAX_VALUE);
  }

  DatabaseAdapterConfig mockedConfig(
      int commitRetries, long commitTimeout, long lowerDefault, long upperDefault, long maxSleep) {
    DatabaseAdapterConfig mock = mock(DatabaseAdapterConfig.class);
    when(mock.getCommitRetries()).thenReturn(commitRetries);
    when(mock.getCommitTimeout()).thenReturn(commitTimeout);
    when(mock.getRetryInitialSleepMillisLower()).thenReturn(lowerDefault);
    when(mock.getRetryInitialSleepMillisUpper()).thenReturn(upperDefault);
    when(mock.getRetryMaxSleepMillis()).thenReturn(maxSleep);
    return mock;
  }

  private String retryErrorMessage(TryLoopState state) {
    return "RETRY ERROR MESSAGE";
  }
}
