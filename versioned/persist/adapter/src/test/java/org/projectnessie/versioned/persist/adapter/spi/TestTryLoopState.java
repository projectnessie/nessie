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
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.spi.TryLoopState.MonotonicClock;

class TestTryLoopState {

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 50})
  void retriesWithinBounds(int retries) throws Exception {
    Long[] times = new Long[retries];
    Arrays.fill(times, 0L);
    MonotonicClock clock = mockedClock(0L, times);

    TryLoopState tryLoopState =
        new TryLoopState(this::retryErrorMessage, mockedConfig(retries, 42L), clock);

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
        new TryLoopState(this::retryErrorMessage, mockedConfig(retries - 1, 42L), clock);

    verify(clock, times(1)).currentNanos();

    for (int i = 0; i < retries - 1; i++) {
      tryLoopState.retry();
    }

    verify(clock, times(retries)).currentNanos();
    verify(clock, times(retries - 1)).sleepMillis(anyLong());

    assertThatThrownBy(tryLoopState::retry)
        .isInstanceOf(ReferenceRetryFailureException.class)
        .hasMessage(retryErrorMessage());
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
        new TryLoopState(this::retryErrorMessage, mockedConfig(retries, timeoutMillis), clock);

    long lower = TryLoopState.INITIAL_LOWER_BOUND;
    long upper = TryLoopState.INITIAL_UPPER_BOUND;

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
        new TryLoopState(this::retryErrorMessage, mockedConfig(retries, 42L), clock);

    verify(clock, times(1)).currentNanos();

    for (int i = 0; i < retries - 1; i++) {
      tryLoopState.retry();
    }

    verify(clock, times(retries)).currentNanos();
    verify(clock, times(retries - 1)).sleepMillis(anyLong());

    assertThatThrownBy(tryLoopState::retry)
        .isInstanceOf(ReferenceRetryFailureException.class)
        .hasMessage(retryErrorMessage());
  }

  MonotonicClock mockedClock(Long t0, Long... times) {
    MonotonicClock mock = spy(MonotonicClock.class);
    when(mock.currentNanos()).thenReturn(t0, times);
    doNothing().when(mock).sleepMillis(anyLong());
    return mock;
  }

  DatabaseAdapterConfig mockedConfig(int commitRetries, long commitTImeout) {
    DatabaseAdapterConfig mock = mock(DatabaseAdapterConfig.class);
    when(mock.getCommitRetries()).thenReturn(commitRetries);
    when(mock.getCommitTimeout()).thenReturn(commitTImeout);
    return mock;
  }

  private String retryErrorMessage() {
    return "RETRY ERROR MESSAGE";
  }
}
