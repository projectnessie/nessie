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
package org.projectnessie.versioned.storage.common.logic;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.AssertionFailureBuilder.assertionFailure;
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
import static org.projectnessie.versioned.storage.common.logic.CommitRetry.commitRetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.TryLoopState.MonotonicClock;
import org.projectnessie.versioned.storage.common.persist.Persist;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCommitRetry {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void commitRetryTimeout() {
    int retries = 3;
    StoreConfig mockedConfig = mockedConfig(retries, Long.MAX_VALUE);

    MonotonicClock clock = mockedClock(retries);
    CommitRetry.TryLoopState tryLoopState = new CommitRetry.TryLoopState(mockedConfig, clock);

    Persist persist = mock(Persist.class);

    AtomicInteger retryCounter = new AtomicInteger();

    soft.assertThatThrownBy(
            () ->
                commitRetry(
                    persist,
                    (p, retryState) -> {
                      retryCounter.incrementAndGet();
                      soft.assertThat(p).isSameAs(persist).isNotNull();
                      throw new RetryException();
                    },
                    tryLoopState))
        .isInstanceOf(RetryTimeoutException.class)
        .asInstanceOf(type(RetryTimeoutException.class))
        .extracting(RetryTimeoutException::getRetry, RetryTimeoutException::getTimeNanos)
        .containsExactly(3, 0L);
    soft.assertThat(retryCounter).hasValue(1 + retries);
  }

  @Test
  public void commitRetryImmediateSuccess() {
    int retries = 3;
    StoreConfig mockedConfig = mockedConfig(retries, Long.MAX_VALUE);

    MonotonicClock clock = mockedClock(retries);
    CommitRetry.TryLoopState tryLoopState = new CommitRetry.TryLoopState(mockedConfig, clock);

    Persist persist = mock(Persist.class);

    AtomicInteger retryCounter = new AtomicInteger();
    AtomicReference<String> result = new AtomicReference<>();

    soft.assertThatCode(
            () ->
                result.set(
                    commitRetry(
                        persist,
                        (p, retryState) -> {
                          retryCounter.incrementAndGet();
                          soft.assertThat(p).isSameAs(persist).isNotNull();
                          return "foo";
                        },
                        tryLoopState)))
        .doesNotThrowAnyException();

    soft.assertThat(retryCounter).hasValue(1);
    soft.assertThat(result).hasValue("foo");
  }

  @Test
  public void commitRetrySuccessAfterRetry() {
    int retries = 3;
    StoreConfig mockedConfig = mockedConfig(retries, Long.MAX_VALUE);

    MonotonicClock clock = mockedClock(retries);
    CommitRetry.TryLoopState tryLoopState = new CommitRetry.TryLoopState(mockedConfig, clock);

    Persist persist = mock(Persist.class);

    AtomicInteger retryCounter = new AtomicInteger();
    AtomicReference<String> result = new AtomicReference<>();

    soft.assertThatCode(
            () ->
                result.set(
                    commitRetry(
                        persist,
                        (p, retryState) -> {
                          if (retryCounter.incrementAndGet() == 1) {
                            throw new RetryException();
                          }
                          soft.assertThat(p).isSameAs(persist).isNotNull();
                          return "foo";
                        },
                        tryLoopState)))
        .doesNotThrowAnyException();

    soft.assertThat(retryCounter).hasValue(2);
    soft.assertThat(result).hasValue("foo");
  }

  @Test
  public void commitRetrySuccessAfterRetryUnmocked() {
    StoreConfig config = mockedConfig(3, Long.MAX_VALUE, 1, 1000, 1);
    Persist persist = mock(Persist.class);
    when(persist.config()).thenReturn(config);

    AtomicInteger retryCounter = new AtomicInteger();
    AtomicReference<String> result = new AtomicReference<>();

    soft.assertThatCode(
            () ->
                result.set(
                    commitRetry(
                        persist,
                        (p, retryState) -> {
                          soft.assertThat(p).isSameAs(persist);

                          if (retryCounter.incrementAndGet() == 1) {
                            throw new RetryException();
                          }

                          return "foo";
                        })))
        .doesNotThrowAnyException();

    soft.assertThat(retryCounter).hasValue(2);
    soft.assertThat(result).hasValue("foo");

    verify(persist, times(1)).config();
  }

  @Test
  public void sleepConsidersAttemptDuration() {
    StoreConfig mockedConfig =
        mockedConfig(Integer.MAX_VALUE, Long.MAX_VALUE, 100, 100, Long.MAX_VALUE);

    MonotonicClock clock = mockedClock(3);
    CommitRetry.TryLoopState tryLoopState = new CommitRetry.TryLoopState(mockedConfig, clock);

    soft.assertThat(tryLoopState.retry(MILLISECONDS.toNanos(20))).isTrue();
    verify(clock, times(1)).sleepMillis(80L);

    // bounds doubled

    soft.assertThat(tryLoopState.retry(MILLISECONDS.toNanos(30))).isTrue();
    verify(clock, times(1)).sleepMillis(170L);
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 5, 50, 100, 200})
  public void doesNotSleepLongerThanMax(long maxSleep) {
    int retries = 50;
    MonotonicClock clock = mockedClock(retries);

    long initialLower = 1;
    long initialUpper = 2;

    long lower = initialLower;
    long upper = initialUpper;
    CommitRetry.TryLoopState tryLoopState =
        new CommitRetry.TryLoopState(
            mockedConfig(retries, Long.MAX_VALUE, 1, upper, maxSleep), clock);

    verify(clock, times(1)).currentNanos();

    InOrder inOrderClock = inOrder(clock);

    for (int i = 0; i < retries; i++) {
      soft.assertThat(tryLoopState.retry(0L)).isTrue();
      long finalLower = lower;
      long finalUpper = upper;
      ArgumentMatcher<Long> matcher =
          new ArgumentMatcher<>() {
            @Override
            public boolean matches(Long l) {
              return l >= finalLower && l <= finalUpper && l <= maxSleep;
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
      } else {
        upper = maxSleep;
      }
    }

    verify(clock, times(1 + retries)).currentNanos();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 50})
  public void retriesWithinBounds(int retries) {
    MonotonicClock clock = mockedClock(retries);

    CommitRetry.TryLoopState tryLoopState =
        new CommitRetry.TryLoopState(mockedConfig(retries, 42L), clock);

    verify(clock, times(1)).currentNanos();

    for (int i = 0; i < retries; i++) {
      soft.assertThat(tryLoopState.retry(0L)).isTrue();
    }

    verify(clock, times(1 + retries)).currentNanos();
    verify(clock, times(retries)).sleepMillis(anyLong());
  }

  @Test
  public void retryUnsuccessful() {
    int retries = 3;

    MonotonicClock clock = mockedClock(retries);

    CommitRetry.TryLoopState tryLoopState =
        new CommitRetry.TryLoopState(mockedConfig(retries, 42L), clock);

    for (int i = 0; i < retries; i++) {
      soft.assertThat(tryLoopState.retry(0L)).isTrue();
    }

    soft.assertThat(tryLoopState.retry(0L)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 50})
  public void retriesOutOfBounds(int retries) {
    MonotonicClock clock = mockedClock(retries);

    CommitRetry.TryLoopState tryLoopState =
        new CommitRetry.TryLoopState(mockedConfig(retries - 1, 42L), clock);

    verify(clock, times(1)).currentNanos();

    for (int i = 0; i < retries - 1; i++) {
      soft.assertThat(tryLoopState.retry(0L)).isTrue();
    }

    verify(clock, times(retries)).currentNanos();
    verify(clock, times(retries - 1)).sleepMillis(anyLong());

    soft.assertThat(tryLoopState.retry(0L)).isFalse();
  }

  @Test
  public void sleepDurations() {
    int retries = 10;

    MonotonicClock clock = mockedClock(retries);

    // Must be "big" enough so that the upper/lower sleep-time-bounds doubling exceed this value
    long timeoutMillis = 42L;

    CommitRetry.TryLoopState tryLoopState =
        new CommitRetry.TryLoopState(mockedConfig(retries, timeoutMillis), clock);

    long lower = StoreConfig.DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER;
    long upper = StoreConfig.DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER;

    for (int i = 0; i < retries; i++) {
      soft.assertThat(tryLoopState.retry(0L)).isTrue();

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
  public void retriesOutOfTime(int retries) {
    Long[] times = new Long[retries];
    Arrays.fill(times, 0L);
    times[retries - 1] = MILLISECONDS.toNanos(43L);
    MonotonicClock clock = mockedClock(0L, times);

    CommitRetry.TryLoopState tryLoopState =
        new CommitRetry.TryLoopState(mockedConfig(retries, 42L), clock);

    verify(clock, times(1)).currentNanos();

    for (int i = 0; i < retries - 1; i++) {
      soft.assertThat(tryLoopState.retry(0L)).isTrue();
    }

    verify(clock, times(retries)).currentNanos();
    verify(clock, times(retries - 1)).sleepMillis(anyLong());

    soft.assertThat(tryLoopState.retry(0L)).isFalse();

    // Trigger the `if (unsuccessful)` case in CommitRetry.TryLoopState.retry
    soft.assertThat(tryLoopState.retry(0L)).isFalse();
  }

  @Nested
  class Telemetry {
    @RegisterExtension public final OpenTelemetryExtension otel = OpenTelemetryExtension.create();

    private List<EventData> eventsFrom(Runnable action) {
      Span span = otel.getOpenTelemetry().getTracer("test").spanBuilder("test").startSpan();
      try (Scope ignored = span.makeCurrent()) {
        action.run();
      }
      span.end();

      String spanId = span.getSpanContext().getSpanId();
      Optional<SpanData> spanData =
          otel.getSpans().stream().filter(s -> s.getSpanId().equals(spanId)).findFirst();

      return spanData
          .orElseThrow(
              () ->
                  assertionFailure()
                      .actual(otel.getSpans())
                      .message("Expected Span not found: " + spanId)
                      .build())
          .getEvents();
    }

    @Test
    void commitRetryEvents() {
      List<EventData> events = eventsFrom(TestCommitRetry.this::commitRetrySuccessAfterRetry);
      // 1 sleep due to failure induced by commitRetrySuccessAfterRetry()
      soft.assertThat(events).hasSize(1);
      soft.assertThat(events.get(0).getName()).isEqualTo("nessie.commit-retry.sleep");
      soft.assertThat(events)
          .extracting(
              e ->
                  e.getAttributes().asMap().keySet().stream().map(AttributeKey::getKey).findFirst())
          .containsExactly(Optional.of("nessie.commit-retry.sleep.duration-millis"));
    }
  }

  MonotonicClock mockedClock(int retries) {
    Long[] times = new Long[retries];
    Arrays.fill(times, 0L);
    return mockedClock(0L, times);
  }

  MonotonicClock mockedClock(Long t0, Long... times) {
    MonotonicClock mock = spy(MonotonicClock.class);
    when(mock.currentNanos()).thenReturn(t0, times);
    doNothing().when(mock).sleepMillis(anyLong());
    return mock;
  }

  StoreConfig mockedConfig(int retries, long commitTimeout) {
    return mockedConfig(retries, commitTimeout, 5L, 25L, Long.MAX_VALUE);
  }

  StoreConfig mockedConfig(
      int commitRetries, long commitTimeout, long lowerDefault, long upperDefault, long maxSleep) {
    StoreConfig mock = mock(StoreConfig.class);
    when(mock.commitRetries()).thenReturn(commitRetries);
    when(mock.commitTimeoutMillis()).thenReturn(commitTimeout);
    when(mock.retryInitialSleepMillisLower()).thenReturn(lowerDefault);
    when(mock.retryInitialSleepMillisUpper()).thenReturn(upperDefault);
    when(mock.retryMaxSleepMillis()).thenReturn(maxSleep);
    return mock;
  }
}
