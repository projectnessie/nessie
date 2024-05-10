/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.files.api;

import static java.time.Clock.systemUTC;
import static java.time.Instant.now;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.InstanceOfAssertFactories.optional;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestBackendThrottledException {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void nonRetryableHttp() {
    for (int i = 0; i <= 999; i++) {
      // HTTP 429 and 503
      if (i == 429 || i == 503) {
        continue;
      }
      soft.assertThat(
              BackendThrottledException.fromHttpStatusCode(
                  i,
                  header -> null,
                  systemUTC(),
                  instant -> new BackendThrottledException(instant, "hello"),
                  Duration.of(10, SECONDS)))
          .isEmpty();
    }
  }

  static Instant now = now().truncatedTo(SECONDS);

  @ParameterizedTest
  @MethodSource
  public void retryableDefaults(String retryAfterHeader) {
    Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
    Duration fallback = Duration.of(10, SECONDS);
    Instant expected = clock.instant().plus(fallback);

    for (int statusCode : new int[] {429, 503}) {
      soft.assertThat(
              BackendThrottledException.fromHttpStatusCode(
                  statusCode,
                  header -> retryAfterHeader,
                  clock,
                  instant -> new BackendThrottledException(instant, "hello"),
                  fallback))
          .get()
          .extracting(BackendThrottledException::retryNotBefore, optional(Instant.class))
          .get()
          .isEqualTo(expected);
    }
  }

  static Stream<String> retryableDefaults() {
    return Stream.of(
        null,
        "dummy",
        "0",
        "-1",
        "" + Integer.MIN_VALUE,
        formatInstant(now),
        formatInstant(now.minus(1, SECONDS)));
  }

  static String formatInstant(Instant instant) {
    return RFC_1123_DATE_TIME.format(instant.atZone(ZoneId.of("UTC")));
  }

  @ParameterizedTest
  @MethodSource
  public void retryableWithHeader(String retryAfterHeader, Instant expected) {
    Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
    Duration fallback = Duration.of(10, SECONDS);

    for (int statusCode : new int[] {429, 503}) {
      soft.assertThat(
              BackendThrottledException.fromHttpStatusCode(
                  statusCode,
                  header -> retryAfterHeader,
                  clock,
                  instant -> new BackendThrottledException(instant, "hello"),
                  fallback))
          .get()
          .extracting(BackendThrottledException::retryNotBefore, optional(Instant.class))
          .get()
          .isEqualTo(expected);
    }
  }

  static Stream<Arguments> retryableWithHeader() {
    return Stream.of(
        arguments("42", now.plus(42, SECONDS)),
        arguments(formatInstant(now.plus(42, SECONDS)), now.plus(42, SECONDS)),
        arguments("86400", now.plus(86400, SECONDS)),
        arguments(formatInstant(now.plus(123, DAYS)), now.plus(123, DAYS)));
  }
}
