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
package org.projectnessie.client.auth.oauth2;

import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOAuth2ClientUtils {

  @ParameterizedTest
  @MethodSource
  void testTokenExpirationTime(
      Instant now, Token token, Duration defaultLifespan, Instant expected) {
    Instant expirationTime = OAuth2ClientUtils.tokenExpirationTime(now, token, defaultLifespan);
    assertThat(expirationTime).isEqualTo(expected);
  }

  static Stream<Arguments> testTokenExpirationTime() {
    Instant now = Instant.now();
    Duration defaultLifespan = Duration.ofHours(1);
    Instant customExpirationTime = now.plus(Duration.ofMinutes(1));
    return Stream.of(
        // expiration time from the token response => custom expiration time
        Arguments.of(
            now,
            ImmutableRefreshToken.builder()
                .payload("access-initial")
                .expirationTime(customExpirationTime)
                .build(),
            defaultLifespan,
            customExpirationTime),
        // no expiration time in the response, token is a JWT, exp claim present => exp claim
        Arguments.of(
            now,
            ImmutableRefreshToken.builder().payload(TestJwtToken.JWT_NON_EMPTY).build(),
            defaultLifespan,
            TestJwtToken.JWT_EXP_CLAIM),
        // no expiration time in the response, token is a JWT, but no exp claim => default lifespan
        Arguments.of(
            now,
            ImmutableRefreshToken.builder().payload(TestJwtToken.JWT_EMPTY).build(),
            defaultLifespan,
            now.plus(defaultLifespan)));
  }

  @ParameterizedTest
  @MethodSource
  void testShortestDelay(
      Instant now,
      Instant accessExp,
      Instant refreshExp,
      Duration safetyWindow,
      Duration minRefreshDelay,
      Duration expected) {
    Duration actual =
        OAuth2ClientUtils.shortestDelay(now, accessExp, refreshExp, safetyWindow, minRefreshDelay);
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testShortestDelay() {
    Instant now = Instant.now();
    Duration oneMinute = Duration.ofMinutes(1);
    Duration thirtySeconds = Duration.ofSeconds(30);
    Duration defaultWindow = Duration.ofSeconds(10);
    Duration oneSecond = Duration.ofSeconds(1);
    return Stream.of(
        // refresh token < access token
        Arguments.of(
            now,
            now.plus(oneMinute),
            now.plus(thirtySeconds),
            defaultWindow,
            oneSecond,
            thirtySeconds.minus(defaultWindow)),
        // refresh token > access token
        Arguments.of(
            now,
            now.plus(thirtySeconds),
            now.plus(oneMinute),
            defaultWindow,
            oneSecond,
            thirtySeconds.minus(defaultWindow)),
        // access token already expired: MIN_REFRESH_DELAY
        Arguments.of(
            now, now.minus(oneMinute), now.plus(oneMinute), defaultWindow, oneSecond, oneSecond),
        // refresh token already expired: MIN_REFRESH_DELAY
        Arguments.of(
            now, now.plus(oneMinute), now.minus(oneMinute), defaultWindow, oneSecond, oneSecond),
        // expirationTime - safety window > MIN_REFRESH_DELAY
        Arguments.of(
            now, now.plus(oneMinute), now.plus(oneMinute), thirtySeconds, oneSecond, thirtySeconds),
        // expirationTime - safety window <= MIN_REFRESH_DELAY
        Arguments.of(
            now, now.plus(oneMinute), now.plus(oneMinute), oneMinute, oneSecond, oneSecond),
        // expirationTime - safety window <= ZERO (immediate refresh use case)
        Arguments.of(now, now.plus(oneMinute), now.plus(oneMinute), oneMinute, ZERO, ZERO));
  }
}
