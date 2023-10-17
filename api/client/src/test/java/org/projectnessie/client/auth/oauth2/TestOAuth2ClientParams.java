/*
 * Copyright (C) 2023 Dremio
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SCOPES;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientParams.MIN_IDLE_INTERVAL;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientParams.MIN_REFRESH_DELAY;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOAuth2ClientParams {

  @ParameterizedTest
  @MethodSource
  void testCheck(ImmutableOAuth2ClientParams.Builder params, Throwable expected) {
    @SuppressWarnings("ResultOfMethodCallIgnored")
    Throwable actual = catchThrowable(params::build);
    if (expected == null) {
      assertThat(actual).isNull();
    } else {
      assertThat(actual).isInstanceOf(expected.getClass()).hasMessage(expected.getMessage());
    }
  }

  static Stream<Arguments> testCheck() {
    return Stream.of(
        Arguments.of(
            newBuilder().clientId(""), new IllegalArgumentException("client ID must not be empty")),
        Arguments.of(
            newBuilder().clientSecret(""),
            new IllegalArgumentException("client secret must not be empty")),
        Arguments.of(
            newBuilder().grantType("invalid"),
            new IllegalArgumentException(
                "grant type must be either 'client_credentials' or 'password'")),
        Arguments.of(
            newBuilder().grantType("password"),
            new IllegalArgumentException("username must be set if grant type is 'password'")),
        Arguments.of(
            newBuilder().grantType("password").username(""),
            new IllegalArgumentException("username must be set if grant type is 'password'")),
        Arguments.of(
            newBuilder().grantType("password").username("Alice"),
            new IllegalArgumentException("password must be set if grant type is 'password'")),
        Arguments.of(
            newBuilder().grantType("password").username("Alice").password(""),
            new IllegalArgumentException("password must be set if grant type is 'password'")),
        Arguments.of(
            newBuilder().defaultAccessTokenLifespan(MIN_REFRESH_DELAY.minusSeconds(1)),
            new IllegalArgumentException(
                "default token lifespan must be greater than or equal to " + MIN_REFRESH_DELAY)),
        Arguments.of(
            newBuilder().refreshSafetyWindow(MIN_REFRESH_DELAY.minusSeconds(1)),
            new IllegalArgumentException(
                "refresh safety window must be greater than or equal to " + MIN_REFRESH_DELAY)),
        Arguments.of(
            newBuilder()
                .refreshSafetyWindow(Duration.ofMinutes(10))
                .defaultAccessTokenLifespan(Duration.ofMinutes(5)),
            new IllegalArgumentException(
                "refresh safety window must be less than the default token lifespan")),
        Arguments.of(
            newBuilder().preemptiveTokenRefreshIdleTimeout(MIN_IDLE_INTERVAL.minusSeconds(1)),
            new IllegalArgumentException(
                "preemptive token refresh idle timeout must be greater than or equal to "
                    + MIN_IDLE_INTERVAL)),
        Arguments.of(
            newBuilder().backgroundThreadIdleTimeout(Duration.ZERO),
            new IllegalArgumentException("Core threads must have nonzero keep alive times")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromConfig(Properties config, OAuth2ClientParams expected, Throwable expectedThrowable) {
    if (config != null && expected != null) {
      OAuth2ClientParams actual = OAuth2ClientParams.fromConfig(config::getProperty);
      assertThat(actual)
          .isNotNull()
          .extracting(
              OAuth2ClientParams::getTokenEndpoint,
              OAuth2ClientParams::getClientId,
              OAuth2ClientParams::getClientSecret,
              OAuth2ClientParams::getDefaultAccessTokenLifespan,
              OAuth2ClientParams::getRefreshSafetyWindow,
              OAuth2ClientParams::getScope)
          .containsExactly(
              expected.getTokenEndpoint(),
              expected.getClientId(),
              expected.getClientSecret(),
              expected.getDefaultAccessTokenLifespan(),
              expected.getRefreshSafetyWindow(),
              expected.getScope());
      assertThat(actual.getHttpClient()).isNotNull();
      assertThat(actual.getExecutor()).isNotNull();
      assertThat(actual.getObjectMapper()).isEqualTo(OAuth2ClientParams.OBJECT_MAPPER);
    } else {
      Function<String, String> cfg = config == null ? null : config::getProperty;
      Throwable actual = catchThrowable(() -> OAuth2ClientParams.fromConfig(cfg));
      assertThat(actual)
          .isInstanceOf(expectedThrowable.getClass())
          .hasMessage(expectedThrowable.getMessage());
    }
  }

  static Stream<Arguments> testFromConfig() {
    return Stream.of(
        Arguments.of(null, null, new NullPointerException("config must not be null")),
        Arguments.of(
            generateConfig(CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT),
            null,
            new NullPointerException("token endpoint must not be null")),
        Arguments.of(
            generateConfig(CONF_NESSIE_OAUTH2_CLIENT_ID),
            null,
            new NullPointerException("client ID must not be null")),
        Arguments.of(
            generateConfig(CONF_NESSIE_OAUTH2_CLIENT_SECRET),
            null,
            new NullPointerException("client secret must not be null")),
        Arguments.of(
            generateConfig(null),
            newBuilder()
                .refreshSafetyWindow(Duration.ofSeconds(10))
                .defaultAccessTokenLifespan(Duration.ofSeconds(30))
                .scope("test")
                .build(),
            null));
  }

  private static ImmutableOAuth2ClientParams.Builder newBuilder() {
    return OAuth2ClientParams.builder()
        .clientId("Alice")
        .clientSecret("s3cr3t")
        .tokenEndpoint(URI.create("https://example.com/token"));
  }

  private static Properties generateConfig(String keyToExclude) {
    Properties config = new Properties();
    config.setProperty(CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT, "https://example.com/token");
    config.setProperty(CONF_NESSIE_OAUTH2_CLIENT_ID, "Alice");
    config.setProperty(CONF_NESSIE_OAUTH2_CLIENT_SECRET, "s3cr3t");
    config.setProperty(CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW, "PT10S");
    config.setProperty(CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN, "PT30S");
    config.setProperty(CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "test");
    if (keyToExclude != null) {
      config.remove(keyToExclude);
    }
    return config;
  }
}
