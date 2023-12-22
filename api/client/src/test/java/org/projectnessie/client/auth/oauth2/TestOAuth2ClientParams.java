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
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_WEB_PORT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_AUTH_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_BACKGROUND_THREAD_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SCOPES;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEFAULT_REFRESH_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_USERNAME;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientParams.MIN_IDLE_INTERVAL;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientParams.MIN_REFRESH_DELAY;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
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
    assertThat(actual).isInstanceOf(expected.getClass()).hasMessage(expected.getMessage());
  }

  static Stream<Arguments> testCheck() {
    return Stream.of(
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("http://example.com?query")),
            new IllegalArgumentException("Token endpoint must not have a query part")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("http://example.com#fragment")),
            new IllegalArgumentException("Token endpoint must not have a fragment part")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("http://example.com?query")),
            new IllegalArgumentException("Authorization endpoint must not have a query part")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("http://example.com#fragment")),
            new IllegalArgumentException("Authorization endpoint must not have a fragment part")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token")),
            new IllegalArgumentException("client ID must not be empty")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("")
                .tokenEndpoint(URI.create("https://example.com/token")),
            new IllegalArgumentException("client secret must not be empty")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.TOKEN_EXCHANGE),
            new IllegalArgumentException(
                "grant type must be either 'client_credentials', 'password' or 'authorization_code'")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.PASSWORD),
            new IllegalArgumentException("username must be set if grant type is 'password'")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.PASSWORD)
                .username(""),
            new IllegalArgumentException("username must be set if grant type is 'password'")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.PASSWORD)
                .username("Alice"),
            new IllegalArgumentException("password must be set if grant type is 'password'")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .password("")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.PASSWORD)
                .username("Alice")
                .password(""),
            new IllegalArgumentException("password must be set if grant type is 'password'")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.AUTHORIZATION_CODE),
            new IllegalArgumentException(
                "authorization endpoint must be set if grant type is 'authorization_code'")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .grantType(GrantType.AUTHORIZATION_CODE)
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("http://example.com"))
                .authorizationCodeFlowWebServerPort(-1),
            new IllegalArgumentException(
                "authorization code flow: web server port must be between 0 and 65535 (inclusive)")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .grantType(GrantType.AUTHORIZATION_CODE)
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("http://example.com"))
                .authorizationCodeFlowTimeout(Duration.ofSeconds(1)),
            new IllegalArgumentException(
                "authorization code flow: timeout must be greater than or equal to PT10S")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .defaultAccessTokenLifespan(MIN_REFRESH_DELAY.minusSeconds(1)),
            new IllegalArgumentException(
                "default token lifespan must be greater than or equal to " + MIN_REFRESH_DELAY)),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .refreshSafetyWindow(MIN_REFRESH_DELAY.minusSeconds(1)),
            new IllegalArgumentException(
                "refresh safety window must be greater than or equal to " + MIN_REFRESH_DELAY)),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .refreshSafetyWindow(Duration.ofMinutes(10))
                .defaultAccessTokenLifespan(Duration.ofMinutes(5)),
            new IllegalArgumentException(
                "refresh safety window must be less than the default token lifespan")),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .preemptiveTokenRefreshIdleTimeout(MIN_IDLE_INTERVAL.minusSeconds(1)),
            new IllegalArgumentException(
                "preemptive token refresh idle timeout must be greater than or equal to "
                    + MIN_IDLE_INTERVAL)),
        Arguments.of(
            OAuth2ClientParams.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .backgroundThreadIdleTimeout(Duration.ZERO),
            new IllegalArgumentException("Core threads must have nonzero keep alive times")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromConfig(
      Map<String, String> config, OAuth2ClientParams expected, Throwable expectedThrowable) {
    if (config != null && expected != null) {
      OAuth2ClientParams actual = OAuth2ClientParams.fromConfig(config::get);
      assertThat(actual)
          .usingRecursiveComparison()
          .ignoringFields("objectMapper", "executor", "httpClient")
          .isEqualTo(expected);
      assertThat(actual.getHttpClient()).isNotNull();
      assertThat(actual.getExecutor()).isNotNull();
      assertThat(actual.getObjectMapper()).isEqualTo(OAuth2ClientParams.OBJECT_MAPPER);
    } else {
      Function<String, String> cfg = config == null ? null : config::get;
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
            ImmutableMap.of(
                CONF_NESSIE_OAUTH2_CLIENT_ID, "Alice",
                CONF_NESSIE_OAUTH2_CLIENT_SECRET, "s3cr3t",
                CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW, "PT10S",
                CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN, "PT30S",
                CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "test"),
            null,
            new NullPointerException("token endpoint must not be null")),
        Arguments.of(
            ImmutableMap.of(
                CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT, "https://example.com/token",
                CONF_NESSIE_OAUTH2_CLIENT_SECRET, "s3cr3t",
                CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW, "PT10S",
                CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN, "PT30S",
                CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "test"),
            null,
            new NullPointerException("client ID must not be null")),
        Arguments.of(
            ImmutableMap.of(
                CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT, "https://example.com/token",
                CONF_NESSIE_OAUTH2_CLIENT_ID, "Alice",
                CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW, "PT10S",
                CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN, "PT30S",
                CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "test"),
            null,
            new NullPointerException("client secret must not be null")),
        Arguments.of(
            ImmutableMap.builder()
                .put(CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT, "https://example.com/token")
                .put(CONF_NESSIE_OAUTH2_AUTH_ENDPOINT, "https://example.com/auth")
                .put(CONF_NESSIE_OAUTH2_GRANT_TYPE, "authorization_code")
                .put(CONF_NESSIE_OAUTH2_CLIENT_ID, "Client")
                .put(CONF_NESSIE_OAUTH2_CLIENT_SECRET, "w00t")
                .put(CONF_NESSIE_OAUTH2_USERNAME, "Alice")
                .put(CONF_NESSIE_OAUTH2_PASSWORD, "s3cr3t")
                .put(CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "test")
                .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED, "false")
                .put(CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN, "PT30S")
                .put(CONF_NESSIE_OAUTH2_DEFAULT_REFRESH_TOKEN_LIFESPAN, "PT30S")
                .put(CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW, "PT10S")
                .put(CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT, "PT10S")
                .put(CONF_NESSIE_OAUTH2_BACKGROUND_THREAD_IDLE_TIMEOUT, "PT10S")
                .put(CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_WEB_PORT, "8080")
                .put(CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_TIMEOUT, "PT30S")
                .build(),
            OAuth2ClientParams.builder()
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("https://example.com/auth"))
                .grantType(GrantType.AUTHORIZATION_CODE)
                .clientId("Client")
                .clientSecret("w00t")
                .username("Alice")
                .password("s3cr3t")
                .scope("test")
                .tokenExchangeEnabled(false)
                .defaultAccessTokenLifespan(Duration.ofSeconds(30))
                .defaultRefreshTokenLifespan(Duration.ofSeconds(30))
                .refreshSafetyWindow(Duration.ofSeconds(10))
                .preemptiveTokenRefreshIdleTimeout(Duration.ofSeconds(10))
                .backgroundThreadIdleTimeout(Duration.ofSeconds(10))
                .authorizationCodeFlowWebServerPort(8080)
                .authorizationCodeFlowTimeout(Duration.ofSeconds(30))
                .build(),
            null));
  }
}
