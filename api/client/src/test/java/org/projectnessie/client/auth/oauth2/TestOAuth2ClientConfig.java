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

import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
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
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEVICE_AUTH_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_POLL_INTERVAL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_ISSUER_URL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_AUDIENCE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ISSUER_URL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_RESOURCE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SCOPES;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_TOKEN_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_USERNAME;
import static org.projectnessie.client.auth.oauth2.TokenExchangeConfig.CURRENT_ACCESS_TOKEN;
import static org.projectnessie.client.auth.oauth2.TokenExchangeConfig.CURRENT_REFRESH_TOKEN;
import static org.projectnessie.client.auth.oauth2.TokenExchangeConfig.NO_TOKEN;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOAuth2ClientConfig {

  @ParameterizedTest
  @MethodSource
  void testCheck(OAuth2ClientConfig.Builder config, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(config::build)
        .withMessage(
            "OAuth2 authentication is missing some parameters and could not be initialized: "
                + join(", ", expected));
  }

  static Stream<Arguments> testCheck() {
    return Stream.of(
        Arguments.of(
            OAuth2ClientConfig.builder().clientId("Alice").clientSecret("s3cr3t"),
            singletonList(
                "either issuer URL or token endpoint must be set (nessie.authentication.oauth2.issuer-url / nessie.authentication.oauth2.token-endpoint)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("http://example.com?query")),
            singletonList(
                "Token endpoint must not have a query part (nessie.authentication.oauth2.token-endpoint)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("http://example.com#fragment")),
            singletonList(
                "Token endpoint must not have a fragment part (nessie.authentication.oauth2.token-endpoint)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("http://example.com?query")),
            singletonList(
                "Authorization endpoint must not have a query part (nessie.authentication.oauth2.auth-endpoint)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("http://example.com#fragment")),
            singletonList(
                "Authorization endpoint must not have a fragment part (nessie.authentication.oauth2.auth-endpoint)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token")),
            singletonList("client ID must not be empty (nessie.authentication.oauth2.client-id)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("client1")
                .tokenEndpoint(URI.create("https://example.com/token")),
            singletonList(
                "client secret must not be empty when grant type is 'client_credentials' (nessie.authentication.oauth2.grant-type / nessie.authentication.oauth2.client-secret)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.REFRESH_TOKEN),
            singletonList(
                "grant type must be either 'client_credentials', 'password', 'authorization_code' or 'device_code' (nessie.authentication.oauth2.grant-type)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.PASSWORD),
            asList(
                "username must be set if grant type is 'password' (nessie.authentication.oauth2.username)",
                "password must be set if grant type is 'password' (nessie.authentication.oauth2.password)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.PASSWORD)
                .username(""),
            asList(
                "username must be set if grant type is 'password' (nessie.authentication.oauth2.username)",
                "password must be set if grant type is 'password' (nessie.authentication.oauth2.password)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.PASSWORD)
                .username("Alice"),
            singletonList(
                "password must be set if grant type is 'password' (nessie.authentication.oauth2.password)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.PASSWORD)
                .username("Alice")
                .password(""),
            singletonList(
                "password must be set if grant type is 'password' (nessie.authentication.oauth2.password)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.AUTHORIZATION_CODE),
            singletonList(
                "either issuer URL or authorization endpoint must be set if grant type is 'authorization_code' (nessie.authentication.oauth2.issuer-url / nessie.authentication.oauth2.auth-endpoint)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .grantType(GrantType.AUTHORIZATION_CODE)
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("http://example.com"))
                .authorizationCodeFlowWebServerPort(-1),
            singletonList(
                "authorization code flow: web server port must be between 0 and 65535 (inclusive) (nessie.authentication.oauth2.auth-code-flow.web-port)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .grantType(GrantType.AUTHORIZATION_CODE)
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("http://example.com"))
                .authorizationCodeFlowTimeout(Duration.ofSeconds(1)),
            singletonList(
                "authorization code flow: timeout must be greater than or equal to PT30S (nessie.authentication.oauth2.auth-code-flow.timeout)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.DEVICE_CODE),
            singletonList(
                "either issuer URL or device authorization endpoint must be set if grant type is 'device_code' (nessie.authentication.oauth2.issuer-url / nessie.authentication.oauth2.auth-endpoint)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .grantType(GrantType.DEVICE_CODE)
                .tokenEndpoint(URI.create("https://example.com/token"))
                .deviceAuthEndpoint(URI.create("http://example.com"))
                .deviceCodeFlowTimeout(Duration.ofSeconds(1)),
            singletonList(
                "device code flow: timeout must be greater than or equal to PT30S (nessie.authentication.oauth2.device-code-flow.timeout)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .grantType(GrantType.DEVICE_CODE)
                .tokenEndpoint(URI.create("https://example.com/token"))
                .deviceAuthEndpoint(URI.create("http://example.com"))
                .deviceCodeFlowPollInterval(Duration.ofSeconds(1)),
            singletonList(
                "device code flow: poll interval must be greater than or equal to PT5S (nessie.authentication.oauth2.device-code-flow.poll-interval)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .defaultAccessTokenLifespan(Duration.ofSeconds(2)),
            asList(
                "default token lifespan must be greater than or equal to PT10S (nessie.authentication.oauth2.default-access-token-lifespan)",
                "refresh safety window must be less than the default token lifespan (nessie.authentication.oauth2.refresh-safety-window/nessie.authentication.oauth2.default-access-token-lifespan)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .refreshSafetyWindow(Duration.ofMillis(100)),
            singletonList(
                "refresh safety window must be greater than or equal to PT1S (nessie.authentication.oauth2.refresh-safety-window)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .refreshSafetyWindow(Duration.ofMinutes(10))
                .defaultAccessTokenLifespan(Duration.ofMinutes(5)),
            singletonList(
                "refresh safety window must be less than the default token lifespan (nessie.authentication.oauth2.refresh-safety-window/nessie.authentication.oauth2.default-access-token-lifespan)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .preemptiveTokenRefreshIdleTimeout(Duration.ofMillis(100)),
            singletonList(
                "preemptive token refresh idle timeout must be greater than or equal to PT1S (nessie.authentication.oauth2.preemptive-token-refresh-idle-timeout)")),
        Arguments.of(
            OAuth2ClientConfig.builder()
                .clientId("Alice")
                .clientSecret("s3cr3t")
                .tokenEndpoint(URI.create("https://example.com/token"))
                .backgroundThreadIdleTimeout(Duration.ZERO),
            singletonList(
                "background thread idle timeout must be greater than zero (nessie.authentication.oauth2.background-thread-idle-timeout)")));
  }

  @ParameterizedTest
  @MethodSource
  void testFromConfig(
      Map<String, String> config, OAuth2ClientConfig expected, Throwable expectedThrowable) {
    if (config != null && expected != null) {
      OAuth2ClientConfig actual =
          (OAuth2ClientConfig) OAuth2AuthenticatorConfig.fromConfigSupplier(config::get);
      assertThat(actual)
          .usingRecursiveComparison()
          .ignoringFields(
              "clientName",
              "clientSecret",
              "password",
              "objectMapper",
              "executor",
              "httpClient",
              "discoveryData",
              "resolvedTokenEndpoint",
              "resolvedAuthEndpoint")
          .isEqualTo(expected);
      assertThat(actual.getHttpClient()).isNotNull();
      assertThat(actual.getExecutor()).isNotNull();
      assertThat(actual.getObjectMapper()).isNotNull();
    } else {
      Function<String, String> cfg = config == null ? null : config::get;
      Throwable actual = catchThrowable(() -> OAuth2AuthenticatorConfig.fromConfigSupplier(cfg));
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
                CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT, "https://example.com/token",
                CONF_NESSIE_OAUTH2_CLIENT_SECRET, "s3cr3t",
                CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW, "PT10S",
                CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN, "PT30S",
                CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "test"),
            null,
            new IllegalArgumentException(
                "OAuth2 authentication is missing some parameters and could not be initialized: client ID must not be empty (nessie.authentication.oauth2.client-id)")),
        Arguments.of(
            ImmutableMap.builder()
                .put(CONF_NESSIE_OAUTH2_ISSUER_URL, "https://example.com/")
                .put(CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT, "https://example.com/token")
                .put(CONF_NESSIE_OAUTH2_AUTH_ENDPOINT, "https://example.com/auth")
                .put(CONF_NESSIE_OAUTH2_DEVICE_AUTH_ENDPOINT, "https://example.com/device")
                .put(CONF_NESSIE_OAUTH2_GRANT_TYPE, "authorization_code")
                .put(CONF_NESSIE_OAUTH2_CLIENT_ID, "Client")
                .put(CONF_NESSIE_OAUTH2_CLIENT_SECRET, "w00t")
                .put(CONF_NESSIE_OAUTH2_USERNAME, "Alice")
                .put(CONF_NESSIE_OAUTH2_PASSWORD, "s3cr3t")
                .put(CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "test")
                .put(CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN, "PT30S")
                .put(CONF_NESSIE_OAUTH2_DEFAULT_REFRESH_TOKEN_LIFESPAN, "PT30S")
                .put(CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW, "PT10S")
                .put(CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT, "PT10S")
                .put(CONF_NESSIE_OAUTH2_BACKGROUND_THREAD_IDLE_TIMEOUT, "PT10S")
                .put(CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_WEB_PORT, "8080")
                .put(CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_TIMEOUT, "PT30S")
                .put(CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_POLL_INTERVAL, "PT8S")
                .put(CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_TIMEOUT, "PT45S")
                .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED, "true")
                .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ISSUER_URL, "https://token-exchange.com/")
                .put(
                    CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_TOKEN_ENDPOINT, "https://token-exchange.com/")
                .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_ID, "token-exchange-client-id")
                .put(
                    CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_SECRET, "token-exchange-client-secret")
                .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SCOPES, "scope1 scope2")
                .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_AUDIENCE, "audience")
                .put(
                    CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_RESOURCE,
                    "https://token-exchange.com/resource")
                .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN, "subject-token")
                .put(
                    CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN_TYPE,
                    TypedToken.URN_ID_TOKEN.toString())
                .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN, "actor-token")
                .put(
                    CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN_TYPE,
                    TypedToken.URN_JWT.toString())
                .build(),
            OAuth2ClientConfig.builder()
                .issuerUrl(URI.create("https://example.com/"))
                .tokenEndpoint(URI.create("https://example.com/token"))
                .authEndpoint(URI.create("https://example.com/auth"))
                .deviceAuthEndpoint(URI.create("https://example.com/device"))
                .grantType(GrantType.AUTHORIZATION_CODE)
                .clientId("Client")
                .clientSecret("w00t")
                .username("Alice")
                .password("s3cr3t")
                .addScope("test")
                .defaultAccessTokenLifespan(Duration.ofSeconds(30))
                .defaultRefreshTokenLifespan(Duration.ofSeconds(30))
                .refreshSafetyWindow(Duration.ofSeconds(10))
                .preemptiveTokenRefreshIdleTimeout(Duration.ofSeconds(10))
                .backgroundThreadIdleTimeout(Duration.ofSeconds(10))
                .authorizationCodeFlowWebServerPort(8080)
                .authorizationCodeFlowTimeout(Duration.ofSeconds(30))
                .deviceCodeFlowPollInterval(Duration.ofSeconds(8))
                .deviceCodeFlowTimeout(Duration.ofSeconds(45))
                .tokenExchangeConfig(
                    TokenExchangeConfig.builder()
                        .enabled(true)
                        .issuerUrl(URI.create("https://token-exchange.com/"))
                        .tokenEndpoint(URI.create("https://token-exchange.com/"))
                        .clientId("token-exchange-client-id")
                        .clientSecret("token-exchange-client-secret")
                        .audience("audience")
                        .addScopes("scope1", "scope2")
                        .resource(URI.create("https://token-exchange.com/resource"))
                        .subjectToken(TypedToken.of("subject-token", TypedToken.URN_ID_TOKEN))
                        .actorToken(TypedToken.of("actor-token", TypedToken.URN_JWT))
                        .build())
                .build(),
            null));
  }

  @Test
  void testTokenExchangeStaticTokens() {
    ImmutableMap<String, String> map =
        ImmutableMap.<String, String>builder()
            .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED, "true")
            .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN, "static-subject")
            .put(
                CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN_TYPE,
                TypedToken.URN_SAML1.toString())
            .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN, "static-actor")
            .put(
                CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN_TYPE, TypedToken.URN_SAML2.toString())
            .build();
    OAuth2ClientConfig config =
        OAuth2ClientConfig.builder()
            .issuerUrl(URI.create("https://example.com/"))
            .clientId("Client")
            .clientSecret("w00t")
            .tokenExchangeConfig(TokenExchangeConfig.fromConfigSupplier(map::get))
            .build();
    TypedToken subjectToken =
        config
            .getTokenExchangeConfig()
            .getSubjectTokenProvider()
            .apply(AccessToken.of("dynamic-access", "Bearer", null), null);
    assertThat(subjectToken.getPayload()).isEqualTo("static-subject");
    assertThat(subjectToken.getTokenType()).isEqualTo(TypedToken.URN_SAML1);
    TypedToken actorToken =
        config
            .getTokenExchangeConfig()
            .getActorTokenProvider()
            .apply(AccessToken.of("dynamic-access", "Bearer", null), null);
    assertThat(actorToken.getPayload()).isEqualTo("static-actor");
    assertThat(actorToken.getTokenType()).isEqualTo(TypedToken.URN_SAML2);
  }

  @Test
  void testTokenExchangeDynamicTokens() {
    ImmutableMap<String, String> map =
        ImmutableMap.<String, String>builder()
            .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED, "true")
            .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN, CURRENT_ACCESS_TOKEN)
            .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN, CURRENT_REFRESH_TOKEN)
            .build();
    OAuth2ClientConfig config =
        OAuth2ClientConfig.builder()
            .issuerUrl(URI.create("https://example.com/"))
            .clientId("Client")
            .clientSecret("w00t")
            .tokenExchangeConfig(TokenExchangeConfig.fromConfigSupplier(map::get))
            .build();
    TypedToken subjectToken =
        config
            .getTokenExchangeConfig()
            .getSubjectTokenProvider()
            .apply(AccessToken.of("dynamic-access", "Bearer", null), null);
    assertThat(subjectToken.getPayload()).isEqualTo("dynamic-access");
    assertThat(subjectToken.getTokenType()).isEqualTo(TypedToken.URN_ACCESS_TOKEN);
    TypedToken actorToken =
        config
            .getTokenExchangeConfig()
            .getActorTokenProvider()
            .apply(
                AccessToken.of("dynamic-access", "Bearer", null),
                RefreshToken.of("dynamic-refresh", null));
    assertThat(actorToken.getPayload()).isEqualTo("dynamic-refresh");
    assertThat(actorToken.getTokenType()).isEqualTo(TypedToken.URN_REFRESH_TOKEN);
  }

  @Test
  void testTokenExchangeNoActorToken() {
    ImmutableMap<String, String> map =
        ImmutableMap.<String, String>builder()
            .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED, "true")
            .put(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN, NO_TOKEN)
            .build();
    OAuth2ClientConfig config =
        OAuth2ClientConfig.builder()
            .issuerUrl(URI.create("https://example.com/"))
            .clientId("Client")
            .clientSecret("w00t")
            .tokenExchangeConfig(TokenExchangeConfig.fromConfigSupplier(map::get))
            .build();
    TypedToken subjectToken =
        config
            .getTokenExchangeConfig()
            .getSubjectTokenProvider()
            .apply(AccessToken.of("dynamic-access", "Bearer", null), null);
    assertThat(subjectToken.getPayload()).isEqualTo("dynamic-access");
    assertThat(subjectToken.getTokenType()).isEqualTo(TypedToken.URN_ACCESS_TOKEN);
    TypedToken actorToken =
        config
            .getTokenExchangeConfig()
            .getActorTokenProvider()
            .apply(
                AccessToken.of("dynamic-access", "Bearer", null),
                RefreshToken.of("dynamic-refresh", null));
    assertThat(actorToken).isNull();
  }
}
