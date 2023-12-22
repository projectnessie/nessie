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
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_USERNAME;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_AUTHORIZATION_CODE_FLOW_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_BACKGROUND_THREAD_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_REFRESH_SAFETY_WINDOW;
import static org.projectnessie.client.http.impl.HttpUtils.checkArgument;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;

@Value.Immutable
interface OAuth2ClientParams {

  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  Duration MIN_REFRESH_DELAY = Duration.ofSeconds(1);
  Duration MIN_IDLE_INTERVAL = Duration.ofSeconds(1);
  Duration MIN_AUTHORIZATION_CODE_FLOW_TIMEOUT = Duration.ofSeconds(10);

  URI getTokenEndpoint();

  Optional<URI> getAuthEndpoint();

  String getClientId();

  String getClientSecret();

  Optional<String> getUsername();

  Optional<String> getPassword();

  Optional<String> getScope();

  @Value.Default
  default GrantType getGrantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  @Value.Default
  default ObjectMapper getObjectMapper() {
    return OBJECT_MAPPER;
  }

  @Value.Default
  default Duration getDefaultAccessTokenLifespan() {
    return Duration.parse(DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN);
  }

  @Value.Default
  default Duration getDefaultRefreshTokenLifespan() {
    return Duration.parse(DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN);
  }

  @Value.Default
  default Duration getRefreshSafetyWindow() {
    return Duration.parse(DEFAULT_REFRESH_SAFETY_WINDOW);
  }

  @Value.Default
  default boolean getTokenExchangeEnabled() {
    return true;
  }

  @Value.Default
  default ScheduledExecutorService getExecutor() {
    return new OAuth2TokenRefreshExecutor(getBackgroundThreadIdleTimeout());
  }

  /**
   * For how long the OAuth2 client should keep the tokens fresh, if the client is not being
   * actively used.
   */
  @Value.Default
  default Duration getPreemptiveTokenRefreshIdleTimeout() {
    return Duration.parse(DEFAULT_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT);
  }

  /**
   * The maximum time a background thread can be idle before it is closed. Only relevant when using
   * the default {@link #getExecutor() executor}.
   */
  @Value.Default
  default Duration getBackgroundThreadIdleTimeout() {
    return Duration.parse(DEFAULT_BACKGROUND_THREAD_IDLE_TIMEOUT);
  }

  @Value.Default
  default HttpClient.Builder getHttpClient() {
    // See https://www.rfc-editor.org/rfc/rfc6749#section-2.3.1: The
    // authorization server MUST support the HTTP Basic authentication scheme
    // for authenticating clients that were issued a client password.
    HttpAuthentication authentication =
        BasicAuthenticationProvider.create(getClientId(), getClientSecret());
    return HttpClient.builder()
        .setBaseUri(getTokenEndpoint().resolve("/"))
        .setObjectMapper(getObjectMapper())
        .setAuthentication(authentication)
        .setDisableCompression(true);
  }

  @Value.Default
  default Supplier<Instant> getClock() {
    return Clock.systemUTC()::instant;
  }

  @Value.Default
  default Duration getAuthorizationCodeFlowTimeout() {
    return Duration.parse(DEFAULT_AUTHORIZATION_CODE_FLOW_TIMEOUT);
  }

  OptionalInt getAuthorizationCodeFlowWebServerPort();

  @Value.Check
  default void check() {
    checkArgument(
        getTokenEndpoint().getQuery() == null, "Token endpoint must not have a query part");
    checkArgument(
        getTokenEndpoint().getFragment() == null, "Token endpoint must not have a fragment part");
    if (getAuthEndpoint().isPresent()) {
      checkArgument(
          getAuthEndpoint().get().getQuery() == null,
          "Authorization endpoint must not have a query part");
      checkArgument(
          getAuthEndpoint().get().getFragment() == null,
          "Authorization endpoint must not have a fragment part");
    }
    checkArgument(!getClientId().isEmpty(), "client ID must not be empty");
    checkArgument(!getClientSecret().isEmpty(), "client secret must not be empty");
    String grantType = getGrantType().canonicalName();
    checkArgument(
        grantType.equals(CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS)
            || grantType.equals(CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD)
            || grantType.equals(CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE),
        "grant type must be either '%s', '%s' or '%s'",
        CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS,
        CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD,
        CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE);
    if (grantType.equals(CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD)) {
      checkArgument(
          getUsername().isPresent() && !getUsername().get().isEmpty(),
          "username must be set if grant type is '%s'",
          CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD);
      checkArgument(
          getPassword().isPresent() && !getPassword().get().isEmpty(),
          "password must be set if grant type is '%s'",
          CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD);
    }
    if (grantType.equals(CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE)) {
      checkArgument(
          getAuthEndpoint().isPresent(),
          "authorization endpoint must be set if grant type is '%s'",
          CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE);
      if (getAuthorizationCodeFlowWebServerPort().isPresent()) {
        checkArgument(
            getAuthorizationCodeFlowWebServerPort().getAsInt() >= 0
                && getAuthorizationCodeFlowWebServerPort().getAsInt() <= 65535,
            "authorization code flow: web server port must be between 0 and 65535 (inclusive)");
      }
      checkArgument(
          getAuthorizationCodeFlowTimeout().compareTo(MIN_AUTHORIZATION_CODE_FLOW_TIMEOUT) >= 0,
          "authorization code flow: timeout must be greater than or equal to %s",
          MIN_AUTHORIZATION_CODE_FLOW_TIMEOUT);
    }
    checkArgument(
        getDefaultAccessTokenLifespan().compareTo(MIN_REFRESH_DELAY) >= 0,
        "default token lifespan must be greater than or equal to %s",
        MIN_REFRESH_DELAY);
    checkArgument(
        getRefreshSafetyWindow().compareTo(MIN_REFRESH_DELAY) >= 0,
        "refresh safety window must be greater than or equal to %s",
        MIN_REFRESH_DELAY);
    checkArgument(
        getRefreshSafetyWindow().compareTo(getDefaultAccessTokenLifespan()) < 0,
        "refresh safety window must be less than the default token lifespan");
    checkArgument(
        getPreemptiveTokenRefreshIdleTimeout().compareTo(MIN_IDLE_INTERVAL) >= 0,
        "preemptive token refresh idle timeout must be greater than or equal to %s",
        MIN_IDLE_INTERVAL);
  }

  static ImmutableOAuth2ClientParams.Builder builder() {
    return ImmutableOAuth2ClientParams.builder();
  }

  static OAuth2ClientParams fromConfig(Function<String, String> config) {
    Objects.requireNonNull(config, "config must not be null");
    return ImmutableOAuth2ClientParams.builder()
        .tokenEndpoint(
            URI.create(
                Objects.requireNonNull(
                    config.apply(CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT),
                    "token endpoint must not be null")))
        .authEndpoint(
            Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_AUTH_ENDPOINT)).map(URI::create))
        .clientId(
            Objects.requireNonNull(
                config.apply(CONF_NESSIE_OAUTH2_CLIENT_ID), "client ID must not be null"))
        .clientSecret(
            Objects.requireNonNull(
                config.apply(CONF_NESSIE_OAUTH2_CLIENT_SECRET), "client secret must not be null"))
        .username(Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_USERNAME)))
        .password(Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_PASSWORD)))
        .grantType(
            Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_GRANT_TYPE))
                .map(GrantType::fromCanonicalName)
                .orElse(GrantType.CLIENT_CREDENTIALS))
        .scope(Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_CLIENT_SCOPES)))
        .defaultAccessTokenLifespan(
            Duration.parse(
                Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN))
                    .orElse(DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN)))
        .defaultRefreshTokenLifespan(
            Duration.parse(
                Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_DEFAULT_REFRESH_TOKEN_LIFESPAN))
                    .orElse(DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN)))
        .refreshSafetyWindow(
            Duration.parse(
                Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW))
                    .orElse(DEFAULT_REFRESH_SAFETY_WINDOW)))
        .tokenExchangeEnabled(
            Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED))
                .map(Boolean::parseBoolean)
                .orElse(true))
        .preemptiveTokenRefreshIdleTimeout(
            Duration.parse(
                Optional.ofNullable(
                        config.apply(CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT))
                    .orElse(DEFAULT_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT)))
        .backgroundThreadIdleTimeout(
            Duration.parse(
                Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_BACKGROUND_THREAD_IDLE_TIMEOUT))
                    .orElse(DEFAULT_BACKGROUND_THREAD_IDLE_TIMEOUT)))
        .authorizationCodeFlowWebServerPort(
            Optional.ofNullable(config.apply(CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_WEB_PORT))
                .map(Integer::parseInt)
                .map(OptionalInt::of)
                .orElse(OptionalInt.empty()))
        .authorizationCodeFlowTimeout(
            Duration.parse(
                Optional.ofNullable(
                        config.apply(CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_TIMEOUT))
                    .orElse(DEFAULT_AUTHORIZATION_CODE_FLOW_TIMEOUT)))
        .build();
  }
}
