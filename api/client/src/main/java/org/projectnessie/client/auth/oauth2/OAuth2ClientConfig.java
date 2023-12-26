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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD;
import static org.projectnessie.client.http.impl.HttpUtils.checkArgument;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.immutables.value.Value;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;

/**
 * Subtype of {@link OAuth2AuthenticatorConfig} that contains configuration options that are not
 * exposed to the user.
 */
@Value.Immutable
@SuppressWarnings("immutables:subtype")
abstract class OAuth2ClientConfig implements OAuth2AuthenticatorConfig {

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static final Duration MIN_REFRESH_DELAY = Duration.ofSeconds(1);

  static final Duration MIN_IDLE_INTERVAL = Duration.ofSeconds(1);

  private static final Duration MIN_AUTHORIZATION_CODE_FLOW_TIMEOUT = Duration.ofSeconds(10);

  static OAuth2ClientConfig.Builder builder() {
    return ImmutableOAuth2ClientConfig.builder();
  }

  @Value.NonAttribute
  byte[] getAndClearClientSecret() {
    return OAuth2Utils.getArrayAndClear(getClientSecret());
  }

  @Value.NonAttribute
  byte[] getAndClearPassword() {
    return getPassword().map(OAuth2Utils::getArrayAndClear).orElse(null);
  }

  @Value.Default
  Supplier<Instant> getClock() {
    return Clock.systemUTC()::instant;
  }

  @Value.Lazy
  JsonNode getOpenIdProviderMetadata() {
    URI issuerUrl = getIssuerUrl().orElseThrow(IllegalStateException::new);
    try (HttpClient client = newHttpClientBuilder().setBaseUri(issuerUrl).build()) {
      return OAuth2Utils.fetchOpenIdProviderMetadata(client);
    }
  }

  @Value.Lazy
  URI getResolvedTokenEndpoint() {
    if (getTokenEndpoint().isPresent()) {
      return getTokenEndpoint().get();
    }
    JsonNode json = getOpenIdProviderMetadata();
    if (json.has("token_endpoint")) {
      return URI.create(json.get("token_endpoint").asText());
    }
    throw new IllegalStateException("Discovery data does not contain a token endpoint");
  }

  @Value.Lazy
  URI getResolvedAuthEndpoint() {
    if (getAuthEndpoint().isPresent()) {
      return getAuthEndpoint().get();
    }
    JsonNode json = getOpenIdProviderMetadata();
    if (json.has("authorization_endpoint")) {
      return URI.create(json.get("authorization_endpoint").asText());
    }
    throw new IllegalStateException("Discovery data does not contain an authorization endpoint");
  }

  @Value.Lazy // required because it will consume the client secret
  HttpAuthentication getBasicAuthentication() {
    byte[] clientSecret = getAndClearClientSecret();
    return BasicAuthenticationProvider.create(
        getClientId(), new String(clientSecret, StandardCharsets.UTF_8));
  }

  @Value.NonAttribute
  HttpClient.Builder newHttpClientBuilder() {
    return HttpClient.builder()
        .setObjectMapper(getObjectMapper())
        .setSslContext(getSslContext().orElse(null))
        .setDisableCompression(true);
  }

  @Value.Check
  void check() {
    checkArgument(
        getIssuerUrl().isPresent() || getTokenEndpoint().isPresent(),
        "either issuer URL or token endpoint must be set");
    if (getTokenEndpoint().isPresent()) {
      checkArgument(
          getTokenEndpoint().get().getQuery() == null, "Token endpoint must not have a query part");
      checkArgument(
          getTokenEndpoint().get().getFragment() == null,
          "Token endpoint must not have a fragment part");
    }
    if (getAuthEndpoint().isPresent()) {
      checkArgument(
          getAuthEndpoint().get().getQuery() == null,
          "Authorization endpoint must not have a query part");
      checkArgument(
          getAuthEndpoint().get().getFragment() == null,
          "Authorization endpoint must not have a fragment part");
    }
    checkArgument(!getClientId().isEmpty(), "client ID must not be empty");
    checkArgument(getClientSecret().remaining() > 0, "client secret must not be empty");
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
          getPassword().isPresent() && getPassword().get().remaining() > 0,
          "password must be set if grant type is '%s'",
          CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD);
    }
    if (grantType.equals(CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE)) {
      checkArgument(
          getIssuerUrl().isPresent() || getAuthEndpoint().isPresent(),
          "either issuer URL or authorization endpoint must be set if grant type is '%s'",
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

  static void applyConfigOption(
      Function<String, String> config,
      String option,
      Function<String, OAuth2AuthenticatorConfig.Builder> setter) {
    applyConfigOption(config, option, setter, Function.identity());
  }

  static <T> void applyConfigOption(
      Function<String, String> config,
      String option,
      Function<T, OAuth2AuthenticatorConfig.Builder> setter,
      Function<String, T> converter) {
    String s = config.apply(option);
    if (s != null) {
      setter.apply(converter.apply(s));
    }
  }

  interface Builder extends OAuth2AuthenticatorConfig.Builder {

    @CanIgnoreReturnValue
    Builder from(OAuth2AuthenticatorConfig config);

    @Override
    Builder issuerUrl(URI issuerUrl);

    @Override
    Builder tokenEndpoint(URI tokenEndpoint);

    @Override
    Builder authEndpoint(URI authEndpoint);

    @Override
    Builder grantType(GrantType grantType);

    @Override
    Builder clientId(String clientId);

    @Override
    Builder clientSecret(ByteBuffer clientSecret);

    @Override
    default Builder clientSecret(String clientSecret) {
      return clientSecret(ByteBuffer.wrap(clientSecret.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    Builder username(String username);

    @Override
    Builder password(ByteBuffer password);

    default Builder password(String password) {
      return password(ByteBuffer.wrap(password.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    Builder scope(String scope);

    @Override
    Builder tokenExchangeEnabled(boolean tokenExchangeEnabled);

    @Override
    Builder defaultAccessTokenLifespan(Duration defaultAccessTokenLifespan);

    @Override
    Builder defaultRefreshTokenLifespan(Duration defaultRefreshTokenLifespan);

    @Override
    Builder refreshSafetyWindow(Duration refreshSafetyWindow);

    @Override
    Builder preemptiveTokenRefreshIdleTimeout(Duration preemptiveTokenRefreshIdleTimeout);

    @Override
    Builder backgroundThreadIdleTimeout(Duration backgroundThreadIdleTimeout);

    @Override
    Builder authorizationCodeFlowTimeout(Duration authorizationCodeFlowTimeout);

    @Override
    Builder authorizationCodeFlowWebServerPort(int authorizationCodeFlowWebServerPort);

    @Override
    Builder sslContext(SSLContext sslContext);

    @Override
    Builder objectMapper(ObjectMapper objectMapper);

    @Override
    Builder executor(ScheduledExecutorService executor);

    @CanIgnoreReturnValue
    Builder clock(Supplier<Instant> clock);

    OAuth2ClientConfig build();
  }
}
