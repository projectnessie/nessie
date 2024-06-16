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

import static java.lang.String.format;
import static java.lang.String.join;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_WEB_PORT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_AUTH_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_BACKGROUND_THREAD_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_POLL_INTERVAL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_DEVICE_CODE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_ISSUER_URL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_USERNAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.immutables.value.Value;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;

/**
 * Subtype of {@link OAuth2AuthenticatorConfig} that contains configuration options that are not
 * exposed to the user. Most of the configuration options are defaults and/or guardrails, and their
 * values can only be changed during tests.
 */
@Value.Immutable
@SuppressWarnings("immutables:subtype")
abstract class OAuth2ClientConfig implements OAuth2AuthenticatorConfig {

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static OAuth2ClientConfig.Builder builder() {
    return ImmutableOAuth2ClientConfig.builder();
  }

  @Value.Derived
  String getClientName() {
    return "nessie-oauth2-client-" + OAuth2Utils.randomAlphaNumString(4);
  }

  @Value.Default
  Duration getMinDefaultAccessTokenLifespan() {
    return Duration.ofSeconds(10);
  }

  @Value.Default
  Duration getMinRefreshSafetyWindow() {
    return Duration.ofSeconds(1);
  }

  @Value.Default
  Duration getMinPreemptiveTokenRefreshIdleTimeout() {
    return Duration.ofSeconds(1);
  }

  @Value.Default
  Duration getMinAuthorizationCodeFlowTimeout() {
    return Duration.ofSeconds(30);
  }

  @Value.Default
  Duration getMinDeviceCodeFlowTimeout() {
    return Duration.ofSeconds(30);
  }

  @Value.Default
  Duration getMinDeviceCodeFlowPollInterval() {
    return Duration.ofSeconds(5); // mandated by the specs
  }

  @Value.Default
  public boolean ignoreDeviceCodeFlowServerPollInterval() {
    return false;
  }

  @Value.Default
  PrintStream getConsole() {
    return System.out;
  }

  @Value.Default
  Supplier<Instant> getClock() {
    return Clock.systemUTC()::instant;
  }

  String getClientIdForTokenExchange() {
    if (!getTokenExchangeConfig().isEnabled()
        || !getTokenExchangeConfig().getClientId().isPresent()) {
      return getClientId();
    }
    return getTokenExchangeConfig().getClientId().get();
  }

  @Value.Derived
  boolean isPublicClient() {
    return !getClientSecret().isPresent();
  }

  boolean isPublicClientForTokenExchange() {
    if (!getTokenExchangeConfig().isEnabled()
        || !getTokenExchangeConfig().getClientId().isPresent()) {
      return isPublicClient();
    }
    return !getTokenExchangeConfig().getClientSecret().isPresent();
  }

  @Value.Lazy
  JsonNode getOpenIdProviderMetadata() {
    URI issuerUrl = getIssuerUrl().orElseThrow(IllegalStateException::new);
    return OAuth2Utils.fetchOpenIdProviderMetadata(getHttpClient(), issuerUrl);
  }

  @Value.Lazy
  JsonNode getOpenIdProviderMetadataForTokenExchange() {
    if (!getTokenExchangeConfig().isEnabled()
        || !getTokenExchangeConfig().getIssuerUrl().isPresent()) {
      return getOpenIdProviderMetadata();
    }
    URI issuerUrl = getTokenExchangeConfig().getIssuerUrl().get();
    return OAuth2Utils.fetchOpenIdProviderMetadata(getHttpClient(), issuerUrl);
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
    throw new IllegalStateException("OpenID provider metadata does not contain a token endpoint");
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
    throw new IllegalStateException(
        "OpenID provider metadata does not contain an authorization endpoint");
  }

  @Value.Lazy
  URI getResolvedDeviceAuthEndpoint() {
    if (getDeviceAuthEndpoint().isPresent()) {
      return getDeviceAuthEndpoint().get();
    }
    JsonNode json = getOpenIdProviderMetadata();
    if (json.has("device_authorization_endpoint")) {
      return URI.create(json.get("device_authorization_endpoint").asText());
    }
    throw new IllegalStateException(
        "OpenID provider metadata does not contain a device authorization endpoint");
  }

  @Value.Lazy
  URI getResolvedTokenEndpointForTokenExchange() {
    if (!getTokenExchangeConfig().isEnabled()
        || (!getTokenExchangeConfig().getIssuerUrl().isPresent()
            && !getTokenExchangeConfig().getTokenEndpoint().isPresent())) {
      return getResolvedTokenEndpoint();
    }
    if (getTokenExchangeConfig().getTokenEndpoint().isPresent()) {
      return getTokenExchangeConfig().getTokenEndpoint().get();
    }
    JsonNode json = getOpenIdProviderMetadataForTokenExchange();
    if (json.has("token_endpoint")) {
      return URI.create(json.get("token_endpoint").asText());
    }
    throw new IllegalStateException("OpenID provider metadata does not contain a token endpoint");
  }

  @Value.Lazy
  List<String> getScopesForTokenExchange() {
    if (!getTokenExchangeConfig().isEnabled()) {
      return getScopes();
    }
    List<String> scopes = getTokenExchangeConfig().getScopes();
    if (scopes.equals(TokenExchangeConfig.SCOPES_INHERIT)) {
      return getScopes();
    }
    return scopes;
  }

  /**
   * Returns the BASIC {@link HttpAuthentication} that will be used to authenticate with the OAuth2
   * server, for all endpoints that require such authentication.
   *
   * <p>The value is lazily computed then memoized; this is required because creating the {@link
   * HttpAuthentication} object will consume the client secret. It can be safely reused for all
   * requests since it's immutable and its close method is a no-op.
   */
  @Value.Lazy
  Optional<HttpAuthentication> getBasicAuthentication() {
    return getClientSecret()
        .map(s -> BasicAuthenticationProvider.create(getClientId(), s.getString()));
  }

  @Value.Lazy
  Optional<HttpAuthentication> getBasicAuthenticationForTokenExchange() {
    if (!getTokenExchangeConfig().isEnabled()
        || !getTokenExchangeConfig().getClientId().isPresent()) {
      return getBasicAuthentication();
    }
    return getTokenExchangeConfig()
        .getClientSecret()
        .map(
            s ->
                BasicAuthenticationProvider.create(
                    getTokenExchangeConfig().getClientId().get(), s.getString()));
  }

  /**
   * Returns the {@link HttpClient} that will be used to communicate with the OAuth2 server.
   *
   * <p>Note that it does not have any authentication configured, so each request must be
   * authenticated explicitly. The appropriate authentication object can be obtained from {@link
   * #getBasicAuthentication()}.
   */
  @Value.Lazy
  HttpClient getHttpClient() {
    return HttpClient.builder()
        .setObjectMapper(getObjectMapper())
        .setSslContext(getSslContext().orElse(null))
        .setDisableCompression(true)
        .addResponseFilter(this::checkErrorResponse)
        .build();
  }

  private void checkErrorResponse(ResponseContext responseContext) {
    try {
      Status status = responseContext.getResponseCode();
      if (status.getCode() >= 400) {
        if (!responseContext.isJsonCompatibleResponse()) {
          throw genericError(status);
        }
        InputStream is = responseContext.getErrorStream();
        if (is != null) {
          try {
            ErrorResponse errorResponse = getObjectMapper().readValue(is, ErrorResponse.class);
            throw new OAuth2Exception(status, errorResponse);
          } catch (IOException ignored) {
            throw genericError(status);
          }
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new HttpClientException(e);
    }
  }

  private static HttpClientException genericError(Status status) {
    return new HttpClientException(
        "OAuth2 server replied with HTTP status code: " + status.getCode());
  }

  private static void check(
      List<String> violations, String paramKey, boolean cond, String msg, Object... args) {
    if (!cond) {
      if (args.length == 0) {
        violations.add(msg + " (" + paramKey + ")");
      } else {
        violations.add(format(msg, args) + " (" + paramKey + ")");
      }
    }
  }

  @Value.Check
  void check() {
    List<String> violations = new ArrayList<>();

    check(
        violations,
        CONF_NESSIE_OAUTH2_CLIENT_ID,
        !getClientId().isEmpty(),
        "client ID must not be empty");
    check(
        violations,
        CONF_NESSIE_OAUTH2_GRANT_TYPE + " / " + CONF_NESSIE_OAUTH2_CLIENT_SECRET,
        getClientSecret().isPresent() || getGrantType() != GrantType.CLIENT_CREDENTIALS,
        "client secret must not be empty when grant type is '%s'",
        CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS);
    check(
        violations,
        CONF_NESSIE_OAUTH2_ISSUER_URL + " / " + CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT,
        getIssuerUrl().isPresent() || getTokenEndpoint().isPresent(),
        "either issuer URL or token endpoint must be set");
    if (getTokenEndpoint().isPresent()) {
      check(
          violations,
          CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT,
          getTokenEndpoint().get().getQuery() == null,
          "Token endpoint must not have a query part");
      check(
          violations,
          CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT,
          getTokenEndpoint().get().getFragment() == null,
          "Token endpoint must not have a fragment part");
    }
    if (getAuthEndpoint().isPresent()) {
      check(
          violations,
          CONF_NESSIE_OAUTH2_AUTH_ENDPOINT,
          getAuthEndpoint().get().getQuery() == null,
          "Authorization endpoint must not have a query part");
      check(
          violations,
          CONF_NESSIE_OAUTH2_AUTH_ENDPOINT,
          getAuthEndpoint().get().getFragment() == null,
          "Authorization endpoint must not have a fragment part");
    }
    GrantType grantType = getGrantType();
    check(
        violations,
        CONF_NESSIE_OAUTH2_GRANT_TYPE,
        grantType.isInitial(),
        "grant type must be either '%s', '%s', '%s' or '%s'",
        CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS,
        CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD,
        CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE,
        CONF_NESSIE_OAUTH2_GRANT_TYPE_DEVICE_CODE);
    if (grantType == GrantType.PASSWORD) {
      check(
          violations,
          CONF_NESSIE_OAUTH2_USERNAME,
          getUsername().isPresent() && !getUsername().get().isEmpty(),
          "username must be set if grant type is '%s'",
          CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD);
      check(
          violations,
          CONF_NESSIE_OAUTH2_PASSWORD,
          getPassword().isPresent() && getPassword().get().isNotEmpty(),
          "password must be set if grant type is '%s'",
          CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD);
    }
    if (grantType == GrantType.AUTHORIZATION_CODE) {
      check(
          violations,
          CONF_NESSIE_OAUTH2_ISSUER_URL + " / " + CONF_NESSIE_OAUTH2_AUTH_ENDPOINT,
          getIssuerUrl().isPresent() || getAuthEndpoint().isPresent(),
          "either issuer URL or authorization endpoint must be set if grant type is '%s'",
          CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE);
      if (getAuthorizationCodeFlowWebServerPort().isPresent()) {
        check(
            violations,
            CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_WEB_PORT,
            getAuthorizationCodeFlowWebServerPort().getAsInt() >= 0
                && getAuthorizationCodeFlowWebServerPort().getAsInt() <= 65535,
            "authorization code flow: web server port must be between 0 and 65535 (inclusive)");
      }
      check(
          violations,
          CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_TIMEOUT,
          getAuthorizationCodeFlowTimeout().compareTo(getMinAuthorizationCodeFlowTimeout()) >= 0,
          "authorization code flow: timeout must be greater than or equal to %s",
          getMinAuthorizationCodeFlowTimeout());
    }
    if (grantType == GrantType.DEVICE_CODE) {
      check(
          violations,
          CONF_NESSIE_OAUTH2_ISSUER_URL + " / " + CONF_NESSIE_OAUTH2_AUTH_ENDPOINT,
          getIssuerUrl().isPresent() || getDeviceAuthEndpoint().isPresent(),
          "either issuer URL or device authorization endpoint must be set if grant type is '%s'",
          CONF_NESSIE_OAUTH2_GRANT_TYPE_DEVICE_CODE);
      check(
          violations,
          CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_POLL_INTERVAL,
          getDeviceCodeFlowPollInterval().compareTo(getMinDeviceCodeFlowPollInterval()) >= 0,
          "device code flow: poll interval must be greater than or equal to %s",
          getMinDeviceCodeFlowPollInterval());
      check(
          violations,
          CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_TIMEOUT,
          getDeviceCodeFlowTimeout().compareTo(getMinDeviceCodeFlowTimeout()) >= 0,
          "device code flow: timeout must be greater than or equal to %s",
          getMinDeviceCodeFlowTimeout());
    }
    check(
        violations,
        CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN,
        getDefaultAccessTokenLifespan().compareTo(getMinDefaultAccessTokenLifespan()) >= 0,
        "default token lifespan must be greater than or equal to %s",
        getMinDefaultAccessTokenLifespan());
    check(
        violations,
        CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW,
        getRefreshSafetyWindow().compareTo(getMinRefreshSafetyWindow()) >= 0,
        "refresh safety window must be greater than or equal to %s",
        getMinRefreshSafetyWindow());
    check(
        violations,
        CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW
            + "/"
            + CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN,
        getRefreshSafetyWindow().compareTo(getDefaultAccessTokenLifespan()) < 0,
        "refresh safety window must be less than the default token lifespan");
    check(
        violations,
        CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT,
        getPreemptiveTokenRefreshIdleTimeout().compareTo(getMinPreemptiveTokenRefreshIdleTimeout())
            >= 0,
        "preemptive token refresh idle timeout must be greater than or equal to %s",
        getMinPreemptiveTokenRefreshIdleTimeout());
    check(
        violations,
        CONF_NESSIE_OAUTH2_BACKGROUND_THREAD_IDLE_TIMEOUT,
        getBackgroundThreadIdleTimeout().compareTo(Duration.ZERO) > 0,
        "background thread idle timeout must be greater than zero");

    if (!violations.isEmpty()) {
      throw new IllegalArgumentException(
          "OAuth2 authentication is missing some parameters and could not be initialized: "
              + join(", ", violations));
    }
  }

  static void applyConfigOption(
      Function<String, String> config, String option, Consumer<String> setter) {
    applyConfigOption(config, option, setter, Function.identity());
  }

  static <T> void applyConfigOption(
      Function<String, String> config,
      String option,
      Consumer<T> setter,
      Function<String, T> converter) {
    String s = config.apply(option);
    if (s != null) {
      setter.accept(converter.apply(s));
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
    Builder deviceAuthEndpoint(URI deviceAuthEndpoint);

    @Override
    Builder grantType(GrantType grantType);

    @Override
    Builder clientId(String clientId);

    @Override
    Builder clientSecret(Secret clientSecret);

    default Builder clientSecret(String clientSecret) {
      return clientSecret(new Secret(clientSecret));
    }

    @Override
    Builder username(String username);

    @Override
    Builder password(Secret password);

    default Builder password(String password) {
      return password(new Secret(password));
    }

    @Override
    Builder addScope(String scope);

    @Override
    Builder addScopes(String... scopes);

    @Override
    Builder scopes(Iterable<String> scopes);

    @Override
    Builder tokenExchangeConfig(TokenExchangeConfig tokenExchangeConfig);

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
    Builder deviceCodeFlowTimeout(Duration deviceCodeFlowTimeout);

    @Override
    Builder deviceCodeFlowPollInterval(Duration deviceCodeFlowPollInterval);

    @Override
    Builder sslContext(SSLContext sslContext);

    @Override
    Builder objectMapper(ObjectMapper objectMapper);

    @Override
    Builder executor(ScheduledExecutorService executor);

    @CanIgnoreReturnValue
    Builder minDefaultAccessTokenLifespan(Duration minDefaultAccessTokenLifespan);

    @CanIgnoreReturnValue
    Builder minRefreshSafetyWindow(Duration minRefreshSafetyWindow);

    @CanIgnoreReturnValue
    Builder minPreemptiveTokenRefreshIdleTimeout(Duration minPreemptiveTokenRefreshIdleTimeout);

    @CanIgnoreReturnValue
    Builder minAuthorizationCodeFlowTimeout(Duration minAuthorizationCodeFlowTimeout);

    @CanIgnoreReturnValue
    Builder minDeviceCodeFlowTimeout(Duration minDeviceCodeFlowTimeout);

    @CanIgnoreReturnValue
    Builder minDeviceCodeFlowPollInterval(Duration minDeviceCodeFlowPollInterval);

    @CanIgnoreReturnValue
    Builder ignoreDeviceCodeFlowServerPollInterval(boolean ignoreDeviceCodeFlowServerPollInterval);

    @CanIgnoreReturnValue
    Builder console(PrintStream console);

    @CanIgnoreReturnValue
    Builder clock(Supplier<Instant> clock);

    OAuth2ClientConfig build();
  }
}
