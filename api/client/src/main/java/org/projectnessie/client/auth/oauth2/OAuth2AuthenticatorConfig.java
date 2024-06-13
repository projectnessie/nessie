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
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEVICE_AUTH_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_POLL_INTERVAL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_ISSUER_URL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_USERNAME;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_AUTHORIZATION_CODE_FLOW_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_BACKGROUND_THREAD_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_DEVICE_CODE_FLOW_POLL_INTERVAL;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_DEVICE_CODE_FLOW_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_REFRESH_SAFETY_WINDOW;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientConfig.applyConfigOption;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import org.immutables.value.Value;
import org.projectnessie.client.NessieConfigConstants;

/** Configuration options for {@link OAuth2Authenticator}. */
@Value.Immutable(lazyhash = true)
public interface OAuth2AuthenticatorConfig {

  /**
   * Creates a new {@link OAuth2AuthenticatorConfig} from the given configuration supplier.
   *
   * @param config the configuration supplier
   * @return a new {@link OAuth2AuthenticatorConfig}
   * @throws NullPointerException if {@code config} is {@code null}, or a required configuration
   *     option is missing
   * @throws IllegalArgumentException if the configuration is otherwise invalid
   * @see NessieConfigConstants
   */
  static OAuth2AuthenticatorConfig fromConfigSupplier(Function<String, String> config) {
    Objects.requireNonNull(config, "config must not be null");
    String clientId = config.apply(CONF_NESSIE_OAUTH2_CLIENT_ID);
    String clientSecret = config.apply(CONF_NESSIE_OAUTH2_CLIENT_SECRET);
    OAuth2ClientConfig.Builder builder =
        OAuth2ClientConfig.builder()
            // No need to validate client ID+secret here, those are validated in
            // `OAuth2ClientConfig.check()`, which has a more human friendly validation.
            .clientId(clientId != null ? clientId : "")
            .clientSecret(clientSecret != null ? clientSecret : "");
    applyConfigOption(config, CONF_NESSIE_OAUTH2_ISSUER_URL, builder::issuerUrl, URI::create);
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT, builder::tokenEndpoint, URI::create);
    applyConfigOption(config, CONF_NESSIE_OAUTH2_AUTH_ENDPOINT, builder::authEndpoint, URI::create);
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_DEVICE_AUTH_ENDPOINT, builder::deviceAuthEndpoint, URI::create);
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_GRANT_TYPE, builder::grantType, GrantType::fromConfigName);
    applyConfigOption(config, CONF_NESSIE_OAUTH2_USERNAME, builder::username);
    applyConfigOption(config, CONF_NESSIE_OAUTH2_PASSWORD, builder::password);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_CLIENT_SCOPES,
        scope -> Arrays.stream(scope.split(" ")).forEach(builder::addScope));
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN,
        builder::defaultAccessTokenLifespan,
        Duration::parse);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_DEFAULT_REFRESH_TOKEN_LIFESPAN,
        builder::defaultRefreshTokenLifespan,
        Duration::parse);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW,
        builder::refreshSafetyWindow,
        Duration::parse);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT,
        builder::preemptiveTokenRefreshIdleTimeout,
        Duration::parse);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_BACKGROUND_THREAD_IDLE_TIMEOUT,
        builder::backgroundThreadIdleTimeout,
        Duration::parse);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_TIMEOUT,
        builder::authorizationCodeFlowTimeout,
        Duration::parse);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_WEB_PORT,
        builder::authorizationCodeFlowWebServerPort,
        Integer::parseInt);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_TIMEOUT,
        builder::deviceCodeFlowTimeout,
        Duration::parse);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_POLL_INTERVAL,
        builder::deviceCodeFlowPollInterval,
        Duration::parse);
    TokenExchangeConfig tokenExchangeConfig = TokenExchangeConfig.fromConfigSupplier(config);
    builder.tokenExchangeConfig(tokenExchangeConfig);
    return builder.build();
  }

  /**
   * The root URL of the OpenID Connect identity issuer provider, which will be used for discovering
   * supported endpoints and their locations.
   *
   * <p>Endpoint discovery is performed using the OpenID Connect Discovery metadata published by the
   * issuer. See <a href="https://openid.net/specs/openid-connect-discovery-1_0.html">OpenID Connect
   * Discovery 1.0</a> for more information.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_ISSUER_URL
   */
  Optional<URI> getIssuerUrl();

  /**
   * The OAuth2 token endpoint. Either this or {@link #getIssuerUrl()} must be set.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT
   */
  Optional<URI> getTokenEndpoint();

  /**
   * The OAuth2 authorization endpoint. Either this or {@link #getIssuerUrl()} must be set, if the
   * grant type is {@link GrantType#AUTHORIZATION_CODE}.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_AUTH_ENDPOINT
   */
  Optional<URI> getAuthEndpoint();

  /**
   * The OAuth2 device authorization endpoint. Either this or {@link #getIssuerUrl()} must be set,
   * if the grant type is {@link GrantType#DEVICE_CODE}.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_DEVICE_AUTH_ENDPOINT
   */
  Optional<URI> getDeviceAuthEndpoint();

  /**
   * The OAuth2 grant type. Defaults to {@link GrantType#CLIENT_CREDENTIALS}.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_GRANT_TYPE
   */
  @Value.Default
  default GrantType getGrantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  /**
   * The OAuth2 client ID. Must be set.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_CLIENT_ID
   */
  String getClientId();

  /**
   * The OAuth2 client secret. Must be set, if required by the IdP.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_CLIENT_SECRET
   */
  Optional<Secret> getClientSecret();

  /**
   * The OAuth2 username. Only relevant for {@link GrantType#PASSWORD} grant type.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_USERNAME
   */
  Optional<String> getUsername();

  /**
   * The OAuth2 password. Only relevant for {@link GrantType#PASSWORD} grant type.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_PASSWORD
   */
  Optional<Secret> getPassword();

  @Value.Derived
  @Deprecated
  default Optional<String> getScope() {
    return getScopes().stream().reduce((a, b) -> a + " " + b);
  }

  /**
   * The OAuth2 scopes. Optional.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_CLIENT_SCOPES
   */
  List<String> getScopes();

  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  @Value.Default
  default boolean getTokenExchangeEnabled() {
    return true;
  }

  /** The token exchange configuration. Optional. */
  @Value.Default
  default TokenExchangeConfig getTokenExchangeConfig() {
    return TokenExchangeConfig.DISABLED;
  }

  /**
   * The default access token lifespan. Optional, defaults to {@link
   * NessieConfigConstants#DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN}.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN
   */
  @Value.Default
  default Duration getDefaultAccessTokenLifespan() {
    return Duration.parse(DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN);
  }

  /**
   * The default refresh token lifespan. Optional, defaults to {@link
   * NessieConfigConstants#DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN}.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_DEFAULT_REFRESH_TOKEN_LIFESPAN
   */
  @Value.Default
  default Duration getDefaultRefreshTokenLifespan() {
    return Duration.parse(DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN);
  }

  /**
   * The refresh safety window. A new token will be fetched when the current token's remaining
   * lifespan is less than this value. Optional, defaults to {@link
   * NessieConfigConstants#DEFAULT_REFRESH_SAFETY_WINDOW}.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW
   */
  @Value.Default
  default Duration getRefreshSafetyWindow() {
    return Duration.parse(DEFAULT_REFRESH_SAFETY_WINDOW);
  }

  /**
   * For how long the OAuth2 client should keep the tokens fresh, if the client is not being
   * actively used. Defaults to {@link
   * NessieConfigConstants#DEFAULT_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT}.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT
   */
  @Value.Default
  default Duration getPreemptiveTokenRefreshIdleTimeout() {
    return Duration.parse(DEFAULT_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT);
  }

  /**
   * The maximum time a background thread can be idle before it is closed. Only relevant when using
   * the default {@link #getExecutor() executor}. Defaults to {@link
   * NessieConfigConstants#DEFAULT_BACKGROUND_THREAD_IDLE_TIMEOUT}.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_BACKGROUND_THREAD_IDLE_TIMEOUT
   */
  @Value.Default
  default Duration getBackgroundThreadIdleTimeout() {
    return Duration.parse(DEFAULT_BACKGROUND_THREAD_IDLE_TIMEOUT);
  }

  /**
   * How long to wait for an authorization code. Defaults to {@link
   * NessieConfigConstants#DEFAULT_AUTHORIZATION_CODE_FLOW_TIMEOUT}. Only relevant when using the
   * {@link GrantType#AUTHORIZATION_CODE} grant type.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_TIMEOUT
   */
  @Value.Default
  default Duration getAuthorizationCodeFlowTimeout() {
    return Duration.parse(DEFAULT_AUTHORIZATION_CODE_FLOW_TIMEOUT);
  }

  /**
   * The port to use for the local web server that listens for the authorization code.
   *
   * <p>When running a client inside a container make sure to specify a port and forward the port to
   * the container host.
   *
   * <p>If not set or set to zero, a random port from the dynamic client port range will be used.
   * Only relevant when using the {@link GrantType#AUTHORIZATION_CODE} grant type.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_WEB_PORT
   */
  OptionalInt getAuthorizationCodeFlowWebServerPort();

  /**
   * How long to wait for the device code flow to complete. Defaults to {@link
   * NessieConfigConstants#DEFAULT_DEVICE_CODE_FLOW_TIMEOUT}. Only relevant when using the {@link
   * GrantType#DEVICE_CODE} grant type.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_TIMEOUT
   */
  @Value.Default
  default Duration getDeviceCodeFlowTimeout() {
    return Duration.parse(DEFAULT_DEVICE_CODE_FLOW_TIMEOUT);
  }

  /**
   * How often to poll the token endpoint. Defaults to {@link
   * NessieConfigConstants#DEFAULT_DEVICE_CODE_FLOW_POLL_INTERVAL}. Only relevant when using the
   * {@link GrantType#DEVICE_CODE} grant type.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_POLL_INTERVAL
   */
  @Value.Default
  default Duration getDeviceCodeFlowPollInterval() {
    return Duration.parse(DEFAULT_DEVICE_CODE_FLOW_POLL_INTERVAL);
  }

  /**
   * The SSL context to use for HTTPS connections to the authentication provider, if the server uses
   * a self-signed certificate or a certificate signed by a CA that is not in the default trust
   * store of the JVM. Optional; if not set, the default SSL context is used.
   */
  Optional<SSLContext> getSslContext();

  /**
   * The {@link ObjectMapper} to use for JSON serialization and deserialization. Defaults to a
   * vanilla instance.
   */
  @Value.Default
  default ObjectMapper getObjectMapper() {
    return OAuth2ClientConfig.OBJECT_MAPPER;
  }

  /**
   * The executor to use for background tasks such as refreshing tokens. Defaults to a thread pool
   * with daemon threads, and a single thread initially. The pool will grow as needed and can also
   * shrink to zero threads if no activity is detected for {@link
   * #getBackgroundThreadIdleTimeout()}.
   */
  Optional<ScheduledExecutorService> getExecutor();

  static OAuth2AuthenticatorConfig.Builder builder() {
    return ImmutableOAuth2AuthenticatorConfig.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(OAuth2AuthenticatorConfig config);

    @CanIgnoreReturnValue
    Builder issuerUrl(URI issuerUrl);

    @CanIgnoreReturnValue
    Builder tokenEndpoint(URI tokenEndpoint);

    @CanIgnoreReturnValue
    Builder authEndpoint(URI authEndpoint);

    @CanIgnoreReturnValue
    Builder deviceAuthEndpoint(URI deviceAuthEndpoint);

    @CanIgnoreReturnValue
    Builder grantType(GrantType grantType);

    @CanIgnoreReturnValue
    Builder clientId(String clientId);

    @CanIgnoreReturnValue
    Builder clientSecret(Secret clientSecret);

    default Builder clientSecret(String clientSecret) {
      return clientSecret(new Secret(clientSecret));
    }

    @CanIgnoreReturnValue
    Builder username(String username);

    @CanIgnoreReturnValue
    Builder password(Secret password);

    default Builder password(String password) {
      return password(new Secret(password));
    }

    @CanIgnoreReturnValue
    @Deprecated
    default Builder scope(String scope) {
      Arrays.stream(scope.split(" ")).forEach(this::addScope);
      return this;
    }

    @CanIgnoreReturnValue
    Builder addScope(String scope);

    @CanIgnoreReturnValue
    Builder addScopes(String... scopes);

    @CanIgnoreReturnValue
    Builder scopes(Iterable<String> scopes);

    @Deprecated
    @CanIgnoreReturnValue
    Builder tokenExchangeEnabled(boolean tokenExchangeEnabled);

    @CanIgnoreReturnValue
    Builder tokenExchangeConfig(TokenExchangeConfig tokenExchangeConfig);

    @CanIgnoreReturnValue
    Builder defaultAccessTokenLifespan(Duration defaultAccessTokenLifespan);

    @CanIgnoreReturnValue
    Builder defaultRefreshTokenLifespan(Duration defaultRefreshTokenLifespan);

    @CanIgnoreReturnValue
    Builder refreshSafetyWindow(Duration refreshSafetyWindow);

    @CanIgnoreReturnValue
    Builder preemptiveTokenRefreshIdleTimeout(Duration preemptiveTokenRefreshIdleTimeout);

    @CanIgnoreReturnValue
    Builder backgroundThreadIdleTimeout(Duration backgroundThreadIdleTimeout);

    @CanIgnoreReturnValue
    Builder authorizationCodeFlowTimeout(Duration authorizationCodeFlowTimeout);

    @CanIgnoreReturnValue
    Builder authorizationCodeFlowWebServerPort(int authorizationCodeFlowWebServerPort);

    @CanIgnoreReturnValue
    Builder deviceCodeFlowTimeout(Duration deviceCodeFlowTimeout);

    @CanIgnoreReturnValue
    Builder deviceCodeFlowPollInterval(Duration deviceCodeFlowPollInterval);

    @CanIgnoreReturnValue
    Builder sslContext(SSLContext sslContext);

    @CanIgnoreReturnValue
    Builder objectMapper(ObjectMapper objectMapper);

    @CanIgnoreReturnValue
    Builder executor(ScheduledExecutorService executor);

    OAuth2AuthenticatorConfig build();
  }
}
