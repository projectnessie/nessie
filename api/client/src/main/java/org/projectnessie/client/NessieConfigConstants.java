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
package org.projectnessie.client;

import org.projectnessie.client.auth.NessieAuthenticationProvider;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPageGroup;

/** Configuration constants for Nessie clients. */
@SuppressWarnings("JavadocDeclaration")
@ConfigPageGroup(name = "client-config")
public final class NessieConfigConstants {
  /** Config property name ({@value #CONF_NESSIE_URI}) for the Nessie service URL. */
  @ConfigItem public static final String CONF_NESSIE_URI = "nessie.uri";

  /**
   * Username used for the insecure {@code BASIC} authentication.
   *
   * @deprecated "basic" HTTP authentication is not considered secure. Use {@link
   *     #CONF_NESSIE_AUTH_TOKEN} instead.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  @ConfigItem(section = "Basic Authentication")
  public static final String CONF_NESSIE_USERNAME = "nessie.authentication.username";

  /**
   * Password used for the insecure {@code BASIC} authentication.
   *
   * @deprecated "basic" HTTP authentication is not considered secure. Use {@link
   *     #CONF_NESSIE_AUTH_TOKEN} instead.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  @ConfigItem(section = "Basic Authentication")
  public static final String CONF_NESSIE_PASSWORD = "nessie.authentication.password";

  /** Token used for {@code BEARER} authentication. */
  @ConfigItem(section = "Bearer Authentication")
  public static final String CONF_NESSIE_AUTH_TOKEN = "nessie.authentication.token";

  /**
   * OAuth2 issuer URL.
   *
   * <p>The root URL of the OpenID Connect identity issuer provider, which will be used for
   * discovering supported endpoints and their locations. For Keycloak, this is typically the realm
   * URL: {@code https://<keycloak-server>/realms/<realm-name>}.
   *
   * <p>Endpoint discovery is performed using the OpenID Connect Discovery metadata published by the
   * issuer. See <a href="https://openid.net/specs/openid-connect-discovery-1_0.html">OpenID Connect
   * Discovery 1.0</a> for more information.
   *
   * <p>Either this property or {@link #CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT} must be set.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_ISSUER_URL =
      "nessie.authentication.oauth2.issuer-url";

  /**
   * URL of the OAuth2 token endpoint. For Keycloak, this is typically {@code
   * https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/token}.
   *
   * <p>Either this property or {@link #CONF_NESSIE_OAUTH2_ISSUER_URL} must be set. In case it is
   * not set, the token endpoint will be discovered from the {@link #CONF_NESSIE_OAUTH2_ISSUER_URL
   * issuer URL}, using the OpenID Connect Discovery metadata published by the issuer.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT =
      "nessie.authentication.oauth2.token-endpoint";

  /**
   * URL of the OAuth2 authorization endpoint. For Keycloak, this is typically {@code
   * https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/auth}.
   *
   * <p>If using the "authorization_code" grant type, either this property or {@link
   * #CONF_NESSIE_OAUTH2_ISSUER_URL} must be set. In case it is not set, the authorization endpoint
   * will be discovered from the {@link #CONF_NESSIE_OAUTH2_ISSUER_URL issuer URL}, using the OpenID
   * Connect Discovery metadata published by the issuer.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_AUTH_ENDPOINT =
      "nessie.authentication.oauth2.auth-endpoint";

  /**
   * URL of the OAuth2 device authorization endpoint. For Keycloak, this is typically {@code
   * http://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/auth/device}.
   *
   * <p>If using the "Device Code" grant type, either this property or {@link
   * #CONF_NESSIE_OAUTH2_ISSUER_URL} must be set.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_DEVICE_AUTH_ENDPOINT =
      "nessie.authentication.oauth2.device-auth-endpoint";

  public static final String CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS =
      "client_credentials";

  public static final String CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD = "password";

  public static final String CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE =
      "authorization_code";

  public static final String CONF_NESSIE_OAUTH2_GRANT_TYPE_DEVICE_CODE = "device_code";

  /**
   * The grant type to use when authenticating against the OAuth2 server. Valid values are:
   *
   * <ul>
   *   <li>{@value #CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS}
   *   <li>{@value #CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD}
   *   <li>{@value #CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE}
   *   <li>{@value #CONF_NESSIE_OAUTH2_GRANT_TYPE_DEVICE_CODE}
   * </ul>
   *
   * Optional, defaults to {@value #CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS}.
   *
   * <p>Depending on the grant type, different properties must be provided.
   *
   * <p>For the "client_credentials" grant type, the following properties must be provided:
   *
   * <ul>
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT token endpoint} or {@linkplain
   *       #CONF_NESSIE_OAUTH2_ISSUER_URL issuer URL}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_CLIENT_ID client ID}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_CLIENT_SECRET client secret} (if required)
   * </ul>
   *
   * <p>For the "password" grant type, the following properties must be provided:
   *
   * <ul>
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT token endpoint} or {@linkplain
   *       #CONF_NESSIE_OAUTH2_ISSUER_URL issuer URL}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_CLIENT_ID client ID}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_CLIENT_SECRET client secret} (if required)
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_USERNAME username}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_PASSWORD password}
   * </ul>
   *
   * <p>For the "authorization_code" grant type, the following properties must be provided:
   *
   * <ul>
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT token endpoint} or {@linkplain
   *       #CONF_NESSIE_OAUTH2_ISSUER_URL issuer URL}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_AUTH_ENDPOINT authorization endpoint} or {@linkplain
   *       #CONF_NESSIE_OAUTH2_ISSUER_URL issuer URL}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_CLIENT_ID client ID}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_CLIENT_SECRET client secret} (if required)
   * </ul>
   *
   * <p>For the "device_code" grant type, the following properties must be provided:
   *
   * <ul>
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT token endpoint} or {@linkplain
   *       #CONF_NESSIE_OAUTH2_ISSUER_URL issuer URL}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_DEVICE_AUTH_ENDPOINT device authorization endpoint} or
   *       {@linkplain #CONF_NESSIE_OAUTH2_ISSUER_URL issuer URL}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_CLIENT_ID client ID}
   *   <li>{@linkplain #CONF_NESSIE_OAUTH2_CLIENT_SECRET client secret} (if required)
   * </ul>
   *
   * <p>Both client and user must be properly configured with appropriate permissions in the OAuth2
   * server for the authentication to succeed.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_GRANT_TYPE =
      "nessie.authentication.oauth2.grant-type";

  /**
   * Client ID to use when authenticating against the OAuth2 server. Required if using OAuth2
   * authentication, ignored otherwise.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_CLIENT_ID =
      "nessie.authentication.oauth2.client-id";

  /**
   * Client secret to use when authenticating against the OAuth2 server. Required if using OAuth2
   * authentication, ignored otherwise.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_CLIENT_SECRET =
      "nessie.authentication.oauth2.client-secret";

  /**
   * Username to use when authenticating against the OAuth2 server. Required if using OAuth2
   * authentication and "password" grant type, ignored otherwise.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_USERNAME = "nessie.authentication.oauth2.username";

  /**
   * Password to use when authenticating against the OAuth2 server. Required if using OAuth2
   * authentication and the "password" grant type, ignored otherwise.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_PASSWORD = "nessie.authentication.oauth2.password";

  public static final String DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN = "PT1M";

  /**
   * Default access token lifespan; if the OAuth2 server returns an access token without specifying
   * its expiration time, this value will be used.
   *
   * <p>Optional, defaults to {@value #DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN =
      "nessie.authentication.oauth2.default-access-token-lifespan";

  public static final String DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN = "PT30M";

  /**
   * Default refresh token lifespan. If the OAuth2 server returns a refresh token without specifying
   * its expiration time, this value will be used.
   *
   * <p>Optional, defaults to {@value #DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_DEFAULT_REFRESH_TOKEN_LIFESPAN =
      "nessie.authentication.oauth2.default-refresh-token-lifespan";

  public static final String DEFAULT_REFRESH_SAFETY_WINDOW = "PT10S";

  /**
   * Refresh safety window to use; a new token will be fetched when the current token's remaining
   * lifespan is less than this value. Optional, defaults to {@value
   * #DEFAULT_REFRESH_SAFETY_WINDOW}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW =
      "nessie.authentication.oauth2.refresh-safety-window";

  public static final String DEFAULT_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT = "PT30S";

  /**
   * Defines for how long the OAuth2 provider should keep the tokens fresh, if the client is not
   * being actively used. Setting this value too high may cause an excessive usage of network I/O
   * and thread resources; conversely, when setting it too low, if the client is used again, the
   * calling thread may block if the tokens are expired and need to be renewed synchronously.
   * Optional, defaults to {@value #DEFAULT_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT}. Must be a valid
   * <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_PREEMPTIVE_TOKEN_REFRESH_IDLE_TIMEOUT =
      "nessie.authentication.oauth2.preemptive-token-refresh-idle-timeout";

  public static final String DEFAULT_BACKGROUND_THREAD_IDLE_TIMEOUT = "PT30S";

  /**
   * Defines how long the background thread should be kept running if the client is not being
   * actively used, or no token refreshes are being executed. Optional, defaults to {@value
   * #DEFAULT_BACKGROUND_THREAD_IDLE_TIMEOUT}. Setting this value too high will cause the background
   * thread to keep running even if the client is not used anymore, potentially leaking thread and
   * memory resources; conversely, setting it too low could cause the background thread to be
   * restarted too often. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_BACKGROUND_THREAD_IDLE_TIMEOUT =
      "nessie.authentication.oauth2.background-thread-idle-timeout";

  /**
   * Space-separated list of scopes to include in each request to the OAuth2 server. Optional,
   * defaults to empty (no scopes).
   *
   * <p>The scope names will not be validated by the Nessie client; make sure they are valid
   * according to <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-3.3">RFC 6749
   * Section 3.3</a>.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_CLIENT_SCOPES =
      "nessie.authentication.oauth2.client-scopes";

  /**
   * This setting is no longer used and will be removed in 1.0.0.
   *
   * @deprecated This setting is no longer used and will be removed in 1.0.0.
   * @hidden
   */
  @Deprecated
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED_OLD =
      "nessie.authentication.oauth2.token-exchange-enabled";

  /**
   * Enable OAuth2 token exchange. If enabled, each access token obtained from the OAuth2 server
   * will be exchanged for a new token, using the token endpoint and the token exchange grant type,
   * as defined in <a href="https://datatracker.ietf.org/doc/html/rfc8693">RFC 8693</a>.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED =
      "nessie.authentication.oauth2.token-exchange.enabled";

  /**
   * For token exchanges only. The root URL of an alternate OpenID Connect identity issuer provider,
   * to use when exchanging tokens only.
   *
   * <p>If neither this property nor {@value #CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_TOKEN_ENDPOINT} are
   * defined, the global token endpoint will be used. This means that the same authorization server
   * will be used for both the initial token request and the token exchange.
   *
   * <p>Endpoint discovery is performed using the OpenID Connect Discovery metadata published by the
   * issuer. See <a href="https://openid.net/specs/openid-connect-discovery-1_0.html">OpenID Connect
   * Discovery 1.0</a> for more information.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ISSUER_URL =
      "nessie.authentication.oauth2.token-exchange.issuer-url";

  /**
   * For token exchanges only. The URL of an alternate OAuth2 token endpoint to use when exchanging
   * tokens only.
   *
   * <p>If neither this property nor {@value #CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ISSUER_URL} are
   * defined, the global token endpoint will be used. This means that the same authorization server
   * will be used for both the initial token request and the token exchange.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_TOKEN_ENDPOINT =
      "nessie.authentication.oauth2.token-exchange.token-endpoint";

  /**
   * For token exchanges only. An alternate client ID to use. If not provided, the global client ID
   * will be used. If provided, and if the client is confidential, then its secret must be provided
   * as well with {@value #CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_SECRET} – the global client
   * secret will NOT be used.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_ID =
      "nessie.authentication.oauth2.token-exchange.client-id";

  /**
   * For token exchanges only. The client secret to use, if {@value
   * #CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_ID} is defined and the token exchange client is
   * confidential.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_SECRET =
      "nessie.authentication.oauth2.token-exchange.client-secret";

  /**
   * For token exchanges only. A URI that indicates the target service or resource where the client
   * intends to use the requested security token. Optional.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_RESOURCE =
      "nessie.authentication.oauth2.token-exchange.resource";

  /**
   * For token exchanges only. The logical name of the target service where the client intends to
   * use the requested security token. This serves a purpose similar to the resource parameter but
   * with the client providing a logical name for the target service.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_AUDIENCE =
      "nessie.authentication.oauth2.token-exchange.audience";

  /**
   * For token exchanges only. Space-separated list of scopes to include in each token exchange
   * request to the OAuth2 server. Optional. If undefined, the global scopes configured through
   * {@value #CONF_NESSIE_OAUTH2_CLIENT_SCOPES} will be used. If defined and null or empty, no
   * scopes will be used.
   *
   * <p>The scope names will not be validated by the Nessie client; make sure they are valid
   * according to <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-3.3">RFC 6749
   * Section 3.3</a>.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SCOPES =
      "nessie.authentication.oauth2.token-exchange.scopes";

  /**
   * For token exchanges only. The subject token to exchange. This can take 3 kinds of values:
   *
   * <ul>
   *   <li>The value {@value
   *       org.projectnessie.client.auth.oauth2.TokenExchangeConfig#CURRENT_ACCESS_TOKEN}, if the
   *       client should use its current access token;
   *   <li>The value {@value
   *       org.projectnessie.client.auth.oauth2.TokenExchangeConfig#CURRENT_REFRESH_TOKEN}, if the
   *       client should use its current refresh token (if available);
   *   <li>An arbitrary token: in this case, the client will always use the static token provided
   *       here.
   * </ul>
   *
   * The default is to use the current access token.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN =
      "nessie.authentication.oauth2.token-exchange.subject-token";

  /**
   * For token exchanges only. The type of the subject token. Must be a valid URN. The default is
   * either {@code urn:ietf:params:oauth:token-type:access_token} or {@code
   * urn:ietf:params:oauth:token-type:refresh_token}, depending on the value of {@value
   * #CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN}.
   *
   * <p>If the client is configured to use its access or refresh token as the subject token, please
   * note that if an incorrect token type is provided here, the token exchange could fail.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN_TYPE =
      "nessie.authentication.oauth2.token-exchange.subject-token-type";

  /**
   * For token exchanges only. The actor token to exchange. This can take 4 kinds of values:
   *
   * <ul>
   *   <li>The value {@value org.projectnessie.client.auth.oauth2.TokenExchangeConfig#NO_TOKEN}, if
   *       the client should not include any actor token in the exchange request;
   *   <li>The value {@value
   *       org.projectnessie.client.auth.oauth2.TokenExchangeConfig#CURRENT_ACCESS_TOKEN}, if the
   *       client should use its current access token;
   *   <li>The value {@value
   *       org.projectnessie.client.auth.oauth2.TokenExchangeConfig#CURRENT_REFRESH_TOKEN}, if the
   *       client should use its current refresh token (if available);
   *   <li>An arbitrary token: in this case, the client will always use the static token provided
   *       here.
   * </ul>
   *
   * The default is to not include any actor token.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN =
      "nessie.authentication.oauth2.token-exchange.actor-token";

  /**
   * For token exchanges only. The type of the actor token. Must be a valid URN. The default is
   * either {@code urn:ietf:params:oauth:token-type:access_token} or {@code
   * urn:ietf:params:oauth:token-type:refresh_token}, depending on the value of {@value
   * #CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN}.
   *
   * <p>If the client is configured to use its access or refresh token as the actor token, please
   * note that if an incorrect token type is provided here, the token exchange could fail.
   */
  @ConfigItem(section = "OAuth2 Authentication Token Exchange")
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN_TYPE =
      "nessie.authentication.oauth2.token-exchange.actor-token-type";

  /**
   * Port of the OAuth2 authorization code flow web server.
   *
   * <p>When running a client inside a container make sure to specify a port and forward the port to
   * the container host.
   *
   * <p>The port used for the internal web server that listens for the authorization code callback.
   * This is only used if the grant type to use is {@value
   * #CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE}.
   *
   * <p>Optional; if not present, a random port will be used.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_WEB_PORT =
      "nessie.authentication.oauth2.auth-code-flow.web-port";

  /**
   * Defines how long the client should wait for the authorization code flow to complete. This is
   * only used if the grant type to use is {@value
   * #CONF_NESSIE_OAUTH2_GRANT_TYPE_AUTHORIZATION_CODE}. Optional, defaults to {@value
   * #DEFAULT_AUTHORIZATION_CODE_FLOW_TIMEOUT}.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_AUTHORIZATION_CODE_FLOW_TIMEOUT =
      "nessie.authentication.oauth2.auth-code-flow.timeout";

  public static final String DEFAULT_AUTHORIZATION_CODE_FLOW_TIMEOUT = "PT5M";

  /**
   * Defines how long the client should wait for the device code flow to complete. This is only used
   * if the grant type to use is {@value #CONF_NESSIE_OAUTH2_GRANT_TYPE_DEVICE_CODE}. Optional,
   * defaults to {@value #DEFAULT_DEVICE_CODE_FLOW_TIMEOUT}.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_TIMEOUT =
      "nessie.authentication.oauth2.device-code-flow.timeout";

  public static final String DEFAULT_DEVICE_CODE_FLOW_TIMEOUT = "PT5M";

  /**
   * Defines how often the client should poll the OAuth2 server for the device code flow to
   * complete. This is only used if the grant type to use is {@value
   * #CONF_NESSIE_OAUTH2_GRANT_TYPE_DEVICE_CODE}. Optional, defaults to {@value
   * #DEFAULT_DEVICE_CODE_FLOW_POLL_INTERVAL}.
   */
  @ConfigItem(section = "OAuth2 Authentication")
  public static final String CONF_NESSIE_OAUTH2_DEVICE_CODE_FLOW_POLL_INTERVAL =
      "nessie.authentication.oauth2.device-code-flow.poll-interval";

  public static final String DEFAULT_DEVICE_CODE_FLOW_POLL_INTERVAL = "PT5S";

  /**
   * AWS region used for {@code AWS} authentication, must be configured to the same region as the
   * Nessie setver.
   */
  @ConfigItem(section = "AWS Authentication")
  public static final String CONF_NESSIE_AWS_REGION = "nessie.authentication.aws.region";

  /** AWS profile name used for {@code AWS} authentication (optional). */
  @ConfigItem(section = "AWS Authentication")
  public static final String CONF_NESSIE_AWS_PROFILE = "nessie.authentication.aws.profile";

  /**
   * ID of the authentication provider to use, default is no authentication.
   *
   * <p>Valid values are {@code BASIC}, {@code BEARER}, {@code OAUTH2} and {@code AWS}.
   *
   * <p>The value is matched against the values returned as the supported auth-type by
   * implementations of {@link NessieAuthenticationProvider} across all available authentication
   * providers.
   *
   * <p>Note that "basic" HTTP authentication is not considered secure, use {@code BEARER} instead.
   */
  @ConfigItem public static final String CONF_NESSIE_AUTH_TYPE = "nessie.authentication.type";

  /** Name of the initial Nessie reference, usually {@code main}. */
  @ConfigItem public static final String CONF_NESSIE_REF = "nessie.ref";

  /** Commit ID (hash) on {@value #CONF_NESSIE_REF}, usually not specified. */
  @ConfigItem public static final String CONF_NESSIE_REF_HASH = "nessie.ref.hash";

  /**
   * Enable adding the HTTP headers of an active OpenTracing span to all Nessie requests. Disabled
   * by default.
   */
  @ConfigItem public static final String CONF_NESSIE_TRACING = "nessie.tracing";

  /**
   * Network level read timeout in milliseconds. When running with Java 11, this becomes a request
   * timeout. Default is {@value #DEFAULT_READ_TIMEOUT_MILLIS} ms.
   */
  @ConfigItem(section = "Network")
  public static final String CONF_READ_TIMEOUT = "nessie.transport.read-timeout";

  /**
   * Network level connect timeout in milliseconds, default is {@value
   * #DEFAULT_CONNECT_TIMEOUT_MILLIS}.
   */
  @ConfigItem(section = "Network")
  public static final String CONF_CONNECT_TIMEOUT = "nessie.transport.connect-timeout";

  /**
   * Config property name ({@value #CONF_NESSIE_DISABLE_COMPRESSION}) to disable compression on the
   * network layer, if set to {@code true}.
   */
  @ConfigItem(section = "Network")
  public static final String CONF_NESSIE_DISABLE_COMPRESSION =
      "nessie.transport.disable-compression";

  /**
   * Name of the Nessie client to use. If not specified, the implementation prefers the new Java
   * HTTP client ({@code JavaHttp}), if running on Java 11 or newer, or the Java {@code
   * URLConnection} client. The Apache HTTP client ({@code ApacheHttp}) can be used, if it has been
   * made available on the classpath.
   */
  @ConfigItem public static final String CONF_NESSIE_CLIENT_NAME = "nessie.client-builder-name";

  /**
   * Similar to {@value #CONF_NESSIE_CLIENT_NAME}, but uses a class name.
   *
   * @deprecated Prefer using Nessie client implementation <em>names</em>, configured via {@value
   *     #CONF_NESSIE_CLIENT_NAME}.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  @ConfigItem
  public static final String CONF_NESSIE_CLIENT_BUILDER_IMPL = "nessie.client-builder-impl";

  /**
   * Optional, disables certificate verifications, if set to {@code true}. Can be useful for testing
   * purposes, not recommended for production systems.
   */
  @ConfigItem(section = "Network")
  public static final String CONF_NESSIE_SSL_NO_CERTIFICATE_VERIFICATION =
      "nessie.ssl.no-certificate-verification";

  /**
   * Optional, list of comma-separated cipher suites for SSL connections.
   *
   * <p>This parameter only works on Java 11 and newer with the Java HTTP client.
   */
  @ConfigItem(section = "Network")
  public static final String CONF_NESSIE_SSL_CIPHER_SUITES = "nessie.ssl.cipher-suites";

  /**
   * Optional, list of comma-separated protocols for SSL connections.
   *
   * <p>This parameter only works on Java 11 and newer with the Java HTTP client.
   */
  @ConfigItem(section = "Network")
  public static final String CONF_NESSIE_SSL_PROTOCOLS = "nessie.ssl.protocols";

  /**
   * Optional, comma-separated list of SNI host names for SSL connections.
   *
   * <p>This parameter only works on Java 11 and newer with the Java HTTP client.
   */
  @ConfigItem(section = "Network")
  public static final String CONF_NESSIE_SNI_HOSTS = "nessie.ssl.sni-hosts";

  /**
   * Optional, a single SNI matcher for SSL connections.
   *
   * <p>Takes a single SNI hostname <em>matcher</em>, a regular expression representing the SNI
   * hostnames to match.
   *
   * <p>This parameter only works on Java 11 and newer with the Java HTTP client.
   */
  @ConfigItem(section = "Network")
  public static final String CONF_NESSIE_SNI_MATCHER = "nessie.ssl.sni-matcher";

  /**
   * Optional, allow HTTP/2 upgrade, if set to {@code true}.
   *
   * <p>This parameter only works on Java 11 and newer with the Java HTTP client.
   */
  @ConfigItem(section = "Network / HTTP")
  public static final String CONF_NESSIE_HTTP_2 = "nessie.http2-upgrade";

  /**
   * Optional, specify how redirects are handled.
   *
   * <ul>
   *   <li>{@code NEVER}: Never redirect.
   *   <li>{@code ALWAYS}: Always redirect.
   *   <li>{@code NORMAL}: Always redirect, except from HTTPS URLs to HTTP URLs.
   * </ul>
   *
   * <p>This parameter only works on Java 11 and newer with the Java HTTP client.
   */
  @ConfigItem(section = "Network / HTTP")
  public static final String CONF_NESSIE_HTTP_REDIRECT = "nessie.http-redirects";

  /**
   * Optional, when running on Java 11 force the use of the old {@link java.net.URLConnection} based
   * client for HTTP, if set to {@code true}.
   *
   * @deprecated Use {@link #CONF_NESSIE_CLIENT_NAME} with the value {@code URLConnection}.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public static final String CONF_FORCE_URL_CONNECTION_CLIENT =
      "nessie.force-url-connection-client";

  /**
   * Enables API compatibility check when creating the Nessie client. The default is {@code true}.
   *
   * <p>You can also control this setting by setting the system property {@code
   * nessie.client.enable-api-compatibility-check} to {@code true} or {@code false}.
   */
  @ConfigItem
  public static final String CONF_ENABLE_API_COMPATIBILITY_CHECK =
      "nessie.enable-api-compatibility-check";

  /**
   * Explicitly specify the Nessie API version number to use. The default for this setting depends
   * on the client being used.
   */
  @ConfigItem
  public static final String CONF_NESSIE_CLIENT_API_VERSION = "nessie.client-api-version";

  public static final int DEFAULT_READ_TIMEOUT_MILLIS = 25000;
  public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000;

  private NessieConfigConstants() {
    // empty
  }
}
