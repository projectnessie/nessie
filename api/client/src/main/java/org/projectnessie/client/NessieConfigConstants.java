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

import java.util.Collection;
import java.util.List;
import org.projectnessie.client.auth.NessieAuthenticationProvider;

/** Configuration constants for Nessie clients. */
@SuppressWarnings("JavadocDeclaration")
public final class NessieConfigConstants {
  /** Config property name ({@value #CONF_NESSIE_URI}) for the Nessie service URL. */
  public static final String CONF_NESSIE_URI = "nessie.uri";

  /**
   * Config property name ({@value #CONF_NESSIE_USERNAME}) for the username used for (basic)
   * authentication.
   *
   * @deprecated "basic" HTTP authentication is not considered secure. Use {@link
   *     #CONF_NESSIE_AUTH_TOKEN} instead.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public static final String CONF_NESSIE_USERNAME = "nessie.authentication.username";

  /**
   * Config property name ({@value #CONF_NESSIE_PASSWORD}) for the password used for (basic)
   * authentication.
   *
   * @deprecated "basic" HTTP authentication is not considered secure. Use {@link
   *     #CONF_NESSIE_AUTH_TOKEN} instead.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public static final String CONF_NESSIE_PASSWORD = "nessie.authentication.password";

  /**
   * Config property name ({@value #CONF_NESSIE_AUTH_TOKEN}) for the token used for (bearer)
   * authentication.
   */
  public static final String CONF_NESSIE_AUTH_TOKEN = "nessie.authentication.token";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT}) for the OAuth2
   * authentication provider. The URL of the OAuth2 token endpoint; this should include not only the
   * OAuth2 server's address, but also the path to the token REST resource, if any. For Keycloak,
   * this is typically {@code
   * https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/token}. Required if using
   * OAuth2 authentication, ignored otherwise.
   */
  public static final String CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT =
      "nessie.authentication.oauth2.token-endpoint";

  public static final String CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS =
      "client_credentials";

  public static final String CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD = "password";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_GRANT_TYPE}) for the OAuth2 authentication
   * provider. The grant type to use when authenticating against the OAuth2 server. Valid values
   * are: {@value #CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS} or {@value
   * #CONF_NESSIE_OAUTH2_GRANT_TYPE_PASSWORD}. Optional, defaults to {@value
   * #CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS}.
   *
   * <p>For both grant types, a {@linkplain #CONF_NESSIE_OAUTH2_CLIENT_ID client ID} and {@linkplain
   * #CONF_NESSIE_OAUTH2_CLIENT_SECRET client secret} must be provided; they are used to
   * authenticate the client against the OAuth2 server.
   *
   * <p>Additionally, when using the "password" grant type, a {@linkplain
   * #CONF_NESSIE_OAUTH2_USERNAME username} and {@linkplain #CONF_NESSIE_OAUTH2_PASSWORD password}
   * must also be provided; they are used to authenticate the user.
   *
   * <p>Both client and user must be properly configured with appropriate permissions in the OAuth2
   * server for the authentication to succeed.
   */
  public static final String CONF_NESSIE_OAUTH2_GRANT_TYPE =
      "nessie.authentication.oauth2.grant-type";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_CLIENT_ID}) for the OAuth2 authentication
   * provider. The client ID to use when authenticating against the OAuth2 server. Required if using
   * OAuth2 authentication, ignored otherwise.
   */
  public static final String CONF_NESSIE_OAUTH2_CLIENT_ID =
      "nessie.authentication.oauth2.client-id";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_CLIENT_SECRET}) for the OAuth2 authentication
   * provider. The client secret to use when authenticating against the OAuth2 server. Required if
   * using OAuth2 authentication, ignored otherwise.
   */
  public static final String CONF_NESSIE_OAUTH2_CLIENT_SECRET =
      "nessie.authentication.oauth2.client-secret";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_USERNAME}) for the OAuth2 authentication
   * provider. The username to use when authenticating against the OAuth2 server. Required if using
   * OAuth2 authentication and "password" grant type, ignored otherwise.
   */
  public static final String CONF_NESSIE_OAUTH2_USERNAME = "nessie.authentication.oauth2.username";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_PASSWORD}) for the OAuth2 authentication
   * provider. The user password to use when authenticating against the OAuth2 server. Required if
   * using OAuth2 authentication and the "password" grant type, ignored otherwise.
   */
  public static final String CONF_NESSIE_OAUTH2_PASSWORD = "nessie.authentication.oauth2.password";

  public static final String DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN = "PT1M";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN}) for the
   * OAuth2 authentication provider. The default access token lifespan; if the OAuth2 server returns
   * an access token without specifying its expiration time, this value will be used. Optional,
   * defaults to {@value #DEFAULT_DEFAULT_ACCESS_TOKEN_LIFESPAN}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  public static final String CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN =
      "nessie.authentication.oauth2.default-access-token-lifespan";

  public static final String DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN = "PT30M";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_DEFAULT_REFRESH_TOKEN_LIFESPAN}) for the
   * OAuth2 authentication provider. The default refresh token lifespan; if the OAuth2 server
   * returns a refresh token without specifying its expiration time, this value will be used.
   * Optional, defaults to {@value #DEFAULT_DEFAULT_REFRESH_TOKEN_LIFESPAN}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  public static final String CONF_NESSIE_OAUTH2_DEFAULT_REFRESH_TOKEN_LIFESPAN =
      "nessie.authentication.oauth2.default-refresh-token-lifespan";

  public static final String DEFAULT_REFRESH_SAFETY_WINDOW = "PT10S";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW}) for the OAuth2
   * authentication provider. The refresh safety window to use; a new token will be fetched when the
   * current token's remaining lifespan is less than this value. Optional, defaults to {@value
   * #DEFAULT_REFRESH_SAFETY_WINDOW}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  public static final String CONF_NESSIE_OAUTH2_REFRESH_SAFETY_WINDOW =
      "nessie.authentication.oauth2.refresh-safety-window";

  public static final String DEFAULT_IDLE_INTERVAL = "PT30S";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_IDLE_INTERVAL}) for the OAuth2 authentication
   * provider. The maximum idle interval to use; if the OAuth2 provider is not used after this
   * interval, no more token refreshes will occur, until the provider is used again. Optional,
   * defaults to {@value #DEFAULT_IDLE_INTERVAL}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  public static final String CONF_NESSIE_OAUTH2_IDLE_INTERVAL =
      "nessie.authentication.oauth2.idle-interval";

  public static final String DEFAULT_KEEP_ALIVE_INTERVAL = "PT30S";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_KEEP_ALIVE_INTERVAL}) for the OAuth2
   * authentication provider. The keep alive interval to use; if the OAuth2 provider background
   * thread is not used after this interval, it will be stopped; a new thread will be spawned if the
   * provider is used again. Optional, defaults to {@value #DEFAULT_KEEP_ALIVE_INTERVAL}. Must be a
   * valid <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  public static final String CONF_NESSIE_OAUTH2_KEEP_ALIVE_INTERVAL =
      "nessie.authentication.oauth2.keep-alive-interval";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_CLIENT_SCOPES}) for the OAuth2 authentication
   * provider. Space-separated list of scopes to include in each request to the OAuth2 server.
   * Optional, defaults to empty (no scopes).
   *
   * <p>The scope names will not be validated by the Nessie client; make sure they are valid
   * according to <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-3.3">RFC 6749
   * Section 3.3</a>.
   */
  public static final String CONF_NESSIE_OAUTH2_CLIENT_SCOPES =
      "nessie.authentication.oauth2.client-scopes";

  /**
   * Config property name ({@value #CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED}) for the OAuth2
   * authentication provider. If set to {@code true}, the Nessie client will attempt to exchange
   * access tokens for refresh tokens whenever appropriate. This, however, can only work if the
   * OAuth2 server supports token exchange. Optional, defaults to {@code true} (enabled).
   *
   * <p>Note that recent versions of Keycloak support token exchange, but it is disabled by default.
   * See <a
   * href="https://www.keycloak.org/docs/latest/securing_apps/index.html#internal-token-to-internal-token-exchange">Using
   * token exchange</a> for more information and how to enable this feature.
   */
  public static final String CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED =
      "nessie.authentication.oauth2.token-exchange-enabled";

  /**
   * Config property name ({@value #CONF_NESSIE_AWS_REGION}) for the region used for AWS
   * authentication.
   */
  public static final String CONF_NESSIE_AWS_REGION = "nessie.authentication.aws.region";

  /**
   * Config property name ({@value #CONF_NESSIE_AWS_PROFILE}) for the profile name used for AWS
   * authentication (optional).
   */
  public static final String CONF_NESSIE_AWS_PROFILE = "nessie.authentication.aws.profile";

  /**
   * Config property name ({@value #CONF_NESSIE_AUTH_TYPE}) for the authentication provider ID.
   * Valid values are {@code BASIC}, {@code BEARER} and {@code AWS}.
   *
   * <p>The value is matched against the values returned by {@link
   * NessieAuthenticationProvider#getAuthTypeValue()} of the available authentication providers.
   *
   * <p>Note that "basic" HTTP authentication is not considered secure, use {@code BEARER} instead.
   */
  public static final String CONF_NESSIE_AUTH_TYPE = "nessie.authentication.type";

  /**
   * Config property name ({@value #CONF_NESSIE_REF}) for the nessie reference name used by clients,
   * recommended setting.
   */
  public static final String CONF_NESSIE_REF = "nessie.ref";

  /**
   * Config property name ({@value #CONF_NESSIE_REF_HASH}) for the nessie reference hash used by
   * clients, optional setting.
   */
  public static final String CONF_NESSIE_REF_HASH = "nessie.ref.hash";

  /**
   * Config property name ({@value #CONF_NESSIE_TRACING}) to enable adding the HTTP headers of an
   * active OpenTracing span to all Nessie requests. Valid values are {@code true} and {@code
   * false}.
   */
  public static final String CONF_NESSIE_TRACING = "nessie.tracing";

  /**
   * Config property name ({@value #CONF_READ_TIMEOUT}) for the network transport read-timeout,
   * default is {@value #DEFAULT_READ_TIMEOUT_MILLIS}.
   */
  public static final String CONF_READ_TIMEOUT = "nessie.transport.read-timeout";

  /**
   * Config property name ({@value #CONF_CONNECT_TIMEOUT}) for the network transport connect timeout
   * in milliseconds, default is {@value #DEFAULT_CONNECT_TIMEOUT_MILLIS}.
   */
  public static final String CONF_CONNECT_TIMEOUT = "nessie.transport.connect-timeout";

  /**
   * Config property name ({@value #CONF_NESSIE_DISABLE_COMPRESSION}) to disable compression on the
   * network layer, if set to {@code true}.
   */
  public static final String CONF_NESSIE_DISABLE_COMPRESSION =
      "nessie.transport.disable-compression";

  /** Config property name ({@value #CONF_NESSIE_CLIENT_NAME}) for custom client builder name. */
  public static final String CONF_NESSIE_CLIENT_NAME = "nessie.client-builder-name";

  /**
   * Config property name ({@value #CONF_NESSIE_CLIENT_BUILDER_IMPL}) for custom client builder
   * class name.
   *
   * @deprecated Prefer using Nessie client implementation <em>names</em>, configured via {@link
   *     #CONF_NESSIE_CLIENT_NAME}.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public static final String CONF_NESSIE_CLIENT_BUILDER_IMPL = "nessie.client-builder-impl";

  /**
   * Optional, list of comma-separated cipher suites for SSL connections, see {@link
   * javax.net.ssl.SSLParameters#setCipherSuites(String[])}.
   *
   * <p>This parameter only works on Java 11 and newer.
   */
  public static final String CONF_NESSIE_SSL_CIPHER_SUITES = "nessie.ssl.cipher-suites";

  /**
   * Optional, list of comma-separated protocols for SSL connections, see {@link
   * javax.net.ssl.SSLParameters#setProtocols(String[])}.
   *
   * <p>This parameter only works on Java 11 and newer.
   */
  public static final String CONF_NESSIE_SSL_PROTOCOLS = "nessie.ssl.protocols";

  /**
   * Optional, the SNI host names for SSL connections, see {@link
   * javax.net.ssl.SSLParameters#setServerNames(List)}.
   *
   * <p>Takes a comma-separated list of SNI hostnames.
   *
   * <p>This parameter only works on Java 11 and newer.
   */
  public static final String CONF_NESSIE_SNI_HOSTS = "nessie.ssl.sni-hosts";

  /**
   * Optional, a single SNI matcher for SSL connections, see {@link
   * javax.net.ssl.SSLParameters#setSNIMatchers(Collection)}.
   *
   * <p>Takes a single SNI hostname <em>matcher</em>, a regular expression representing the SNI
   * hostnames to match.
   *
   * <p>This parameter only works on Java 11 and newer.
   */
  public static final String CONF_NESSIE_SNI_MATCHER = "nessie.ssl.sni-matcher";

  /**
   * Optional, allow HTTP/2 upgrade, if set to {@code true}.
   *
   * <p>This parameter only works on Java 11 and newer.
   */
  public static final String CONF_NESSIE_HTTP_2 = "nessie.http2-upgrade";

  /**
   * Optional, specify how redirects are handled.
   *
   * <p>See {@link java.net.http.HttpClient.Redirect}, possible values:
   *
   * <ul>
   *   <li>{@code NEVER}: Never redirect.
   *   <li>{@code ALWAYS}: Always redirect.
   *   <li>{@code NORMAL}: Always redirect, except from HTTPS URLs to HTTP URLs.
   * </ul>
   *
   * <p>This parameter only works on Java 11 and newer.
   */
  public static final String CONF_NESSIE_HTTP_REDIRECT = "nessie.http-redirects";

  /**
   * Optional, when running on Java 11 force the use of the old {@link java.net.URLConnection} based
   * client for HTTP, if set to {@code true}.
   *
   * <p>This parameter only works on Java 11 and newer.
   */
  public static final String CONF_FORCE_URL_CONNECTION_CLIENT =
      "nessie.force-url-connection-client";

  /**
   * Enables API compatibility check when creating the Nessie client. The default is {@code true}.
   *
   * <p>You can also control this setting by setting the system property {@code
   * nessie.client.enable-api-compatibility-check} to {@code true} or {@code false}.
   */
  public static final String CONF_ENABLE_API_COMPATIBILITY_CHECK =
      "nessie.enable-api-compatibility-check";

  public static final int DEFAULT_READ_TIMEOUT_MILLIS = 25000;
  public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000;

  private NessieConfigConstants() {
    // empty
  }
}
