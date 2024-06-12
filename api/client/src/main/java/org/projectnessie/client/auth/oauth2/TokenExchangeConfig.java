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
import static org.projectnessie.client.auth.oauth2.OAuth2ClientConfig.applyConfigOption;
import static org.projectnessie.client.auth.oauth2.TypedToken.URN_ACCESS_TOKEN;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.immutables.value.Value;
import org.projectnessie.client.NessieConfigConstants;

/**
 * Configuration for OAuth2 token exchange.
 *
 * <p>STILL IN BETA. API MAY CHANGE.
 */
@Value.Immutable
public interface TokenExchangeConfig {

  TokenExchangeConfig DISABLED = builder().enabled(false).build();

  String SCOPES_INHERIT = "\\inherit\\";

  static TokenExchangeConfig fromConfigSupplier(Function<String, String> config) {
    String enabled = config.apply(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED);
    if (!Boolean.parseBoolean(enabled)) {
      return DISABLED;
    }
    TokenExchangeConfig.Builder builder = TokenExchangeConfig.builder().enabled(true);
    applyConfigOption(config, CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_ID, builder::clientId);
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_SECRET, builder::clientSecret);
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ISSUER_URL, builder::issuerUrl, URI::create);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_TOKEN_ENDPOINT,
        builder::tokenEndpoint,
        URI::create);
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_RESOURCE, builder::resource, URI::create);
    applyConfigOption(config, CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SCOPES, builder::scope);
    applyConfigOption(config, CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_AUDIENCE, builder::audience);
    String subjectToken = config.apply(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN);
    String actorToken = config.apply(CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN);
    AtomicReference<URI> subjectTokenType = new AtomicReference<>(URN_ACCESS_TOKEN);
    AtomicReference<URI> actorTokenType = new AtomicReference<>(URN_ACCESS_TOKEN);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN_TYPE,
        subjectTokenType::set,
        URI::create);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN_TYPE,
        actorTokenType::set,
        URI::create);
    // If a subject token is statically provided, use it. If no subject token is provided, then let
    // the client be the subject, since a subject token is always required.
    if (subjectToken != null) {
      builder.subjectToken(TypedToken.of(subjectToken, subjectTokenType.get()));
    } else {
      builder.subjectTokenProvider(
          (accessToken, refreshToken) ->
              TypedToken.of(accessToken.getPayload(), subjectTokenType.get()));
    }
    // If an actor token is statically provided, use it. If no actor token is provided, but a
    // subject token is statically provided, then let the client be the actor; otherwise, no actor
    // token should be used.
    if (actorToken != null) {
      builder.actorToken(TypedToken.of(actorToken, actorTokenType.get()));
    } else if (subjectToken != null) {
      builder.actorTokenProvider(
          (accessToken, refreshToken) ->
              TypedToken.of(accessToken.getPayload(), actorTokenType.get()));
    }
    return builder.build();
  }

  /**
   * Whether token exchange is enabled. If enabled, the access token obtained from the OAuth2 server
   * will be exchanged for a new token, using the token endpoint and the token exchange grant type,
   * as defined in <a href="https://datatracker.ietf.org/doc/html/rfc8693">RFC 8693</a>.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ENABLED
   */
  @Value.Default
  default boolean isEnabled() {
    return false;
  }

  /**
   * An alternate client ID to use for token exchanges only. If not provided, the global client ID
   * will be used. If provided, and if the client is confidential, then its secret must be provided
   * with {@link #getClientSecret()} – the global client secret will NOT be used.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_ID
   */
  Optional<String> getClientId();

  /**
   * An alternate client secret to use for token exchanges only. Required if the alternate client
   * obtained from {@link #getClientId()} is confidential.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_CLIENT_SECRET
   */
  Optional<Secret> getClientSecret();

  /**
   * The root URL of an alternate OpenID Connect identity issuer provider, which will be used for
   * discovering supported endpoints and their locations, for token exchange only.
   *
   * <p>If neither this property nor {@link #getTokenEndpoint()} are defined, the global token
   * endpoint will be used. This means that the same authorization server will be used for both the
   * initial token request and the token exchange.
   *
   * <p>Endpoint discovery is performed using the OpenID Connect Discovery metadata published by the
   * issuer. See <a href="https://openid.net/specs/openid-connect-discovery-1_0.html">OpenID Connect
   * Discovery 1.0</a> for more information.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ISSUER_URL
   */
  Optional<URI> getIssuerUrl();

  /**
   * An alternate OAuth2 token endpoint, for token exchange only.
   *
   * <p>If neither this property nor {@link #getIssuerUrl()} are defined, the global token endpoint
   * will be used. This means that the same authorization server will be used for both the initial
   * token request and the token exchange.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_TOKEN_ENDPOINT
   */
  Optional<URI> getTokenEndpoint();

  /**
   * The type of the requested security token. By default, {@link TypedToken#URN_ACCESS_TOKEN}.
   *
   * <p>Currently, it is not possible to request any other token type, so this property is not
   * configurable through system properties.
   */
  @Value.Default
  default URI getRequestedTokenType() {
    return URN_ACCESS_TOKEN;
  }

  /**
   * A URI that indicates the target service or resource where the client intends to use the
   * requested security token.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_RESOURCE
   */
  Optional<URI> getResource();

  /**
   * The logical name of the target service where the client intends to use the requested security
   * token. This serves a purpose similar to the resource parameter but with the client providing a
   * logical name for the target service.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_AUDIENCE
   */
  Optional<String> getAudience();

  /**
   * The OAuth2 scope. Optional.
   *
   * <p>The special value {@link #SCOPES_INHERIT} (default) means that the scope will be inherited
   * from the global OAuth2 configuration.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SCOPES
   */
  @Value.Default
  default String getScope() {
    return SCOPES_INHERIT;
  }

  /**
   * The subject token provider. The provider will be invoked with the current access token (never
   * null) and the current refresh token, or null if none available; and should return a {@link
   * TypedToken} representing the subject token. It must NOT return null.
   *
   * <p>By default, the provider will return the access token itself. This should be suitable for
   * most cases.
   *
   * <p>This property cannot be set through configuration, but only programmatically. The
   * configuration exposes two options: the subject token and its type. These options allow to pass
   * a static subject token only.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_SUBJECT_TOKEN_TYPE
   */
  @Value.Default
  @Value.Auxiliary
  default BiFunction<AccessToken, RefreshToken, TypedToken> getSubjectTokenProvider() {
    return (accessToken, refreshToken) -> TypedToken.fromAccessToken(accessToken);
  }

  /**
   * The actor token provider. The provider will be invoked with the current access token (never
   * null) and the current refresh token, or null if none available; and should return a {@link
   * TypedToken} representing the actor token. If the provider returns null, then no actor token
   * will be used.
   *
   * <p>Actor tokens are useful in delegation scenarios. By default, no actor token is used.
   *
   * <p>This property cannot be set through configuration, but only programmatically. The
   * configuration exposes two options: the actor token and its type. These options allow to pass a
   * static actor token only.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_TOKEN_EXCHANGE_ACTOR_TOKEN_TYPE
   */
  @Value.Default
  @Value.Auxiliary
  default BiFunction<AccessToken, RefreshToken, TypedToken> getActorTokenProvider() {
    return (accessToken, refreshToken) -> null;
  }

  static TokenExchangeConfig.Builder builder() {
    return ImmutableTokenExchangeConfig.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder enabled(boolean enabled);

    @CanIgnoreReturnValue
    Builder clientId(String clientId);

    @CanIgnoreReturnValue
    Builder clientSecret(Secret clientSecret);

    @CanIgnoreReturnValue
    default Builder clientSecret(String clientSecret) {
      return clientSecret(new Secret(clientSecret));
    }

    @CanIgnoreReturnValue
    Builder issuerUrl(URI issuerUrl);

    @CanIgnoreReturnValue
    Builder tokenEndpoint(URI tokenEndpoint);

    @CanIgnoreReturnValue
    Builder requestedTokenType(URI tokenType);

    @CanIgnoreReturnValue
    Builder resource(URI resource);

    @CanIgnoreReturnValue
    Builder audience(String audience);

    @CanIgnoreReturnValue
    Builder scope(String scope);

    @CanIgnoreReturnValue
    Builder subjectTokenProvider(BiFunction<AccessToken, RefreshToken, TypedToken> provider);

    @CanIgnoreReturnValue
    Builder actorTokenProvider(BiFunction<AccessToken, RefreshToken, TypedToken> provider);

    @CanIgnoreReturnValue
    default Builder subjectToken(TypedToken token) {
      return subjectTokenProvider((accessToken, refreshToken) -> token);
    }

    @CanIgnoreReturnValue
    default Builder actorToken(TypedToken token) {
      return actorTokenProvider((accessToken, refreshToken) -> token);
    }

    TokenExchangeConfig build();
  }
}
