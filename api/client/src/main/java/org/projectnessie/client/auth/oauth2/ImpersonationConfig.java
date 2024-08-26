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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_IMPERSONATION_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_IMPERSONATION_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_IMPERSONATION_ENABLED;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_IMPERSONATION_ISSUER_URL;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_IMPERSONATION_SCOPES;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_IMPERSONATION_TOKEN_ENDPOINT;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientConfig.applyConfigOption;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.projectnessie.client.NessieConfigConstants;

/**
 * Configuration for OAuth2 impersonation.
 *
 * <p>STILL IN BETA. API MAY CHANGE.
 */
@Value.Immutable
public interface ImpersonationConfig {

  List<String> SCOPES_INHERIT = Collections.singletonList("\\inherit\\");

  static ImpersonationConfig fromConfigSupplier(Function<String, String> config) {
    ImpersonationConfig.Builder builder = ImpersonationConfig.builder();
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_IMPERSONATION_ENABLED, builder::enabled, Boolean::parseBoolean);
    applyConfigOption(config, CONF_NESSIE_OAUTH2_IMPERSONATION_CLIENT_ID, builder::clientId);
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_IMPERSONATION_CLIENT_SECRET, builder::clientSecret);
    applyConfigOption(
        config, CONF_NESSIE_OAUTH2_IMPERSONATION_ISSUER_URL, builder::issuerUrl, URI::create);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_IMPERSONATION_TOKEN_ENDPOINT,
        builder::tokenEndpoint,
        URI::create);
    applyConfigOption(
        config,
        CONF_NESSIE_OAUTH2_IMPERSONATION_SCOPES,
        scope -> Arrays.stream(scope.split(" ")).forEach(builder::addScope));
    return builder.build();
  }

  /**
   * Whether "impersonation" is enabled. If enabled, the access token obtained from the OAuth2
   * server with the configured initial grant will be exchanged for a new token, using the token
   * exchange grant type.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_IMPERSONATION_ENABLED
   */
  @Value.Default
  default boolean isEnabled() {
    return false;
  }

  /**
   * An alternate client ID to use for impersonations only. If not provided, the global client ID
   * will be used. If provided, and if the client is confidential, then its secret must be provided
   * with {@link #getClientSecret()} – the global client secret will NOT be used.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_IMPERSONATION_CLIENT_ID
   */
  Optional<String> getClientId();

  /**
   * An alternate, static client secret to use for impersonations only. If the alternate client
   * obtained from {@link #getClientId()} is confidential, either this attribute or {@link
   * #getClientSecretSupplier()} must be set.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_IMPERSONATION_CLIENT_SECRET
   */
  @Value.Redacted
  Optional<Secret> getClientSecret();

  /**
   * An alternate client secret supplier to use for impersonations only. If the alternate client
   * obtained from {@link #getClientId()} is confidential, either this attribute or {@link
   * #getClientSecret()} must be set.
   */
  @Value.Redacted
  Optional<Supplier<String>> getClientSecretSupplier();

  /**
   * The root URL of an alternate OpenID Connect identity issuer provider, which will be used for
   * discovering supported endpoints and their locations, but only for impersonation.
   *
   * <p>If neither this property nor {@link #getTokenEndpoint()} are defined, the global token
   * endpoint will be used for impersonation. This means that the same authorization server will be
   * used for both the initial token request and the impersonation token exchange.
   *
   * <p>Endpoint discovery is performed using the OpenID Connect Discovery metadata published by the
   * issuer. See <a href="https://openid.net/specs/openid-connect-discovery-1_0.html">OpenID Connect
   * Discovery 1.0</a> for more information.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_IMPERSONATION_ISSUER_URL
   */
  Optional<URI> getIssuerUrl();

  /**
   * An alternate OAuth2 token endpoint, for impersonation only.
   *
   * <p>If neither this property nor {@link #getIssuerUrl()} are defined, the global token endpoint
   * will be used for impersonation. This means that the same authorization server will be used for
   * both the initial token request and the impersonation token exchange.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_IMPERSONATION_TOKEN_ENDPOINT
   */
  Optional<URI> getTokenEndpoint();

  /**
   * Custom OAuth2 scopes for impersonation only. Optional.
   *
   * <p>The special value {@link #SCOPES_INHERIT} (default) means that the scopes will be inherited
   * from the global OAuth2 configuration.
   *
   * @see NessieConfigConstants#CONF_NESSIE_OAUTH2_IMPERSONATION_SCOPES
   */
  @Value.Default
  default List<String> getScopes() {
    return SCOPES_INHERIT;
  }

  static ImpersonationConfig.Builder builder() {
    return ImmutableImpersonationConfig.builder();
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
    Builder clientSecretSupplier(Supplier<String> clientSecret);

    @CanIgnoreReturnValue
    Builder issuerUrl(URI issuerUrl);

    @CanIgnoreReturnValue
    Builder tokenEndpoint(URI tokenEndpoint);

    @CanIgnoreReturnValue
    Builder addScope(String scope);

    @CanIgnoreReturnValue
    Builder addScopes(String... scopes);

    @CanIgnoreReturnValue
    Builder scopes(Iterable<String> scopes);

    ImpersonationConfig build();
  }
}
