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
package org.projectnessie.catalog.server.auth;

import io.quarkus.oidc.common.runtime.OidcCommonConfig;
import io.quarkus.oidc.common.runtime.OidcCommonUtils;
import io.quarkus.oidc.runtime.OidcConfig;
import io.quarkus.runtime.TlsConfig;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.catalog.service.spi.OAuthHandler;

@ApplicationScoped
public class OAuthHandlerProvider {

  @Inject Vertx vertx;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  OidcConfig oidcConfig;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  TlsConfig tlsConfig;

  /**
   * Produces an application-wide {@link OAuthProxy} instance configured to contact the default
   * tenant's OIDC provider specified in the application configuration under {@code quarkus.oidc.*}.
   *
   * <p>Not all properties are relevant here though; the following are taken into account:
   *
   * <ul>
   *   <li><code>quarkus.oidc.auth-server-url</code> : used to compute the token endpoint;
   *   <li><code>quarkus.oidc.token-path</code> : used to compute the token endpoint;
   *   <li><code>quarkus.oidc.discovery-enabled</code> : used to discover the token endpoint;
   *   <li>All HTTP-level properties are also valid and used to create the underlying {@link
   *       WebClient} instance.
   * </ul>
   *
   * <p>The following properties however are completely ignored, since it's the user's
   * responsibility to provide them:
   *
   * <ul>
   *   <li><code>quarkus.oidc.client-id</code>;
   *   <li><code>quarkus.oidc.credentials.*</code>.
   * </ul>
   *
   * <p>Note: OidcClient instances are configured based on the OidcClientsConfig root, that is,
   * {@code quarkus.oidc-client.<tenant>.<config-property>} (this class is in the oidc-client
   * module). Here however, we create proxy instances based on the "server-side" {@link OidcConfig}
   * root, that is, {@code quarkus.oidc.<tenant>.<config-property>}. This is to spare users the
   * hassle of having to configure the same properties twice, since other parts of the application
   * already use the OidcConfig root to authorize incoming requests, and we need the OAuth proxy to
   * contact the same backend.
   *
   * <p>Note 2: we currently only support a single tenant, so we use the default one here. Users
   * wishing to use multiple tenants will have to implement {@link io.quarkus.oidc.TenantResolver}
   * and/or a {@link io.quarkus.oidc.TenantConfigResolver}, then use the resolved tenant to retrieve
   * the tenant-specific configuration from the {@link OidcConfig} instance.
   */
  @Produces
  @ApplicationScoped
  public OAuthHandler produceOAuthHandler(
      @ConfigProperty(name = "quarkus.oidc.enabled") boolean authEnabled) {
    if (!authEnabled) {
      return new OAuthDisabledHandler();
    }
    OidcCommonConfig tenantConfig = oidcConfig.defaultTenant;
    OidcCommonUtils.verifyCommonConfiguration(tenantConfig, true, true);
    WebClient webClient = createWebClient(tenantConfig);
    String tokenUri = resolveTokenUri(tenantConfig, webClient);
    return new OAuthProxy(webClient, tokenUri, tenantConfig.connectionRetryCount);
  }

  private WebClient createWebClient(OidcCommonConfig tenantConfig) {
    WebClientOptions options = new WebClientOptions();
    OidcCommonUtils.setHttpClientOptions(tenantConfig, tlsConfig, options);
    return WebClient.create(vertx, options);
  }

  private static String resolveTokenUri(OidcCommonConfig tenantConfig, WebClient webClient) {
    String authServerUri = OidcCommonUtils.getAuthServerUrl(tenantConfig);
    String tokenUri = OidcCommonUtils.getOidcEndpointUrl(authServerUri, tenantConfig.tokenPath);
    if (tokenUri == null && tenantConfig.discoveryEnabled.orElse(true)) {
      long connectionDelayMillis = OidcCommonUtils.getConnectionDelayInMillis(tenantConfig);
      tokenUri =
          OidcCommonUtils.discoverMetadata(webClient, authServerUri, connectionDelayMillis)
              .onItem()
              .transform(json -> json.getString("token_endpoint"))
              .await()
              .indefinitely();
    }
    return tokenUri;
  }
}
