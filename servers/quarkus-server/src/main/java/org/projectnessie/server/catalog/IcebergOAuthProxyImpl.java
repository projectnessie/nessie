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
package org.projectnessie.server.catalog;

import io.quarkus.oidc.common.runtime.OidcCommonConfig;
import io.quarkus.oidc.common.runtime.OidcCommonUtils;
import io.quarkus.vertx.web.RoutingExchange;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.enterprise.inject.Vetoed;
import java.net.URI;
import java.util.Optional;
import org.projectnessie.catalog.service.rest.IcebergApiV1AuthResource;
import org.projectnessie.catalog.service.rest.IcebergOAuthProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Vetoed
public class IcebergOAuthProxyImpl implements IcebergOAuthProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergApiV1AuthResource.class);

  private final WebClient webClient;
  private final OidcCommonConfig defaultTenant;
  private final Vertx vertx;

  private volatile Uni<URI> tokenEndpointUri;

  public IcebergOAuthProxyImpl(WebClient webClient, OidcCommonConfig defaultTenant, Vertx vertx) {
    this.webClient = webClient;
    this.defaultTenant = defaultTenant;
    this.vertx = vertx;
    tokenEndpointUri().subscribe().with(uri -> {}, error -> {});
  }

  @Override
  public Optional<URI> resolvedTokenEndpoint() {
    // token endpoint discovery is capped by a timeout already, so no need
    // to specify another timeout here
    return tokenEndpointUri().await().asOptional().indefinitely();
  }

  @Override
  public void forwardToTokenEndpoint(RoutingExchange ex) {
    tokenEndpointUri()
        .flatMap(uri -> forwardRequest(ex, uri))
        .subscribe()
        .with(resp -> handleSuccess(ex, resp), t -> handleFailure(ex, t));
  }

  private Uni<URI> tokenEndpointUri() {
    if (tokenEndpointUri != null) {
      return tokenEndpointUri;
    }
    String authServerUri = OidcCommonUtils.getAuthServerUrl(defaultTenant);
    String tokenUri = OidcCommonUtils.getOidcEndpointUrl(authServerUri, defaultTenant.tokenPath);
    if (tokenUri == null) {
      if (defaultTenant.discoveryEnabled.orElse(true)) {
        return OidcCommonUtils.discoverMetadata(
                webClient,
                OidcCommonUtils.getOidcRequestFilters(),
                null,
                authServerUri,
                OidcCommonUtils.getConnectionDelayInMillis(defaultTenant),
                vertx,
                true)
            .map(json -> json.getString("token_endpoint"))
            .map(URI::create)
            .onItem()
            .invoke(uri -> tokenEndpointUri = Uni.createFrom().item(uri));
      } else {
        return Uni.createFrom()
            .failure(new IllegalStateException("Token endpoint URI is not available"));
      }
    } else {
      tokenEndpointUri = Uni.createFrom().item(URI.create(tokenUri));
      return tokenEndpointUri;
    }
  }

  private Uni<HttpResponse<Buffer>> forwardRequest(RoutingExchange ex, URI uri) {
    return webClient
        .postAbs(uri.toString())
        .putHeaders(MultiMap.newInstance(ex.request().headers()))
        .putHeader("Via", ex.request().version().alpnName() + " nessie-catalog")
        .sendBuffer(Buffer.newInstance(ex.context().body().buffer()))
        .onFailure(OidcCommonUtils.oidcEndpointNotAvailable())
        .retry()
        .withBackOff(
            OidcCommonUtils.CONNECTION_BACKOFF_DURATION,
            OidcCommonUtils.CONNECTION_BACKOFF_DURATION)
        .expireIn(OidcCommonUtils.getConnectionDelayInMillis(defaultTenant));
  }

  private static void handleSuccess(RoutingExchange ex, HttpResponse<Buffer> resp) {
    ex.response().setStatusCode(resp.statusCode());
    resp.headers().forEach(e -> ex.response().putHeader(e.getKey(), e.getValue()));
    ex.response().end(resp.body().getDelegate());
  }

  private static void handleFailure(RoutingExchange ex, Throwable t) {
    LOGGER.warn("OIDC token endpoint failure", t);
    ex.response()
        .setStatusCode(503)
        .end(
            new JsonObject()
                .put("error", "OAuthTokenEndpointError")
                .put("error_description", "OAuth token endpoint encountered an error.")
                .toBuffer());
  }
}
