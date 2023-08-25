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

import io.quarkus.oidc.common.runtime.OidcCommonUtils;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.enterprise.inject.Vetoed;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.projectnessie.catalog.api.errors.OAuthTokenEndpointException;
import org.projectnessie.catalog.api.model.OAuthTokenRequest;
import org.projectnessie.catalog.service.spi.OAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A proxy for the REST Catalog OAuth token endpoint that simply forwards all incoming requests to a
 * configured OIDC backend.
 *
 * <p>This is basically a stripped-down version of {@code
 * io.quarkus.oidc.client.runtime.OidcClientImpl} from {@code io.quarkus:quarkus-oidc-client}.
 */
@Vetoed
public class OAuthProxy implements OAuthHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAuthProxy.class);

  private final WebClient client;
  private final String tokenUri;
  private final long retryCount;

  public OAuthProxy(WebClient client, String tokenUri, long retryCount) {
    this.client = client;
    this.tokenUri = tokenUri;
    this.retryCount = retryCount;
  }

  @Override
  public OAuthTokenResponse getToken(OAuthTokenRequest request) {
    return sendAndReceive(request).await().indefinitely();
  }

  private Uni<OAuthTokenResponse> sendAndReceive(OAuthTokenRequest req) {
    HttpRequest<Buffer> request = client.postAbs(tokenUri);
    req.headers().forEach(request::putHeader);
    return request
        .sendBuffer(Buffer.buffer(req.body()))
        .onFailure(OidcCommonUtils.oidcEndpointNotAvailable())
        .retry()
        .atMost(retryCount) // FIXME should we use connection-delay instead?
        .onFailure()
        .transform(this::handleConnectFailure)
        .onItem()
        .transform(this::parseResponse);
  }

  private OAuthTokenResponse parseResponse(HttpResponse<Buffer> resp)
      throws OAuthTokenEndpointException {
    JsonObject json = resp.bodyAsJsonObject();
    if (resp.statusCode() == 200) {
      return OAuthUtils.tokenResponseFromJson(json);
    } else {
      throw OAuthUtils.errorFromJson(json, resp.statusCode());
    }
  }

  private OAuthTokenEndpointException handleConnectFailure(Throwable t) {
    LOGGER.warn(
        "OIDC Server token endpoint is not available at: {}",
        tokenUri,
        t.getCause() != null ? t.getCause() : t);
    // don't wrap it or reference the original error to avoid information leak
    return new OAuthTokenEndpointException(
        503, // service unavailable
        "OAuthTokenEndpointUnavailable",
        "OAuth token endpoint is unavailable");
  }
}
