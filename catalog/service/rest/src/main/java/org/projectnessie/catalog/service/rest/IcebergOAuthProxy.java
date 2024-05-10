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
package org.projectnessie.catalog.service.rest;

import io.quarkus.vertx.web.RoutingExchange;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.util.Optional;

public interface IcebergOAuthProxy {

  IcebergOAuthProxy DISABLED =
      new IcebergOAuthProxy() {

        @Override
        public Optional<URI> resolvedTokenEndpoint() {
          return Optional.empty();
        }

        @Override
        public void forwardToTokenEndpoint(RoutingExchange ex) {
          ex.response()
              .setStatusCode(418)
              .end(
                  new JsonObject()
                      .put("error", "OAuthTokenEndpointUnavailable")
                      .put(
                          "error_description",
                          "Authentication tokens should be obtained from the configured external provider.")
                      .toBuffer());
        }
      };

  /**
   * The URI of the token endpoint, or empty if authentication is disabled. This method may block if
   * authentication is enabled and URI discovery must be performed.
   */
  Optional<URI> resolvedTokenEndpoint();

  /** Forward the incoming request to the OAuth token endpoint. */
  void forwardToTokenEndpoint(RoutingExchange ex);
}
