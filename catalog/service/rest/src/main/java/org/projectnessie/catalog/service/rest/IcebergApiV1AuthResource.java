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

import io.quarkus.vertx.web.Route;
import io.quarkus.vertx.web.Route.HttpMethod;
import io.quarkus.vertx.web.RouteBase;
import io.quarkus.vertx.web.RoutingExchange;
import jakarta.annotation.security.PermitAll;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.core.MediaType;

@RequestScoped
@RouteBase(
    path = "iceberg",
    consumes = MediaType.APPLICATION_FORM_URLENCODED,
    produces = MediaType.APPLICATION_JSON)
public class IcebergApiV1AuthResource {

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  IcebergOAuthProxy proxy;

  @POST
  @Route(methods = HttpMethod.POST, path = ":ref/v1/oauth/tokens")
  @PermitAll
  public void getTokenWithRef(RoutingExchange ex) {
    proxy.forwardToTokenEndpoint(ex);
  }

  @POST
  @Route(methods = HttpMethod.POST, path = "v1/oauth/tokens")
  @PermitAll
  public void getToken(RoutingExchange ex) {
    proxy.forwardToTokenEndpoint(ex);
  }
}
