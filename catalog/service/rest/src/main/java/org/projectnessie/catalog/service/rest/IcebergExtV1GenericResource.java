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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;

/**
 * Handles Iceberg REST API v1 endpoints that are not strongly associated with a particular entity
 * type.
 */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg-ext")
public class IcebergExtV1GenericResource {

  @Inject IcebergConfigurer icebergConfigurer;
  @Inject IcebergErrorMapper errorMapper;

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.UNKNOWN);
  }

  @Operation(operationId = "iceberg-ext.v1.trinoConfig")
  @GET
  @Path("/v1/client-template/trino")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getTrinoConfig(
      @QueryParam("warehouse") String warehouse, @QueryParam("format") String format) {
    return getTrinoConfig(null, warehouse, format);
  }

  @Operation(operationId = "iceberg-ext.v1.trinoConfig.reference")
  @GET
  @Path("{reference}/v1/client-template/trino")
  @Produces(MediaType.WILDCARD)
  public Response getTrinoConfig(
      @PathParam("reference") String reference,
      @QueryParam("warehouse") String warehouse,
      @QueryParam("format") String format) {
    return icebergConfigurer.trinoConfig(reference, warehouse, format);
  }
}
