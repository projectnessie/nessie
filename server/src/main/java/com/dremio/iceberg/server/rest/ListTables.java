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
package com.dremio.iceberg.server.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tables;
import com.dremio.iceberg.server.ServerConfiguration;
import com.dremio.iceberg.server.auth.Secured;

@Path("tables")
@SecurityScheme(
  name = "iceberg-auth",
  type= SecuritySchemeType.HTTP,
  scheme = "bearer",
  bearerFormat = "JWT"
)
public class ListTables {

  private static final Logger logger = LoggerFactory.getLogger(ListTables.class);
  private final ServerConfiguration config;
  private final Backend backend;

  @Inject
  public ListTables(ServerConfiguration config, Backend backend) {
    this.config = config;
    this.backend = backend;
  }

  @GET
  @Metered
  @ExceptionMetered(name="exception-readall")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name="timed-readall")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List all tables",
    tags = {"tables"},
    security = @SecurityRequirement(
      name = "iceberg-auth",
      scopes = "read:tables"),
    responses = {
      @ApiResponse(
        content = @Content(mediaType = "application/json",
          schema = @Schema(implementation = Tables.class))),
      @ApiResponse(responseCode = "400", description = "Unknown Error") }
  )
  public Response getTables(@Parameter(description = "namespace in which to search", required = false)
                              @QueryParam("namespace") String namespace) {
    Tables tables = new Tables(backend.tableBackend().getAll(namespace, false));
    return Response.ok(tables).build();
  }


  @GET
  @Path("{name}")
  @Metered
  @ExceptionMetered(name="exception-read")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name="timed-read")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTable(@PathParam("name") String name, @Context Request request) {
    Table table = backend.tableBackend().get(name);
    if (table == null || table.isDeleted()) {
      return Response.status(404, "table does not exist").build();
    }
    EntityTag eTag = new EntityTag(table.getVersion().toString());
    return Response.ok(table).tag(eTag).build();
  }

  @GET
  @Path("by-name/{name}")
  @Metered
  @ExceptionMetered(name="exception-read-name")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name="timed-read-name")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTableByName(@PathParam("name") String name, @QueryParam("namespace") String namespace) {
    Optional<Table> table = backend.tableBackend().getAll(namespace, false).stream()
      .filter(t -> t.getTableName().equals(name)).findFirst();
    if (table.isPresent() && !table.get().isDeleted()) {
      EntityTag eTag = new EntityTag(table.get().getVersion().toString());
      return Response.ok(table.get()).tag(eTag).build();
    }
    table = backend.tableBackend().getAll(namespace, false).stream()
      .filter(t -> t.getUuid().equals(name)).findFirst();
    if (table.isPresent() && !table.get().isDeleted()) {
      EntityTag eTag = new EntityTag(table.get().getVersion().toString());
      return Response.ok(table.get()).tag(eTag).build();
    }
    return Response.status(404, "table does not exist").build();
  }

  @POST
  @Metered
  @ExceptionMetered(name="exception-create")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name="timed-create")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setTable(Table table) {
    try {
      Optional<Table> tableExisting = backend.tableBackend().getAll(table.getNamespace(), false).stream()
        .filter(t -> t.getTableName().equals(table.getTableName()))
        .filter(t -> StringUtils.compare(t.getNamespace(), table.getNamespace()) == 0).findFirst();
      if (tableExisting.isPresent()) {
        return Response.status(409).build();
      }
      String id = UUID.randomUUID().toString();
      table.setUuid(id);
      table.incrementVersion();
      backend.tableBackend().create(id, table);
      return Response.status(201).header(HttpHeaders.LOCATION, "tables/" + id).build();
    } catch (Throwable t) {
      return Response.status(400, "something went wrong").build();
    }
  }

  @DELETE
  @Metered
  @ExceptionMetered(name="exception-delete")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name="timed-delete")
  @Path("{name}")
  public Response deleteTable(
    @PathParam("name") String name,
    @DefaultValue("false") @QueryParam("purge") boolean purge) {
    try {
      Table table = backend.tableBackend().get(name);
      if (table == null || table.isDeleted()) {
        return Response.status(404).build();
      }
      if (purge) {
        backend.tableBackend().remove(name);
      } else {
        table = backend.tableBackend().get(name);
        table.setDeleted(true);
        backend.tableBackend().update(name, table);
      }
      return Response.status(200).build();
    } catch (Throwable t) {
      return Response.status(404, "something went wrong").build();
    }
  }

  @PUT
  @Metered
  @ExceptionMetered(name="exception-update")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name="timed-update")
  @Path("{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response updateTable(@PathParam("name") String name, @Context Request request, Table table) {
    EntityTag eTag = new EntityTag(backend.tableBackend().get(name).getVersion().toString());
    Response.ResponseBuilder evaluationResultBuilder = request.evaluatePreconditions(eTag);
    if (evaluationResultBuilder == null) {
      try {
        table.incrementVersion();
        backend.tableBackend().update(name, table);
        return Response.status(200).build();
      } catch (Throwable t) {
        return Response.status(404, "something went wrong").build();
      }
    } else {
      return evaluationResultBuilder.status(412, "Tag not up to date").build();
    }
  }
}
