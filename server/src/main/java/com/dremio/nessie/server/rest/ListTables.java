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

package com.dremio.nessie.server.rest;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.ImmutableTableVersion;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.model.TableVersion;
import com.dremio.nessie.model.VersionedWrapper;
import com.dremio.nessie.server.auth.Secured;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST endpoint for CRUD operations on tables.
 */
@Path("tables")
@SecurityScheme(
    name = "nessie-auth",
    type = SecuritySchemeType.HTTP,
    scheme = "bearer",
    bearerFormat = "JWT"
)
public class ListTables {

  private static final Logger logger = LoggerFactory.getLogger(ListTables.class);
  private final Backend backend;

  @Inject
  public ListTables(Backend backend) {
    this.backend = backend;
  }

  @GET
  @Metered
  @ExceptionMetered(name = "exception-readall")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List all tables",
      tags = {"tables"},
      security = @SecurityRequirement(
        name = "nessie-auth",
        scopes = "read:tables"),
      responses = {
        @ApiResponse(
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = Table[].class))),
        @ApiResponse(responseCode = "400", description = "Unknown Error")}
  )
  public Table[] getTables(
      @Parameter(description = "namespace in which to search")
      @QueryParam("namespace") String namespace) {
    return backend.tableBackend()
                  .getAll(namespace, false)
                  .stream()
                  .map(VersionedWrapper::getObj)
                  .collect(Collectors.toList())
                  .toArray(new Table[]{});
  }


  @GET
  @Path("{name}")
  @Metered
  @ExceptionMetered(name = "exception-read")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-read")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTable(@PathParam("name") String name) {
    VersionedWrapper<Table> table = backend.tableBackend().get(name);
    if (table == null || table.getObj().isDeleted()) {
      return Response.status(404, "table does not exist").build();
    }
    EntityTag entityTag = new EntityTag(Long.toString(table.getVersion().orElse(0)));
    return Response.ok(table).tag(entityTag).build();
  }

  @GET
  @Path("by-name/{name}")
  @Metered
  @ExceptionMetered(name = "exception-read-name")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-read-name")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTableByName(@PathParam("name") String name,
                                 @QueryParam("namespace") String namespace) {
    Optional<VersionedWrapper<Table>> table = backend.tableBackend()
                                                     .getAll(name, namespace, false)
                                                     .stream()
                                                     .filter(t -> t.getObj()
                                                                   .getTableName()
                                                                   .equals(name))
                                                     .findFirst();
    if (table.isPresent() && !table.get().getObj().isDeleted()) {
      EntityTag entityTag = new EntityTag(Long.toString(table.get().getVersion().orElse(0)));
      return Response.ok(table.get()).tag(entityTag).build();
    }
    table = backend.tableBackend().getAll(namespace, false).stream()
                   .filter(t -> t.getObj().getId().equals(name)).findFirst();
    if (table.isPresent() && !table.get().getObj().isDeleted()) {
      EntityTag entityTag = new EntityTag(Long.toString(table.get().getVersion().orElse(0)));
      return Response.ok(table.get()).tag(entityTag).build();
    }
    return Response.status(404, "table does not exist").build();
  }

  @POST
  @Metered
  @ExceptionMetered(name = "exception-create")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-create")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setTable(Table table) {
    try {
      Optional<VersionedWrapper<Table>> tableExisting;
      tableExisting = backend.tableBackend()
                             .getAll(table.getNamespace(), false)
                             .stream()
                             .filter(t -> t.getObj()
                                           .getTableName()
                                           .equals(table.getTableName()))
                             .filter(t -> Objects.equals(t.getObj()
                                                          .getNamespace(),
                                                         table.getNamespace()))
                             .findFirst();
      if (tableExisting.isPresent()) {
        return Response.status(409).build();
      }
      if (table.getMetadataLocation() == null) {
        return Response.status(400, "Table must have a metadata location").build();
      }
      String id = UUID.randomUUID().toString();
      Table newTable = updateVersions(ImmutableTable.builder().from(table).id(id),
                                      table,
                                      id).build();
      backend.tableBackend().create(id, new VersionedWrapper<>(newTable, null));
      return Response.status(201).header(HttpHeaders.LOCATION, "tables/" + id).build();
    } catch (Throwable t) {
      logger.error("unable to complete create", t);
      return Response.status(400, "something went wrong").build();
    }
  }

  private ImmutableTable.Builder updateVersions(ImmutableTable.Builder tableBuilder,
                                                Table table,
                                                String id) {
    long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
    tableBuilder.updateTime(updateTime);
    Map<String, TableVersion> versions = new HashMap<>(table.getVersionList());
    List<String> lastUpdate = versions.entrySet()
                                      .stream()
                                      .filter(x -> x.getValue().getEndTime().isPresent())
                                      .map(Entry::getKey)
                                      .collect(Collectors.toList());
    TableVersion newVersion = ImmutableTableVersion.builder()
                                                   .uuid(id)
                                                   .metadataLocation(table.getMetadataLocation())
                                                   .createTime(updateTime)
                                                   .snapshotId(table.getSnapshots().isEmpty()
                                                                 ? OptionalLong.empty()
                                                                 : OptionalLong.of(
                                                                   table.getSnapshots()
                                                                        .get(table.getSnapshots()
                                                                                  .size() - 1)
                                                                        .getSnapshotId()))
                                                   .build();
    if (lastUpdate.isEmpty()) {
      versions.put(table.getMetadataLocation(), newVersion);
    } else if (lastUpdate.size() == 1) {
      versions.put(table.getMetadataLocation(), newVersion);
      TableVersion lastVersion = versions.get(lastUpdate.get(0));
      TableVersion finishedVersion = ImmutableTableVersion.copyOf(lastVersion)
                                                          .withEndTime(updateTime);
      versions.put(lastUpdate.get(0), finishedVersion);
    } else {
      throw new UnsupportedOperationException("version map is in a bad state");
    }
    return tableBuilder.versionList(versions);
  }

  @DELETE
  @Metered
  @ExceptionMetered(name = "exception-delete")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-delete")
  @Path("{name}")
  public Response deleteTable(
      @PathParam("name") String name,
      @DefaultValue("false") @QueryParam("purge") boolean purge) {
    try {
      //todo make respect etag (same for other
      VersionedWrapper<Table> table = backend.tableBackend().get(name);
      if (table == null || table.getObj().isDeleted()) {
        return Response.status(404).build();
      }
      if (purge) {
        backend.tableBackend().remove(name);
      } else {
        long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
        Table newTable = ImmutableTable.builder().from(table.getObj())
                                       .updateTime(updateTime)
                                       .isDeleted(true)
                                       .build();
        backend.tableBackend().update(name, table.update(newTable));
      }
      return Response.status(200).build();
    } catch (Throwable t) {
      logger.error("unable to complete delete", t);
      return Response.status(400, "something went wrong").build();
    }
  }

  @PUT
  @Metered
  @ExceptionMetered(name = "exception-update")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-update")
  @Path("{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response updateTable(@PathParam("name") String name, @Context Request request,
                              Table table) {
    VersionedWrapper<Table> currentTable = backend.tableBackend().get(name);
    if (currentTable == null) {
      return Response.status(404, "table not found").build();
    }
    EntityTag entityTag = new EntityTag(Long.toString(currentTable.getVersion().orElse(0)));
    Response.ResponseBuilder evaluationResultBuilder = request.evaluatePreconditions(entityTag);
    if (evaluationResultBuilder == null) {
      try {
        Table newTable = updateVersions(ImmutableTable.builder().from(table),
                                        table,
                                        table.getId()).build();
        backend.tableBackend().update(name, currentTable.update(newTable));
        return Response.status(200).build();
      } catch (Throwable t) {
        logger.error("Unable to complete update", t);
        return Response.status(400, "something went wrong").build();
      }
    } else {
      return evaluationResultBuilder.status(412, "Tag not up to date").build();
    }
  }
}
