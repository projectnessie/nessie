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

import java.util.Objects;
import java.util.Optional;
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
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tables;
import com.dremio.iceberg.server.Configuration;
import com.dremio.iceberg.server.auth.Secured;
import com.dremio.iceberg.server.db.Backend;

@Path("tables")
public class ListTables {

  private static final Logger logger = LoggerFactory.getLogger(ListTables.class);
  private Configuration config;
  private Backend backend;

  @Inject
  public ListTables(Configuration config, Backend backend) {
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
  public Response getTables(@QueryParam("namespace") String namespace) {
    System.out.println("FOOBAR!" + namespace);
    Tables tables = new Tables(backend.getAll()
        .getTables()
        .stream()
        .filter(t -> !t.isDeleted())
        .filter(t -> {
          System.out.println("MONKEY " + t.toString());
          if (namespace == null) {
            return true;
          }
          return StringUtils.compare(namespace, t.getNamespace()) == 0;
        })
        .collect(Collectors.toList()));
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
    Table table = backend.get(name);
    if (table == null || table.isDeleted()) {
      return Response.status(404, "table does not exist").build();
    }
    EntityTag eTag = new EntityTag(Integer.toString(table.hashCode()));
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
    Optional<Table> table = backend.getAll().getTables().stream()
      .filter(t -> !t.isDeleted())
      .filter(t -> t.getTableName().equals(name))
      .filter(t -> StringUtils.compare(t.getNamespace(), namespace) == 0).findFirst();
    if (!table.isPresent() || table.get().isDeleted()) {
      return Response.status(404, "table does not exist").build();
    }
    EntityTag eTag = new EntityTag(Integer.toString(table.hashCode()));
    return Response.ok(table.get()).tag(eTag).build();
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
      Optional<Table> tableExisting = backend.getAll().getTables().stream()
        .filter(t -> !t.isDeleted())
        .filter(t -> t.getTableName().equals(table.getTableName()))
        .filter(t -> StringUtils.compare(t.getNamespace(), table.getNamespace()) == 0).findFirst();
      if (tableExisting.isPresent()) {
        return Response.status(409).build();
      }
      String id = UUID.randomUUID().toString();
      table.setUuid(id);
      backend.create(id, table);
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
      if (backend.get(name) == null || backend.get(name).isDeleted()) {
        throw new RuntimeException();
      }
      if (purge) {
        backend.remove(name);
      } else {
        Table table = backend.get(name);
        table.setDeleted(true);
        backend.update(name, table);
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
    EntityTag eTag = new EntityTag(Integer.toString(backend.get(name).hashCode()));
    Response.ResponseBuilder evaluationResultBuilder = request.evaluatePreconditions(eTag);
    if (evaluationResultBuilder == null) {
      try {
        backend.update(name, table);
        return Response.status(200).build();
      } catch (Throwable t) {
        return Response.status(404, "something went wrong").build();
      }
    } else {
      return evaluationResultBuilder.status(412, "Tag not up to date").build();
    }
  }
}
