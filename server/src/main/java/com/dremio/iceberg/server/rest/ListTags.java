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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.TableVersion;
import com.dremio.iceberg.model.Tag;
import com.dremio.iceberg.server.ServerConfiguration;
import com.dremio.iceberg.server.auth.Secured;

@Path("tags")
public class ListTags {

  private static final Logger logger = LoggerFactory.getLogger(ListTags.class);
  private ServerConfiguration config;
  private Backend backend;

  @Inject
  public ListTags(ServerConfiguration config, Backend backend) {
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
  public Tag[] getTables() {
    return backend.tagBackend().getAll(null, false).toArray(new Tag[]{});
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
    Tag tag = backend.tagBackend().get(name);
    if (tag == null || tag.isDeleted()) {
      return Response.status(404, "tag does not exist").build();
    }
    EntityTag eTag = new EntityTag(Long.toString(tag.getVersion().orElse(0)));
    return Response.ok(tag).tag(eTag).build();
  }

  @GET
  @Path("by-name/{name}")
  @Metered
  @ExceptionMetered(name="exception-read-name")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name="timed-read-name")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTableByName(@PathParam("name") String name) {
    Optional<Tag> tag = backend.tagBackend().getAll(null, false).stream()
      .filter(t -> t.getName().equals(name)).findFirst();
    if (!tag.isPresent() || tag.get().isDeleted()) {
      return Response.status(404, "tag does not exist").build();
    }
    EntityTag eTag = new EntityTag(Long.toString(tag.get().getVersion().orElse(0)));
    return Response.ok(tag.get()).tag(eTag).build();
  }

  @POST
  @Metered
  @ExceptionMetered(name="exception-create")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name="timed-create")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setTable(Tag tag) {
    try {
      Optional<Tag> tagExisting = backend.tagBackend().getAll(null, false).stream()
        .filter(t -> t.getName().equals(tag.getName()))
        .findFirst();
      if (tagExisting.isPresent()) {
        return Response.status(409).build();
      }
      String id = UUID.randomUUID().toString();
      long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
      Tag newTag = tag.withUpdateTime(updateTime).withId(id);
      if (newTag.getBaseTag() == null) {
        newTag = createFromCurrentState(newTag);
      } else {
        newTag = createFromTag(newTag);
      }
      backend.tagBackend().create(id, newTag);
      return Response.status(201).header(HttpHeaders.LOCATION, "tags/" + id).build();
    } catch (Throwable t) {
      return Response.status(400, "something went wrong").build();
    }
  }

  private Tag createFromCurrentState(Tag tag) {
    List<Table> tables = backend.tableBackend().getAll(false);
    List<TableVersion> currentVersions = tables.stream()
      .map(t -> t.getTableVersion(t.getMetadataLocation()))
      .collect(Collectors.toList());
    return tag.withSnapshots(currentVersions);
  }

  private Tag createFromTag(Tag tag) {
    Tag parentTag = backend.tagBackend().get(tag.getBaseTag());
    return tag.withSnapshots(parentTag.tableSnapshotKeys().stream().map(parentTag::getTableSnapshot).collect(Collectors.toList()));
  }

  private Response verifyAllSnapshot(Map<String, TableVersion> snapshots) {
    for (Map.Entry<String, TableVersion> table: snapshots.entrySet()) {
      Response checkResponse = verifySnapshot(table.getValue());
      if (checkResponse != null) {
        return checkResponse;
      }
    }
    return null;
  }

  private Response verifySnapshot(TableVersion tableVersion) {
    Table table = backend.tableBackend().get(tableVersion.getUuid());
    if (table == null) {
      return Response.status(404, "table " + tableVersion + " is not a valid table id")
        .build();
    }
    boolean found = table.tableVersions().stream().map(table::getTableVersion).anyMatch(t->t.equals(tableVersion));
    if (!found) {
      return Response.status(404,
        "snapshot was not found for table " + tableVersion).build();
    }
    return null;
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
      Tag tag = backend.tagBackend().get(name);
      if (tag == null || tag.isDeleted()) {
        return Response.status(404).build();
      }
      if (purge) {
        backend.tagBackend().remove(name);
      } else {
        Tag newTag = tag
          .withUpdateTime(ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli())
          .withDeleted(true);
        backend.tagBackend().update(name, newTag);
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
  public Response updateTable(@PathParam("name") String name, @Context Request request, Tag tag) {
    EntityTag eTag = new EntityTag(Long.toString(backend.tagBackend().get(name).getVersion().orElse(0L)));
    Response.ResponseBuilder evaluationResultBuilder = request.evaluatePreconditions(eTag);
    if (evaluationResultBuilder == null) {
      try {
        List<Response> checkResponse = tag.tableSnapshotKeys().stream()
            .map(tag::getTableSnapshot)
            .map(this::verifySnapshot)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        if (!checkResponse.isEmpty()) {
          return checkResponse.get(0);
        }
        Tag newTag = tag.withUpdateTime(ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli());
        backend.tagBackend().update(name, newTag);
        return Response.status(200).build();
      } catch (Throwable t) {
        return Response.status(404, "something went wrong").build();
      }
    } else {
      return evaluationResultBuilder.status(412, "Tag not up to date").build();
    }
  }
}
