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

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.iceberg.model.Branch;
import com.dremio.iceberg.model.BranchTable;
import com.dremio.iceberg.model.VersionedWrapper;
import com.dremio.iceberg.server.auth.Secured;
import com.dremio.iceberg.server.jgit.JGitContainer;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Principal;
import java.text.ParseException;
import java.util.Set;
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
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.glassfish.jersey.message.internal.HttpHeaderReader;
import org.glassfish.jersey.message.internal.MatchingEntityTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST endpoint for CRUD operations on tables.
 */
@Path("objects")
@SecurityScheme(
  name = "iceberg-auth",
  type = SecuritySchemeType.HTTP,
  scheme = "bearer",
  bearerFormat = "JWT"
)
public class TableBranch {
  //todo do I need a 'get all tables in a branch?'

  private static final Logger logger = LoggerFactory.getLogger(ListTables.class);
  private final JGitContainer backend;

  @Inject
  public TableBranch(JGitContainer backend) {
    this.backend = backend;
  }

  @GET
  @Metered
  @ExceptionMetered(name = "exception-readall")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall")
  @Produces(MediaType.APPLICATION_JSON)
  public Response tags() {
    try {
      return Response.ok(backend.getBranches().toArray(new Branch[0])).build();
    } catch (IOException e) {
      return exception(e);
    }
  }

  @GET
  @Path("{branch}")
  public Response branch(@PathParam("branch") String branchName) {
    try {
      VersionedWrapper<Branch> branch = backend.getBranch(branchName);
      if (branch == null) {
        return Response.status(404).entity("branch not found").build();
      }
      EntityTag entityTag = new EntityTag(Long.toString(branch.getVersion().orElse(0)));
      return Response.ok(branch.getObj()).tag(entityTag).build();
    } catch (IOException e) {
      return exception(e);
    }
  }

  @GET
  @Path("{branch}/{table}")
  public Response branchTable(@PathParam("branch") String branch,
                              @PathParam("table") String tableName) {
    try {
      VersionedWrapper<BranchTable> table = backend.getTable(branch, tableName);
      if (table == null) {
        return Response.status(404).entity("table not found on branch").build();
      }
      EntityTag entityTag = new EntityTag(Long.toString(table.getVersion().orElse(0)));
      return Response.ok(table.getObj()).tag(entityTag).build();
    } catch (IOException e) {
      return exception(e);
    }
  }

  @POST
  @Path("{branch}")
  public Response createBranch(@PathParam("branch") String branchName,
                               @Context SecurityContext securityContext,
                               Branch branch) {
    try {
      //todo check if exists
      backend.create(branchName, branch.getId(), securityContext.getUserPrincipal());
      return Response.ok().build();
    } catch (IOException e) {
      return exception(e);
    }

  }

  @POST
  @Metered
  @ExceptionMetered(name = "exception-update")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-update")
  @Path("{branch}/{table}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createTable(@PathParam("branch") String branch,
                              @PathParam("table") String tableName,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers,
                              BranchTable table) {
    //todo check if exists
    return singleCommit(branch, tableName, securityContext, headers, table);
  }

  private Response singleCommit(String branch,
                                String tableName,
                                SecurityContext securityContext,
                                HttpHeaders headers,
                                BranchTable table) {
    Long ifMatch = version(headers);
    if (ifMatch == null || ifMatch < 0) {
      return Response.status(412, "Tag not up to date").build();
    }
    return update(tableName, branch, table, securityContext.getUserPrincipal(), ifMatch);
  }

  @DELETE
  @Path("{branch}")
  public Response deleteBranch(@PathParam("branch") String branch,
                               @DefaultValue("false") @QueryParam("purge") boolean purge) {
    throw new UnsupportedOperationException("NYI");
  }

  @DELETE
  @Path("{branch}/{table}")
  public Response deleteTable(@PathParam("branch") String branch,
                              @PathParam("table") String table,
                              @DefaultValue("false") @QueryParam("purge") boolean purge) {
    throw new UnsupportedOperationException("NYI");
  }

  @PUT
  @Path("{branch}")
  public Response updateBatch(@PathParam("branch") String branch,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers,
                              BranchTable[] batchUpdate) {
    try {
      //todo check if exists
      Long ifMatch = version(headers);
      if (ifMatch == null || ifMatch < 0) {
        return Response.status(412, "Tag not up to date").build();
      }
      backend.commit(branch, securityContext.getUserPrincipal(), ifMatch, batchUpdate);
      return Response.ok().build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  @PUT
  @Path("{branch}/{table}")
  public Response update(@PathParam("branch") String branch,
                         @PathParam("table") String table,
                         @Context SecurityContext securityContext,
                         @Context HttpHeaders headers,
                         BranchTable update) {
    //todo check if exists
    return singleCommit(branch, table, securityContext, headers, update);
  }

  private Response update(String table,
                          String branch,
                          BranchTable branchTable,
                          Principal principal,
                          Long ifMatch) {
    if (!table.equals(branchTable.getId())) {
      return Response.status(404)
                     .entity("Can't update this table, table update is not correct")
                     .build();
    }
    try {
      backend.commit(branch, principal, ifMatch, branchTable);
      return Response.ok().build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  private static Long version(HttpHeaders headers) {
    try {
      String ifMatch = headers.getHeaderString(HttpHeaders.IF_MATCH);
      Set<MatchingEntityTag> ifMatchCondition = HttpHeaderReader
        .readMatchingEntityTag(ifMatch);
      return ifMatchCondition.stream()
                             .findFirst()
                             .map(x -> Long.parseLong(x.getValue()))
                             .orElse(-1L);
    } catch (ParseException e) {
      return null;
    }
  }

  private static String exceptionString(Exception e) {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  private static Response exception(Exception e) {
    String exceptionAsString = exceptionString(e);
    return Response.status(500)
                   .entity(Entity.entity(exceptionAsString, MediaType.APPLICATION_JSON_TYPE))
                   .build();
  }
}
