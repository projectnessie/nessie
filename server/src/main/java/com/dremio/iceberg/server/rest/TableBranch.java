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
import com.dremio.iceberg.server.auth.Secured;
import com.dremio.iceberg.server.jgit.JGitContainer;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import java.io.IOException;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  @GET
  @Path("{branch}")
  public Response branch(@PathParam("branch") String branchName) {
    try {
      Branch branch = backend.getBranch(branchName);
      if (branch == null) {
        return Response.status(404).entity("branch not found").build();
      }
      return Response.ok(branch).build();
    } catch (IOException e) {
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  @GET
  @Path("{branch}/{table}")
  public Response branchTable(@PathParam("branch") String branch,
                                 @PathParam("table") String tableName) {
    try {
      BranchTable table = backend.getTable(branch, tableName);
      if (table == null) {
        return Response.status(404).entity("table not found on branch").build();
      }
      return Response.ok(table).build();
    } catch (IOException e) {
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  @POST
  @Path("{branch}")
  public Response createBranch(@PathParam("branch") String branchName,
                               Branch branch) {
    try {
      backend.create(branchName);
      return Response.ok().build();
    } catch (IOException e) {
      return Response.status(500).entity(e.getMessage()).build();
    }

  }

  @POST
  @Path("{branch}/{table}")
  public Response createTable(@PathParam("branch") String branch,
                              @PathParam("table") String tableName,
                              BranchTable table) {
    try {
      backend.commit(branch, tableName, table);
      return Response.ok().build();
    } catch (IOException e) {
      return Response.status(500).entity(e.getMessage()).build();
    }

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
  public Response updateBatch(@PathParam("branch") String branch, Object batchUpdate) {
    throw new UnsupportedOperationException("NYI");
  }

  @PUT
  @Path("{branch}/{table}")
  public Response update(@PathParam("branch") String branch,
                         @PathParam("table") String table,
                         Object update) {
    throw new UnsupportedOperationException("NYI");
  }
}
