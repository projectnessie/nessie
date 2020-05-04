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
import com.dremio.nessie.jgit.JGitContainer;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.BranchTable;
import com.dremio.nessie.model.HeadVersionPair;
import com.dremio.nessie.model.ImmutableBranchTable;
import com.dremio.nessie.model.VersionedStringWrapper;
import com.dremio.nessie.server.auth.Secured;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Principal;
import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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
  name = "nessie-auth",
  type = SecuritySchemeType.HTTP,
  scheme = "bearer",
  bearerFormat = "JWT"
)
public class TableBranch {
  //todo do I need a 'get all tables in a branch?'
  //todo git like log

  private static final Logger logger = LoggerFactory.getLogger(ListTables.class);
  private static final Joiner SLASH = Joiner.on("/");
  private static final Splitter UNSLASH = Splitter.on("/");
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
  public Response branch(@PathParam("branch") String branchName,
                         @DefaultValue("false") @QueryParam("showtables") boolean tables,
                         @DefaultValue("all") @QueryParam("namespace") String namespace) {
    try {
      VersionedStringWrapper<Branch> branch = backend.getBranch(branchName);
      if (branch == null) {
        return Response.status(404).entity("branch not found").build();
      }
      if (!tables) {
        return Response.ok(branch.getObj()).tag(tagFromTable(branch)).build();
      }
      List<String> tableList = backend.getTables(branch.getObj().getId(), namespace.equals("all")
      ? null : namespace);
      return Response.ok(tableList.toArray(new String[0])).build();
    } catch (IOException e) {
      return exception(e);
    }
  }

  private static EntityTag tagFromTable(HeadVersionPair obj) {
    String version = Long.toString(obj.getVersion());
    String commit = obj.getHead();
    return new EntityTag(SLASH.join(commit, version));
  }

  private static <T> EntityTag tagFromTable(VersionedStringWrapper<T> obj) {
    String version = Long.toString(obj.getVersion().orElse(0));
    String commit = obj.getVersionString();
    return new EntityTag(SLASH.join(commit, version));
  }

  @GET
  @Path("{branch}/{table}")
  public Response branchTable(@PathParam("branch") String branch,
                              @PathParam("table") String tableName) {
    try {
      VersionedStringWrapper<BranchTable> table = backend.getTable(branch, tableName);
      if (table == null) {
        return Response.status(404).entity("table not found on branch").build();
      }
      return Response.ok(table.getObj()).tag(tagFromTable(table)).build();
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
      if (backend.getBranch(branchName) != null) {
        return Response.status(404).entity("branch " + branch + " already exist").build();
      }
      VersionedStringWrapper<Branch> newBranch = backend.create(branchName,
                                                                branch.getId(),
                                                                securityContext.getUserPrincipal());
      return Response.created(null).tag(tagFromTable(newBranch)).build(); //todo URI
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
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      if (backend.getTable(branch, tableName) != null) {
        return Response.status(404)
                       .entity("table " + tableName + " already exists on " + branch)
                       .build();
      }
    } catch (IOException e) {
      return exception(e);
    }
    return singleCommit(branch, tableName, securityContext, headers, table, true);
  }

  private Response singleCommit(String branch,
                                String tableName,
                                SecurityContext securityContext,
                                HttpHeaders headers,
                                BranchTable table,
                                boolean post) {
    HeadVersionPair ifMatch = version(headers);
    if (ifMatch == null || ifMatch.getVersion() < 0) {
      return Response.status(412, "Tag not up to date").build();
    }
    return update(tableName, branch, table, securityContext.getUserPrincipal(), ifMatch, post);
  }

  @DELETE
  @Path("{branch}")
  public Response deleteBranch(@PathParam("branch") String branch,
                               @DefaultValue("false") @QueryParam("purge") boolean purge,
                               @Context SecurityContext securityContext,
                               @Context HttpHeaders headers) {
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      HeadVersionPair ifMatch = version(headers);
      if (ifMatch == null || ifMatch.getVersion() < 0) {
        return Response.status(412, "Tag not up to date").build();
      }
      backend.deleteBranch(branch, ifMatch, purge);
      return Response.ok().build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  @DELETE
  @Path("{branch}/{table}")
  public Response deleteTable(@PathParam("branch") String branch,
                              @PathParam("table") String table,
                              @DefaultValue("false") @QueryParam("purge") boolean purge,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers) {
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      VersionedStringWrapper<BranchTable> branchTable = backend.getTable(branch, table);
      if (backend.getTable(branch, table) == null) {
        return Response.status(404)
                       .entity("table " + table + " does not exists on " + branch)
                       .build();
      }
      HeadVersionPair ifMatch = version(headers);
      if (ifMatch == null || ifMatch.getVersion() < 0) {
        return Response.status(412, "Tag not up to date").build();
      }
      ImmutableBranchTable deletedTable = ImmutableBranchTable.builder()
                                                              .from(branchTable.getObj())
                                                              .isDeleted(true)
                                                              .build();
      backend.commit(branch, securityContext.getUserPrincipal(), ifMatch, deletedTable);
      return Response.ok().build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  @PUT
  @Path("{branch}")
  public Response updateBatch(@PathParam("branch") String branch,
                              @DefaultValue("null") @QueryParam("promote") String mergeBranch,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers,
                              BranchTable[] batchUpdate) {
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      HeadVersionPair ifMatch = version(headers);
      if (ifMatch == null || ifMatch.getVersion() < 0) {
        return Response.status(412, "Tag not up to date").build();
      }
      if (mergeBranch != null && !mergeBranch.equals("null")) {
        HeadVersionPair result = backend.promote(branch, mergeBranch, ifMatch);
        return Response.ok().tag(tagFromTable(result)).build();
      }
      VersionedStringWrapper<BranchTable> hV = backend.commit(branch,
                                                              securityContext.getUserPrincipal(),
                                                              ifMatch,
                                                              batchUpdate);
      return Response.ok().tag(tagFromTable(hV)).build();
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
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      if (backend.getTable(branch, table) == null) {
        return Response.status(404)
                       .entity("table " + table + " does not exists on " + branch)
                       .build();
      }
    } catch (IOException e) {
      return exception(e);
    }
    return singleCommit(branch, table, securityContext, headers, update, false);
  }

  private Response update(String table,
                          String branch,
                          BranchTable branchTable,
                          Principal principal,
                          HeadVersionPair ifMatch,
                          boolean post) {
    if (!table.equals(branchTable.getId())) {
      return Response.status(404)
                     .entity("Can't update this table, table update is not correct")
                     .build();
    }
    try {
      VersionedStringWrapper<BranchTable> headVersion = backend.commit(branch,
                                                                       principal,
                                                                       ifMatch,
                                                                       branchTable);
      if (post) {
        return Response.created(null).tag(tagFromTable(headVersion)).build(); //todo uri
      }
      return Response.ok().tag(tagFromTable(headVersion)).build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  private static HeadVersionPair version(HttpHeaders headers) {
    try {
      String ifMatch = headers.getHeaderString(HttpHeaders.IF_MATCH);
      Set<MatchingEntityTag> ifMatchCondition = HttpHeaderReader
        .readMatchingEntityTag(ifMatch);
      MatchingEntityTag headVersion = ifMatchCondition.stream()
                                                      .findFirst()
                                                      .orElse(null);
      Iterable<String> parts = UNSLASH.split(headVersion.getValue());
      Iterator<String> iter = parts.iterator();
      String head = iter.next();
      String version = iter.next();
      return new HeadVersionPair(head, Long.parseLong(version));
    } catch (ParseException | NullPointerException | NoSuchElementException e) {
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
