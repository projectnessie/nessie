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
import com.dremio.nessie.auth.User;
import com.dremio.nessie.backend.BranchController;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.CommitMeta.Action;
import com.dremio.nessie.model.ImmutableCommitMeta;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.server.auth.Secured;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Principal;
import java.text.ParseException;
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
public class TableBranchOperations {
  //todo git like log

  private static final Logger logger = LoggerFactory.getLogger(TableBranchOperations.class);
  private final BranchController backend;

  @Inject
  public TableBranchOperations(BranchController backend) {
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
  @Metered
  @ExceptionMetered(name = "exception-readall-tables")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall-tables")
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{branch}/tables")
  public Response branch(@PathParam("branch") String branchName,
                         @DefaultValue("all") @QueryParam("namespace") String namespace) {
    try {
      Branch branch = backend.getBranch(branchName);
      if (branch == null) {
        return Response.status(404).entity("branch not found").build();
      }
      List<String> tableList = backend.getTables(branch.getId(), namespace.equals("all")
          ? null : namespace);
      return Response.ok(tableList.toArray(new String[0])).build();
    } catch (IOException e) {
      return exception(e);
    }
  }

  @GET
  @Metered
  @ExceptionMetered(name = "exception-readall-branches")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall-branches")
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{branch}")
  public Response branchTables(@PathParam("branch") String branchName,
                               @DefaultValue("all") @QueryParam("namespace") String namespace) {
    try {
      Branch branch = backend.getBranch(branchName);
      if (branch == null) {
        return Response.status(404).entity("branch not found").build();
      }
      return Response.ok(branch).tag(tagFromTable(branch)).build();
    } catch (IOException e) {
      return exception(e);
    }
  }

  private static EntityTag tagFromTable(String obj) {
    return new EntityTag(obj);
  }

  private static EntityTag tagFromTable(Branch obj) {
    return new EntityTag(obj.getId());
  }

  @GET
  @Metered
  @ExceptionMetered(name = "exception-readall-table")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall-table")
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{branch}/{table}")
  public Response branchTable(@PathParam("branch") String branch,
                              @PathParam("table") String tableName,
                              @DefaultValue("false") @QueryParam("metadata") boolean metadata) {
    try {
      Table table = backend.getTable(branch, tableName, metadata);
      if (table == null) {
        return Response.status(404).entity("table not found on branch").build();
      }
      return Response.ok(table).build();
    } catch (IOException e) {
      return exception(e);
    }
  }

  @POST
  @Metered
  @ExceptionMetered(name = "exception-readall-branch")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall-branch")
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{branch}")
  public Response createBranch(@PathParam("branch") String branchName,
                               @Context SecurityContext securityContext,
                               @DefaultValue("unknown") @QueryParam("reason") String reason,
                               Branch branch) {
    try {
      if (backend.getBranch(branchName) != null) {
        return Response.status(404).entity("branch " + branch + " already exist").build();
      }
      Branch newBranch = backend.create(branchName,
                                        branch.getId(),
                                        meta(securityContext.getUserPrincipal(),
                                             reason,
                                             1,
                                             branchName,
                                             Action.CREATE_BRANCH));
      return Response.created(null).tag(tagFromTable(newBranch)).build();
    } catch (IOException e) {
      return exception(e);
    }

  }

  @POST
  @Metered
  @ExceptionMetered(name = "exception-create-table")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-create-table")
  @Path("{branch}/{table}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createTable(@PathParam("branch") String branch,
                              @PathParam("table") String tableName,
                              @DefaultValue("unknown") @QueryParam("reason") String reason,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers,
                              Table table) {
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      if (backend.getTable(branch, tableName, false) != null) {
        return Response.status(404)
                       .entity("table " + tableName + " already exists on " + branch)
                       .build();
      }
    } catch (IOException e) {
      return exception(e);
    }
    return singleCommit(branch, tableName, securityContext, headers, table, reason, true);
  }

  private Response singleCommit(String branch,
                                String tableName,
                                SecurityContext securityContext,
                                HttpHeaders headers,
                                Table table,
                                String reason,
                                boolean post) {
    String ifMatch = version(headers);
    if (ifMatch == null) {
      return Response.status(412, "Tag not up to date").build();
    }
    Principal principal = securityContext.getUserPrincipal();
    return update(tableName, branch, table, principal, ifMatch, reason, post);
  }

  @DELETE
  @Metered
  @ExceptionMetered(name = "exception-delete-branch")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-delete-branch")
  @Path("{branch}")
  public Response deleteBranch(@PathParam("branch") String branch,
                               @DefaultValue("false") @QueryParam("purge") boolean purge,
                               @DefaultValue("unknown") @QueryParam("reason") String reason,
                               @Context SecurityContext securityContext,
                               @Context HttpHeaders headers) {
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      String ifMatch = version(headers);
      if (ifMatch == null) {
        return Response.status(412, "Tag not up to date").build();
      }
      backend.deleteBranch(branch, ifMatch, purge, meta(securityContext.getUserPrincipal(),
                                                        reason,
                                                        1,
                                                        branch,
                                                        Action.DELETE_BRANCH));
      return Response.ok().build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  @DELETE
  @Metered
  @ExceptionMetered(name = "exception-delete-table")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-delete-table")
  @Path("{branch}/{table}")
  public Response deleteTable(@PathParam("branch") String branch,
                              @PathParam("table") String table,
                              @DefaultValue("unknown") @QueryParam("reason") String reason,
                              @DefaultValue("false") @QueryParam("purge") boolean purge,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers) {
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      Table branchTable = backend.getTable(branch, table, false);
      if (branchTable == null) {
        return Response.status(404)
                       .entity("table " + table + " does not exists on " + branch)
                       .build();
      }
      String ifMatch = version(headers);
      if (ifMatch == null) {
        return Response.status(412, "Tag not up to date").build();
      }
      ImmutableTable deletedTable = ImmutableTable.builder()
                                                  .from(branchTable)
                                                  .isDeleted(true)
                                                  .build();
      backend.commit(branch, meta(securityContext.getUserPrincipal(),
                                  reason + ";" + table,
                                  1,
                                  branch,
                                  Action.COMMIT
      ), ifMatch, deletedTable);
      return Response.ok().build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  @PUT
  @Metered
  @ExceptionMetered(name = "exception-promote")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-promote")
  @Path("{branch}/cherry-pick")
  public Response cpBranch(@PathParam("branch") String branch,
                           @Context SecurityContext securityContext,
                           @Context HttpHeaders headers,
                           @DefaultValue("unknown") @QueryParam("reason") String reason,
                           @QueryParam("promote") String mergeBranch,
                           @QueryParam("namespace") String namespace) {
    try {
      if (mergeBranch == null) {
        return Response.status(401).entity("branch to cherry pick from is null").build();
      }
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      String ifMatch = version(headers);
      if (ifMatch == null) {
        return Response.status(412, "Tag not up to date").build();
      }
      String result = backend.promote(branch,
                                      mergeBranch,
                                      ifMatch,
                                      meta(securityContext.getUserPrincipal(),
                                           reason + ";" + mergeBranch,
                                           1,
                                           branch,
                                           Action.CHERRY_PICK),
                                      false,
                                      true,
                                      namespace);
      return Response.ok().tag(tagFromTable(result)).build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  @PUT
  @Metered
  @ExceptionMetered(name = "exception-cherry-pick")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-cherry-pick")
  @Path("{branch}/promote")
  public Response promoteBranch(@PathParam("branch") String branch,
                                @Context SecurityContext securityContext,
                                @Context HttpHeaders headers,
                                @DefaultValue("unknown") @QueryParam("reason") String reason,
                                @QueryParam("promote") String mergeBranch,
                                @DefaultValue("false") @QueryParam("force") boolean force) {
    try {
      if (mergeBranch == null) {
        return Response.status(401).entity("branch to merge from is null").build();
      }
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      String ifMatch = version(headers);
      if (ifMatch == null) {
        return Response.status(412, "Tag not up to date").build();
      }
      String result = backend.promote(branch,
                                      mergeBranch,
                                      ifMatch,
                                      meta(securityContext.getUserPrincipal(),
                                           reason + ";" + mergeBranch,
                                           1,
                                           branch,
                                           force ? Action.FORCE_MERGE : Action.MERGE),
                                      force,
                                      false,
                                      null);
      return Response.ok().tag(tagFromTable(result)).build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  @PUT
  @Metered
  @ExceptionMetered(name = "exception-commit")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-commit")
  @Path("{branch}")
  public Response updateBatch(@PathParam("branch") String branch,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers,
                              @DefaultValue("unknown") @QueryParam("reason") String reason,
                              Table[] batchUpdate) {
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      String ifMatch = version(headers);
      if (ifMatch == null) {
        return Response.status(412, "Tag not up to date").build();
      }
      String headVersion = backend.commit(branch,
                                          meta(securityContext.getUserPrincipal(),
                                            reason,
                                            batchUpdate.length,
                                            branch,
                                            Action.COMMIT),
                                          ifMatch,
                                          batchUpdate);
      return Response.ok().tag(tagFromTable(headVersion)).build();
    } catch (IOException e) {
      return exception(e);
    } catch (IllegalStateException e) {
      return Response.status(412, "Tag not up to date" + exceptionString(e)).build();
    }
  }

  @PUT
  @Metered
  @ExceptionMetered(name = "exception-commit-table")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-commit-table")
  @Path("{branch}/{table}")
  public Response update(@PathParam("branch") String branch,
                         @PathParam("table") String table,
                         @DefaultValue("unknown") @QueryParam("reason") String reason,
                         @Context SecurityContext securityContext,
                         @Context HttpHeaders headers,
                         Table update) {
    try {
      if (backend.getBranch(branch) == null) {
        return Response.status(404).entity("branch " + branch + " does not exist").build();
      }
      if (backend.getTable(branch, table, false) == null) {
        return Response.status(404)
                       .entity("table " + table + " does not exists on " + branch)
                       .build();
      }
    } catch (IOException e) {
      return exception(e);
    }
    return singleCommit(branch, table, securityContext, headers, update, reason, false);
  }

  private Response update(String table,
                          String branch,
                          Table branchTable,
                          Principal principal,
                          String ifMatch,
                          String reason,
                          boolean post) {
    if (!table.equals(branchTable.getId())) {
      return Response.status(404)
                     .entity("Can't update this table, table update is not correct")
                     .build();
    }
    try {
      String headVersion = backend.commit(branch,
                                          meta(principal,
                                               reason + ";" + table,
                                               1,
                                               branch,
                                               Action.COMMIT),
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

  private static String version(HttpHeaders headers) {
    try {
      String ifMatch = headers.getHeaderString(HttpHeaders.IF_MATCH);
      Set<MatchingEntityTag> ifMatchCondition = HttpHeaderReader
          .readMatchingEntityTag(ifMatch);
      MatchingEntityTag headVersion = ifMatchCondition.stream()
                                                      .findFirst()
                                                      .orElse(null);
      return headVersion.getValue();
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

  private CommitMeta meta(Principal principal,
                                       String comment,
                                       int changes,
                                       String branch,
                                       Action action) {
    return ImmutableCommitMeta.builder()
                              .email(email(principal))
                              .commiter(name(principal))
                              .comment(comment)
                              .changes(changes)
                              .branch(branch)
                              .action(action)
                              .build();
  }

  private String name(Principal principal) {
    return principal == null ? "" : principal.getName();
  }

  private String email(Principal principal) {
    try {
      User user = (User) principal;
      String email = user.email();
      return email == null ? "" : email;
    } catch (Exception e) {
      logger.warn("unable to cast principal {} to user and retrieve email", principal);
      return "";
    }
  }
}
