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

package com.dremio.nessie.services.rest;

import static com.dremio.nessie.services.rest.Util.meta;
import static com.dremio.nessie.services.rest.Util.version;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.SecurityContext;

import org.eclipse.microprofile.metrics.annotation.Metered;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.CommitMeta.Action;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.ReferenceWithType;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.services.VersionStoreAdapter;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.google.common.collect.Lists;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;

/**
 * REST endpoint for CRUD operations on branches/tables.
 */
@ApplicationScoped
@Path("objects")
@SecurityScheme(
    name = "nessie-auth",
    type = SecuritySchemeType.HTTP,
    scheme = "bearer",
    bearerFormat = "JWT"
)
public class TableBranchOperations {

  private static final Logger logger = LoggerFactory.getLogger(TableBranchOperations.class);

  private final VersionStoreAdapter backend;

  @Inject
  public TableBranchOperations(VersionStoreAdapter backend) {
    this.backend = backend;
  }

  /**
   * get all refs.
   */
  @GET
  @Metered
  //@RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Fetch all refs endpoint",
      tags = {"get", "ref"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(
          description = "All known refs",
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ReferenceWithType[].class))),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response refs() {
    ReferenceWithType[] refs = backend.getAllReferences().stream().map(ReferenceWithType::of).toArray(ReferenceWithType[]::new);
    return Response.ok(refs).build();
  }

  /**
   * get ref details.
   */
  @GET
  @Metered
  //@RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall-refs")
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{ref}")
  @Operation(summary = "Fetch details of a ref endpoint",
      tags = {"get", "ref"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(
          description = "Ref details",
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ReferenceWithType.class))),
        @ApiResponse(responseCode = "404", description = "Ref not found"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response ref(@Parameter(description = "name of ref to fetch", required = true)
                                 @PathParam("ref") String refName) throws ReferenceNotFoundException {
    Reference ref = backend.getReference(refName);
    return Response.ok(ReferenceWithType.of(ref)).tag(tagFromTable(ref)).build();
  }

  private static EntityTag tagFromTable(Reference obj) {
    return new EntityTag(obj.getId());
  }

  /**
   * get a table in a specific ref.
   */
  @GET
  @Metered
  //@RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall-table")
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{ref}/tables/{table}")
  @Operation(summary = "Fetch details of a table endpoint",
      tags = {"get", "table"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(
          description = "Details of table on ref",
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = Table.class))),
        @ApiResponse(responseCode = "404", description = "Table not found on ref"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from ref")}
  )
  public Response refTable(@Parameter(description = "name of ref to search on", required = true)
                                @PathParam("ref") String ref,
                              @Parameter(description = "table name to search for", required = true)
                                @PathParam("table") String tableName,
                              @Parameter(description = "fetch all metadata on table")
                                @DefaultValue("false") @QueryParam("metadata") boolean metadata) throws ReferenceNotFoundException {
    //todo metadata ignored until that becomes a clearer pattern
    Table table = backend.getTableOnReference(ref, tableName);
    if (table == null) {
      throw ReferenceNotFoundException.forReference(BranchName.of(ref));
    }
    return Response.ok(table).build();
  }

  /**
   * create a ref.
   */
  @POST
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-readall-ref")
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("{ref}")
  @Operation(summary = "create ref endpoint",
      tags = {"post", "ref"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(responseCode = "409", description = "Ref already exists"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response createRef(@Parameter(description = "name of ref to be created", required = true)
                                 @PathParam("ref") String refName,
                               @RequestBody(description = "ref object to be created",
                               content = @Content(schema = @Schema(implementation = ReferenceWithType.class)))
                                 ReferenceWithType ref) throws ReferenceAlreadyExistsException, ReferenceNotFoundException {
    Reference newRef = backend.createRef(refName, ref.getReference().getId(), ref.getType());
    return Response.created(null).tag(tagFromTable(newRef)).build();
  }

  /**
   * create a table on a specific ref.
   */
  @POST
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-create-table")
  @Path("{ref}/tables/{table}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "create table on ref endpoint",
      tags = {"post", "table"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(responseCode = "404", description = "Ref doesn't exists"),
        @ApiResponse(responseCode = "400", description = "Table already exists"),
        @ApiResponse(responseCode = "412", description = "update conflict, tag out of date"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response createTable(@Parameter(description = "ref on which table will be created", required = true)
                                @PathParam("ref") String ref,
                              @Parameter(description = "name of table to be created", required = true)
                                @PathParam("table") String tableName,
                              @Parameter(description = "reason for this action for audit purposes")
                                @DefaultValue("unknown") @QueryParam("reason") String reason,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers,
                              @RequestBody(description = "table object to be created",
                                content = @Content(schema = @Schema(implementation = Table.class)))
                                  Table table)
      throws ReferenceNotFoundException, ReferenceConflictException, ReferenceAlreadyExistsException {
    Table existing = backend.getTableOnReference(ref, tableName);
    if (existing != null) {
      throw ReferenceAlreadyExistsException.forReference(BranchName.of(ref));
    }
    return singleCommit(ref, tableName, securityContext, headers, table, reason, true);
  }

  private Response singleCommit(String ref,
                                String tableName,
                                SecurityContext securityContext,
                                HttpHeaders headers,
                                Table table,
                                String reason,
                                boolean post) throws ReferenceConflictException, ReferenceNotFoundException {
    String hash = version(headers).orElseThrow(() -> new ReferenceConflictException("ETag header not supplied"));
    Principal principal = securityContext.getUserPrincipal();
    backend.commit(ref,
                   hash,
                   meta(principal, reason + ";" + table, 1, ref, Action.COMMIT),
                   Collections.singletonList(Put.of(VersionStoreAdapter.keyFromUrlString(tableName), table)));
    ResponseBuilder response = post ? Response.created(null) : Response.ok();
    return response.build();
  }

  /**
   * delete a ref.
   */
  @DELETE
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-delete-ref")
  @Path("{ref}")
  @Operation(summary = "delete ref endpoint",
      tags = {"delete", "ref"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(responseCode = "404", description = "Ref doesn't exists"),
        @ApiResponse(responseCode = "412", description = "update conflict, tag out of date"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response deleteRef(@Parameter(description = "ref to delete", required = true)
                                 @PathParam("ref") String ref,
                               @Context HttpHeaders headers) throws ReferenceConflictException, ReferenceNotFoundException {
    String hash = version(headers).orElseThrow(() -> new ReferenceConflictException("ETag header not supplied"));
    backend.deleteRef(ref, hash);
    return Response.ok().build();
  }

  static class CommitMetaModel extends HashMap<String, CommitMeta> {
    //because swagger is annoying
  }

  /**
   * delete a single table.
   */
  @DELETE
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-delete-table")
  @Path("{ref}/tables/{table}")
  @Operation(summary = "delete table on ref endpoint",
      tags = {"delete", "table"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(responseCode = "404", description = "Ref/table doesn't exists"),
        @ApiResponse(responseCode = "412", description = "update conflict, tag out of date"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response deleteTable(@Parameter(description = "ref on which to delete table", required = true)
                                @PathParam("ref") String ref,
                              @Parameter(description = "table to delete", required = true)
                                @PathParam("table") String table,
                              @Parameter(description = "reason for this action for audit purposes")
                                @DefaultValue("unknown") @QueryParam("reason") String reason,
                              @Parameter(description = "purge all data about ref")
                                @DefaultValue("false") @QueryParam("purge") boolean purge,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers) throws ReferenceConflictException, ReferenceNotFoundException {
    String hash = version(headers).orElseThrow(() -> new ReferenceConflictException("ETag header not supplied"));
    CommitMeta meta = meta(securityContext.getUserPrincipal(), reason + ";" + table, 1, ref, Action.COMMIT);
    backend.commit(ref, hash, meta, Lists.newArrayList(Delete.of(VersionStoreAdapter.keyFromUrlString(table))));
    return Response.ok().build();
  }


  /**
   * assign a Ref to a hash.
   */
  @PUT
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-assign")
  @Path("{ref}")
  @Operation(summary = "assign hash to ref endpoint",
      tags = {"put", "commit"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(responseCode = "404", description = "Ref doesn't exists"),
        @ApiResponse(responseCode = "412", description = "update conflict, tag out of date"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response updateBatch(@Parameter(description = "ref on which to assign, may not yet exist", required = true)
                                @PathParam("ref") String ref,
                              @Parameter(description = "name of ref to take commits from", required = true)
                              @QueryParam("target") String targetRef,
                              @Context HttpHeaders headers
                              ) throws ReferenceConflictException, ReferenceNotFoundException {
    String hash = version(headers).orElseThrow(() -> new ReferenceConflictException("ETag header not supplied"));
    backend.assign(ref, hash, targetRef);
    return Response.ok().build();
  }

  /**
   * update a single table on a ref.
   */
  @PUT
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-commit-table")
  @Path("{ref}/tables/{table}")
  @Operation(summary = "update via commit single table to ref endpoint",
      tags = {"put", "commit"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(responseCode = "404", description = "Ref/table doesn't exists"),
        @ApiResponse(responseCode = "412", description = "update conflict, tag out of date"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response update(@Parameter(description = "ref on which to add merges", required = true)
                           @PathParam("ref") String ref,
                         @Parameter(description = "table which will be changed", required = true)
                           @PathParam("table") String table,
                         @Parameter(description = "reason for this action for audit purposes")
                           @DefaultValue("unknown") @QueryParam("reason") String reason,
                         @Context SecurityContext securityContext,
                         @Context HttpHeaders headers,
                         Table update) throws ReferenceConflictException, ReferenceNotFoundException {
    return singleCommit(ref, table, securityContext, headers, update, reason, false);
  }

  /**
   * commit log for a ref.
   */
  @GET
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-log-ref")
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{ref}/log")
  @Operation(summary = "log ref endpoint",
      tags = {"log", "ref"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(
          description = "all commits on a ref",
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = CommitMetaModel.class))),
        @ApiResponse(responseCode = "404", description = "Ref doesn't exists"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response commitLog(@Parameter(description = "ref to show log from", required = true)
                            @PathParam("ref") String ref,
                            @Context HttpHeaders headers) throws ReferenceNotFoundException {
    Map<String, CommitMeta> log = backend.log(ref);
    return Response.ok(log).build();
  }

  /**
   * cherry pick mergeRef  onto ref.
   */
  @PUT
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-transplant")
  @Path("{ref}/transplant")
  @Operation(summary = "transplant commits from mergeRef to ref endpoint",
      tags = {"put", "commit"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(responseCode = "401", description = "no merge ref supplied"),
        @ApiResponse(responseCode = "404", description = "Ref doesn't exists"),
        @ApiResponse(responseCode = "412", description = "update conflict, tag out of date"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response transplantRef(@Parameter(description = "ref on which to add merges", required = true)
                                @PathParam("ref") String ref,
                                @Context SecurityContext securityContext,
                                @Context HttpHeaders headers,
                                @Parameter(description = "reason for this action for audit purposes")
                                @QueryParam("promote") String mergeHashes) throws ReferenceConflictException, ReferenceNotFoundException {
    if (mergeHashes == null || mergeHashes.isEmpty()) {
      throw new WebApplicationException("ref to cherry pick from is null", Response.Status.BAD_REQUEST);
    }
    String hash = version(headers).orElseThrow(() -> new ReferenceConflictException("ETag header not supplied"));
    List<String> transplantHashes = Arrays.stream(mergeHashes.split(",")).collect(Collectors.toList());
    backend.transplant(ref, hash, transplantHashes);
    return Response.ok().build();
  }

  /**
   * merge mergeRef onto ref, optionally forced.
   */
  @PUT
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-merge")
  @Path("{ref}/merge")
  @Operation(summary = "merge commits from mergeRef to ref endpoint",
      tags = {"put", "commit"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(responseCode = "401", description = "no merge ref supplied"),
        @ApiResponse(responseCode = "404", description = "Ref doesn't exists"),
        @ApiResponse(responseCode = "412", description = "update conflict, tag out of date"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response mergeRef(@Parameter(description = "ref on which to add merges", required = true)
                           @PathParam("ref") String ref,
                           @Context HttpHeaders headers,
                           @Parameter(description = "hash to take commits from", required = true)
                           @QueryParam("promote") String mergeHash) throws ReferenceConflictException, ReferenceNotFoundException {
    if (mergeHash == null) {
      throw new WebApplicationException("hash to merge from is null", Response.Status.BAD_REQUEST);
    }
    String hash = version(headers).orElseThrow(() -> new ReferenceConflictException("ETag header not supplied"));
    backend.merge(ref, mergeHash, hash);
    return Response.ok().build();
  }

  /**
   * get all tables on a ref.
   */
  @GET
  @Metered
  //@RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall-tables")
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{ref}/tables")
  @Operation(summary = "Fetch all tables on a ref endpoint",
      tags = {"get", "table"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(
          description = "all tables on a ref",
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = String[].class))),
        @ApiResponse(responseCode = "404", description = "Ref not found"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response refTables(@Parameter(description = "name of ref to fetch from", required = true)
                            @PathParam("ref") String refName,
                            @Parameter(description = "filter for namespace")
                            @DefaultValue("all") @QueryParam("namespace") String namespace) throws ReferenceNotFoundException {
    //todo filter by namespace once namespace is better defined
    Stream<Key> tables = backend.getAllTables(refName);
    String[] tableArray = tables.map(VersionStoreAdapter::urlStringFromKey)
                                .toArray(String[]::new);
    return Response.ok(tableArray).build();
  }

  /**
   * update multiple tables on a ref.
   */
  @PUT
  @Metered
  //@RolesAllowed({"admin"})
  @Timed(name = "timed-commit-multi-table")
  @Path("{ref}/tables")
  @Operation(summary = "update via commit multiple tables to ref endpoint",
      tags = {"put", "commit"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(responseCode = "404", description = "Ref/table doesn't exists"),
        @ApiResponse(responseCode = "412", description = "update conflict, tag out of date"),
        @ApiResponse(responseCode = "500", description = "Could not fetch data from backend")}
  )
  public Response updateMulti(@Parameter(description = "ref on which to add merges", required = true)
                              @PathParam("ref") String ref,
                              @Parameter(description = "reason for this action for audit purposes")
                              @DefaultValue("unknown") @QueryParam("reason") String reason,
                              @Context SecurityContext securityContext,
                              @Context HttpHeaders headers,
                              Table[] update) throws ReferenceConflictException, ReferenceNotFoundException {
    String hash = version(headers).orElseThrow(() -> new ReferenceConflictException("ETag header not supplied"));
    Principal principal = securityContext.getUserPrincipal();
    List<com.dremio.nessie.versioned.Operation<Table>> ops = Arrays.stream(update).map(t -> {
      Key key = VersionStoreAdapter.keyFromUrlString(t.getId());
      return t.isDeleted() ? Delete.<Table>of(key) : Put.of(key, t);
    }).collect(Collectors.toList());
    backend.commit(ref,
                   hash,
                   meta(principal, reason, update.length, ref, Action.COMMIT),
                   ops);
    return Response.ok().build();
  }
}
