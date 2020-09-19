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
package com.dremio.nessie.api;

import java.util.List;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;

import com.dremio.nessie.error.NessieAlreadyExistsException;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.EntriesResponse;
import com.dremio.nessie.model.LogResponse;
import com.dremio.nessie.model.Merge;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.ReferenceUpdate;
import com.dremio.nessie.model.Transplant;

@Consumes(value = MediaType.APPLICATION_JSON)
@Path("/")
public interface TreeApi {

  public static final String EXPECTED = "expected";

  /**
   * Get all references.
   */
  @GET
  @Path("trees")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get all references")
  @APIResponses({@APIResponse(responseCode = "200", description = "Fetched all references.")})
  List<Reference> getAllReferences();

  /**
   * Get details of a particular ref, if it exists.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("tree/{ref}")
  @Operation(summary = "Fetch details of a reference")
  @APIResponses({
      @APIResponse(responseCode = "404", description = "Ref not found")
    })
  Reference getReferenceByName(
      @NotNull @Parameter(description = "name of ref to fetch") @PathParam("ref") String refName)
      throws NessieNotFoundException;

  /**
   * Get details for the default reference.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("tree/default")
  @Operation(summary = "Get default branch for commits and reads")
  @APIResponses({
      @APIResponse(responseCode = "404", description = "Ref not found")
    })
  Branch getDefaultBranch();

  /**
   * create a ref.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("tree")
  @Operation(summary = "Create Reference")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Created successfully."),
      @APIResponse(responseCode = "409", description = "Reference already exists")}
  )
  void createNewReference(@RequestBody(description = "Reference to create") Reference reference)
      throws NessieAlreadyExistsException, NessieNotFoundException, NessieConflictException;

  /**
   * Delete a reference.
   */
  @DELETE
  @Path("tree")
  @Operation(summary = "Delete ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Deleted successfully."),
      @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "update conflict"),
    })
  void deleteReference(
      @RequestBody(description = "Reference to create") Reference reference) throws NessieConflictException, NessieNotFoundException;

  /**
   * Assign a Reference to a hash.
   */
  @PUT
  @Path("tree/${ref}")
  @Operation(summary = "Set a reference to a specific hash")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Assigned successfully."),
      @APIResponse(responseCode = "404", description = "Reference doesn't exists"),
      @APIResponse(responseCode = "412", description = "Update conflict")
    })
  void assignReference(
      @NotNull @Parameter(description = "Ref on which to assign, may not yet exist") @PathParam("ref") String ref,
      @RequestBody(description = "Expected and new reference") ReferenceUpdate reference)
          throws NessieNotFoundException, NessieConflictException;


  /**
   * commit log for a ref.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("tree/{ref}/log")
  @Operation(summary = "Get commit log for a reference")
  @APIResponses({
      @APIResponse(description = "all commits on a ref"),
      @APIResponse(responseCode = "404", description = "Ref doesn't exists")})
  LogResponse getCommitLog(@NotNull @Parameter(description = "ref to show log from") @PathParam("ref") String ref)
          throws NessieNotFoundException;

  /**
   * cherry pick a set of commits into a branch.
   */
  @PUT
  @Path("tree/transplant")
  @Operation(summary = "transplant commits from mergeRef to ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Merged successfully."),
      @APIResponse(responseCode = "401", description = "no merge ref supplied"),
      @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "update conflict")}
  )
  void transplantCommitsIntoBranch(
      @Parameter(description = "commit message") @QueryParam("message") String message,
      @RequestBody(description = "Branch and hashes to transplant") Transplant transplant)
          throws NessieNotFoundException, NessieConflictException;

  /**
   * merge mergeRef onto ref, optionally forced.
   */
  @PUT
  @Path("tree/merge")
  @Operation(summary = "merge commits from mergeRef to ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Merged successfully."),
      @APIResponse(responseCode = "401", description = "no merge ref supplied"),
      @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "update conflict")}
  )
  void mergeRefIntoBranch(@NotNull @RequestBody(description = "Merge operation") Merge merge)
          throws NessieNotFoundException, NessieConflictException;


  /**
   * get all objects for a ref.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("tree/{ref}/all")
  @Operation(summary = "Fetch all entries for a given reference")
  @APIResponses({
      @APIResponse(description = "all objects for a reference"),
      @APIResponse(responseCode = "200", description = "Returned successfully."),
      @APIResponse(responseCode = "404", description = "Ref not found")}
  )
  public EntriesResponse getEntries(
      @NotNull @Parameter(description = "name of ref to fetch from") @PathParam("ref") String refName)
          throws NessieNotFoundException;


}
