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

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;

@Consumes(value = MediaType.APPLICATION_JSON)
@Path("contents")
public interface ContentsApi {

  /**
   * Get the properties of an object.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{key}/{ref}")
  @Operation(summary = "Fetch details of a table endpoint")
  @APIResponses({
      @APIResponse(responseCode = "200", description = "Information for table"),
      @APIResponse(responseCode = "404", description = "Table not found on ref")
    })
  Contents getContents(
      @Parameter(description = "object name to search for") @PathParam("key") ContentsKey key,
      @Parameter(description = "Name of ref to read from. Default branch if not provided.") @PathParam("ref") String ref
      ) throws NessieNotFoundException;

  /**
   * Get the properties of an object.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{key}")
  @Operation(summary = "Fetch details of a table endpoint for default branch")
  @APIResponses({
      @APIResponse(responseCode = "200", description = "Information for table"),
      @APIResponse(responseCode = "404", description = "Table not found on ref")
    })
  Contents getContents(@Parameter(description = "object name to search for") @PathParam("key") ContentsKey key)
      throws NessieNotFoundException;

  /**
   * create/update a table on default branch.
   */
  @POST
  @Path("{key}/{hash}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "update contents for path")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Contents updated successfully."),
      @APIResponse(responseCode = "404", description = "Provided ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "Update conflict")})
  public void setContents(
      @NotNull @Parameter(description = "object name to search for") @PathParam("key") ContentsKey key,
      @NotNull @Parameter(description = "Expected hash of branch.") @PathParam("hash") String hash,
      @Parameter(description = "Commit message") @QueryParam("message") String message,
      @NotNull @RequestBody(description = "Contents to be upserted") Contents contents)
      throws NessieNotFoundException, NessieConflictException;

  /**
   * create/update a table on a specific ref.
   */
  @POST
  @Path("{key}/{branch}/{hash}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "update contents for path")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Contents updated successfully."),
      @APIResponse(responseCode = "404", description = "Provided ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "Update conflict")})
  public void setContents(
      @NotNull @Parameter(description = "object name to search for") @PathParam("key") ContentsKey key,
      @Parameter(description = "Branch to change, defaults to default branch.") @PathParam("branch") String branch,
      @NotNull @Parameter(description = "Expected hash of branch.") @PathParam("hash") String hash,
      @Parameter(description = "Commit message") @QueryParam("message") String message,
      @NotNull @RequestBody(description = "Contents to be upserted") Contents contents)
      throws NessieNotFoundException, NessieConflictException;



  /**
   * Delete a single object on default branch.
   */
  @DELETE
  @Path("{key}/{hash}")
  @Operation(summary = "delete object on ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Deleted successfully."),
      @APIResponse(responseCode = "404", description = "Provided ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "Update conflict"),
      }
  )
  public void deleteContents(
      @Parameter(description = "object name to search for") @PathParam("key") ContentsKey key,
      @Parameter(description = "Expected hash of branch.") @PathParam("hash") String hash,
      @Parameter(description = "Commit message") @QueryParam("message") String message
      ) throws NessieNotFoundException, NessieConflictException;

  /**
   * Delete a single object.
   */
  @DELETE
  @Path("{key}/{branch}/{hash}")
  @Operation(summary = "delete object on ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Deleted successfully."),
      @APIResponse(responseCode = "404", description = "Provided ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "Update conflict"),
      }
  )
  public void deleteContents(
      @Parameter(description = "object name to search for") @PathParam("key") ContentsKey key,
      @Parameter(description = "Branch to delete from, defaults to default branch.") @PathParam("branch") String branch,
      @Parameter(description = "Expected hash of branch.") @PathParam("hash") String hash,
      @Parameter(description = "Commit message") @QueryParam("message") String message
      ) throws NessieNotFoundException, NessieConflictException;


}
