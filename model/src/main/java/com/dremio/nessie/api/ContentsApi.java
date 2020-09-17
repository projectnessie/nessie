package com.dremio.nessie.api;

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

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.MultiContents;
import com.dremio.nessie.model.NessieObjectKey;
import com.dremio.nessie.model.PutContents;

@Path("contents")
public interface ContentsApi {

  /**
   * Get the properties of an object.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{object}")
  @Operation(summary = "Fetch details of a table endpoint")
  @APIResponses({
      @APIResponse(responseCode = "200", description = "Information for table"),
      @APIResponse(responseCode = "404", description = "Table not found on ref")
    })
  Contents getObjectForReference(
      @Parameter(description = "name of ref to search on. Default branch if not provided.") @QueryParam("ref") String ref,
      @NotNull @Parameter(description = "object name to search for") @PathParam("object") NessieObjectKey objectName
      ) throws NessieNotFoundException;

  /**
   * create/update a table on a specific ref.
   */
  @POST
  @Path("{object}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "create table on ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "404", description = "Branch doesn't exists"),
      @APIResponse(responseCode = "412", description = "Update conflict")})
  public void setContents(
      @NotNull @Parameter(description = "name of table to be created") @PathParam("object") NessieObjectKey objectName,
      @Parameter(description = "commit message") @QueryParam("message") String message,
      @NotNull @RequestBody(description = "branch and contents to be created/updated") PutContents contents)
      throws NessieNotFoundException, NessieConflictException;

  /**
   * Delete a single object.
   */
  @DELETE
  @Path("{object}")
  @Operation(summary = "delete object on ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "Update conflict"),
      }
  )
  public void deleteObject(
      @NotNull @Parameter(description = "object to delete") @PathParam("object") NessieObjectKey objectName,
      @Parameter(description = "commit message") @QueryParam("message") String message,
      @NotNull @RequestBody(description = "Branch with id.") Branch branch
      ) throws NessieNotFoundException, NessieConflictException;

  @PUT
  @Path("multi")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "create table on ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "404", description = "Branch doesn't exists"),
      @APIResponse(responseCode = "412", description = "Update conflict")})
  public void commitMultipleOperations(
      @Parameter(description = "Commit message") @QueryParam("message") String message,
      @NotNull @RequestBody(description = "Branch and operations") MultiContents operations)
      throws NessieNotFoundException, NessieConflictException;
}
