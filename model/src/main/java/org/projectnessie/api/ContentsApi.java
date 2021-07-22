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
package org.projectnessie.api;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.model.Validation;

@Consumes(value = MediaType.APPLICATION_JSON)
@Path("contents")
public interface ContentsApi {

  /** Get the properties of an object. */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{key}")
  @Operation(
      summary = "Get object content associated with key.",
      description =
          "This operation returns a consistent view for a contents-key in a branch or tag.\n"
              + "\n"
              + "Nessie may return a 'Contents' object, that is updated compared to the value "
              + "that has been passed when the 'Contents' object has been commited, to reflect "
              + "a more recent, but semantically equal state. "
              + "For example, the Iceberg table-metadata-location returned in 'IcebergSnapshot' "
              + "reflects the most recent table-metadata, which still references the Iceberg "
              + "snapshot.")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Information for table",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {@ExampleObject(ref = "iceberg")},
                schema = @Schema(implementation = Contents.class))),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "404", description = "Table not found on ref")
  })
  Contents getContents(
      @Valid
          @Parameter(
              description = "object name to search for",
              examples = {@ExampleObject(ref = "ContentsKey")})
          @PathParam("key")
          ContentsKey key,
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          @Parameter(
              description = "Reference to use. Defaults to default branch if not provided.",
              examples = {@ExampleObject(ref = "ref")})
          @QueryParam("ref")
          String ref,
      @Nullable
          @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          @Parameter(
              description = "a particular hash on the given ref",
              examples = {@ExampleObject(ref = "hash")})
          @QueryParam("hashOnRef")
          String hashOnRef)
      throws NessieNotFoundException;

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Get multiple objects' content.",
      description =
          "Similar to 'getContents', but takes multiple 'ContentKey's and returns a consistent "
              + "view for the contents-key in a branch or tag.\n"
              + "\n"
              + "Nessie may return 'Contents' objects, that are updated compared to the values "
              + "that have been passed when the 'Contents' objects have been committed, to reflect "
              + "a more recent, but semantically equal state. "
              + "For example, the Iceberg table-metadata-location returned in 'IcebergSnapshot' "
              + "reflects the most recent table-metadata, which still references the Iceberg "
              + "snapshot.")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Retrieved successfully.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(implementation = MultiGetContentsResponse.class))),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "404", description = "Provided ref doesn't exists")
  })
  MultiGetContentsResponse getMultipleContents(
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          @Parameter(
              description = "Reference to use. Defaults to default branch if not provided.",
              examples = {@ExampleObject(ref = "ref")})
          @QueryParam("ref")
          String ref,
      @Nullable
          @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          @Parameter(
              description = "a particular hash on the given ref",
              examples = {@ExampleObject(ref = "hash")})
          @QueryParam("hashOnRef")
          String hashOnRef,
      @Valid @NotNull @RequestBody(description = "Keys to retrieve.")
          MultiGetContentsRequest request)
      throws NessieNotFoundException;
}
