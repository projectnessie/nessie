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
package org.projectnessie.api.http;

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
public interface HttpContentsApi {

  /**
   * This operation returns the {@link Contents} for a {@link ContentsKey} in a named-reference (a
   * {@link org.projectnessie.model.Branch} or {@link org.projectnessie.model.Tag}).
   *
   * <p>If the table-metadata is tracked globally (Iceberg), Nessie returns a {@link Contents}
   * object, that contains the most up-to-date part for the globally tracked part (Iceberg:
   * table-metadata) plus the per-Nessie-reference/hash specific part (Iceberg: snapshot-ID).
   *
   * @param key the {@link ContentsKey}s to retrieve
   * @param ref named-reference to retrieve the contents for
   * @param hashOnRef hash on {@code ref} to retrieve the contents for, translates to {@code HEAD},
   *     if missing/{@code null}
   * @return list of {@link org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey}s
   * @throws NessieNotFoundException if {@code ref} or {@code hashOnRef} does not exist
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{key}")
  @Operation(
      summary = "Get object content associated with a key.",
      description =
          "This operation returns the contents-value for a contents-key in a named-reference "
              + "(a branch or tag).\n"
              + "\n"
              + "If the table-metadata is tracked globally (Iceberg), "
              + "Nessie returns a 'Contents' object, that contains the most up-to-date part for "
              + "the globally tracked part (Iceberg: table-metadata) plus the "
              + "per-Nessie-reference/hash specific part (Iceberg: snapshot-ID).")
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
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or read object content for a key"),
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
              examples = {@ExampleObject(ref = "nullHash"), @ExampleObject(ref = "hash")})
          @QueryParam("hashOnRef")
          String hashOnRef)
      throws NessieNotFoundException;

  /**
   * Similar to {@link #getContents(ContentsKey, String, String)}, but takes multiple {@link
   * ContentsKey}s and returns the {@link Contents} for the one or more {@link ContentsKey}s in a
   * named-reference (a {@link org.projectnessie.model.Branch} or {@link
   * org.projectnessie.model.Tag}).
   *
   * <p>If the table-metadata is tracked globally (Iceberg), Nessie returns a {@link Contents}
   * object, that contains the most up-to-date part for the globally tracked part (Iceberg:
   * table-metadata) plus the per-Nessie-reference/hash specific part (Iceberg: snapshot-ID).
   *
   * @param ref named-reference to retrieve the contents for
   * @param hashOnRef hash on {@code ref} to retrieve the contents for, translates to {@code HEAD},
   *     if missing/{@code null}
   * @param request the {@link ContentsKey}s to retrieve
   * @return list of {@link org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey}s
   * @throws NessieNotFoundException if {@code ref} or {@code hashOnRef} does not exist
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Get multiple objects' content.",
      description =
          "Similar to 'getContents', but takes multiple 'ContentKey's and returns the "
              + "contents-values for the one or more contents-keys in a named-reference "
              + "(a branch or tag).\n"
              + "\n"
              + "If the table-metadata is tracked globally (Iceberg), "
              + "Nessie returns a 'Contents' object, that contains the most up-to-date part for "
              + "the globally tracked part (Iceberg: table-metadata) plus the "
              + "per-Nessie-reference/hash specific part (Iceberg: snapshot-ID).")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Retrieved successfully.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(implementation = MultiGetContentsResponse.class))),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or read object content for a key"),
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
              examples = {@ExampleObject(ref = "nullHash"), @ExampleObject(ref = "hash")})
          @QueryParam("hashOnRef")
          String hashOnRef,
      @Valid @NotNull @RequestBody(description = "Keys to retrieve.")
          MultiGetContentsRequest request)
      throws NessieNotFoundException;
}
