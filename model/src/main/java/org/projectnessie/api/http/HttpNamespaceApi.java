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

import javax.validation.constraints.NotNull;
import javax.ws.rs.BeanParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.projectnessie.api.NamespaceApi;
import org.projectnessie.api.params.MultipleNamespacesParams;
import org.projectnessie.api.params.NamespaceParams;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.Namespace;

@Path("namespaces")
@Consumes(value = MediaType.APPLICATION_JSON)
public interface HttpNamespaceApi extends NamespaceApi {

  @Override
  @PUT
  @Path("/namespace/{ref}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Creates a Namespace")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returned Namespace.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {
                  @ExampleObject(ref = "namespace"),
                },
                schema = @Schema(implementation = Namespace.class))),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to create namespace"),
    @APIResponse(responseCode = "404", description = "Reference not found"),
    @APIResponse(responseCode = "409", description = "Namespace already exists"),
  })
  Namespace createNamespace(@BeanParam @NotNull NamespaceParams params)
      throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException;

  @Override
  @DELETE
  @Path("/namespace/{ref}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Deletes a Namespace")
  @APIResponses({
    @APIResponse(responseCode = "200", description = "Namespace successfully deleted."),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to delete namespace"),
    @APIResponse(responseCode = "404", description = "Reference or Namespace not found"),
    @APIResponse(responseCode = "409", description = "Namespace not empty"),
  })
  void deleteNamespace(@BeanParam @NotNull NamespaceParams params)
      throws NessieReferenceNotFoundException, NessieNamespaceNotEmptyException,
          NessieNamespaceNotFoundException;

  @Override
  @GET
  @Path("/namespace/{ref}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Retrieves a Namespace")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returned Namespace.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {
                  @ExampleObject(ref = "namespace"),
                },
                schema = @Schema(implementation = Namespace.class))),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to retrieve namespace"),
    @APIResponse(responseCode = "404", description = "Reference or Namespace not found"),
  })
  Namespace getNamespace(@BeanParam @NotNull NamespaceParams params)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException;

  @Override
  @GET
  @Path("/{ref}")
  @Produces(MediaType.APPLICATION_JSON)
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returns Namespaces with a given prefix.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {
                  @ExampleObject(ref = "namespacesResponse"),
                },
                schema = @Schema(implementation = GetNamespacesResponse.class))),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to retrieve namespaces"),
    @APIResponse(responseCode = "404", description = "Reference not found"),
  })
  GetNamespacesResponse getNamespaces(@BeanParam @NotNull MultipleNamespacesParams params)
      throws NessieReferenceNotFoundException;
}
