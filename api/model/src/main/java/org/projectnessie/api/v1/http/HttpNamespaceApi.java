/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.api.v1.http;

import com.fasterxml.jackson.annotation.JsonView;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.projectnessie.api.v1.NamespaceApi;
import org.projectnessie.api.v1.params.MultipleNamespacesParams;
import org.projectnessie.api.v1.params.NamespaceParams;
import org.projectnessie.api.v1.params.NamespaceUpdate;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.ser.Views;

@Tag(name = "v1")
@Path("v1/namespaces")
@Consumes(MediaType.APPLICATION_JSON)
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
  @JsonView(Views.V1.class)
  Namespace createNamespace(
      @BeanParam @NotNull NamespaceParams params, @NotNull @RequestBody Namespace namespace)
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
  @JsonView(Views.V1.class)
  void deleteNamespace(@BeanParam @NotNull NamespaceParams params)
      throws NessieReferenceNotFoundException,
          NessieNamespaceNotEmptyException,
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
  @JsonView(Views.V1.class)
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
  @JsonView(Views.V1.class)
  GetNamespacesResponse getNamespaces(@BeanParam @NotNull MultipleNamespacesParams params)
      throws NessieReferenceNotFoundException;

  @Override
  @POST
  @Path("/namespace/{ref}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Updates namespace properties for the given namespace."),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to update namespace properties"),
    @APIResponse(responseCode = "404", description = "Reference or Namespace not found"),
  })
  @JsonView(Views.V1.class)
  void updateProperties(
      @BeanParam @NotNull NamespaceParams params,
      @RequestBody(
              description = "Namespace properties to update/delete.",
              content = {
                @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    examples = {@ExampleObject(ref = "namespaceUpdate")})
              })
          @NotNull
          NamespaceUpdate namespaceUpdate)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException;
}
