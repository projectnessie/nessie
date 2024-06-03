/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.rest;

import static java.lang.String.format;

import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateNamespaceRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateNamespaceResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergGetNamespaceResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListNamespacesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesResponse;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;
import org.projectnessie.client.api.UpdateNamespaceResult;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Namespace;

/** Handles Iceberg REST API v1 endpoints that are associated with namespaces. */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1NamespaceResource extends IcebergApiV1ResourceBase {

  @Inject IcebergErrorMapper errorMapper;

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.NAMESPACE);
  }

  @POST
  @Path("/v1/{prefix}/namespaces")
  @Blocking
  public IcebergCreateNamespaceResponse createNamespace(
      @PathParam("prefix") String prefix,
      @Valid IcebergCreateNamespaceRequest createNamespaceRequest)
      throws IOException {
    ParsedReference ref = decodePrefix(prefix).parsedReference();

    Namespace ns =
        nessieApi
            .createNamespace()
            .refName(ref.name())
            .hashOnRef(ref.hashWithRelativeSpec())
            .namespace(createNamespaceRequest.namespace().toNessieNamespace())
            .properties(createNamespaceRequest.properties())
            .create();

    return IcebergCreateNamespaceResponse.builder()
        .namespace(createNamespaceRequest.namespace())
        .putAllProperties(ns.getProperties())
        .build();
  }

  @DELETE
  @Path("/v1/{prefix}/namespaces/{namespace}")
  @Blocking
  public void dropNamespace(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    nessieApi
        .deleteNamespace()
        .refName(namespaceRef.referenceName())
        .hashOnRef(namespaceRef.hashWithRelativeSpec())
        .namespace(namespaceRef.namespace())
        .delete();
  }

  @GET
  @Path("/v1/{prefix}/namespaces")
  @Blocking
  public IcebergListNamespacesResponse listNamespaces(
      @PathParam("prefix") String prefix,
      @QueryParam("parent") String parent,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize)
      throws IOException {

    IcebergListNamespacesResponse.Builder response = IcebergListNamespacesResponse.builder();

    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, parent);
    Namespace namespace = namespaceRef.namespace();
    String celFilter =
        "entry.contentType == 'NAMESPACE'"
            + ((namespace != null && !namespace.isEmpty())
                ? format(
                    " && size(entry.keyElements) == %d && entry.encodedKey.startsWith('%s.')",
                    namespace.getElementCount() + 1, namespace.name())
                : " && size(entry.keyElements) == 1");

    listContent(namespaceRef, pageToken, pageSize, true, celFilter, response::nextPageToken)
        .map(EntriesResponse.Entry::getContent)
        .map(Namespace.class::cast)
        .map(IcebergNamespace::fromNessieNamespace)
        .forEach(response::addNamespace);

    return response.build();
  }

  @HEAD
  @Path("/v1/{prefix}/namespaces/{namespace}")
  @Blocking
  public void namespaceExists(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    nessieApi
        .getNamespace()
        .refName(namespaceRef.referenceName())
        .hashOnRef(namespaceRef.hashWithRelativeSpec())
        .namespace(namespaceRef.namespace())
        .get();
  }

  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}")
  @Blocking
  public IcebergGetNamespaceResponse loadNamespaceMetadata(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    Namespace ns =
        nessieApi
            .getNamespace()
            .refName(namespaceRef.referenceName())
            .hashOnRef(namespaceRef.hashWithRelativeSpec())
            .namespace(namespaceRef.namespace())
            .get();

    return IcebergGetNamespaceResponse.builder()
        .namespace(IcebergNamespace.fromNessieNamespace(ns))
        .putAllProperties(ns.getProperties())
        .build();
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/properties")
  @Blocking
  public IcebergUpdateNamespacePropertiesResponse updateProperties(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergUpdateNamespacePropertiesRequest updateNamespacePropertiesRequest)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    UpdateNamespaceResult namespaceUpdate =
        nessieApi
            .updateProperties()
            .refName(namespaceRef.referenceName())
            .hashOnRef(namespaceRef.hashWithRelativeSpec())
            .namespace(namespaceRef.namespace())
            .updateProperties(updateNamespacePropertiesRequest.updates())
            .removeProperties(new HashSet<>(updateNamespacePropertiesRequest.removals()))
            .updateWithResponse();

    IcebergUpdateNamespacePropertiesResponse.Builder response =
        IcebergUpdateNamespacePropertiesResponse.builder();

    Map<String, String> oldProperties = namespaceUpdate.getNamespaceBeforeUpdate().getProperties();
    Map<String, String> newProperties = namespaceUpdate.getNamespace().getProperties();

    oldProperties.keySet().stream()
        .filter(k -> !newProperties.containsKey(k))
        .forEach(response::addRemoved);

    Stream.concat(
            updateNamespacePropertiesRequest.removals().stream(),
            updateNamespacePropertiesRequest.updates().keySet().stream())
        .filter(k -> !oldProperties.containsKey(k))
        .forEach(response::addMissing);

    newProperties.entrySet().stream()
        .filter(
            e -> {
              String newValue = oldProperties.get(e.getKey());
              return !e.getValue().equals(newValue);
            })
        .map(Map.Entry::getKey)
        .forEach(response::addUpdated);

    return response.build();
  }
}
