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
import static org.projectnessie.error.ContentKeyErrorDetails.contentKeyErrorDetails;
import static org.projectnessie.model.Content.Type.NAMESPACE;
import static org.projectnessie.services.impl.RefUtil.toReference;
import static org.projectnessie.services.rest.common.RestCommon.updateCommitMeta;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateNamespaceRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateNamespaceResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergGetNamespaceResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListNamespacesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesResponse;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.services.spi.PagedResponseHandler;
import org.projectnessie.storage.uri.StorageUri;

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

  @Operation(operationId = "iceberg.v1.createNamespace")
  @POST
  @Path("/v1/{prefix}/namespaces")
  @Blocking
  public IcebergCreateNamespaceResponse createNamespace(
      @PathParam("prefix") String prefix,
      @Valid IcebergCreateNamespaceRequest createNamespaceRequest)
      throws IOException {
    ParsedReference ref = decodePrefix(prefix).parsedReference();

    // TODO might want to prevent setting 'location'

    Map<String, String> properties = createNamespaceRequest.properties();

    Namespace namespace =
        ImmutableNamespace.builder()
            .elements(createNamespaceRequest.namespace().levels())
            .properties(properties)
            .build();
    ContentKey key = namespace.toContentKey();

    GetMultipleContentsResponse contentsResponse;
    try {
      contentsResponse =
          contentService.getMultipleContents(
              ref.name(), ref.hashWithRelativeSpec(), List.of(key), false, false);
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    Map<ContentKey, Content> contentMap = contentsResponse.toContentsMap();
    Content existing = contentMap.get(key);
    if (existing != null) {
      if (existing instanceof Namespace) {
        throw new NessieNamespaceAlreadyExistsException(
            contentKeyErrorDetails(key),
            String.format("Namespace '%s' already exists", key.toCanonicalString()));
      } else {
        throw new NessieNamespaceAlreadyExistsException(
            contentKeyErrorDetails(key),
            String.format(
                "Another content object with name '%s' already exists", key.toCanonicalString()));
      }
    }

    Operations ops =
        ImmutableOperations.builder()
            .addOperations(org.projectnessie.model.Operation.Put.of(key, namespace))
            .commitMeta(updateCommitMeta("update namespace " + key))
            .build();

    treeService.commitMultipleOperations(
        contentsResponse.getEffectiveReference().getName(),
        contentsResponse.getEffectiveReference().getHash(),
        ops);

    return IcebergCreateNamespaceResponse.builder()
        .namespace(createNamespaceRequest.namespace())
        .putAllProperties(properties)
        .build();
  }

  @Operation(operationId = "iceberg.v1.dropNamespace")
  @DELETE
  @Path("/v1/{prefix}/namespaces/{namespace}")
  @Blocking
  public void dropNamespace(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    ContentKey key = namespaceRef.namespace().toContentKey();
    ContentResponse contentResponse =
        contentService.getContent(
            key, namespaceRef.referenceName(), namespaceRef.hashWithRelativeSpec(), false, false);
    if (!(contentResponse.getContent() instanceof Namespace)) {
      throw new NessieNamespaceNotFoundException(
          contentKeyErrorDetails(key),
          String.format("Namespace '%s' does not exist", key.toCanonicalString()));
    }

    Reference ref = contentResponse.getEffectiveReference();

    boolean notEmpty =
        treeService.getEntries(
            ref.getName(),
            ref.getHash(),
            null,
            format("entry.encodedKey.startsWith('%s.')", key.toPathString()),
            null,
            false,
            new PagedResponseHandler<>() {
              boolean found;

              @Override
              public boolean addEntry(EntriesResponse.Entry entry) {
                if (found) {
                  return false;
                }
                found = true;
                return true;
              }

              @Override
              public Boolean build() {
                return found;
              }

              @Override
              public void hasMore(String pagingToken) {}
            },
            h -> toReference(h),
            null,
            null,
            key,
            null);

    if (notEmpty) {
      throw new NessieNamespaceNotEmptyException(
          contentKeyErrorDetails(key),
          String.format("Namespace '%s' is not empty", key.toCanonicalString()));
    }

    Operations ops =
        ImmutableOperations.builder()
            .addOperations(org.projectnessie.model.Operation.Delete.of(key))
            .commitMeta(updateCommitMeta("delete namespace " + key))
            .build();

    treeService.commitMultipleOperations(ref.getName(), ref.getHash(), ops);
  }

  @Operation(operationId = "iceberg.v1.listNamespaces")
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

    listContent(namespaceRef, "NAMESPACE", pageToken, pageSize, true, response::nextPageToken)
        .map(EntriesResponse.Entry::getContent)
        .map(Namespace.class::cast)
        .map(IcebergNamespace::fromNessieNamespace)
        .forEach(response::addNamespace);

    return response.build();
  }

  @Operation(operationId = "iceberg.v1.namespaceExists")
  @HEAD
  @Path("/v1/{prefix}/namespaces/{namespace}")
  @Blocking
  public void namespaceExists(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    ContentKey key = namespaceRef.namespace().toContentKey();
    Content c =
        contentService
            .getContent(
                key,
                namespaceRef.referenceName(),
                namespaceRef.hashWithRelativeSpec(),
                false,
                false)
            .getContent();
    if (!(c instanceof Namespace)) {
      throw new NessieNamespaceNotFoundException(
          contentKeyErrorDetails(key),
          String.format("Namespace '%s' does not exist", key.toCanonicalString()));
    }
  }

  @Operation(operationId = "iceberg.v1.loadNamespaceMetadata")
  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}")
  @Blocking
  public IcebergGetNamespaceResponse loadNamespaceMetadata(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace)
      throws IOException {
    DecodedPrefix decoded = decodePrefix(prefix);
    NamespaceRef namespaceRef = decodeNamespaceRef(decoded, namespace);
    Namespace nessieNamespace = namespaceRef.namespace();

    List<ContentKey> keysInOrder = new ArrayList<>(nessieNamespace.getElementCount());
    for (int i = 0; i < nessieNamespace.getElementCount(); i++) {
      ContentKey key = ContentKey.of(nessieNamespace.getElements().subList(0, i + 1));
      keysInOrder.add(key);
    }

    GetMultipleContentsResponse namespaces =
        contentService.getMultipleContents(
            namespaceRef.referenceName(),
            namespaceRef.hashWithRelativeSpec(),
            keysInOrder,
            false,
            false);
    Map<ContentKey, Content> namespacesMap = namespaces.toContentsMap();

    Content content = namespacesMap.get(nessieNamespace.toContentKey());
    if (content == null || !content.getType().equals(NAMESPACE)) {
      throw new NessieContentNotFoundException(
          nessieNamespace.toContentKey(), namespaceRef.referenceName());
    }
    nessieNamespace = (Namespace) content;

    Map<String, String> properties = new HashMap<>(nessieNamespace.getProperties());
    if (!properties.containsKey("location")) {
      StorageUri location = null;
      List<String> remainingElements = null;

      // Find the nearest namespace with a 'location' property and start from there
      for (int n = keysInOrder.size() - 2; n >= 0; n--) {
        Content parent = namespacesMap.get(keysInOrder.get(n));
        if (parent != null && parent.getType().equals(NAMESPACE)) {
          Namespace parentNamespace = (Namespace) parent;
          String parentLocationString = parentNamespace.getProperties().get("location");
          if (parentLocationString != null) {
            location = StorageUri.of(parentLocationString);
            remainingElements =
                nessieNamespace.getElements().subList(n + 1, nessieNamespace.getElementCount());
          }
        }
      }

      // No parent namespace has a 'location' property, start from the warehouse
      if (location == null) {
        WarehouseConfig warehouse = catalogConfig.getWarehouse(decoded.warehouse());
        location = StorageUri.of(warehouse.location()).withTrailingSeparator();
        remainingElements = nessieNamespace.getElements();
      }

      for (String element : remainingElements) {
        location = location.resolve(element).withTrailingSeparator();
      }

      properties.put("location", location.toString());
    }

    return IcebergGetNamespaceResponse.builder()
        .namespace(IcebergNamespace.fromNessieNamespace(nessieNamespace))
        .putAllProperties(properties)
        .build();
  }

  @Operation(operationId = "iceberg.v1.updateNamespaceProperties")
  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/properties")
  @Blocking
  public IcebergUpdateNamespacePropertiesResponse updateProperties(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergUpdateNamespacePropertiesRequest updateNamespacePropertiesRequest)
      throws IOException {
    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    // TODO might want to prevent setting 'location'

    ContentKey key = namespaceRef.namespace().toContentKey();
    GetMultipleContentsResponse namespaces =
        contentService.getMultipleContents(
            namespaceRef.referenceName(),
            namespaceRef.hashWithRelativeSpec(),
            List.of(key),
            false,
            true);
    Reference ref = namespaces.getEffectiveReference();
    Map<ContentKey, Content> namespacesMap = namespaces.toContentsMap();

    Content content = namespacesMap.get(key);
    if (content == null || !content.getType().equals(NAMESPACE)) {
      throw new NessieContentNotFoundException(key, namespaceRef.referenceName());
    }
    Namespace oldNamespace = (Namespace) content;

    HashMap<String, String> newProperties = new HashMap<>(oldNamespace.getProperties());
    updateNamespacePropertiesRequest.removals().forEach(newProperties::remove);
    newProperties.putAll(updateNamespacePropertiesRequest.updates());

    Namespace updatedNamespace =
        Namespace.builder().from(oldNamespace).properties(newProperties).build();

    Operations ops =
        ImmutableOperations.builder()
            .addOperations(org.projectnessie.model.Operation.Put.of(key, updatedNamespace))
            .commitMeta(updateCommitMeta("update namespace " + key))
            .build();

    treeService.commitMultipleOperations(ref.getName(), ref.getHash(), ops);

    IcebergUpdateNamespacePropertiesResponse.Builder response =
        IcebergUpdateNamespacePropertiesResponse.builder();

    oldNamespace.getProperties().keySet().stream()
        .filter(k -> !newProperties.containsKey(k))
        .forEach(response::addRemoved);

    Stream.concat(
            updateNamespacePropertiesRequest.removals().stream(),
            updateNamespacePropertiesRequest.updates().keySet().stream())
        .filter(k -> !oldNamespace.getProperties().containsKey(k))
        .forEach(response::addMissing);

    newProperties.entrySet().stream()
        .filter(
            e -> {
              String newValue = oldNamespace.getProperties().get(e.getKey());
              return !e.getValue().equals(newValue);
            })
        .map(Map.Entry::getKey)
        .forEach(response::addUpdated);

    return response.build();
  }
}
