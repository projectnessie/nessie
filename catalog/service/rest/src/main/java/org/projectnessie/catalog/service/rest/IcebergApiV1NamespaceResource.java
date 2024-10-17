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
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_CREATE_ENTITY;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_DROP_ENTITY;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_UPDATE_ENTITY;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.META_SET_LOCATION;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.META_SET_PROPERTIES;
import static org.projectnessie.error.ContentKeyErrorDetails.contentKeyErrorDetails;
import static org.projectnessie.model.Content.Type.NAMESPACE;
import static org.projectnessie.services.impl.RefUtil.toReference;
import static org.projectnessie.versioned.RequestMeta.API_READ;
import static org.projectnessie.versioned.RequestMeta.API_WRITE;
import static org.projectnessie.versioned.RequestMeta.apiWrite;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace;
import org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateNamespaceRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateNamespaceResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergGetNamespaceResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListNamespacesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateNamespacePropertiesResponse;
import org.projectnessie.catalog.service.config.LakehouseConfig;
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
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.spi.PagedResponseHandler;
import org.projectnessie.storage.uri.StorageUri;
import org.projectnessie.versioned.RequestMeta;
import org.projectnessie.versioned.VersionStore;

/** Handles Iceberg REST API v1 endpoints that are associated with namespaces. */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1NamespaceResource extends IcebergApiV1ResourceBase {

  @Inject IcebergErrorMapper errorMapper;

  @SuppressWarnings("unused")
  public IcebergApiV1NamespaceResource() {
    this(null, null, null, null, null);
  }

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public IcebergApiV1NamespaceResource(
      ServerConfig serverConfig,
      LakehouseConfig lakehouseConfig,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext) {
    super(serverConfig, lakehouseConfig, store, authorizer, accessContext);
  }

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
              ref.name(), ref.hashWithRelativeSpec(), List.of(key), false, API_READ);
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

    RequestMeta.RequestMetaBuilder requestMeta =
        apiWrite().addKeyAction(key, CATALOG_CREATE_ENTITY.name());
    if (!namespace.getProperties().isEmpty()) {
      requestMeta.addKeyAction(key, META_SET_PROPERTIES.name());
      String location = namespace.getProperties().get("location");
      if (location != null) {
        requestMeta.addKeyAction(key, META_SET_LOCATION.name());
        catalogService
            .validateStorageLocation(location)
            .ifPresent(
                msg -> {
                  throw new IllegalArgumentException(
                      format(
                          "Location for namespace '%s' cannot be associated with any configured object storage location: %s",
                          key, msg));
                });
      }
    }

    treeService.commitMultipleOperations(
        contentsResponse.getEffectiveReference().getName(),
        contentsResponse.getEffectiveReference().getHash(),
        ops,
        requestMeta.build());

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

    var ref = new AtomicReference<Reference>();
    var entries =
        treeService.getEntries(
            namespaceRef.referenceName(),
            namespaceRef.hashWithRelativeSpec(),
            null,
            null,
            null,
            false,
            new PagedResponseHandler<List<EntriesResponse.Entry>, EntriesResponse.Entry>() {
              final List<EntriesResponse.Entry> entries = new ArrayList<>();

              @Override
              public boolean addEntry(EntriesResponse.Entry entry) {
                if (entries.size() == 2) {
                  return false;
                }
                entries.add(entry);
                return true;
              }

              @Override
              public List<EntriesResponse.Entry> build() {
                return entries;
              }

              @Override
              public void hasMore(String pagingToken) {}
            },
            h -> ref.set(toReference(h)),
            null,
            null,
            key,
            List.of());

    if (entries.isEmpty()) {
      throw new NessieNamespaceNotFoundException(
          contentKeyErrorDetails(key),
          String.format("Namespace '%s' does not exist", key.toCanonicalString()));
    }
    if (!NAMESPACE.equals(entries.get(0).getType())) {
      throw new NessieNamespaceNotFoundException(
          contentKeyErrorDetails(key),
          String.format("Namespace '%s' does not exist", key.toCanonicalString()));
    }
    if (entries.size() > 1) {
      throw new NessieNamespaceNotEmptyException(
          contentKeyErrorDetails(key),
          String.format("Namespace '%s' is not empty", key.toCanonicalString()));
    }

    Operations ops =
        ImmutableOperations.builder()
            .addOperations(Delete.of(key))
            .commitMeta(updateCommitMeta("delete namespace " + key))
            .build();

    RequestMeta.RequestMetaBuilder requestMeta =
        apiWrite().addKeyAction(key, CATALOG_DROP_ENTITY.name());
    treeService.commitMultipleOperations(
        ref.get().getName(), ref.get().getHash(), ops, requestMeta.build());
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
                API_READ)
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
            API_READ);
    Map<ContentKey, Content> namespacesMap = namespaces.toContentsMap();

    ContentKey contentKey = nessieNamespace.toContentKey();
    Content content = namespacesMap.get(contentKey);
    if (content == null || !content.getType().equals(NAMESPACE)) {
      throw new NessieContentNotFoundException(contentKey, namespaceRef.referenceName());
    }
    nessieNamespace = (Namespace) content;

    Map<String, String> properties = new HashMap<>(nessieNamespace.getProperties());
    if (!properties.containsKey("location")) {
      WarehouseConfig warehouse = lakehouseConfig.catalog().getWarehouse(decoded.warehouse());
      StorageUri location =
          catalogService
              .locationForEntity(warehouse, contentKey, keysInOrder, namespacesMap)
              .withTrailingSeparator();
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

    ContentKey key = namespaceRef.namespace().toContentKey();
    GetMultipleContentsResponse namespaces =
        contentService.getMultipleContents(
            namespaceRef.referenceName(),
            namespaceRef.hashWithRelativeSpec(),
            List.of(key),
            false,
            API_WRITE);
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

    RequestMeta.RequestMetaBuilder requestMeta =
        apiWrite().addKeyAction(key, CATALOG_UPDATE_ENTITY.name());
    if (!updateNamespacePropertiesRequest.removals().isEmpty()) {
      if (updateNamespacePropertiesRequest.removals().contains("location")) {
        requestMeta.addKeyAction(key, CatalogOps.META_REMOVE_LOCATION_PROPERTY.name());
      }
    }
    if (!updateNamespacePropertiesRequest.updates().isEmpty()) {
      requestMeta.addKeyAction(key, CatalogOps.META_SET_PROPERTIES.name());
      String location = updateNamespacePropertiesRequest.updates().get("location");
      if (location != null) {
        requestMeta.addKeyAction(key, CatalogOps.META_SET_LOCATION.name());
        catalogService
            .validateStorageLocation(location)
            .ifPresent(
                msg -> {
                  throw new IllegalArgumentException(
                      format(
                          "Location for namespace '%s' cannot be associated with any configured object storage location: %s",
                          key, msg));
                });
      }
    }
    treeService.commitMultipleOperations(ref.getName(), ref.getHash(), ops, requestMeta.build());

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
