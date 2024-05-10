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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier.fromNessieContentKey;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSchema.addSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddViewVersion.addViewVersion;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID.assignUUID;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentSchema.setCurrentSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentViewVersion.setCurrentViewVersion;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetLocation.setLocation;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetProperties.setProperties;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.UpgradeFormatVersion.upgradeFormatVersion;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitViewRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateViewRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListTablesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadViewResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRenameTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.CatalogEntityAlreadyExistsException;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.Operation;

/** Handles Iceberg REST API v1 endpoints that are associated with views. */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1ViewResource extends IcebergApiV1ResourceBase {

  @Inject IcebergErrorMapper errorMapper;

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.VIEW);
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/views")
  @Blocking
  public Uni<IcebergLoadViewResponse> createView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergCreateViewRequest createViewRequest)
      throws IOException {
    TableRef tableRef = decodeTableRefWithHash(prefix, namespace, createViewRequest.name());

    createViewRequest.viewVersion();

    IcebergSchema schema = createViewRequest.schema();
    String location = createViewRequest.location();
    if (location == null) {
      location = defaultTableLocation(createViewRequest.location(), tableRef);
    }
    Map<String, String> properties = new HashMap<>();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(createViewRequest.properties());

    List<IcebergMetadataUpdate> updates =
        Arrays.asList(
            assignUUID(randomUUID().toString()),
            upgradeFormatVersion(1),
            addSchema(schema, 0),
            setCurrentSchema(-1),
            setLocation(location),
            setProperties(properties),
            addViewVersion(createViewRequest.viewVersion()),
            setCurrentViewVersion(-1L));

    GetMultipleContentsResponse contentResponse =
        nessieApi
            .getContent()
            .refName(tableRef.reference().name())
            .hashOnRef(tableRef.reference().hashWithRelativeSpec())
            .key(tableRef.contentKey())
            .getWithResponse();
    if (!contentResponse.getContents().isEmpty()) {
      Content existing = contentResponse.getContents().get(0).getContent();
      throw new CatalogEntityAlreadyExistsException(
          false, ICEBERG_VIEW, tableRef.contentKey(), existing.getType());
    }
    checkBranch(contentResponse.getEffectiveReference());

    IcebergCommitViewRequest updateTableReq =
        IcebergCommitViewRequest.builder()
            .identifier(fromNessieContentKey(tableRef.contentKey()))
            .addAllUpdates(updates)
            .addRequirement(IcebergUpdateRequirement.AssertCreate.assertTableDoesNotExist())
            .build();

    return createOrUpdateView(tableRef, updateTableReq)
        .map(snap -> loadViewResultFromSnapshotResponse(snap, IcebergLoadViewResponse.builder()));
  }

  private IcebergLoadViewResponse loadViewResultFromSnapshotResponse(
      SnapshotResponse snap, IcebergLoadViewResponse.Builder builder) {
    IcebergViewMetadata viewMetadata = (IcebergViewMetadata) snap.entityObject().orElseThrow();
    return loadViewResult(snapshotMetadataLocation(snap), viewMetadata, builder);
  }

  private IcebergLoadViewResponse loadViewResult(
      String metadataLocation,
      IcebergViewMetadata viewMetadata,
      IcebergLoadViewResponse.Builder builder) {
    return builder.metadata(viewMetadata).metadataLocation(metadataLocation).build();
  }

  @DELETE
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public void dropView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @QueryParam("purgeRequested") @DefaultValue("false") Boolean purgeRequested)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, view);

    ContentResponse resp = fetchIcebergView(tableRef);
    Branch ref = checkBranch(resp.getEffectiveReference());

    nessieApi
        .commitMultipleOperations()
        .branch(ref)
        .commitMeta(fromMessage(format("Drop ICEBERG_VIEW %s", tableRef.contentKey())))
        .operation(Operation.Delete.of(tableRef.contentKey()))
        .commitWithResponse();
  }

  private ContentResponse fetchIcebergView(TableRef tableRef) throws NessieNotFoundException {
    ContentResponse content =
        nessieApi
            .getContent()
            .refName(tableRef.reference().name())
            .hashOnRef(tableRef.reference().hashWithRelativeSpec())
            .getSingle(tableRef.contentKey());
    checkArgument(
        content.getContent().getType().equals(ICEBERG_VIEW),
        "View is not an Iceberg view, it is of type %s",
        content.getContent().getType());
    return content;
  }

  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}/views")
  @Blocking
  public IcebergListTablesResponse listViews(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize)
      throws IOException {

    IcebergListTablesResponse.Builder response = IcebergListTablesResponse.builder();

    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);
    String celFilter =
        format(
            "entry.contentType == 'ICEBERG_VIEW' && entry.encodedKey.startsWith('%s.')",
            namespaceRef.namespace().toPathString());

    listContent(namespaceRef, pageToken, pageSize, false, celFilter, response::nextPageToken)
        .map(e -> fromNessieContentKey(e.getName()))
        .forEach(response::addIdentifier);

    return response.build();
  }

  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public Uni<IcebergLoadViewResponse> loadView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view)
      throws IOException {

    TableRef tableRef = decodeTableRef(prefix, namespace, view);
    return loadView(tableRef);
  }

  private Uni<IcebergLoadViewResponse> loadView(TableRef tableRef) throws NessieNotFoundException {
    ContentKey key = tableRef.contentKey();

    return snapshotResponse(
            key,
            SnapshotReqParams.forSnapshotHttpReq(tableRef.reference(), "iceberg", null),
            ICEBERG_VIEW)
        .map(snap -> loadViewResultFromSnapshotResponse(snap, IcebergLoadViewResponse.builder()));
  }

  @POST
  @Path("/v1/{prefix}/views/rename")
  @Blocking
  public void renameView(
      @PathParam("prefix") String prefix,
      @Valid @NotNull IcebergRenameTableRequest renameTableRequest)
      throws IOException {

    renameContent(prefix, renameTableRequest, ICEBERG_VIEW);
  }

  @HEAD
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public void viewExists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, view);

    fetchIcebergView(tableRef);
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public Uni<IcebergLoadViewResponse> updateView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @Valid IcebergCommitViewRequest commitViewRequest)
      throws IOException {
    TableRef tableRef = decodeTableRefWithHash(prefix, namespace, view);

    return createOrUpdateView(tableRef, commitViewRequest)
        .map(
            snap -> {
              IcebergViewMetadata viewMetadata =
                  (IcebergViewMetadata) snap.entityObject().orElseThrow();
              return IcebergLoadViewResponse.builder()
                  .metadata(viewMetadata)
                  .metadataLocation(snapshotMetadataLocation(snap))
                  .build();
            });
  }

  Uni<SnapshotResponse> createOrUpdateView(
      TableRef tableRef, IcebergCommitViewRequest commitViewRequest) throws IOException {

    boolean isCreate =
        commitViewRequest.requirements().stream()
            .anyMatch(IcebergUpdateRequirement.AssertCreate.class::isInstance);
    if (isCreate) {
      List<IcebergUpdateRequirement> invalidRequirements =
          commitViewRequest.requirements().stream()
              .filter(req -> !(req instanceof IcebergUpdateRequirement.AssertCreate))
              .collect(Collectors.toList());
      checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    IcebergCatalogOperation op =
        IcebergCatalogOperation.builder()
            .updates(commitViewRequest.updates())
            .requirements(commitViewRequest.requirements())
            .key(tableRef.contentKey())
            .type(ICEBERG_VIEW)
            .build();

    CatalogCommit commit = CatalogCommit.builder().addOperations(op).build();

    SnapshotReqParams reqParams =
        SnapshotReqParams.forSnapshotHttpReq(tableRef.reference(), "iceberg", null);

    CatalogService.CatalogUriResolver catalogUriResolver = new CatalogUriResolverImpl(uriInfo);

    return Uni.createFrom()
        .completionStage(
            catalogService.commit(tableRef.reference(), commit, reqParams, catalogUriResolver))
        .map(Stream::findFirst)
        .map(Optional::orElseThrow);
  }
}
