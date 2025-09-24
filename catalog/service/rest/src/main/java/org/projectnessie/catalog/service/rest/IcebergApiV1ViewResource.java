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
import static java.util.UUID.randomUUID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier.fromNessieContentKey;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_CREATE_ENTITY;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_DROP_ENTITY;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_UPDATE_ENTITY;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSchema.addSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddViewVersion.addViewVersion;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID.assignUUID;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentSchema.setCurrentSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentViewVersion.setCurrentViewVersion;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetProperties.setProperties;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.UpgradeFormatVersion.upgradeFormatVersion;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;
import static org.projectnessie.versioned.RequestMeta.apiWrite;

import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitViewRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateViewRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListTablesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadViewResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRenameTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operations;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.RequestMeta;
import org.projectnessie.versioned.VersionStore;

/** Handles Iceberg REST API v1 endpoints that are associated with views. */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1ViewResource extends IcebergApiV1ResourceBase {

  @Inject IcebergErrorMapper errorMapper;

  @SuppressWarnings("unused")
  public IcebergApiV1ViewResource() {
    this(null, null, null, null, null);
  }

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public IcebergApiV1ViewResource(
      ServerConfig serverConfig,
      LakehouseConfig lakehouseConfig,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext) {
    super(serverConfig, lakehouseConfig, store, authorizer, accessContext);
  }

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.VIEW);
  }

  @Operation(operationId = "iceberg.v1.createView")
  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/views")
  @Blocking
  public Uni<IcebergLoadViewResponse> createView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergCreateViewRequest createViewRequest)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, createViewRequest.name());

    createViewRequest.viewVersion();

    Map<String, String> properties = createEntityProperties(createViewRequest.properties());

    List<IcebergMetadataUpdate> updates =
        Lists.newArrayList(
            assignUUID(randomUUID().toString()),
            upgradeFormatVersion(1),
            addSchema(createViewRequest.schema()),
            setCurrentSchema(-1),
            setProperties(properties),
            addViewVersion(createViewRequest.viewVersion()),
            setCurrentViewVersion(-1L));

    createEntityCommonOps(tableRef, ICEBERG_VIEW, updates);

    IcebergCommitViewRequest updateTableReq =
        IcebergCommitViewRequest.builder()
            .identifier(fromNessieContentKey(tableRef.contentKey()))
            .addAllUpdates(updates)
            .addRequirement(IcebergUpdateRequirement.AssertCreate.assertTableDoesNotExist())
            .build();

    return createOrUpdateEntity(tableRef, updateTableReq, ICEBERG_VIEW, CATALOG_CREATE_ENTITY)
        .map(snap -> loadViewResultFromSnapshotResponse(snap, IcebergLoadViewResponse.builder()));
  }

  private IcebergLoadViewResponse loadViewResultFromSnapshotResponse(
      SnapshotResponse snap, IcebergLoadViewResponse.Builder builder) {
    IcebergView content = (IcebergView) snap.content();
    IcebergViewMetadata viewMetadata =
        (IcebergViewMetadata)
            snap.entityObject()
                .orElseThrow(() -> new IllegalStateException("entity object missing"));
    return loadViewResult(content.getMetadataLocation(), viewMetadata, builder);
  }

  private IcebergLoadViewResponse loadViewResult(
      String metadataLocation,
      IcebergViewMetadata viewMetadata,
      IcebergLoadViewResponse.Builder builder) {
    return builder.metadata(viewMetadata).metadataLocation(metadataLocation).build();
  }

  @Operation(operationId = "iceberg.v1.dropView")
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

    Operations ops =
        ImmutableOperations.builder()
            .addOperations(Delete.of(tableRef.contentKey()))
            .commitMeta(updateCommitMeta(format("Drop ICEBERG_VIEW %s", tableRef.contentKey())))
            .build();

    RequestMeta.RequestMetaBuilder requestMeta =
        apiWrite().addKeyAction(tableRef.contentKey(), CATALOG_DROP_ENTITY.name());
    treeService.commitMultipleOperations(ref.getName(), ref.getHash(), ops, requestMeta.build());
  }

  private ContentResponse fetchIcebergView(TableRef tableRef) throws NessieNotFoundException {
    return fetchIcebergEntity(tableRef, ICEBERG_VIEW, "view", false, false);
  }

  @Operation(operationId = "iceberg.v1.listViews")
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

    listContent(namespaceRef, "ICEBERG_VIEW", pageToken, pageSize, false, response::nextPageToken)
        .map(e -> fromNessieContentKey(e.getName()))
        .forEach(response::addIdentifier);

    return response.build();
  }

  @Operation(operationId = "iceberg.v1.loadView")
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
            ICEBERG_VIEW,
            ICEBERG_V1)
        .map(snap -> loadViewResultFromSnapshotResponse(snap, IcebergLoadViewResponse.builder()));
  }

  @Operation(operationId = "iceberg.v1.renameView")
  @POST
  @Path("/v1/{prefix}/views/rename")
  @Blocking
  public void renameView(
      @PathParam("prefix") String prefix,
      @Valid @NotNull IcebergRenameTableRequest renameTableRequest)
      throws IOException {

    renameContent(prefix, renameTableRequest, ICEBERG_VIEW);
  }

  @Operation(operationId = "iceberg.v1.viewExists")
  @HEAD
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public void viewExists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, view);

    fetchIcebergEntity(tableRef, ICEBERG_VIEW, "view", false, true);
  }

  @Operation(operationId = "iceberg.v1.updateView")
  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/views/{view}")
  @Blocking
  public Uni<IcebergLoadViewResponse> updateView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @Valid IcebergCommitViewRequest commitViewRequest)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, view);

    return createOrUpdateEntity(tableRef, commitViewRequest, ICEBERG_VIEW, CATALOG_UPDATE_ENTITY)
        .map(
            snap -> {
              IcebergViewMetadata viewMetadata =
                  (IcebergViewMetadata)
                      snap.entityObject()
                          .orElseThrow(() -> new IllegalStateException("entity object missing"));
              IcebergView content = (IcebergView) snap.content();
              return IcebergLoadViewResponse.builder()
                  .metadata(viewMetadata)
                  .metadataLocation(content.getMetadataLocation())
                  .build();
            });
  }
}
