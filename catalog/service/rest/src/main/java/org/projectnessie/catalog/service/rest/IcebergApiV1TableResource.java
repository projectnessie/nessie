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
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.unpartitioned;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder.unsorted;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier.fromNessieContentKey;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.GC_ENABLED;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_CREATE_ENTITY;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_DROP_ENTITY;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_UPDATE_ENTITY;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieTableSnapshotToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergTableSnapshot;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddPartitionSpec.addPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSchema.addSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSortOrder.addSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID.assignUUID;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentSchema.setCurrentSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultPartitionSpec.setDefaultPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultSortOrder.setDefaultSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetProperties.setProperties;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.UpgradeFormatVersion.upgradeFormatVersion;
import static org.projectnessie.catalog.service.files.MetadataUtil.readMetadata;
import static org.projectnessie.catalog.service.rest.TableRef.tableRef;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;
import static org.projectnessie.versioned.RequestMeta.API_WRITE;
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
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToIntFunction;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.metrics.IcebergMetricsReport;
import org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergTableMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListTablesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadCredentialsResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadTableResult;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRegisterTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRenameTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.ImmutableIcebergLoadCredentialsResponse;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.service.api.CatalogEntityAlreadyExistsException;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operations;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.storage.uri.StorageUri;
import org.projectnessie.versioned.RequestMeta.RequestMetaBuilder;
import org.projectnessie.versioned.VersionStore;

/** Handles Iceberg REST API v1 endpoints that are associated with tables. */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1TableResource extends IcebergApiV1ResourceBase {

  @Inject IcebergConfigurer icebergConfigurer;
  @Inject IcebergErrorMapper errorMapper;

  @SuppressWarnings("unused")
  public IcebergApiV1TableResource() {
    this(null, null, null, null, null);
  }

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public IcebergApiV1TableResource(
      ServerConfig serverConfig,
      LakehouseConfig lakehouseConfig,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext) {
    super(serverConfig, lakehouseConfig, store, authorizer, accessContext);
  }

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.TABLE);
  }

  @Operation(operationId = "iceberg.v1.loadTable")
  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public Uni<IcebergLoadTableResponse> loadTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("snapshots") String snapshots,
      @HeaderParam("X-Iceberg-Access-Delegation") String dataAccess)
      throws IOException {

    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    return this.loadTable(tableRef, prefix, dataAccess, false);
  }

  @Operation(operationId = "iceberg.v1.loadCredentials")
  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials")
  @Blocking
  public Uni<IcebergLoadCredentialsResponse> loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("X-Iceberg-Access-Delegation") String dataAccess)
      throws IOException {

    return loadTable(prefix, namespace, table, null, dataAccess)
        .map(
            loadTableResponse -> {
              var creds = loadTableResponse.storageCredentials();

              return ImmutableIcebergLoadCredentialsResponse.of(creds);
            });
  }

  private Uni<IcebergLoadTableResponse> loadTable(
      TableRef tableRef, String prefix, String dataAccess, boolean writeAccessValidated)
      throws NessieNotFoundException {
    ContentKey key = tableRef.contentKey();

    WarehouseConfig warehouse = lakehouseConfig.catalog().getWarehouse(tableRef.warehouse());

    return snapshotResponse(
            key,
            SnapshotReqParams.forSnapshotHttpReq(tableRef.reference(), "iceberg", null),
            ICEBERG_TABLE,
            ICEBERG_V1)
        .map(
            snap ->
                loadTableResultFromSnapshotResponse(
                    snap,
                    IcebergLoadTableResponse.builder(),
                    warehouse.location(),
                    prefix,
                    key,
                    dataAccess,
                    writeAccessValidated));
  }

  private <R extends IcebergLoadTableResult, B extends IcebergLoadTableResult.Builder<R, B>>
      R loadTableResultFromSnapshotResponse(
          SnapshotResponse snap,
          B builder,
          String warehouseLocation,
          String prefix,
          ContentKey contentKey,
          String dataAccess,
          boolean writeAccessValidated) {
    IcebergTableMetadata tableMetadata =
        (IcebergTableMetadata)
            snap.entityObject()
                .orElseThrow(() -> new IllegalStateException("entity object missing"));
    if (!tableMetadata.properties().containsKey(GC_ENABLED)) {
      tableMetadata =
          IcebergTableMetadata.builder()
              .from(tableMetadata)
              .putProperty(GC_ENABLED, "false")
              .build();
    }
    IcebergTable content = (IcebergTable) snap.content();

    if (!writeAccessValidated) {
      // Check whether the current user has write access to the table, if that hasn't been already
      // checked by the caller.
      try {
        contentService.getContent(
            contentKey,
            snap.effectiveReference().getName(),
            snap.effectiveReference().getHash(),
            false,
            API_WRITE);
        writeAccessValidated = true;
      } catch (Exception ignore) {
      }
    }

    return loadTableResult(
        content.getMetadataLocation(),
        snap.nessieSnapshot(),
        warehouseLocation,
        tableMetadata,
        builder,
        prefix,
        contentKey,
        dataAccess,
        writeAccessValidated);
  }

  private <R extends IcebergLoadTableResult, B extends IcebergLoadTableResult.Builder<R, B>>
      R loadTableResult(
          String metadataLocation,
          NessieEntitySnapshot<?> nessieSnapshot,
          String warehouseLocation,
          IcebergTableMetadata tableMetadata,
          B builder,
          String prefix,
          ContentKey contentKey,
          String dataAccess,
          boolean writeAccessGranted) {

    IcebergTableConfig config =
        icebergConfigurer.icebergConfigPerTable(
            nessieSnapshot,
            warehouseLocation,
            tableMetadata,
            prefix,
            contentKey,
            dataAccess,
            writeAccessGranted);

    // Create a new `IcebergTableMetadata`, if needed.
    IcebergTableMetadata resultMetadata =
        config
            .updatedMetadataProperties()
            .map(
                newProps ->
                    IcebergTableMetadata.builder().from(tableMetadata).properties(newProps).build())
            .orElse(tableMetadata);

    return builder
        .metadata(resultMetadata)
        .metadataLocation(metadataLocation)
        .putAllConfig(config.config())
        .build();
  }

  private ContentResponse fetchIcebergTable(TableRef tableRef, boolean forWrite)
      throws NessieNotFoundException {
    return fetchIcebergEntity(tableRef, ICEBERG_TABLE, "table", forWrite, false);
  }

  @Operation(operationId = "iceberg.v1.createTable")
  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/tables")
  @Blocking
  public Uni<IcebergCreateTableResponse> createTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergCreateTableRequest createTableRequest,
      @HeaderParam("X-Iceberg-Access-Delegation") String dataAccess)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, createTableRequest.name());

    IcebergSortOrder sortOrder = createTableRequest.writeOrder();
    if (sortOrder == null) {
      sortOrder = unsorted();
    }
    IcebergPartitionSpec spec = createTableRequest.partitionSpec();
    if (spec == null) {
      spec = unpartitioned();
    }

    Map<String, String> properties = createEntityProperties(createTableRequest.properties());
    properties.putIfAbsent(GC_ENABLED, "false");

    String uuid = randomUUID().toString();

    List<IcebergMetadataUpdate> updates =
        Lists.newArrayList(
            assignUUID(uuid),
            upgradeFormatVersion(2),
            addSchema(createTableRequest.schema()),
            setCurrentSchema(-1),
            addPartitionSpec(spec),
            setDefaultPartitionSpec(-1),
            addSortOrder(sortOrder),
            setDefaultSortOrder(-1),
            setProperties(properties));

    WarehouseConfig warehouse = createEntityCommonOps(tableRef, ICEBERG_TABLE, updates);

    if (createTableRequest.stageCreate()) {
      NessieTableSnapshot snapshot =
          new IcebergTableMetadataUpdateState(
                  newIcebergTableSnapshot(uuid), tableRef.contentKey(), false)
              .applyUpdates(updates)
              .snapshot();

      IcebergTableMetadata stagedTableMetadata =
          nessieTableSnapshotToIceberg(
              snapshot,
              Optional.empty(),
              map -> map.put(IcebergTableMetadata.STAGED_PROPERTY, "true"));

      return Uni.createFrom()
          .item(
              this.loadTableResult(
                  null,
                  snapshot,
                  warehouse.location(),
                  stagedTableMetadata,
                  IcebergCreateTableResponse.builder(),
                  prefix,
                  tableRef.contentKey(),
                  dataAccess,
                  true));
    }

    IcebergUpdateTableRequest updateTableReq =
        IcebergUpdateTableRequest.builder()
            .identifier(fromNessieContentKey(tableRef.contentKey()))
            .addAllUpdates(updates)
            .addRequirement(IcebergUpdateRequirement.AssertCreate.assertTableDoesNotExist())
            .build();

    return createOrUpdateEntity(tableRef, updateTableReq, ICEBERG_TABLE, CATALOG_CREATE_ENTITY)
        .map(
            snap ->
                this.loadTableResultFromSnapshotResponse(
                    snap,
                    IcebergCreateTableResponse.builder(),
                    warehouse.location(),
                    prefix,
                    tableRef.contentKey(),
                    dataAccess,
                    true));
  }

  @Operation(operationId = "iceberg.v1.registerTable")
  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/register")
  @Blocking
  public Uni<IcebergLoadTableResponse> registerTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergRegisterTableRequest registerTableRequest,
      @HeaderParam("X-Iceberg-Access-Delegation") String dataAccess)
      throws IOException {

    TableRef tableRef = decodeTableRef(prefix, namespace, registerTableRequest.name());

    try {
      ContentResponse response = fetchIcebergTable(tableRef, false);
      throw new CatalogEntityAlreadyExistsException(
          false, ICEBERG_TABLE, tableRef.contentKey(), response.getContent().getType());
    } catch (NessieContentNotFoundException e) {
      // this is what we want
    }

    ParsedReference reference = requireNonNull(tableRef.reference());
    Branch ref = checkBranch(treeService.getReferenceByName(reference.name(), FetchOption.MINIMAL));

    RequestMetaBuilder requestMeta =
        apiWrite().addKeyAction(tableRef.contentKey(), CatalogOps.CATALOG_REGISTER_ENTITY.name());

    Optional<TableRef> catalogTableRef =
        uriInfo.resolveTableFromUri(registerTableRequest.metadataLocation());
    boolean nessieCatalogUri = uriInfo.isNessieCatalogUri(registerTableRequest.metadataLocation());
    if (catalogTableRef.isPresent() && nessieCatalogUri) {
      // In case the metadataLocation in the IcebergRegisterTableRequest contains a URI for _this_
      // Nessie Catalog, use the existing data/objects.

      // Taking a "shortcut" here, we use the 'old Content object' and re-add it in a Nessie commit.

      TableRef ctr = catalogTableRef.get();

      ContentResponse contentResponse = fetchIcebergTable(ctr, true);
      // It's technically a new table for Nessie, so need to clear the content-ID.
      Content newContent = contentResponse.getContent().withId(null);

      Operations ops =
          ImmutableOperations.builder()
              .addOperations(Put.of(ctr.contentKey(), newContent))
              .commitMeta(
                  updateCommitMeta(
                      format(
                          "Register Iceberg table '%s' from '%s'",
                          ctr.contentKey(), registerTableRequest.metadataLocation())))
              .build();
      CommitResponse committed =
          treeService.commitMultipleOperations(
              ref.getName(), ref.getHash(), ops, requestMeta.build());

      return this.loadTable(
          TableRef.tableRef(
              ctr.contentKey(),
              ParsedReference.parsedReference(
                  committed.getTargetBranch().getName(),
                  committed.getTargetBranch().getHash(),
                  BRANCH),
              tableRef.warehouse()),
          prefix,
          dataAccess,
          true);
    } else if (nessieCatalogUri) {
      throw new IllegalArgumentException(
          "Cannot register an Iceberg table using the URI "
              + registerTableRequest.metadataLocation());
    }

    // Register table from "external" metadata-location

    IcebergTableMetadata tableMetadata;
    try (InputStream metadataInput =
        readMetadata(objectIO, StorageUri.of(registerTableRequest.metadataLocation()))) {
      tableMetadata =
          IcebergJson.objectMapper().readValue(metadataInput, IcebergTableMetadata.class);
    }

    catalogService
        .validateStorageLocation(tableMetadata.location())
        .ifPresent(
            msg -> {
              throw new IllegalArgumentException(
                  format(
                      "Location for table '%s' to be registered cannot be associated with any configured object storage location: %s",
                      tableRef.contentKey(), msg));
            });

    ToIntFunction<Integer> safeUnbox = i -> i != null ? i : 0;

    Content newContent =
        IcebergTable.of(
            registerTableRequest.metadataLocation(),
            tableMetadata.currentSnapshotIdAsLong(),
            safeUnbox.applyAsInt(tableMetadata.currentSchemaId()),
            safeUnbox.applyAsInt(tableMetadata.defaultSpecId()),
            safeUnbox.applyAsInt(tableMetadata.defaultSortOrderId()));
    Operations ops =
        ImmutableOperations.builder()
            .addOperations(Put.of(tableRef.contentKey(), newContent))
            .commitMeta(
                updateCommitMeta(
                    format(
                        "Register Iceberg table '%s' from '%s'",
                        tableRef.contentKey(), registerTableRequest.metadataLocation())))
            .build();
    CommitResponse committed =
        treeService.commitMultipleOperations(
            ref.getName(), ref.getHash(), ops, requestMeta.build());

    return this.loadTable(
        tableRef(
            tableRef.contentKey(),
            parsedReference(
                committed.getTargetBranch().getName(),
                committed.getTargetBranch().getHash(),
                committed.getTargetBranch().getType()),
            tableRef.warehouse()),
        prefix,
        dataAccess,
        true);
  }

  @Operation(operationId = "iceberg.v1.dropTable")
  @DELETE
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public void dropTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("purgeRequested") @DefaultValue("false") Boolean purgeRequested)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    ContentResponse resp = fetchIcebergTable(tableRef, false);
    Branch ref = checkBranch(resp.getEffectiveReference());

    Operations ops =
        ImmutableOperations.builder()
            .addOperations(Delete.of(tableRef.contentKey()))
            .commitMeta(updateCommitMeta(format("Drop ICEBERG_TABLE %s", tableRef.contentKey())))
            .build();

    RequestMetaBuilder requestMeta =
        apiWrite().addKeyAction(tableRef.contentKey(), CATALOG_DROP_ENTITY.name());
    treeService.commitMultipleOperations(ref.getName(), ref.getHash(), ops, requestMeta.build());
  }

  @Operation(operationId = "iceberg.v1.listTables")
  @GET
  @Path("/v1/{prefix}/namespaces/{namespace}/tables")
  @Blocking
  public IcebergListTablesResponse listTables(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize)
      throws IOException {
    IcebergListTablesResponse.Builder response = IcebergListTablesResponse.builder();

    NamespaceRef namespaceRef = decodeNamespaceRef(prefix, namespace);

    listContent(namespaceRef, "ICEBERG_TABLE", pageToken, pageSize, false, response::nextPageToken)
        .map(e -> fromNessieContentKey(e.getName()))
        .forEach(response::addIdentifier);

    return response.build();
  }

  @POST
  @Path("/v1/{prefix}/tables/rename")
  @Blocking
  public void renameTable(
      @PathParam("prefix") String prefix,
      @Valid @NotNull IcebergRenameTableRequest renameTableRequest)
      throws IOException {

    renameContent(prefix, renameTableRequest, ICEBERG_TABLE);
  }

  @Operation(operationId = "iceberg.v1.tableExists")
  @HEAD
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public void tableExists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    fetchIcebergEntity(tableRef, ICEBERG_TABLE, "table", false, true);
  }

  @Operation(operationId = "iceberg.v1.tableMetrics")
  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics")
  @Blocking
  public void reportMetrics(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @Valid @NotNull IcebergMetricsReport reportMetricsRequest) {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    // Using the effective reference from ContentResponse would be wrong here, because we do not
    // know the commit ID for/on which the metrics were generated, unless the hash is included in
    // TableRef.

    pushMetrics(tableRef, reportMetricsRequest);
  }

  private void pushMetrics(TableRef tableRef, IcebergMetricsReport report) {
    // TODO push metrics to "somewhere".
    // TODO note that metrics for "staged tables" are also received, even if those do not yet exist
  }

  @Operation(operationId = "iceberg.v1.updateTable")
  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public Uni<IcebergCommitTableResponse> updateTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @Valid IcebergUpdateTableRequest commitTableRequest)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    // TODO allow updating the "location" property, rely on AuthZ
    // TODO verify the "location" can be mapped to a configured object-store

    return createOrUpdateEntity(tableRef, commitTableRequest, ICEBERG_TABLE, CATALOG_UPDATE_ENTITY)
        .map(
            snap -> {
              IcebergTableMetadata tableMetadata =
                  (IcebergTableMetadata)
                      snap.entityObject()
                          .orElseThrow(() -> new IllegalStateException("entity object missing"));
              IcebergTable content = (IcebergTable) snap.content();
              return IcebergCommitTableResponse.builder()
                  .metadata(tableMetadata)
                  .metadataLocation(content.getMetadataLocation())
                  .build();
            });
  }
}
