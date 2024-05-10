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
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.unpartitioned;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder.unsorted;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier.fromNessieContentKey;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieTableSnapshotToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergTableSnapshot;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddPartitionSpec.addPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSchema.addSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSortOrder.addSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID.assignUUID;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentSchema.setCurrentSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultPartitionSpec.setDefaultPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultSortOrder.setDefaultSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetLocation.setLocation;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetProperties.setProperties;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.UpgradeFormatVersion.upgradeFormatVersion;
import static org.projectnessie.catalog.service.rest.TableRef.tableRef;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;

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
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.metrics.IcebergMetricsReport;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergTableMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergListTablesResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadTableResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadTableResult;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRegisterTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRenameTableRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateTableRequest;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.CatalogEntityAlreadyExistsException;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;

/** Handles Iceberg REST API v1 endpoints that are associated with tables. */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1TableResource extends IcebergApiV1ResourceBase {

  @Inject IcebergConfigurer icebergConfigurer;
  @Inject IcebergErrorMapper errorMapper;

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.TABLE);
  }

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

    return this.loadTable(tableRef, prefix);
  }

  private Uni<IcebergLoadTableResponse> loadTable(TableRef tableRef, String prefix)
      throws NessieNotFoundException {
    ContentKey key = tableRef.contentKey();

    return snapshotResponse(
            key,
            SnapshotReqParams.forSnapshotHttpReq(tableRef.reference(), "iceberg", null),
            ICEBERG_TABLE)
        .map(
            snap ->
                loadTableResultFromSnapshotResponse(
                    snap, IcebergLoadTableResponse.builder(), prefix, key));
  }

  private <R extends IcebergLoadTableResult, B extends IcebergLoadTableResult.Builder<R, B>>
      R loadTableResultFromSnapshotResponse(
          SnapshotResponse snap, B builder, String prefix, ContentKey contentKey) {
    IcebergTableMetadata tableMetadata = (IcebergTableMetadata) snap.entityObject().orElseThrow();
    return loadTableResult(
        snapshotMetadataLocation(snap), tableMetadata, builder, prefix, contentKey);
  }

  private <R extends IcebergLoadTableResult, B extends IcebergLoadTableResult.Builder<R, B>>
      R loadTableResult(
          String metadataLocation,
          IcebergTableMetadata tableMetadata,
          B builder,
          String prefix,
          ContentKey contentKey) {
    return builder
        .metadata(tableMetadata)
        .metadataLocation(metadataLocation)
        .putAllConfig(icebergConfigurer.icebergConfigPerTable(tableMetadata, prefix, contentKey))
        .build();
  }

  private ContentResponse fetchIcebergTable(TableRef tableRef) throws NessieNotFoundException {
    ContentResponse content =
        nessieApi
            .getContent()
            .refName(tableRef.reference().name())
            .hashOnRef(tableRef.reference().hashWithRelativeSpec())
            .getSingle(tableRef.contentKey());
    checkArgument(
        content.getContent().getType().equals(ICEBERG_TABLE),
        "Table is not an Iceberg table, it is of type %s",
        content.getContent().getType());
    return content;
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/tables")
  @Blocking
  public Uni<IcebergCreateTableResponse> createTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergCreateTableRequest createTableRequest,
      @HeaderParam("X-Iceberg-Access-Delegation") String dataAccess)
      throws IOException {
    TableRef tableRef = decodeTableRefWithHash(prefix, namespace, createTableRequest.name());

    IcebergSortOrder sortOrder = createTableRequest.writeOrder();
    if (sortOrder == null) {
      sortOrder = unsorted();
    }
    IcebergPartitionSpec spec = createTableRequest.partitionSpec();
    if (spec == null) {
      spec = unpartitioned();
    }
    IcebergSchema schema = createTableRequest.schema();
    String location = createTableRequest.location();
    if (location == null) {
      location = defaultTableLocation(createTableRequest.location(), tableRef);
    }
    checkArgument(
        objectIO.isValidUri(URI.create(location)), "Unsupported table location: " + location);
    Map<String, String> properties = new HashMap<>();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(createTableRequest.properties());

    List<IcebergMetadataUpdate> updates =
        Arrays.asList(
            assignUUID(randomUUID().toString()),
            upgradeFormatVersion(2),
            addSchema(schema, 0),
            setCurrentSchema(-1),
            addPartitionSpec(spec),
            setDefaultPartitionSpec(-1),
            addSortOrder(sortOrder),
            setDefaultSortOrder(-1),
            setLocation(location),
            setProperties(properties));

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
          false, ICEBERG_TABLE, tableRef.contentKey(), existing.getType());
    }
    checkBranch(contentResponse.getEffectiveReference());

    if (createTableRequest.stageCreate()) {
      NessieTableSnapshot snapshot =
          new IcebergTableMetadataUpdateState(
                  newIcebergTableSnapshot(updates), tableRef.contentKey(), false)
              .applyUpdates(updates)
              .snapshot();

      IcebergTableMetadata stagedTableMetadata =
          nessieTableSnapshotToIceberg(snapshot, Optional.empty(), map -> {});

      return Uni.createFrom()
          .item(
              this.loadTableResult(
                  null,
                  stagedTableMetadata,
                  IcebergCreateTableResponse.builder(),
                  prefix,
                  tableRef.contentKey()));
    }

    IcebergUpdateTableRequest updateTableReq =
        IcebergUpdateTableRequest.builder()
            .identifier(fromNessieContentKey(tableRef.contentKey()))
            .addAllUpdates(updates)
            .addRequirement(IcebergUpdateRequirement.AssertCreate.assertTableDoesNotExist())
            .build();

    return createOrUpdateTable(tableRef, updateTableReq)
        .map(
            snap ->
                this.loadTableResultFromSnapshotResponse(
                    snap, IcebergCreateTableResponse.builder(), prefix, tableRef.contentKey()));
  }

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/register")
  @Blocking
  public Uni<IcebergLoadTableResponse> registerTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @Valid IcebergRegisterTableRequest registerTableRequest)
      throws IOException {

    TableRef tableRef = decodeTableRef(prefix, namespace, registerTableRequest.name());

    try {
      ContentResponse response = fetchIcebergTable(tableRef);
      throw new CatalogEntityAlreadyExistsException(
          false, ICEBERG_TABLE, tableRef.contentKey(), response.getContent().getType());
    } catch (NessieContentNotFoundException e) {
      // this is what we want
    }

    Branch ref = checkBranch(nessieApi.getReference().refName(tableRef.reference().name()).get());

    Optional<TableRef> catalogTableRef =
        uriInfo.resolveTableFromUri(registerTableRequest.metadataLocation());
    boolean nessieCatalogUri = uriInfo.isNessieCatalogUri(registerTableRequest.metadataLocation());
    if (catalogTableRef.isPresent() && nessieCatalogUri) {
      // In case the metadataLocation in the IcebergRegisterTableRequest contains a URI for _this_
      // Nessie Catalog, use the existing data/objects.

      // Taking a "shortcut" here, we use the 'old Content object' and re-add it in a Nessie commit.

      TableRef ctr = catalogTableRef.get();

      ContentResponse contentResponse = fetchIcebergTable(ctr);
      // It's technically a new table for Nessie, so need to clear the content-ID.
      Content newContent = contentResponse.getContent().withId(null);

      CommitResponse committed =
          nessieApi
              .commitMultipleOperations()
              .branch(ref)
              .commitMeta(
                  fromMessage(
                      format(
                          "Register Iceberg table '%s' from '%s'",
                          ctr.contentKey(), registerTableRequest.metadataLocation())))
              .operation(Operation.Put.of(ctr.contentKey(), newContent))
              .commitWithResponse();

      return this.loadTable(
          TableRef.tableRef(
              ctr.contentKey(),
              ParsedReference.parsedReference(
                  committed.getTargetBranch().getName(),
                  committed.getTargetBranch().getHash(),
                  BRANCH),
              null),
          prefix);
    } else if (nessieCatalogUri) {
      throw new IllegalArgumentException(
          "Cannot register an Iceberg table using the URI "
              + registerTableRequest.metadataLocation());
    }

    // Register table from "external" metadata-location

    IcebergTableMetadata tableMetadata;
    try (InputStream metadataInput =
        objectIO.readObject(URI.create(registerTableRequest.metadataLocation()))) {
      tableMetadata =
          IcebergJson.objectMapper().readValue(metadataInput, IcebergTableMetadata.class);
    }

    ToIntFunction<Integer> safeUnbox = i -> i != null ? i : 0;

    Content newContent =
        IcebergTable.of(
            registerTableRequest.metadataLocation(),
            tableMetadata.currentSnapshotId(),
            safeUnbox.applyAsInt(tableMetadata.currentSchemaId()),
            safeUnbox.applyAsInt(tableMetadata.defaultSpecId()),
            safeUnbox.applyAsInt(tableMetadata.defaultSortOrderId()));
    CommitResponse committed =
        nessieApi
            .commitMultipleOperations()
            .branch(ref)
            .commitMeta(
                fromMessage(
                    format(
                        "Register Iceberg table '%s' from '%s'",
                        tableRef.contentKey(), registerTableRequest.metadataLocation())))
            .operation(Operation.Put.of(tableRef.contentKey(), newContent))
            .commitWithResponse();

    return this.loadTable(
        tableRef(
            tableRef.contentKey(),
            parsedReference(
                committed.getTargetBranch().getName(),
                committed.getTargetBranch().getHash(),
                committed.getTargetBranch().getType()),
            null),
        prefix);
  }

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

    ContentResponse resp = fetchIcebergTable(tableRef);
    Branch ref = checkBranch(resp.getEffectiveReference());

    nessieApi
        .commitMultipleOperations()
        .branch(ref)
        .commitMeta(fromMessage(format("Drop ICEBERG_TABLE %s", tableRef.contentKey())))
        .operation(Operation.Delete.of(tableRef.contentKey()))
        .commitWithResponse();
  }

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
    String celFilter =
        format(
            "entry.contentType == 'ICEBERG_TABLE' && entry.encodedKey.startsWith('%s.')",
            namespaceRef.namespace().toPathString());

    listContent(namespaceRef, pageToken, pageSize, false, celFilter, response::nextPageToken)
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

  @HEAD
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public void tableExists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    fetchIcebergTable(tableRef);
  }

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

  @POST
  @Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  @Blocking
  public Uni<IcebergCommitTableResponse> updateTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @Valid IcebergUpdateTableRequest commitTableRequest)
      throws IOException {
    TableRef tableRef = decodeTableRefWithHash(prefix, namespace, table);

    return createOrUpdateTable(tableRef, commitTableRequest)
        .map(
            snap -> {
              IcebergTableMetadata tableMetadata =
                  (IcebergTableMetadata) snap.entityObject().orElseThrow();
              return IcebergCommitTableResponse.builder()
                  .metadata(tableMetadata)
                  .metadataLocation(snapshotMetadataLocation(snap))
                  .build();
            });
  }

  Uni<SnapshotResponse> createOrUpdateTable(
      TableRef tableRef, IcebergUpdateTableRequest commitTableRequest) throws IOException {

    boolean isCreate =
        commitTableRequest.requirements().stream()
            .anyMatch(IcebergUpdateRequirement.AssertCreate.class::isInstance);
    if (isCreate) {
      List<IcebergUpdateRequirement> invalidRequirements =
          commitTableRequest.requirements().stream()
              .filter(req -> !(req instanceof IcebergUpdateRequirement.AssertCreate))
              .collect(Collectors.toList());
      checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    IcebergCatalogOperation op =
        IcebergCatalogOperation.builder()
            .updates(commitTableRequest.updates())
            .requirements(commitTableRequest.requirements())
            .key(tableRef.contentKey())
            .type(ICEBERG_TABLE)
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
