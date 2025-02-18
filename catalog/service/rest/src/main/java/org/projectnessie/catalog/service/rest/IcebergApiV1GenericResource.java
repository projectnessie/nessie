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

import static java.util.Objects.requireNonNull;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_UPDATE_MULTIPLE;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergEndpoint.icebergEndpoint;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.security.PermitAll;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
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
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitTransactionRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergConfigResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergEndpoint;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.VersionStore;

/**
 * Handles Iceberg REST API v1 endpoints that are not strongly associated with a particular entity
 * type.
 */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1GenericResource extends IcebergApiV1ResourceBase {

  @Inject IcebergConfigurer icebergConfigurer;
  @Inject IcebergErrorMapper errorMapper;

  public static final String V1_NAMESPACES = "/v1/{prefix}/namespaces";
  public static final String V1_NAMESPACE = "/v1/{prefix}/namespaces/{namespace}";
  public static final String V1_NAMESPACE_PROPERTIES =
      "/v1/{prefix}/namespaces/{namespace}/properties";
  public static final String V1_TABLES = "/v1/{prefix}/namespaces/{namespace}/tables";
  public static final String V1_TABLE = "/v1/{prefix}/namespaces/{namespace}/tables/{table}";
  public static final String V1_TABLE_REGISTER = "/v1/{prefix}/namespaces/{namespace}/register";
  public static final String V1_TABLE_METRICS =
      "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics";
  public static final String V1_TABLE_RENAME = "/v1/{prefix}/tables/rename";
  public static final String V1_TRANSACTIONS_COMMIT = "/v1/{prefix}/transactions/commit";
  public static final String V1_VIEWS = "/v1/{prefix}/namespaces/{namespace}/views";
  public static final String V1_VIEW = "/v1/{prefix}/namespaces/{namespace}/views/{view}";
  public static final String V1_VIEW_RENAME = "/v1/{prefix}/views/rename";

  // NOT yet implemented
  public static final String V1_TABLE_LOAD_CREDENTIALS =
      "/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials";

  // NOT implemented
  public static final String V1_TABLE_PLAN_SUBMIT =
      "/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan";
  public static final String V1_TABLE_PLAN_FETCH_RESULT =
      "/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}";
  public static final String V1_TABLE_PLAN_FETCH_SCAN_TASKS =
      "/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks";

  // namespace endpoints
  public static final IcebergEndpoint V1_LIST_NAMESPACES = icebergEndpoint("GET", V1_NAMESPACES);
  public static final IcebergEndpoint V1_LOAD_NAMESPACE = icebergEndpoint("GET", V1_NAMESPACE);
  public static final IcebergEndpoint V1_NAMESPACE_EXISTS = icebergEndpoint("HEAD", V1_NAMESPACE);
  public static final IcebergEndpoint V1_CREATE_NAMESPACE = icebergEndpoint("POST", V1_NAMESPACES);
  public static final IcebergEndpoint V1_UPDATE_NAMESPACE =
      icebergEndpoint("POST", V1_NAMESPACE_PROPERTIES);
  public static final IcebergEndpoint V1_DELETE_NAMESPACE = icebergEndpoint("DELETE", V1_NAMESPACE);
  public static final IcebergEndpoint V1_COMMIT_TRANSACTION =
      icebergEndpoint("POST", V1_TRANSACTIONS_COMMIT);

  // table endpoints
  public static final IcebergEndpoint V1_LIST_TABLES = icebergEndpoint("GET", V1_TABLES);
  public static final IcebergEndpoint V1_LOAD_TABLE = icebergEndpoint("GET", V1_TABLE);
  public static final IcebergEndpoint V1_TABLE_EXISTS = icebergEndpoint("HEAD", V1_TABLE);
  public static final IcebergEndpoint V1_CREATE_TABLE = icebergEndpoint("POST", V1_TABLES);
  public static final IcebergEndpoint V1_UPDATE_TABLE = icebergEndpoint("POST", V1_TABLE);
  public static final IcebergEndpoint V1_DELETE_TABLE = icebergEndpoint("DELETE", V1_TABLE);
  public static final IcebergEndpoint V1_RENAME_TABLE = icebergEndpoint("POST", V1_TABLE_RENAME);
  public static final IcebergEndpoint V1_REGISTER_TABLE =
      icebergEndpoint("POST", V1_TABLE_REGISTER);
  public static final IcebergEndpoint V1_REPORT_METRICS = icebergEndpoint("POST", V1_TABLE_METRICS);

  // view endpoints
  public static final IcebergEndpoint V1_LIST_VIEWS = icebergEndpoint("GET", V1_VIEWS);
  public static final IcebergEndpoint V1_LOAD_VIEW = icebergEndpoint("GET", V1_VIEW);
  public static final IcebergEndpoint V1_VIEW_EXISTS = icebergEndpoint("HEAD", V1_VIEW);
  public static final IcebergEndpoint V1_CREATE_VIEW = icebergEndpoint("POST", V1_VIEWS);
  public static final IcebergEndpoint V1_UPDATE_VIEW = icebergEndpoint("POST", V1_VIEW);
  public static final IcebergEndpoint V1_DELETE_VIEW = icebergEndpoint("DELETE", V1_VIEW);
  public static final IcebergEndpoint V1_RENAME_VIEW = icebergEndpoint("POST", V1_VIEW_RENAME);

  static final List<IcebergEndpoint> ENDPOINTS =
      List.of(
          // namespace endpoints
          V1_LIST_NAMESPACES,
          V1_LOAD_NAMESPACE,
          V1_NAMESPACE_EXISTS,
          V1_CREATE_NAMESPACE,
          V1_UPDATE_NAMESPACE,
          V1_DELETE_NAMESPACE,
          V1_COMMIT_TRANSACTION,

          // table endpoints
          V1_LIST_TABLES,
          V1_LOAD_TABLE,
          V1_TABLE_EXISTS,
          V1_CREATE_TABLE,
          V1_UPDATE_TABLE,
          V1_DELETE_TABLE,
          V1_RENAME_TABLE,
          V1_REGISTER_TABLE,
          V1_REPORT_METRICS,

          // view endpoints
          V1_LIST_VIEWS,
          V1_LOAD_VIEW,
          V1_VIEW_EXISTS,
          V1_CREATE_VIEW,
          V1_UPDATE_VIEW,
          V1_DELETE_VIEW,
          V1_RENAME_VIEW
          //
          );

  @SuppressWarnings("unused")
  public IcebergApiV1GenericResource() {
    this(null, null, null, null, null);
  }

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public IcebergApiV1GenericResource(
      ServerConfig serverConfig,
      LakehouseConfig lakehouseConfig,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext) {
    super(serverConfig, lakehouseConfig, store, authorizer, accessContext);
  }

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.UNKNOWN);
  }

  /** Exposes the Iceberg REST configuration for the Nessie default branch. */
  @Operation(operationId = "iceberg.v1.getConfig")
  @GET
  @Path("/v1/config")
  public IcebergConfigResponse getConfig(@QueryParam("warehouse") String warehouse) {
    return getConfig(null, warehouse);
  }

  /**
   * Exposes the Iceberg REST configuration for the named Nessie {@code reference} in the
   * {@code @Path} parameter.
   */
  @Operation(operationId = "iceberg.v1.getConfig.reference")
  @GET
  @Path("{reference}/v1/config")
  public IcebergConfigResponse getConfig(
      @PathParam("reference") String reference, @QueryParam("warehouse") String warehouse) {
    IcebergConfigResponse.Builder configResponse = IcebergConfigResponse.builder();
    icebergConfigurer.icebergWarehouseConfig(
        reference, warehouse, configResponse::putDefault, configResponse::putOverride);
    configResponse.endpoints(ENDPOINTS);
    return configResponse.build();
  }

  @Operation(operationId = "iceberg.v1.getToken")
  @POST
  @Path("/v1/oauth/tokens")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @PermitAll
  public Response getToken() {
    return Response.status(Response.Status.NOT_IMPLEMENTED)
        .entity(
            Map.of(
                "error",
                "NotImplemented",
                "error_description",
                "Endpoint not implemented: please configure "
                    + "the catalog client with the oauth2-server-uri property."))
        .build();
  }

  @Operation(operationId = "iceberg.v1.getToken.reference")
  @POST
  @Path("/{reference}/v1/oauth/tokens")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @PermitAll
  public Response getToken(@PathParam("reference") String ignored) {
    return getToken();
  }

  @Operation(operationId = "iceberg.v1.commitTransaction")
  @POST
  @Path("/v1/{prefix}/transactions/commit")
  @Blocking
  public Uni<Void> commitTransaction(
      @PathParam("prefix") String prefix,
      @Valid IcebergCommitTransactionRequest commitTransactionRequest)
      throws IOException {
    DecodedPrefix decoded = decodePrefix(prefix);
    ParsedReference ref = decoded.parsedReference();

    CatalogCommit.Builder commit = CatalogCommit.builder();
    commitTransactionRequest.tableChanges().stream()
        .map(
            tableChange ->
                IcebergCatalogOperation.builder()
                    .updates(tableChange.updates())
                    .requirements(tableChange.requirements())
                    .key(requireNonNull(tableChange.identifier()).toNessieContentKey())
                    .type(ICEBERG_TABLE)
                    .build())
        .forEach(commit::addOperations);

    SnapshotReqParams reqParams = SnapshotReqParams.forSnapshotHttpReq(ref, "iceberg", null);

    // Although we don't return anything, need to make sure that the commit operation starts and all
    // results are consumed.
    return Uni.createFrom()
        .completionStage(
            catalogService.commit(
                ref,
                commit.build(),
                reqParams,
                this::updateCommitMeta,
                CATALOG_UPDATE_MULTIPLE.name(),
                ICEBERG_V1))
        .map(stream -> stream.reduce(null, (ident, snap) -> ident, (i1, i2) -> i1));
  }
}
