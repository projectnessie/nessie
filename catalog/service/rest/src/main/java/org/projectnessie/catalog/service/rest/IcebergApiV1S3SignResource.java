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

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse;
import org.projectnessie.catalog.service.api.SignerKeysService;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.objtypes.SignerKey;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;
import org.projectnessie.model.ContentKey;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.VersionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Iceberg REST API v1 endpoints that are not strongly associated with a particular entity
 * type.
 */
@SuppressWarnings("CdiInjectionPointsInspection")
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1S3SignResource extends IcebergApiV1ResourceBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergApiV1S3SignResource.class);

  @Inject RequestSigner signer;
  @Inject IcebergErrorMapper errorMapper;
  @Inject SignerKeysService signerKeysService;
  @Inject UriInfo uriInfo;

  Clock clock = Clock.systemUTC();

  @SuppressWarnings("unused")
  public IcebergApiV1S3SignResource() {
    this(null, null, null, null, null);
  }

  @Inject
  public IcebergApiV1S3SignResource(
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

  @Operation(operationId = "iceberg.v1.s3sign.blob")
  @POST
  @Path("/v1/{prefix}/s3sign/{signedParams}")
  @Blocking
  public Uni<IcebergS3SignResponse> s3signWIthOpaqueParams(
      IcebergS3SignRequest request,
      @PathParam("prefix") String prefix,
      @PathParam("signedParams") String signedParams) {

    SignerParams signerParams = SignerParams.fromPathParam(signedParams);
    SignerKey signerKey = signerKeysService.getSignerKey(signerParams.keyName());

    SignerSignature signerSignature = signerParams.signerSignature();
    Optional<String> verifyError =
        signerSignature.verify(signerKey, signerParams.signature(), clock.instant());

    if (verifyError.isPresent()) {
      LOGGER.warn("{} for request {}", verifyError.get(), uriInfo.getRequestUri());
      throw new IllegalArgumentException("Invalid signature");
    }

    return ImmutableIcebergS3SignParams.builder()
        .request(request)
        .ref(decodePrefix(prefix).parsedReference())
        .key(ContentKey.fromPathString(signerSignature.identifier()))
        .warehouseLocation(signerSignature.warehouseLocation())
        .writeLocations(signerSignature.writeLocations())
        .readLocations(signerSignature.readLocations())
        .catalogService(catalogService)
        .signer(signer)
        .build()
        .verifyAndSign();
  }

  @Operation(operationId = "iceberg.v1.s3sign")
  @POST
  @Path("/v1/{prefix}/s3-sign/{identifier}")
  @Blocking
  public Uni<IcebergS3SignResponse> s3sign(
      IcebergS3SignRequest request,
      @PathParam("prefix") String prefix,
      @PathParam("identifier") String identifier,
      @NotNull @QueryParam("b") String warehouseLocation,
      @QueryParam("w") List<String> writeLocations,
      @QueryParam("r") List<String> readLocations,
      @NotNull @QueryParam("e") Long expirationTimestamp,
      @NotNull @QueryParam("k") String keyName,
      @NotNull @QueryParam("s") String signature) {

    SignerKey signerKey = signerKeysService.getSignerKey(keyName);

    Optional<String> verifyError =
        SignerSignature.builder()
            .prefix(prefix)
            .identifier(identifier)
            .warehouseLocation(warehouseLocation)
            .writeLocations(writeLocations)
            .readLocations(readLocations)
            .expirationTimestamp(expirationTimestamp)
            .build()
            .verify(signerKey, signature, clock.instant());

    if (verifyError.isPresent()) {
      LOGGER.warn("{} for request {}", verifyError.get(), uriInfo.getRequestUri());
      throw new IllegalArgumentException("Invalid signature");
    }

    return ImmutableIcebergS3SignParams.builder()
        .request(request)
        .ref(decodePrefix(prefix).parsedReference())
        .key(ContentKey.fromPathString(identifier))
        .warehouseLocation(warehouseLocation)
        .writeLocations(writeLocations)
        .readLocations(readLocations)
        .catalogService(catalogService)
        .signer(signer)
        .build()
        .verifyAndSign();
  }
}
