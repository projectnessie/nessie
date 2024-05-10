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
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse.icebergS3SignResponse;
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
import java.net.URI;
import java.util.Optional;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.api.SigningRequest;
import org.projectnessie.catalog.files.api.SigningResponse;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCommitTransactionRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergConfigResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.rest.IcebergErrorMapper.IcebergEntityKind;
import org.projectnessie.model.ContentKey;

/**
 * Handles Iceberg REST API v1 endpoints that are not strongly associated with a particular entity
 * type.
 */
@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces({MediaType.APPLICATION_JSON})
@Path("iceberg")
public class IcebergApiV1GenericResource extends IcebergApiV1ResourceBase {

  @Inject RequestSigner signer;
  @Inject S3Options<?> s3options;
  @Inject IcebergConfigurer icebergConfigurer;
  @Inject IcebergErrorMapper errorMapper;

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.UNKNOWN);
  }

  /** Exposes the Iceberg REST configuration for the Nessie default branch. */
  @GET
  @Path("/v1/config")
  @PermitAll
  public IcebergConfigResponse getConfig(@QueryParam("warehouse") String warehouse) {
    return getConfig(null, warehouse);
  }

  /**
   * Exposes the Iceberg REST configuration for the named Nessie {@code reference} in the
   * {@code @Path} parameter.
   */
  @GET
  @Path("{reference}/v1/config")
  @PermitAll
  public IcebergConfigResponse getConfig(
      @PathParam("reference") String reference, @QueryParam("warehouse") String warehouse) {
    return IcebergConfigResponse.builder()
        .defaults(icebergConfigurer.icebergConfigDefaults(reference, warehouse))
        .overrides(icebergConfigurer.icebergConfigOverrides(reference, warehouse))
        .build();
  }

  // TODO inject some path parameters to identify the table in a secure way to then do the
  //  access-check against the table and eventually sign the request.
  //  The endpoint can be tweaked sing the S3_SIGNER_ENDPOINT property, signing to be
  //  enabled via S3_REMOTE_SIGNING_ENABLED.
  @POST
  @Path("/v1/{prefix}/s3-sign/{identifier}")
  @Blocking
  public IcebergS3SignResponse s3sign(
      IcebergS3SignRequest request,
      @PathParam("prefix") String prefix,
      @PathParam("identifier") String identifier) {
    ParsedReference ref = decodePrefix(prefix).parsedReference();

    URI uri = URI.create(request.uri());

    Optional<String> bucket = s3options.extractBucket(uri);
    Optional<String> body = Optional.ofNullable(request.body());

    SigningRequest signingRequest =
        SigningRequest.signingRequest(
            uri, request.method(), request.region(), bucket, body, request.headers());

    SigningResponse signed = signer.sign(ref.name(), identifier, signingRequest);

    return icebergS3SignResponse(signed.uri().toString(), signed.headers());
  }

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
            tableChange -> {
              ContentKey key = requireNonNull(tableChange.identifier()).toNessieContentKey();

              return IcebergCatalogOperation.builder()
                  .updates(tableChange.updates())
                  .requirements(tableChange.requirements())
                  .key(key)
                  .type(ICEBERG_TABLE)
                  .build();
            })
        .forEach(commit::addOperations);

    SnapshotReqParams reqParams = SnapshotReqParams.forSnapshotHttpReq(ref, "iceberg", null);

    CatalogService.CatalogUriResolver catalogUriResolver = new CatalogUriResolverImpl(uriInfo);

    // Although we don't return anything, need to make sure that the commit operation starts and all
    // results are consumed.
    return Uni.createFrom()
        .completionStage(catalogService.commit(ref, commit.build(), reqParams, catalogUriResolver))
        .map(stream -> stream.reduce(null, (ident, snap) -> ident, (i1, i2) -> i1));
  }
}
