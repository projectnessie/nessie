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
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse;
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
public class IcebergApiV1S3SignResource extends IcebergApiV1ResourceBase {

  @Inject RequestSigner signer;
  @Inject S3Options<?> s3options;
  @Inject IcebergErrorMapper errorMapper;

  @ServerExceptionMapper
  public Response mapException(Exception ex) {
    return errorMapper.toResponse(ex, IcebergEntityKind.UNKNOWN);
  }

  @POST
  @Path("/v1/{prefix}/s3-sign/{identifier}")
  @Blocking
  public Uni<IcebergS3SignResponse> s3sign(
      IcebergS3SignRequest request,
      @PathParam("prefix") String prefix,
      @PathParam("identifier") String identifier,
      @QueryParam("loc") String baseLocation) {
    return ImmutableIcebergS3SignParams.builder()
        .request(request)
        .ref(decodePrefix(prefix).parsedReference())
        .key(ContentKey.fromPathString(identifier))
        .baseLocation(baseLocation)
        .catalogService(catalogService)
        .signer(signer)
        .s3options(s3options)
        .build()
        .verifyAndSign();
  }
}
