/*
 * Copyright (C) 2023 Dremio
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

import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_UPDATE_ENTITY;
import static org.projectnessie.catalog.service.api.SnapshotReqParams.forSnapshotHttpReq;
import static org.projectnessie.catalog.service.rest.ExternalBaseUri.parseRefPathString;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Validation.REF_NAME_PATH_ELEMENT_REGEX;
import static org.projectnessie.services.authz.ApiContext.apiContext;
import static org.projectnessie.versioned.RequestMeta.API_READ;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.jboss.resteasy.reactive.RestMulti;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;
import org.projectnessie.services.authz.ApiContext;

@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Path("catalog/v1")
public class NessieCatalogResource extends AbstractCatalogResource {

  static final ApiContext CATALOG_V0 = apiContext("Catalog", 0);

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/snapshots")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Multi<Object> tableSnapshots(
      @PathParam("ref") String ref,
      @QueryParam("key") List<ContentKey> keys,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    SnapshotReqParams reqParams = forSnapshotHttpReq(parseRefPathString(ref), format, specVersion);

    AtomicReference<Reference> effectiveReference = new AtomicReference<>();

    // The order of the returned items does not necessarily match the order of the requested items,
    // Nessie's getContents() does neither.

    // This operation can block --> @Blocking
    Stream<Supplier<CompletionStage<SnapshotResponse>>> snapshots =
        catalogService.retrieveSnapshots(
            reqParams, keys, effectiveReference::set, API_READ, CATALOG_V0);

    Multi<Object> multi =
        Multi.createFrom()
            .items(snapshots)
            .capDemandsTo(2)
            .map(Multi.createFrom()::completionStage)
            .flatMap(m -> m)
            .map(SnapshotResponse::entityObject)
            .flatMap(Multi.createFrom()::optional);

    // TODO This implementation just returns a "bare" array built from the `Multi`. It would be much
    //  nicer to return a wrapping object, or at least a trailing object with additional information
    //  like the effective reference for the Nessie Catalog response format.
    //  See https://github.com/orgs/resteasy/discussions/4032

    RestMulti.SyncRestMulti.Builder<Object> restMulti = RestMulti.fromMultiData(multi);
    nessieResponseHeaders(effectiveReference.get(), restMulti::header);
    return restMulti.build();
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/snapshot/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> tableSnapshot(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    return snapshotBased(
        key,
        forSnapshotHttpReq(parseRefPathString(ref), format, specVersion),
        ICEBERG_TABLE,
        CATALOG_V0);
  }

  @POST
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/commit")
  @Blocking
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> commit(
      @PathParam("ref") String ref,
      @RequestBody CatalogCommit commit,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws BaseNessieClientServerException {

    ParsedReference reference = parseRefPathString(ref);

    SnapshotReqParams reqParams = forSnapshotHttpReq(reference, format, specVersion);

    return Uni.createFrom()
        .completionStage(
            catalogService.commit(
                reference,
                commit,
                reqParams,
                this::updateCommitMeta,
                CATALOG_UPDATE_ENTITY.name(),
                CATALOG_V0))
        .map(v -> Response.ok().build());
  }
}
