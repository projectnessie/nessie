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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.versioned.RequestMeta.API_READ;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.net.URLEncoder;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.rest.common.RestCommon;

abstract class AbstractCatalogResource {
  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  CatalogService catalogService;

  @Inject HttpHeaders httpHeaders;

  @Inject ObjectIO objectIO;

  @Context ExternalBaseUri uriInfo;

  Uni<Response> snapshotBased(
      ContentKey key,
      SnapshotReqParams snapshotReqParams,
      Content.Type expectedType,
      ApiContext apiContext)
      throws NessieNotFoundException {
    return snapshotResponse(key, snapshotReqParams, expectedType, apiContext)
        .map(AbstractCatalogResource::snapshotToResponse);
  }

  Uni<SnapshotResponse> snapshotResponse(
      ContentKey key,
      SnapshotReqParams snapshotReqParams,
      Content.Type expectedType,
      ApiContext apiContext)
      throws NessieNotFoundException {
    return Uni.createFrom()
        .completionStage(
            catalogService.retrieveSnapshot(
                snapshotReqParams, key, expectedType, API_READ, apiContext));
  }

  private static Response snapshotToResponse(SnapshotResponse snapshot) {
    // TODO need the effective Nessie reference incl commit-ID here, add as a HTTP response header?

    Optional<Object> entity = snapshot.entityObject();
    if (entity.isPresent()) {
      return finalizeResponse(Response.ok(entity.get()), snapshot);
    }

    // TODO do we need a BufferedOutputStream via StreamingOutput.write ?
    return finalizeResponse(Response.ok((StreamingOutput) snapshot::produce), snapshot);
  }

  private static Response finalizeResponse(
      Response.ResponseBuilder response, SnapshotResponse snapshot) {
    response
        .header("Content-Disposition", "attachment; filename=\"" + snapshot.fileName() + "\"")
        .header("Content-Type", snapshot.contentType());
    nessieResponseHeaders(snapshot.effectiveReference(), response::header);
    return response.build();
  }

  static void nessieResponseHeaders(Reference reference, BiConsumer<String, String> header) {
    header.accept("Nessie-Reference", URLEncoder.encode(reference.toPathString(), UTF_8));
  }

  CommitMeta updateCommitMeta(String message) {
    return RestCommon.updateCommitMeta(CommitMeta.builder().message(message), httpHeaders).build();
  }
}
