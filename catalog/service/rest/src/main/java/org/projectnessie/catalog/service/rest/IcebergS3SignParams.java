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
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergError.icebergError;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse.icebergS3SignResponse;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.immutables.value.Value;
import org.immutables.value.Value.Check;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.api.SigningRequest;
import org.projectnessie.catalog.files.api.SigningResponse;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3Utils;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
abstract class IcebergS3SignParams {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergS3SignParams.class);

  abstract IcebergS3SignRequest request();

  abstract ParsedReference ref();

  abstract ContentKey key();

  abstract String baseLocation();

  abstract CatalogService catalogService();

  abstract RequestSigner signer();

  abstract S3BucketOptions s3options();

  @Check
  public void check() {
    checkArgument(
        URI.create(baseLocation()).getScheme().equals("s3"), "baseLocation must be an S3 URI");
  }

  @Value.Lazy
  String requestedS3Uri() {
    return S3Utils.asS3Location(request().uri());
  }

  @Value.Lazy
  boolean write() {
    return request().method().equalsIgnoreCase("PUT")
        || request().method().equalsIgnoreCase("POST")
        || request().method().equalsIgnoreCase("DELETE")
        || request().method().equalsIgnoreCase("PATCH");
  }

  Uni<IcebergS3SignResponse> verifyAndSign() {
    return fetchSnapshot()
        .call(this::checkForbiddenLocations)
        .onItem()
        .transformToMulti(this::collectAllowedLocations)
        .filter(requestedS3Uri()::startsWith)
        .toUni()
        .onItem()
        .ifNull()
        .failWith(this::unauthorized)
        .replaceWith(request().uri())
        .map(this::sign);
  }

  private Uni<SnapshotResponse> fetchSnapshot() {
    try {
      // TODO pass write() to CatalogServiceImpl.retrieveSnapshot() once #8768 is merged
      CompletionStage<SnapshotResponse> stage =
          catalogService()
              .retrieveSnapshot(
                  SnapshotReqParams.forSnapshotHttpReq(ref(), "iceberg", null), key(), null);
      // consider an import failure as a non-existing content:
      // signing will be authorized for the future location only.
      return Uni.createFrom().completionStage(stage).onFailure().recoverWithNull();
    } catch (NessieContentNotFoundException ignored) {
      return Uni.createFrom().nullItem();
    } catch (NessieNotFoundException e) {
      return Uni.createFrom().failure(e);
    }
  }

  private Uni<?> checkForbiddenLocations(SnapshotResponse snapshotResponse) {
    if (snapshotResponse != null && write()) {
      Content content = snapshotResponse.content();
      // TODO disallow all table and view metadata objects, not only the metadata json location
      String metadataLocation = null;
      if (content instanceof IcebergTable) {
        metadataLocation = ((IcebergTable) content).getMetadataLocation();
      } else if (content instanceof IcebergView) {
        metadataLocation = ((IcebergView) content).getMetadataLocation();
      }
      if (metadataLocation != null
          && requestedS3Uri().equals(S3Utils.normalizeS3Scheme(metadataLocation))) {
        return Uni.createFrom().failure(unauthorized());
      }
    }
    return Uni.createFrom().item(snapshotResponse);
  }

  private Multi<String> collectAllowedLocations(SnapshotResponse snapshotResponse) {
    if (snapshotResponse == null) {
      // table does not exist: only allow writes to its future location
      return Multi.createFrom().item(baseLocation());
    } else {
      NessieEntitySnapshot<?> snapshot = snapshotResponse.nessieSnapshot();
      // table exists: collect all locations, current and historical
      return Multi.createFrom()
          .emitter(
              e -> {
                // check the base location sent with the request matches the current
                // iceberg location (see IcebergConfigurer: they must match)
                String expectedBaseLocation =
                    S3Utils.normalizeS3Scheme(Objects.requireNonNull(snapshot.icebergLocation()));
                if (baseLocation().equals(expectedBaseLocation)) {
                  e.emit(expectedBaseLocation);
                  // Allow reading from ancient locations, but only allow writes to the current
                  // location
                  if (!write()) {
                    for (String s : snapshot.additionalKnownLocations()) {
                      e.emit(S3Utils.normalizeS3Scheme(s));
                    }
                  }
                  e.complete();
                } else {
                  e.fail(unauthorized());
                }
              });
    }
  }

  private IcebergS3SignResponse sign(String uriToSign) {
    URI uri = URI.create(uriToSign);
    Optional<String> bucket = s3options().extractBucket(uri);
    Optional<String> body = Optional.ofNullable(request().body());

    SigningRequest signingRequest =
        SigningRequest.signingRequest(
            uri, request().method(), request().region(), bucket, body, request().headers());

    SigningResponse signed = signer().sign(signingRequest);

    return icebergS3SignResponse(signed.uri().toString(), signed.headers());
  }

  private IcebergException unauthorized() {
    IcebergException exception =
        new IcebergException(
            icebergError(
                Status.FORBIDDEN.getStatusCode(),
                "NotAuthorizedException",
                "URI not allowed for signing: " + request().uri(),
                List.of()));
    LOGGER.warn(
        "Unauthorized signing request: key: {}, ref: {}, request uri: {}, request method: {}",
        key(),
        ref(),
        request().uri(),
        request().method(),
        exception);
    return exception;
  }
}
