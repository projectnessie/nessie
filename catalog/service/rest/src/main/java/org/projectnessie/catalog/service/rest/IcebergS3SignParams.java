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
import static java.util.Objects.requireNonNull;
import static org.projectnessie.catalog.files.s3.S3Utils.extractBucketName;
import static org.projectnessie.catalog.files.s3.S3Utils.normalizeS3Scheme;
import static org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps.CATALOG_S3_SIGN;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergError.icebergError;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse.icebergS3SignResponse;
import static org.projectnessie.catalog.service.rest.IcebergApiV1ResourceBase.ICEBERG_V1;
import static org.projectnessie.catalog.service.rest.IcebergConfigurer.icebergWriteLocation;
import static org.projectnessie.versioned.RequestMeta.apiRead;
import static org.projectnessie.versioned.RequestMeta.apiWrite;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.immutables.value.Value.Check;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.api.SigningRequest;
import org.projectnessie.catalog.files.api.SigningResponse;
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
import org.projectnessie.model.IcebergContent;
import org.projectnessie.versioned.RequestMeta.RequestMetaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
abstract class IcebergS3SignParams {

  /**
   * Pattern for the new object-storage path layout introduced in Apache Iceberg 1.7.0, checking for
   * the four path elements that contain only {@code 0} or {@code 1}.
   *
   * <p>Example: {@code
   * s3://bucket1/warehouse/1000/1000/1110/10001000/newdb/table_949afb2c-ed93-4702-b390-f1d4a9c59957/my-data-file.txt}
   */
  private static final Pattern NEW_OBJECT_STORAGE_LAYOUT =
      Pattern.compile("[01]{4}/[01]{4}/[01]{4}/[01]{8}/(.*)");

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergS3SignParams.class);

  abstract IcebergS3SignRequest request();

  abstract ParsedReference ref();

  abstract ContentKey key();

  abstract String warehouseLocation();

  abstract List<String> writeLocations();

  abstract List<String> readLocations();

  abstract CatalogService catalogService();

  abstract RequestSigner signer();

  @Check
  void check() {
    checkArgument(
        writeLocations().stream().allMatch(l -> l.startsWith("s3:"))
            && readLocations().stream().allMatch(l -> l.startsWith("s3:")),
        "locations must be S3 URIs");
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
        .filter(this::checkLocation)
        .toUni()
        .onItem()
        .ifNull()
        .failWith(this::unauthorized)
        .replaceWith(request().uri())
        .map(this::sign);
  }

  private boolean checkLocation(String location) {
    return checkLocation(warehouseLocation(), requestedS3Uri(), location);
  }

  @VisibleForTesting
  static boolean checkLocation(String warehouseLocation, String requested, String location) {
    if (requested.startsWith(location)) {
      return true;
    }

    // For files that were written with 'write.object-storage.enabled' enabled, repeat the check but
    // ignore the first S3 path element after the warehouse location

    int warehouseLocationLength = warehouseLocation.length();
    if (warehouseLocationLength == 0) {
      return false;
    }

    if (!warehouseLocation.endsWith("/")) {
      warehouseLocation += "/";
      warehouseLocationLength++;
    }
    if (!location.endsWith("/")) {
      location += "/";
    }

    if (!requested.startsWith(warehouseLocation) || !location.startsWith(warehouseLocation)) {
      return false;
    }

    String requestedPath = requested.substring(warehouseLocationLength);

    Matcher newObjectStorageLayoutMatcher = NEW_OBJECT_STORAGE_LAYOUT.matcher(requestedPath);
    if (newObjectStorageLayoutMatcher.find()) {
      requestedPath = newObjectStorageLayoutMatcher.group(1);
    } else {
      int requestedSlash = requestedPath.indexOf('/');
      if (requestedSlash == -1) {
        return false;
      }
      requestedPath = requestedPath.substring(requestedSlash + 1);
    }

    String locationPath = location.substring(warehouseLocationLength);
    return requestedPath.startsWith(locationPath);
  }

  private Uni<SnapshotResponse> fetchSnapshot() {
    try {
      RequestMetaBuilder requestMeta = write() ? apiWrite() : apiRead();
      requestMeta.addKeyAction(key(), CATALOG_S3_SIGN.name());
      CompletionStage<SnapshotResponse> stage =
          catalogService()
              .retrieveSnapshot(
                  SnapshotReqParams.forSnapshotHttpReq(ref(), "iceberg", null),
                  key(),
                  null,
                  requestMeta.build(),
                  ICEBERG_V1);
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
      if (content instanceof IcebergContent) {
        metadataLocation = ((IcebergContent) content).getMetadataLocation();
      }
      if (metadataLocation != null
          && requestedS3Uri().equals(normalizeS3Scheme(metadataLocation))) {
        return Uni.createFrom().failure(unauthorized());
      }
    }
    return Uni.createFrom().item(snapshotResponse);
  }

  private Multi<String> collectAllowedLocations(SnapshotResponse snapshotResponse) {
    if (snapshotResponse == null) {
      // table does not exist - nothing to write to, nothing to read from
      return Multi.createFrom()
          .items(Stream.concat(writeLocations().stream(), readLocations().stream()));
    } else {
      NessieEntitySnapshot<?> snapshot = snapshotResponse.nessieSnapshot();
      // table exists: collect all locations, current and historical
      return Multi.createFrom()
          .emitter(
              e -> {
                // check the base location sent with the request matches the current
                // iceberg location (see IcebergConfigurer: they must match)
                List<String> expectedBaseLocations = new ArrayList<>();
                String location = normalizeS3Scheme(requireNonNull(snapshot.icebergLocation()));
                expectedBaseLocations.add(location);

                String writeLocation = icebergWriteLocation(snapshot.properties());
                if (writeLocation != null) {
                  writeLocation = normalizeS3Scheme(writeLocation);
                  if (!writeLocation.startsWith(location)) {
                    expectedBaseLocations.add(writeLocation);
                  }
                }

                if (write()) {
                  for (String baseLocation : writeLocations()) {
                    if (expectedBaseLocations.contains(baseLocation)) {
                      e.emit(baseLocation);
                      e.complete();
                      return;
                    }
                  }
                } else {
                  Iterator<String> locations =
                      Stream.concat(writeLocations().stream(), readLocations().stream()).iterator();
                  while (locations.hasNext()) {
                    String baseLocation = locations.next();
                    if (expectedBaseLocations.contains(baseLocation)) {
                      e.emit(baseLocation);
                      // Allow reading from ancient locations, but only allow writes to the current
                      // location
                      for (String s : snapshot.additionalKnownLocations()) {
                        e.emit(normalizeS3Scheme(s));
                      }
                      e.complete();
                      return;
                    }
                  }
                }

                e.fail(unauthorized());
              });
    }
  }

  private IcebergS3SignResponse sign(String uriToSign) {
    URI uri = URI.create(uriToSign);
    Optional<String> bucket = extractBucketName(uri);
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
    String s3Uri = requestedS3Uri();
    LOGGER.warn(
        "Unauthorized signing request: key: {}, ref: {}, s3-uri: {}, request uri: {}, request method: {}, warehouse: {}, writeable locations: {}, readable locations: {}",
        key(),
        ref(),
        s3Uri,
        request().uri(),
        request().method(),
        warehouseLocation(),
        writeLocations(),
        readLocations());
    return exception;
  }
}
