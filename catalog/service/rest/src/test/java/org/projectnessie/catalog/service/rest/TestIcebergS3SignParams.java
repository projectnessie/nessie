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

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.projectnessie.catalog.service.rest.IcebergApiV1ResourceBase.ICEBERG_V1;
import static org.projectnessie.versioned.RequestMeta.apiRead;
import static org.projectnessie.versioned.RequestMeta.apiWrite;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.ImmutableSigningResponse;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.api.SigningResponse;
import org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignResponse;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.NessieView;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.versioned.RequestMeta;

@ExtendWith({MockitoExtension.class, SoftAssertionsExtension.class})
class TestIcebergS3SignParams {

  @InjectSoftAssertions SoftAssertions soft;
  @Mock CatalogService catalogService;
  @Mock RequestSigner signer;

  static final String warehouseLocation = "s3://bucket/warehouse/";
  static final String locationPart = "ns/table1_cafebabe";

  // Before Iceberg 1.7.0
  static final String objectStoragePartOld = "iI5Yww";
  // Since Iceberg 1.7.0
  static final String objectStoragePartNew = "1000/1000/1110/10001000";

  static final String baseLocation = warehouseLocation + locationPart;
  static final String oldBaseLocation = "s3://old-bucket/ns/table1";
  static final String metadataLocation = baseLocation + "/metadata/metadata.json";

  static final String s3BucketAws = "https://bucket.s3.amazonaws.com/warehouse/";
  static final String dataFileUri = s3BucketAws + locationPart + "/data/file1.parquet";
  static final String dataFileUriObjectStorageOld =
      s3BucketAws + objectStoragePartOld + "/" + locationPart + "/data/file1.parquet";
  static final String dataFileUriObjectStorageNew =
      s3BucketAws + objectStoragePartNew + "/" + locationPart + "/data/file1.parquet";

  static final String oldDataFileUri =
      "https://old-bucket.s3.amazonaws.com/ns/table1/data/file1.parquet";

  static final String metadataJsonUri = s3BucketAws + locationPart + "/metadata/metadata.json";

  final IcebergS3SignRequest writeRequest =
      IcebergS3SignRequest.builder()
          .method("DELETE")
          .uri(dataFileUri)
          .region("us-west-2")
          .headers(Map.of())
          .properties(Map.of())
          .body(null)
          .build();
  final IcebergS3SignRequest readRequest =
      IcebergS3SignRequest.builder()
          .method("HEAD")
          .uri(metadataJsonUri)
          .region("us-west-2")
          .headers(Map.of())
          .properties(Map.of())
          .body(null)
          .build();
  final ParsedReference ref =
      ParsedReference.parsedReference("main", "12345678", ReferenceType.BRANCH);
  final ContentKey key = ContentKey.of("ns", "table1");
  final Content table = IcebergTable.of(metadataLocation, 1, 1, 1, 1);
  final NessieTable nessieTable =
      NessieTable.builder().nessieContentId("1234").createdTimestamp(Instant.now()).build();
  final NessieTableSnapshot nessieTableSnapshot =
      NessieTableSnapshot.builder()
          .id(NessieId.randomNessieId())
          .entity(nessieTable)
          .icebergLocation(baseLocation)
          .addAdditionalKnownLocations(oldBaseLocation)
          .lastUpdatedTimestamp(Instant.now())
          .build();
  final SnapshotResponse snapshotResponse =
      SnapshotResponse.forEntity(
          Branch.of("main", "12345678"),
          table,
          "metadata.json",
          "application/json",
          key,
          table,
          nessieTableSnapshot);
  final CompletionStage<SnapshotResponse> successStage =
      CompletableFuture.completedStage(snapshotResponse);
  final SigningResponse signingResponse =
      ImmutableSigningResponse.builder().uri(URI.create(dataFileUri)).build();

  static Stream<Arguments> readMethodsAndUris() {
    return Stream.of("GET", "HEAD", "OPTIONS", "TRACE").flatMap(TestIcebergS3SignParams::addUris);
  }

  static Stream<Arguments> writeMethodsAndUris() {
    return Stream.of("PUT", "POST", "DELETE", "PATCH").flatMap(TestIcebergS3SignParams::addUris);
  }

  static Stream<Arguments> addUris(String method) {
    return Stream.of(
        arguments(method, dataFileUri),
        arguments(method, dataFileUriObjectStorageOld),
        arguments(method, dataFileUriObjectStorageNew));
  }

  @Test
  void checkLocation() {
    // Strip trailing '/'
    String warehouseLocation = TestIcebergS3SignParams.warehouseLocation;
    warehouseLocation = warehouseLocation.substring(0, warehouseLocation.length() - 1);

    String requestedPlain = warehouseLocation + '/' + locationPart + "/data/file1.parquet";
    String requestedObjectStorageOld =
        warehouseLocation + '/' + objectStoragePartOld + '/' + locationPart + "/data/file1.parquet";
    String requestedObjectStorageNew =
        warehouseLocation + '/' + objectStoragePartNew + '/' + locationPart + "/data/file1.parquet";

    String location = warehouseLocation + '/' + locationPart;

    soft.assertThat(IcebergS3SignParams.checkLocation(warehouseLocation, requestedPlain, location))
        .isTrue();
    soft.assertThat(
            IcebergS3SignParams.checkLocation(warehouseLocation, requestedPlain, location + '/'))
        .isTrue();
    soft.assertThat(
            IcebergS3SignParams.checkLocation(warehouseLocation + '/', requestedPlain, location))
        .isTrue();
    soft.assertThat(
            IcebergS3SignParams.checkLocation(
                warehouseLocation + '/', requestedPlain, location + '/'))
        .isTrue();

    soft.assertThat(
            IcebergS3SignParams.checkLocation(
                warehouseLocation, requestedObjectStorageOld, location))
        .isTrue();
    soft.assertThat(
            IcebergS3SignParams.checkLocation(
                warehouseLocation, requestedObjectStorageOld, location + '/'))
        .isTrue();
    soft.assertThat(
            IcebergS3SignParams.checkLocation(
                warehouseLocation + '/', requestedObjectStorageOld, location))
        .isTrue();
    soft.assertThat(
            IcebergS3SignParams.checkLocation(
                warehouseLocation + '/', requestedObjectStorageOld, location + '/'))
        .isTrue();

    soft.assertThat(
            IcebergS3SignParams.checkLocation(
                warehouseLocation, requestedObjectStorageNew, location))
        .isTrue();
    soft.assertThat(
            IcebergS3SignParams.checkLocation(
                warehouseLocation, requestedObjectStorageNew, location + '/'))
        .isTrue();
    soft.assertThat(
            IcebergS3SignParams.checkLocation(
                warehouseLocation + '/', requestedObjectStorageNew, location))
        .isTrue();
    soft.assertThat(
            IcebergS3SignParams.checkLocation(
                warehouseLocation + '/', requestedObjectStorageNew, location + '/'))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("readMethodsAndUris")
  void verifyAndSignSuccessRead(String method, String uri) throws Exception {
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiRead(key)), eq(ICEBERG_V1)))
        .thenReturn(successStage);
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner =
        newBuilder()
            .request(
                IcebergS3SignRequest.builder().from(readRequest).uri(uri).method(method).build())
            .build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @ParameterizedTest
  @MethodSource("writeMethodsAndUris")
  void verifyAndSignSuccessWrite(String method, String uri) throws Exception {
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiWrite(key)), eq(ICEBERG_V1)))
        .thenReturn(successStage);
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner =
        newBuilder()
            .request(
                IcebergS3SignRequest.builder().from(writeRequest).uri(uri).method(method).build())
            .build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @Test
  void verifyAndSignSuccessView() throws Exception {
    NessieView nessieView =
        NessieView.builder().nessieContentId("1234").createdTimestamp(Instant.now()).build();
    NessieViewSnapshot nessieViewSnapshot =
        NessieViewSnapshot.builder()
            .id(NessieId.randomNessieId())
            .entity(nessieView)
            .icebergLocation(baseLocation)
            .addAdditionalKnownLocations(oldBaseLocation)
            .lastUpdatedTimestamp(Instant.now())
            .build();
    Content view = IcebergView.of(metadataLocation, 1, 1);
    SnapshotResponse snapshotResponse =
        SnapshotResponse.forEntity(
            Branch.of("main", "12345678"),
            view,
            "metadata.json",
            "application/json",
            key,
            view,
            nessieViewSnapshot);
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiWrite(key)), eq(ICEBERG_V1)))
        .thenReturn(CompletableFuture.completedStage(snapshotResponse));
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner = newBuilder().build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @Test
  void verifyAndSignSuccessContentNotFound() throws Exception {
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiWrite(key)), eq(ICEBERG_V1)))
        .thenThrow(new NessieContentNotFoundException(key, "main"));
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner = newBuilder().build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @Test
  void verifyAndSignFailureReferenceNotFound() throws Exception {
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiWrite(key)), eq(ICEBERG_V1)))
        .thenThrow(new NessieReferenceNotFoundException("ref not found"));
    IcebergS3SignParams icebergSigner = newBuilder().build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectFailure(response, NessieReferenceNotFoundException.class, "ref not found");
  }

  @Test
  void verifyAndSignSuccessImportFailed() throws Exception {
    CompletionStage<SnapshotResponse> importFailedStage =
        CompletableFuture.failedStage(new RuntimeException("import failed"));
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiWrite(key)), eq(ICEBERG_V1)))
        .thenReturn(importFailedStage);
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner = newBuilder().build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @ParameterizedTest
  @ValueSource(strings = {"GET", "HEAD", "OPTIONS", "TRACE"})
  void verifyAndSignSuccessReadMetadataLocation(String method) throws Exception {
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiRead(key)), eq(ICEBERG_V1)))
        .thenReturn(successStage);
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner =
        newBuilder()
            .request(IcebergS3SignRequest.builder().from(readRequest).method(method).build())
            .build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @ParameterizedTest
  @ValueSource(strings = {"PUT", "POST", "DELETE", "PATCH"})
  void verifyAndSignFailureWriteMetadataLocation(String method) throws Exception {
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiWrite(key)), eq(ICEBERG_V1)))
        .thenReturn(successStage);
    IcebergS3SignParams icebergSigner =
        newBuilder()
            .request(
                IcebergS3SignRequest.builder()
                    .from(writeRequest)
                    .method(method)
                    .uri(metadataJsonUri)
                    .build())
            .build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectFailure(response, "URI not allowed for signing: " + metadataJsonUri);
  }

  @ParameterizedTest
  @ValueSource(strings = {"GET", "HEAD", "OPTIONS", "TRACE"})
  void verifyAndSignSuccessReadAncientLocation(String method) throws Exception {
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiRead(key)), eq(ICEBERG_V1)))
        .thenReturn(successStage);
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner =
        newBuilder()
            .request(
                IcebergS3SignRequest.builder()
                    .from(readRequest)
                    .method(method)
                    .uri(metadataJsonUri)
                    .build())
            .build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @ParameterizedTest
  @ValueSource(strings = {"PUT", "POST", "DELETE", "PATCH"})
  void verifyAndSignFailureWriteAncientLocation(String method) throws Exception {
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiWrite(key)), eq(ICEBERG_V1)))
        .thenReturn(successStage);
    IcebergS3SignParams icebergSigner =
        newBuilder()
            .request(
                IcebergS3SignRequest.builder()
                    .from(writeRequest)
                    .method(method)
                    .uri(oldDataFileUri)
                    .build())
            .warehouseLocation(warehouseLocation)
            .build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectFailure(response, "URI not allowed for signing: " + oldDataFileUri);
  }

  @Test
  void verifyAndSignFailureWrongBaseLocation() throws Exception {
    when(catalogService.retrieveSnapshot(
            any(), eq(key), isNull(), eq(expectedApiWrite(key)), eq(ICEBERG_V1)))
        .thenReturn(successStage);
    IcebergS3SignParams icebergSigner =
        newBuilder()
            .writeLocations(List.of("s3://wrong-bucket/warehouse/" + locationPart + "e"))
            .build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectFailure(response, "URI not allowed for signing: " + dataFileUri);
  }

  private ImmutableIcebergS3SignParams.Builder newBuilder() {
    return ImmutableIcebergS3SignParams.builder()
        .request(writeRequest)
        .ref(ref)
        .key(key)
        .warehouseLocation(warehouseLocation)
        .addWriteLocations(baseLocation)
        .catalogService(catalogService)
        .signer(signer);
  }

  private void expectSuccess(Uni<IcebergS3SignResponse> response) {
    UniAssertSubscriber<IcebergS3SignResponse> subscriber =
        response.subscribe().withSubscriber(UniAssertSubscriber.create());
    IcebergS3SignResponse expected =
        IcebergS3SignResponse.icebergS3SignResponse(dataFileUri, Map.of());
    subscriber.assertCompleted().assertItem(expected);
  }

  private void expectFailure(Uni<IcebergS3SignResponse> response, String message) {
    expectFailure(response, IcebergException.class, message);
  }

  private void expectFailure(
      Uni<IcebergS3SignResponse> response,
      Class<? extends Throwable> exceptionClass,
      String message) {
    UniAssertSubscriber<IcebergS3SignResponse> subscriber =
        response.subscribe().withSubscriber(UniAssertSubscriber.create());
    subscriber.assertFailedWith(exceptionClass, message);
  }

  private static RequestMeta expectedApiRead(ContentKey key) {
    return apiRead().addKeyAction(key, CatalogOps.CATALOG_S3_SIGN.name()).build();
  }

  private static RequestMeta expectedApiWrite(ContentKey key) {
    return apiWrite().addKeyAction(key, CatalogOps.CATALOG_S3_SIGN.name()).build();
  }
}
