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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.ImmutableSigningResponse;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.api.SigningResponse;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
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

@ExtendWith(MockitoExtension.class)
class TestIcebergS3SignParams {

  @Mock CatalogService catalogService;
  @Mock RequestSigner signer;
  @Mock S3BucketOptions s3options;

  final String baseLocation = "s3://bucket/warehouse/ns/table1_cafebabe";
  final String oldBaseLocation = "s3://old-bucket/ns/table1";
  final String metadataLocation = baseLocation + "/metadata/metadata.json";
  final String dataFileUri =
      "https://bucket.s3.amazonaws.com/warehouse/ns/table1_cafebabe/data/file1.parquet";
  final String oldDataFileUri = "https://old-bucket.s3.amazonaws.com/ns/table1/data/file1.parquet";
  final String metadataJsonUri =
      "https://bucket.s3.amazonaws.com/warehouse/ns/table1_cafebabe/metadata/metadata.json";
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

  @ParameterizedTest
  @ValueSource(strings = {"GET", "HEAD", "OPTIONS", "TRACE"})
  void verifyAndSignSuccessRead(String method) throws Exception {
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull())).thenReturn(successStage);
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
  void verifyAndSignSuccessWrite(String method) throws Exception {
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull())).thenReturn(successStage);
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner =
        newBuilder()
            .request(IcebergS3SignRequest.builder().from(writeRequest).method(method).build())
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
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull()))
        .thenReturn(CompletableFuture.completedStage(snapshotResponse));
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner = newBuilder().build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @Test
  void verifyAndSignSuccessContentNotFound() throws Exception {
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull()))
        .thenThrow(new NessieContentNotFoundException(key, "main"));
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner = newBuilder().build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @Test
  void verifyAndSignFailureReferenceNotFound() throws Exception {
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull()))
        .thenThrow(new NessieReferenceNotFoundException("ref not found"));
    IcebergS3SignParams icebergSigner = newBuilder().build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectFailure(response, NessieReferenceNotFoundException.class, "ref not found");
  }

  @Test
  void verifyAndSignSuccessImportFailed() throws Exception {
    CompletionStage<SnapshotResponse> importFailedStage =
        CompletableFuture.failedStage(new RuntimeException("import failed"));
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull())).thenReturn(importFailedStage);
    when(signer.sign(any())).thenReturn(signingResponse);
    IcebergS3SignParams icebergSigner = newBuilder().build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectSuccess(response);
  }

  @ParameterizedTest
  @ValueSource(strings = {"GET", "HEAD", "OPTIONS", "TRACE"})
  void verifyAndSignSuccessReadMetadataLocation(String method) throws Exception {
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull())).thenReturn(successStage);
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
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull())).thenReturn(successStage);
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
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull())).thenReturn(successStage);
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
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull())).thenReturn(successStage);
    IcebergS3SignParams icebergSigner =
        newBuilder()
            .request(
                IcebergS3SignRequest.builder()
                    .from(writeRequest)
                    .method(method)
                    .uri(oldDataFileUri)
                    .build())
            .build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectFailure(response, "URI not allowed for signing: " + oldDataFileUri);
  }

  @Test
  void verifyAndSignFailureWrongBaseLocation() throws Exception {
    when(catalogService.retrieveSnapshot(any(), eq(key), isNull())).thenReturn(successStage);
    IcebergS3SignParams icebergSigner =
        newBuilder().baseLocation("s3://wrong-bucket/warehouse/ns/table1_cafebabee").build();
    Uni<IcebergS3SignResponse> response = icebergSigner.verifyAndSign();
    expectFailure(response, "URI not allowed for signing: " + dataFileUri);
  }

  private ImmutableIcebergS3SignParams.Builder newBuilder() {
    return ImmutableIcebergS3SignParams.builder()
        .request(writeRequest)
        .ref(ref)
        .key(key)
        .baseLocation(baseLocation)
        .catalogService(catalogService)
        .signer(signer)
        .s3options(s3options);
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
}
