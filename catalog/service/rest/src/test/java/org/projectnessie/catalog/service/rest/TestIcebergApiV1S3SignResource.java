/*
 * Copyright (C) 2026 Dremio
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergEndpoint.icebergEndpoint;
import static org.projectnessie.catalog.service.rest.IcebergConfigurer.SIGNER_TOKEN;

import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.catalog.files.api.ImmutableSigningResponse;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignRequest;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SignerKeysService;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.objtypes.SignerKey;
import org.projectnessie.model.ContentKey;
import org.projectnessie.services.config.ServerConfig;

class TestIcebergApiV1S3SignResource {
  private final Instant now = Instant.parse("2026-10-26T12:00:00Z");
  private final SignerKey key =
      SignerKey.builder()
          .name("key")
          .secretKey("01234567890123456789012345678912".getBytes(UTF_8))
          .creationTime(now.minusSeconds(60))
          .rotationTime(now.plusSeconds(3600))
          .expirationTime(now.plusSeconds(7200))
          .build();
  private IcebergApiV1S3SignResource resource;

  @BeforeEach
  void setup() {
    resource =
        new IcebergApiV1S3SignResource(
            mock(ServerConfig.class), mock(LakehouseConfig.class), null, null, null);
    resource.clock = Clock.fixed(now, ZoneOffset.UTC);
    resource.signerKeysService = mock(SignerKeysService.class);
    resource.catalogService = mock(CatalogService.class);
    resource.signer = mock(RequestSigner.class);
    resource.uriInfo = mock(UriInfo.class);
    when(resource.signerKeysService.getSignerKey("key")).thenReturn(key);
    when(resource.lakehouseConfig.s3()).thenReturn(ImmutableS3Options.builder().build());
    var catalog = mock(CatalogConfig.class);
    when(resource.lakehouseConfig.catalog()).thenReturn(catalog);
    when(catalog.resolveWarehouseName(any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(0) == null ? "warehouse" : invocation.getArgument(0));
  }

  private SignerSignature signature(long expiration) {
    return SignerSignature.builder()
        .prefix("main")
        .identifier(ContentKey.of("ns", "table").toPathStringEscaped())
        .warehouseLocation("s3://bucket/")
        .addWriteLocation("s3://bucket/table")
        .expirationTimestamp(expiration)
        .build();
  }

  private IcebergS3SignRequest request(String token, String uri) {
    return IcebergS3SignRequest.builder()
        .region("us-west-2")
        .method("PUT")
        .uri(uri)
        .properties(token == null ? Map.of() : Map.of(SIGNER_TOKEN, token))
        .build();
  }

  @Test
  void advertisesStandardSigningRoute() throws Exception {
    String path =
        IcebergApiV1S3SignResource.class
            .getMethod(
                "s3signWithProperties",
                IcebergS3SignRequest.class,
                String.class,
                String.class,
                String.class)
            .getAnnotation(Path.class)
            .value();
    assertThat(path).isEqualTo("/v1/{prefix}/namespaces/{namespace}/tables/{table}/sign");
    assertThat(IcebergApiV1GenericResource.ENDPOINTS).contains(icebergEndpoint("POST", path));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void bothTransportsSign(boolean properties) throws Exception {
    String uri = "https://bucket.s3.amazonaws.com/table/data.parquet";
    String signingPrefix = "other|warehouse";
    String token =
        SignerSignature.builder()
            .from(signature(now.plusSeconds(60).getEpochSecond()))
            .prefix(signingPrefix)
            .build()
            .toPathParam(key);
    // A table being created may not have a snapshot yet; the signed location remains authoritative.
    when(resource.catalogService.retrieveSnapshot(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(resource.signer.sign(any()))
        .thenReturn(ImmutableSigningResponse.builder().uri(URI.create(uri)).build());
    var request = request(token, uri);
    var response =
        properties
            ? resource.s3signWithProperties(request, "main", "ns", "table@other")
            : resource.s3signWIthOpaqueParams(request, signingPrefix, token);
    assertThat(response.await().indefinitely().uri()).isEqualTo(uri);
    verify(resource.catalogService)
        .retrieveSnapshot(
            argThat(params -> "other".equals(params.ref().name())), any(), any(), any(), any());
  }

  @ParameterizedTest
  @CsvSource({
    "main,ns,table,main",
    "main,ns,table@main,main",
    "main,ns,table@other,other|warehouse",
    "main,ns,`table@other`,other|warehouse",
    "main,ns,table#12345678,main@12345678|warehouse",
    "main,ns,`table@other#12345678`,other@12345678|warehouse",
    "main@12345678,ns,table#12345678,main@12345678",
    "main,ns,table.with.dots,main",
    "main,ns.with.dots,table,main",
    "main,ns\u001fchild,table,main"
  })
  void decodesTableIdentity(String prefix, String namespace, String table, String signingPrefix)
      throws Exception {
    String uri = "https://bucket.s3.amazonaws.com/table/data.parquet";
    TableRef tableRef = resource.decodeTableRef(prefix, namespace, table);
    String token =
        SignerSignature.builder()
            .from(signature(now.plusSeconds(60).getEpochSecond()))
            .prefix(signingPrefix)
            .identifier(tableRef.contentKey().toPathStringEscaped())
            .build()
            .toPathParam(key);
    when(resource.catalogService.retrieveSnapshot(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(resource.signer.sign(any()))
        .thenReturn(ImmutableSigningResponse.builder().uri(URI.create(uri)).build());
    assertThat(
            resource
                .s3signWithProperties(request(token, uri), prefix, namespace, table)
                .await()
                .indefinitely()
                .uri())
        .isEqualTo(uri);
    verify(resource.catalogService)
        .retrieveSnapshot(
            argThat(
                params ->
                    Objects.equals(params.ref().name(), tableRef.reference().name())
                        && Objects.equals(
                            params.ref().hashWithRelativeSpec(),
                            tableRef.reference().hashWithRelativeSpec())),
            eq(tableRef.contentKey()),
            any(),
            any(),
            any());
  }

  @SuppressWarnings("ReactiveStreamsUnusedPublisher")
  @ParameterizedTest
  @CsvSource({"other,table", "ns,other", "ns,table@other", "ns,table#12345678"})
  void rejectsMismatchedTableIdentity(String namespace, String table) {
    String token = signature(now.plusSeconds(60).getEpochSecond()).toPathParam(key);
    assertThatThrownBy(
            () ->
                resource.s3signWithProperties(
                    request(token, "https://bucket.s3.amazonaws.com/table/data.parquet"),
                    "main",
                    namespace,
                    table))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid signer table");
    verifyNoInteractions(resource.catalogService, resource.signer);
  }

  @Test
  void rejectsLocationOutsideSignedScope() throws Exception {
    String token = signature(now.plusSeconds(60).getEpochSecond()).toPathParam(key);
    when(resource.catalogService.retrieveSnapshot(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    assertThatThrownBy(
            () ->
                resource
                    .s3signWithProperties(
                        request(token, "https://bucket.s3.amazonaws.com/other/data.parquet"),
                        "main",
                        "ns",
                        "table")
                    .await()
                    .indefinitely())
        .isInstanceOf(IcebergException.class)
        .hasMessageContaining("URI not allowed for signing");
    verifyNoInteractions(resource.signer);
  }

  @SuppressWarnings("ReactiveStreamsUnusedPublisher")
  @Test
  void missingTokenAndWrongPrefix() {
    assertThatThrownBy(
            () ->
                resource.s3signWithProperties(
                    request(null, "https://bucket/key"), "main", "ns", "table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing signer token");
    String token = signature(now.plusSeconds(60).getEpochSecond()).toPathParam(key);
    assertThatThrownBy(
            () ->
                resource.s3signWithProperties(
                    request(token, "https://bucket/key"), "main|other-warehouse", "ns", "table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid signer prefix");
    verifyNoInteractions(resource.catalogService, resource.signer);
  }

  @SuppressWarnings("ReactiveStreamsUnusedPublisher")
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsExpiredOrTamperedToken(boolean expired) {
    SignerSignature signature = signature((expired ? now : now.plusSeconds(60)).getEpochSecond());
    String token =
        SignerParams.builder()
            .keyName(key.name())
            .signerSignature(signature)
            .signature(expired ? signature.sign(key) : "tampered")
            .build()
            .toPathParam();
    assertThatThrownBy(
            () ->
                resource.s3signWithProperties(
                    request(token, "https://bucket/key"), "main", "ns", "table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid signature");
    verifyNoInteractions(resource.catalogService, resource.signer);
  }
}
