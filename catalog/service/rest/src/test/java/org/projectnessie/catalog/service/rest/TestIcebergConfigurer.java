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

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.catalog.files.s3.S3Utils.normalizeS3Scheme;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;
import static org.projectnessie.catalog.service.rest.IcebergConfigurer.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT;
import static org.projectnessie.catalog.service.rest.IcebergConfigurer.S3_SIGNER_ENDPOINT;
import static org.projectnessie.catalog.service.rest.IcebergConfigurer.S3_SIGNER_URI;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.config.ImmutableS3BucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergS3SignRequest;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SignerKeysService;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.objtypes.SignerKey;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference.ReferenceType;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIcebergConfigurer {
  private static final String CREDENTIALS_ENDPOINT =
      "v1/main/namespaces/ns/tables/table/credentials";
  private static final Map<String, String> GCS_CREDENTIAL =
      Map.of("gcs.oauth2.token", "token", "gcs.oauth2.token-expires-at", "12345");

  @InjectSoftAssertions protected SoftAssertions soft;

  protected IcebergConfigurer icebergConfigurer;
  protected SignerKey signerKey;

  @BeforeEach
  @SuppressWarnings({"UnnecessaryAssignment", "HttpUrlsUsage"})
  protected void setupIcebergConfigurer() {
    icebergConfigurer = new IcebergConfigurer();
    icebergConfigurer.uriInfo = () -> URI.create("http://foo:12434");
    configureS3(ImmutableS3Options.builder().build());
    Instant now = Instant.now();
    signerKey =
        SignerKey.builder()
            .name("foo")
            .secretKey("01234567890123456789012345678912".getBytes(UTF_8))
            .creationTime(now)
            .rotationTime(now.plus(1, DAYS))
            .expirationTime(now.plus(2, DAYS))
            .build();
    icebergConfigurer.signerKeysService =
        new SignerKeysService() {
          @Override
          public SignerKey currentSignerKey() {
            return signerKey;
          }

          @Override
          public SignerKey getSignerKey(String keyName) {
            return signerKey;
          }
        };
  }

  @SuppressWarnings("UnnecessaryAssignment")
  private void configureS3(S3Options s3Options) {
    SecretsProvider secretsProvider =
        ResolvingSecretsProvider.builder()
            .putSecretsManager("plain", unsafePlainTextSecretsProvider(Map.of()))
            .build();
    icebergConfigurer.objectIO =
        new S3ObjectIO(new S3ClientSupplier(null, s3Options, null, secretsProvider), null);
    icebergConfigurer.lakehouseConfig = mock(LakehouseConfig.class);
    when(icebergConfigurer.lakehouseConfig.s3()).thenReturn(s3Options);
  }

  @ParameterizedTest
  @MethodSource
  public void signerTokenPathStyleAccess(
      S3BucketOptions bucketOptions, Optional<Boolean> expectedPathStyleAccess) {
    configureS3(ImmutableS3Options.builder().defaultOptions(bucketOptions).build());

    String loc = "s3://bucket/foo/bar";
    IcebergTableMetadata tm = mock(IcebergTableMetadata.class);
    when(tm.location()).thenReturn(loc);
    when(tm.properties()).thenReturn(Map.of());
    NessieTableSnapshot nessieSnapshot =
        NessieTableSnapshot.builder()
            .lastUpdatedTimestamp(Instant.now())
            .id(NessieId.randomNessieId())
            .entity(
                NessieTable.builder()
                    .nessieContentId(UUID.randomUUID().toString())
                    .createdTimestamp(Instant.now())
                    .build())
            .build();

    IcebergTableConfig tableConfig =
        icebergConfigurer.icebergConfigPerTable(
            nessieSnapshot,
            "s3://bucket/",
            tm,
            "main",
            ContentKey.of("foo", "bar"),
            null,
            null,
            true);

    URI endpoint = URI.create(tableConfig.config().get(S3_SIGNER_ENDPOINT));
    SignerParams signerParams =
        SignerParams.fromPathParam(
            endpoint.getRawPath().substring(endpoint.getRawPath().lastIndexOf('/') + 1));
    soft.assertThat(signerParams.signerSignature().pathStyleAccess())
        .isEqualTo(expectedPathStyleAccess);

    // Unset path-style access on a custom endpoint is bound as virtual-hosted, so the signer
    // reads the bucket from the host instead of the path.
    if (expectedPathStyleAccess.equals(Optional.of(false))) {
      String requestUri = "https://bucket.obs.example.com/foo/bar/data/file.parquet";
      IcebergS3SignParams signParams =
          ImmutableIcebergS3SignParams.builder()
              .request(
                  IcebergS3SignRequest.builder()
                      .region("us-west-2")
                      .method("GET")
                      .uri(requestUri)
                      .headers(Map.of())
                      .properties(Map.of())
                      .build())
              .ref(ParsedReference.parsedReference("main", null, ReferenceType.BRANCH))
              .key(ContentKey.of("foo", "bar"))
              .warehouseLocation("s3://bucket/")
              .writeLocations(List.of("s3://bucket/foo/bar"))
              .pathStyleAccess(signerParams.signerSignature().pathStyleAccess())
              .s3Options(ImmutableS3Options.builder().defaultOptions(bucketOptions).build())
              .catalogService(mock(CatalogService.class))
              .signer(mock(RequestSigner.class))
              .build();
      soft.assertThat(signParams.requestedBucket()).contains("bucket");
      soft.assertThat(signParams.requestedS3Uri())
          .isEqualTo("s3://bucket/foo/bar/data/file.parquet");
    }
  }

  static Stream<Arguments> signerTokenPathStyleAccess() {
    URI endpoint = URI.create("https://obs.example.com");
    return Stream.of(
        // AWS: never bound into the token, so AWS tokens are unchanged
        arguments(ImmutableS3BucketOptions.builder().build(), Optional.empty()),
        arguments(
            ImmutableS3BucketOptions.builder().pathStyleAccess(false).build(), Optional.empty()),
        arguments(
            ImmutableS3BucketOptions.builder().pathStyleAccess(true).build(), Optional.empty()),
        // Custom endpoint: unset path-style access defaults to virtual-hosted (false)
        arguments(
            ImmutableS3BucketOptions.builder().endpoint(endpoint).build(), Optional.of(false)),
        arguments(
            ImmutableS3BucketOptions.builder().endpoint(endpoint).pathStyleAccess(false).build(),
            Optional.of(false)),
        arguments(
            ImmutableS3BucketOptions.builder().endpoint(endpoint).pathStyleAccess(true).build(),
            Optional.of(true)),
        arguments(
            ImmutableS3BucketOptions.builder().externalEndpoint(endpoint).build(),
            Optional.of(false)),
        arguments(
            ImmutableS3BucketOptions.builder()
                .externalEndpoint(endpoint)
                .pathStyleAccess(false)
                .build(),
            Optional.of(false)));
  }

  @ParameterizedTest
  @MethodSource
  public void writeObjectStorageEnabled(
      ContentKey key, Map<String, String> propertiesIn, Map<String, String> expectedProperties) {

    String loc = "s3://bucket/" + String.join("/", key.getElements());

    IcebergTableMetadata tm = mock(IcebergTableMetadata.class);
    when(tm.location()).thenReturn(loc);
    when(tm.properties()).thenReturn(propertiesIn);

    NessieTableSnapshot nessieSnapshot =
        NessieTableSnapshot.builder()
            .lastUpdatedTimestamp(Instant.now())
            .id(NessieId.randomNessieId())
            .entity(
                NessieTable.builder()
                    .nessieContentId(UUID.randomUUID().toString())
                    .createdTimestamp(Instant.now())
                    .build())
            .build();

    IcebergTableConfig config =
        icebergConfigurer.icebergConfigPerTable(
            nessieSnapshot, "s3://bucket/", tm, "main", key, null, null, true);

    soft.assertThat(config.updatedMetadataProperties())
        .isPresent()
        .get(InstanceOfAssertFactories.map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(expectedProperties);
  }

  static Stream<Arguments> writeObjectStorageEnabled() {
    return Stream.of(
        arguments(
            ContentKey.of("n1", "n2", "my_table"),
            Map.of(
                "write.object-storage.enabled",
                "true",
                "write.data.path",
                "s3://other/",
                "write.object-storage.path",
                "s3://other2/",
                "write.folder-storage.path",
                "s3://other3/"),
            Map.of("write.object-storage.enabled", "true", "write.data.path", "s3://bucket/"))
        //
        );
  }

  @Test
  public void icebergConfigPerTableStorageCredentials() {
    IcebergTableConfig tableConfig =
        tableConfigWithStorageCredential(
            "s3://bucket/path/table",
            "s3://bucket/path",
            Map.of("credential-key", "credential-value"),
            CREDENTIALS_ENDPOINT);

    soft.assertThat(tableConfig.config())
        .containsEntry("legacy-key", "legacy-value")
        .doesNotContainKey(GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT);
    soft.assertThat(tableConfig.storageCredentials())
        .singleElement()
        .satisfies(
            credential -> {
              soft.assertThat(credential.prefix()).isEqualTo("s3://bucket/path");
              soft.assertThat(credential.config())
                  .containsEntry("credential-key", "credential-value");
            });
  }

  @Test
  public void icebergConfigPerTableGcsRefreshCredentialsEndpoint() {
    IcebergTableConfig tableConfig =
        tableConfigWithStorageCredential(
            "gs://bucket/path/table", "gs://", GCS_CREDENTIAL, CREDENTIALS_ENDPOINT);

    soft.assertThat(tableConfig.config())
        .containsEntry(GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, CREDENTIALS_ENDPOINT);
  }

  @Test
  public void icebergConfigPerTableGcsWithoutCredentialsEndpoint() {
    IcebergTableConfig tableConfig =
        tableConfigWithStorageCredential("gs://bucket/path/table", "gs://", GCS_CREDENTIAL, null);

    soft.assertThat(tableConfig.config())
        .doesNotContainKey(GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT);
  }

  @Test
  public void icebergTableCredentialsPath() {
    soft.assertThat(
            icebergConfigurer.uriInfo.icebergTableCredentialsPath(
                "main|warehouse", ContentKey.of("ns1", "ns 2", "table")))
        .isEqualTo("v1/main%7Cwarehouse/namespaces/ns1%1Fns+2/tables/table/credentials");
  }

  @SuppressWarnings("UnnecessaryAssignment")
  private IcebergTableConfig tableConfigWithStorageCredential(
      String tableLocation,
      String credentialPrefix,
      Map<String, String> credentialConfig,
      String credentialsEndpoint) {
    ObjectIO objectIO = mock(ObjectIO.class);
    doAnswer(
            invocation -> {
              BiConsumer<String, String> config = invocation.getArgument(1);
              BiConsumer<String, Map<String, String>> storageCredential = invocation.getArgument(2);
              config.accept("legacy-key", "legacy-value");
              storageCredential.accept(credentialPrefix, new HashMap<>(credentialConfig));
              return null;
            })
        .when(objectIO)
        .configureIcebergTable(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyBoolean());
    icebergConfigurer.objectIO = objectIO;

    IcebergTableMetadata tm = mock(IcebergTableMetadata.class);
    when(tm.location()).thenReturn(tableLocation);
    when(tm.properties()).thenReturn(Map.of());

    NessieTableSnapshot nessieSnapshot =
        NessieTableSnapshot.builder()
            .lastUpdatedTimestamp(Instant.now())
            .id(NessieId.randomNessieId())
            .entity(
                NessieTable.builder()
                    .nessieContentId(UUID.randomUUID().toString())
                    .createdTimestamp(Instant.now())
                    .build())
            .build();

    return icebergConfigurer.icebergConfigPerTable(
        nessieSnapshot,
        tableLocation,
        tm,
        "main",
        ContentKey.of("table"),
        credentialsEndpoint,
        null,
        true);
  }

  /** Verify compatibility with Iceberg < 1.5.0 S3 signer properties. */
  @ParameterizedTest
  @MethodSource
  public void icebergConfigPerTable(
      URI baseUri, String loc, String prefix, ContentKey key, String signUri, String signPath) {

    icebergConfigurer.uriInfo = () -> baseUri;

    NessieTableSnapshot nessieSnapshot =
        NessieTableSnapshot.builder()
            .lastUpdatedTimestamp(Instant.now())
            .id(NessieId.randomNessieId())
            .entity(
                NessieTable.builder()
                    .nessieContentId(UUID.randomUUID().toString())
                    .createdTimestamp(Instant.now())
                    .build())
            .build();
    String warehouseLocation = "s3://bucket/";

    IcebergTableMetadata tm = mock(IcebergTableMetadata.class);
    when(tm.location()).thenReturn(loc);
    when(tm.properties()).thenReturn(Map.of());

    IcebergTableConfig tableConfig =
        icebergConfigurer.icebergConfigPerTable(
            nessieSnapshot, warehouseLocation, tm, prefix, key, null, null, true);
    if (signUri != null) {
      soft.assertThat(tableConfig.config()).containsEntry(S3_SIGNER_URI, signUri);
      soft.assertThat(signUri).endsWith("/");
      soft.assertThat(signPath).isNotNull();
    } else {
      soft.assertThat(tableConfig.config()).doesNotContainKey(S3_SIGNER_URI);
    }
    if (signPath != null) {
      URI endpoint = URI.create(tableConfig.config().get(S3_SIGNER_ENDPOINT));
      soft.assertThat(endpoint.getRawPath()).startsWith(signPath);
      soft.assertThat(endpoint.getRawQuery()).isNull();

      SignerParams signerParams =
          SignerParams.fromPathParam(
              endpoint.getRawPath().substring(endpoint.getRawPath().lastIndexOf('/') + 1));
      soft.assertThat(signerParams)
          .extracting(
              SignerParams::keyName,
              p -> p.signerSignature().writeLocations(),
              p -> p.signerSignature().warehouseLocation(),
              p -> p.signerSignature().identifier())
          .containsExactly(
              signerKey.name(),
              List.of(normalizeS3Scheme(loc)),
              warehouseLocation,
              key.toPathStringEscaped());
      soft.assertThat(signPath).doesNotStartWith("/");
      soft.assertThat(signUri).isNotNull();
    } else {
      soft.assertThat(tableConfig.config()).doesNotContainKey(S3_SIGNER_ENDPOINT);
    }
  }

  @SuppressWarnings("HttpUrlsUsage")
  static Stream<Arguments> icebergConfigPerTable() {
    ContentKey key = ContentKey.of("foo", "bar");

    String s3 = "s3://bucket/path/1/2/3";
    String s3a = "s3a://bucket/path/1/2/3";
    String s3n = "s3n://bucket/path/1/2/3";
    String gcs = "gcs://bucket/path/1/2/3";
    String complexPrefix = "main|s3://blah/meep";
    return Stream.of(
        arguments(
            URI.create("http://foo:12434"),
            s3,
            "main",
            key,
            "http://foo:12434/iceberg/",
            "v1/main/s3sign/"),
        // Tables written by HadoopFileIO have metadata.location prefixed with s3a:// while the
        // warehouse is registered with s3://. The writeable derivation must normalize the scheme
        // on both sides of the prefix check; otherwise the writeable[] returned to the S3 signer
        // is empty and every PUT/DELETE is rejected.
        arguments(
            URI.create("http://foo:12434"),
            s3a,
            "main",
            key,
            "http://foo:12434/iceberg/",
            "v1/main/s3sign/"),
        arguments(
            URI.create("http://foo:12434"),
            s3n,
            "main",
            key,
            "http://foo:12434/iceberg/",
            "v1/main/s3sign/"),
        arguments(
            URI.create("http://foo:12434/some/long/prefix/"),
            s3,
            complexPrefix,
            key,
            "http://foo:12434/some/long/prefix/iceberg/",
            "v1/" + encode(complexPrefix, UTF_8) + "/s3sign/"),
        arguments(
            URI.create("https://foo/some/long/prefix/"),
            s3,
            complexPrefix,
            key,
            "https://foo/some/long/prefix/iceberg/",
            "v1/" + encode(complexPrefix, UTF_8) + "/s3sign/"),
        arguments(
            URI.create("http://foo:12434/some/long/prefix/"), gcs, complexPrefix, key, null, null));
  }
}
