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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;
import static org.projectnessie.catalog.service.rest.IcebergConfigurer.S3_SIGNER_ENDPOINT;
import static org.projectnessie.catalog.service.rest.IcebergConfigurer.S3_SIGNER_URI;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.service.api.SignerKeysService;
import org.projectnessie.catalog.service.objtypes.SignerKey;
import org.projectnessie.model.ContentKey;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIcebergConfigurer {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected IcebergConfigurer icebergConfigurer;
  protected SignerKey signerKey;

  @BeforeEach
  protected void setupIcebergConfigurer() {
    SecretsProvider secretsProvider =
        ResolvingSecretsProvider.builder()
            .putSecretsManager("plain", unsafePlainTextSecretsProvider(Map.of()))
            .build();
    S3Options s3Options = ImmutableS3Options.builder().build();

    icebergConfigurer = new IcebergConfigurer();
    icebergConfigurer.uriInfo = () -> URI.create("http://foo:12434");
    icebergConfigurer.objectIO =
        new S3ObjectIO(new S3ClientSupplier(null, s3Options, null, secretsProvider), null);
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
            nessieSnapshot, "s3://bucket/", tm, "main", key, null, true);

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
            nessieSnapshot, warehouseLocation, tm, prefix, key, null, true);
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
              signerKey.name(), List.of(loc), warehouseLocation, key.toPathStringEscaped());
      soft.assertThat(signPath).doesNotStartWith("/");
      soft.assertThat(signUri).isNotNull();
    } else {
      soft.assertThat(tableConfig.config()).doesNotContainKey(S3_SIGNER_ENDPOINT);
    }
  }

  static Stream<Arguments> icebergConfigPerTable() {
    ContentKey key = ContentKey.of("foo", "bar");

    String s3 = "s3://bucket/path/1/2/3";
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
