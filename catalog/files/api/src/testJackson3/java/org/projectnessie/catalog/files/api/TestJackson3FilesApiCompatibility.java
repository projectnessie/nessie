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
package org.projectnessie.catalog.files.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions.AdlsRetryStrategy;
import org.projectnessie.catalog.files.config.GcsBucketOptions.GcsAuthType;
import org.projectnessie.catalog.files.config.ImmutableAdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsNamedFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsNamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.files.config.ImmutableS3BucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3ClientIam;
import org.projectnessie.catalog.files.config.ImmutableS3Config;
import org.projectnessie.catalog.files.config.ImmutableS3Http;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.files.config.ImmutableS3ServerIam;
import org.projectnessie.catalog.files.config.ImmutableSecretStore;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.files.config.S3Options;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TestJackson3FilesApiCompatibility {
  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  @Test
  void signingRequestAndResponseRoundTrip() throws Exception {
    SigningRequest request =
        SigningRequest.signingRequest(
            URI.create("s3://bucket/path/to/file"),
            "PUT",
            "us-east-1",
            Optional.of("bucket"),
            Optional.of("body"),
            Map.of("x-amz-date", List.of("20260707T121314Z")));

    String requestJson = MAPPER.writeValueAsString(request);
    assertThat(MAPPER.readValue(requestJson, SigningRequest.class)).isEqualTo(request);

    SigningResponse response =
        ImmutableSigningResponse.of(
            URI.create("https://example.com/bucket/path/to/file"),
            Map.of("x-amz-checksum-sha256", List.of("checksum")));

    String responseJson = MAPPER.writeValueAsString(response);
    assertThat(MAPPER.readValue(responseJson, SigningResponse.class)).isEqualTo(response);
  }

  @Test
  void s3ConfigAndOptionsRoundTrip() throws Exception {
    S3Config config =
        ImmutableS3Config.builder()
            .http(ImmutableS3Http.builder().connectTimeout(Duration.ofSeconds(3)).build())
            .trustAllCertificates(false)
            .trustStore(
                ImmutableSecretStore.builder()
                    .path(Path.of("/var/run/secrets/truststore.p12"))
                    .type("PKCS12")
                    .password(URI.create("urn:nessie-secret:truststore-password"))
                    .build())
            .build();

    String configJson = MAPPER.writeValueAsString(config);
    assertThat(MAPPER.readValue(configJson, S3Config.class)).isEqualTo(config);

    S3Options options =
        ImmutableS3Options.builder()
            .defaultOptions(
                ImmutableS3BucketOptions.builder()
                    .endpoint(URI.create("https://s3.example.com"))
                    .region("us-east-1")
                    .clientIam(
                        ImmutableS3ClientIam.builder()
                            .enabled(true)
                            .assumeRole("arn:aws:iam::123456789012:role/client")
                            .externalId("external")
                            .build())
                    .serverIam(
                        ImmutableS3ServerIam.builder()
                            .enabled(true)
                            .assumeRole("arn:aws:iam::123456789012:role/server")
                            .build())
                    .build())
            .putBucket(
                "warehouse",
                ImmutableS3NamedBucketOptions.builder()
                    .name("warehouse")
                    .authority("bucket")
                    .pathPrefix("warehouse")
                    .region("eu-central-1")
                    .build())
            .build();

    String optionsJson = MAPPER.writeValueAsString(options);
    assertThat(MAPPER.readValue(optionsJson, S3Options.class)).isEqualTo(options);
  }

  @Test
  void gcsAndAdlsOptionsRoundTrip() throws Exception {
    var gcsOptions =
        ImmutableGcsOptions.builder()
            .defaultOptions(
                ImmutableGcsBucketOptions.builder()
                    .host(URI.create("https://storage.googleapis.com"))
                    .externalHost(URI.create("https://external.example.com"))
                    .projectId("project")
                    .quotaProjectId("quota")
                    .authType(GcsAuthType.SERVICE_ACCOUNT)
                    .build())
            .putBucket(
                "bucket",
                ImmutableGcsNamedBucketOptions.builder()
                    .name("bucket")
                    .authority("bucket-authority")
                    .pathPrefix("warehouse")
                    .userProject("user-project")
                    .readChunkSize(1024)
                    .writeChunkSize(2048)
                    .build())
            .build();

    String gcsJson = MAPPER.writeValueAsString(gcsOptions);
    assertThat(MAPPER.readValue(gcsJson, ImmutableGcsOptions.class)).isEqualTo(gcsOptions);

    var adlsOptions =
        ImmutableAdlsOptions.builder()
            .readBlockSize(1024)
            .writeBlockSize(2048L)
            .defaultOptions(
                ImmutableAdlsFileSystemOptions.builder()
                    .endpoint("https://account.dfs.core.windows.net")
                    .externalEndpoint("https://external.example.com")
                    .retryPolicy(AdlsRetryStrategy.EXPONENTIAL_BACKOFF)
                    .maxRetries(3)
                    .tryTimeout(Duration.ofSeconds(30))
                    .build())
            .putFileSystem(
                "filesystem",
                ImmutableAdlsNamedFileSystemOptions.builder()
                    .name("filesystem")
                    .authority("account")
                    .pathPrefix("warehouse")
                    .build())
            .build();

    String adlsJson = MAPPER.writeValueAsString(adlsOptions);
    assertThat(MAPPER.readValue(adlsJson, ImmutableAdlsOptions.class)).isEqualTo(adlsOptions);
  }
}
