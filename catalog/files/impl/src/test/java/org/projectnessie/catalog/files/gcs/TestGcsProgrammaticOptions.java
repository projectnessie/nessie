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
package org.projectnessie.catalog.files.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.files.gcs.GcsBucketOptions.GcsAuthType;

class TestGcsProgrammaticOptions {

  @Test
  void normalize() {
    GcsOptions input =
        ImmutableGcsProgrammaticOptions.builder()
            .readTimeout(Duration.ofSeconds(1))
            .connectTimeout(Duration.ofSeconds(2))
            .maxAttempts(2)
            .logicalTimeout(Duration.ofSeconds(3))
            .totalTimeout(Duration.ofSeconds(4))
            .initialRetryDelay(Duration.ofSeconds(5))
            .maxRetryDelay(Duration.ofSeconds(6))
            .retryDelayMultiplier(7)
            .initialRpcTimeout(Duration.ofSeconds(8))
            .maxRpcTimeout(Duration.ofSeconds(9))
            .rpcTimeoutMultiplier(10)
            .defaultOptions(
                ImmutableGcsNamedBucketOptions.builder()
                    .host(URI.create("https://host"))
                    .externalHost(URI.create("https://externalHost"))
                    .userProject("userProject")
                    .projectId("projectId")
                    .quotaProjectId("quotaProjectId")
                    .clientLibToken("client lib token")
                    .authType(GcsAuthType.NONE)
                    .readChunkSize(1)
                    .writeChunkSize(2)
                    .deleteBatchSize(3)
                    .build())
            .putBuckets(
                "bucket1",
                ImmutableGcsNamedBucketOptions.builder()
                    .host(URI.create("https://host1"))
                    .externalHost(URI.create("https://externalHost1"))
                    .userProject("userProject1")
                    .projectId("projectId1")
                    .quotaProjectId("quotaProjectId1")
                    .clientLibToken("client lib token1")
                    .authType(GcsAuthType.ACCESS_TOKEN)
                    .readChunkSize(4)
                    .writeChunkSize(5)
                    .deleteBatchSize(6)
                    .build())
            .putBuckets(
                "bucket2",
                ImmutableGcsNamedBucketOptions.builder()
                    .name("my-bucket-2")
                    .host(URI.create("https://host2"))
                    .externalHost(URI.create("https://externalHost2"))
                    .userProject("userProject2")
                    .projectId("projectId2")
                    .quotaProjectId("quotaProjectId2")
                    .clientLibToken("client lib token2")
                    .authType(GcsAuthType.SERVICE_ACCOUNT)
                    .readChunkSize(7)
                    .writeChunkSize(8)
                    .deleteBatchSize(9)
                    .build())
            .build();
    GcsOptions expected =
        ImmutableGcsProgrammaticOptions.builder()
            .readTimeout(Duration.ofSeconds(1))
            .connectTimeout(Duration.ofSeconds(2))
            .maxAttempts(2)
            .logicalTimeout(Duration.ofSeconds(3))
            .totalTimeout(Duration.ofSeconds(4))
            .initialRetryDelay(Duration.ofSeconds(5))
            .maxRetryDelay(Duration.ofSeconds(6))
            .retryDelayMultiplier(7)
            .initialRpcTimeout(Duration.ofSeconds(8))
            .maxRpcTimeout(Duration.ofSeconds(9))
            .rpcTimeoutMultiplier(10)
            .defaultOptions(
                ImmutableGcsNamedBucketOptions.builder()
                    .host(URI.create("https://host"))
                    .externalHost(URI.create("https://externalHost"))
                    .userProject("userProject")
                    .projectId("projectId")
                    .quotaProjectId("quotaProjectId")
                    .clientLibToken("client lib token")
                    .authType(GcsAuthType.NONE)
                    .readChunkSize(1)
                    .writeChunkSize(2)
                    .deleteBatchSize(3)
                    .build())
            .putBuckets(
                "bucket1",
                ImmutableGcsNamedBucketOptions.builder()
                    .name("bucket1")
                    .host(URI.create("https://host1"))
                    .externalHost(URI.create("https://externalHost1"))
                    .userProject("userProject1")
                    .projectId("projectId1")
                    .quotaProjectId("quotaProjectId1")
                    .clientLibToken("client lib token1")
                    .authType(GcsAuthType.ACCESS_TOKEN)
                    .readChunkSize(4)
                    .writeChunkSize(5)
                    .deleteBatchSize(6)
                    .build())
            .putBuckets(
                "my-bucket-2",
                ImmutableGcsNamedBucketOptions.builder()
                    .name("my-bucket-2")
                    .host(URI.create("https://host2"))
                    .externalHost(URI.create("https://externalHost2"))
                    .userProject("userProject2")
                    .projectId("projectId2")
                    .quotaProjectId("quotaProjectId2")
                    .clientLibToken("client lib token2")
                    .authType(GcsAuthType.SERVICE_ACCOUNT)
                    .readChunkSize(7)
                    .writeChunkSize(8)
                    .deleteBatchSize(9)
                    .build())
            .build();
    GcsOptions actual = GcsProgrammaticOptions.normalize(input);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void normalizeBuckets() {
    assertThat(
            ImmutableGcsProgrammaticOptions.builder()
                .putBuckets("fs1", ImmutableGcsNamedBucketOptions.builder().build())
                .putBuckets(
                    "fs2", ImmutableGcsNamedBucketOptions.builder().name("my-bucket").build())
                .build()
                .buckets())
        .containsOnlyKeys("fs1", "my-bucket");
    assertThatThrownBy(
            () ->
                ImmutableGcsProgrammaticOptions.builder()
                    .putBuckets("bucket1", ImmutableGcsNamedBucketOptions.builder().build())
                    .putBuckets(
                        "bucket2", ImmutableGcsNamedBucketOptions.builder().name("bucket1").build())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Duplicate GCS bucket name 'bucket1', check your GCS bucket configurations");
  }
}
