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
package org.projectnessie.catalog.files.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.net.URI;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.config.GcsBucketOptions.GcsAuthType;
import org.projectnessie.storage.uri.StorageUri;

class TestGcsOptions {

  @ParameterizedTest
  @MethodSource
  public void resolveOptionsForUri(GcsOptions options, StorageUri uri, String expectedName) {
    GcsNamedBucketOptions resolved = options.resolveOptionsForUri(uri);
    assertThat(resolved.name()).isEqualTo(Optional.ofNullable(expectedName));
  }

  static Stream<Arguments> resolveOptionsForUri() {
    return Stream.of(
        arguments(
            ImmutableGcsOptions.builder()
                .putBucket(
                    "foo",
                    ImmutableGcsNamedBucketOptions.builder()
                        .name("my-bucket")
                        .authority("my-authority")
                        .pathPrefix("foop")
                        .build())
                .putBucket(
                    "bar",
                    ImmutableGcsNamedBucketOptions.builder()
                        .name("other-bucket")
                        .authority("my-authority")
                        .pathPrefix("baaz")
                        .build())
                .build(),
            StorageUri.of("s3://my-authority/foop"),
            "my-bucket"),
        //
        arguments(
            ImmutableGcsOptions.builder()
                .putBucket(
                    "foo",
                    ImmutableGcsNamedBucketOptions.builder()
                        .name("my-bucket")
                        .authority("my-authority")
                        .pathPrefix("foop")
                        .build())
                .putBucket(
                    "baz",
                    ImmutableGcsNamedBucketOptions.builder()
                        .name("too-short-path-prefix")
                        .authority("my-authority")
                        .pathPrefix("fo")
                        .build())
                .putBucket(
                    "bar",
                    ImmutableGcsNamedBucketOptions.builder()
                        .name("other-bucket")
                        .authority("my-authority")
                        .pathPrefix("baaz")
                        .build())
                .build(),
            StorageUri.of("s3://my-authority/foop"),
            "my-bucket"),
        //
        arguments(
            ImmutableGcsOptions.builder()
                .putBucket(
                    "foo",
                    ImmutableGcsNamedBucketOptions.builder()
                        .name("my-bucket")
                        .authority("my-authority")
                        .pathPrefix("foop")
                        .build())
                .putBucket(
                    "bar",
                    ImmutableGcsNamedBucketOptions.builder()
                        .name("other-bucket")
                        .authority("my-authority")
                        .pathPrefix("baaz")
                        .build())
                .build(),
            StorageUri.of("s3://my-authority/baaz/blah/moo"),
            "other-bucket"),
        //
        arguments(
            ImmutableGcsOptions.builder()
                .putBucket(
                    "foo",
                    ImmutableGcsNamedBucketOptions.builder()
                        .name("my-bucket")
                        .authority("my-authority")
                        .pathPrefix("foop")
                        .build())
                .putBucket(
                    "bar",
                    ImmutableGcsNamedBucketOptions.builder()
                        .name("other-bucket")
                        .authority("my-authority")
                        .pathPrefix("baaz")
                        .build())
                .build(),
            StorageUri.of("s3://my-authority/"),
            null));
  }

  @Test
  void deepClone() {
    GcsOptions input =
        ImmutableGcsOptions.builder()
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
            .putBucket(
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
            .putBucket(
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
        ImmutableGcsOptions.builder()
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
            .putBucket(
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
            .putBucket(
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
    GcsOptions actual = input.deepClone();
    assertThat(actual).isEqualTo(expected);
  }
}
