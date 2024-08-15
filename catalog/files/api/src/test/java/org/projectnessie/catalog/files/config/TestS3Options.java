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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestS3Options {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void effectiveOptionsForBucketIamOptions(
      S3Options options,
      S3BucketOptions expected,
      S3Iam expectedClientIam,
      S3Iam expectedServerIam) {
    S3BucketOptions actual = options.effectiveOptionsForBucket(Optional.of("bucket"));
    soft.assertThat(actual).isEqualTo(expected);
    soft.assertThat(actual.getEnabledClientIam()).isEqualTo(Optional.ofNullable(expectedClientIam));
    soft.assertThat(actual.getEnabledServerIam()).isEqualTo(Optional.ofNullable(expectedServerIam));
  }

  static Stream<Arguments> effectiveOptionsForBucketIamOptions() {
    S3ClientIam noClientIam = ImmutableS3ClientIam.builder().build();
    S3ServerIam noServerIam = ImmutableS3ServerIam.builder().build();
    return Stream.of(
        //
        // #1 - enabled on default AND bucket
        arguments(
            ImmutableS3Options.builder()
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .clientIam(
                            ImmutableS3ClientIam.builder()
                                .policy("default-policy")
                                .externalId("default-id")
                                .roleSessionName("default-rsn")
                                .enabled(true)
                                .build())
                        .build())
                .putBucket(
                    "bucket",
                    ImmutableS3NamedBucketOptions.builder().clientIam(noClientIam).build())
                .build(),
            // expected options
            ImmutableS3NamedBucketOptions.builder()
                .name("bucket")
                .clientIam(
                    ImmutableS3ClientIam.builder()
                        .policy("default-policy")
                        .externalId("default-id")
                        .roleSessionName("default-rsn")
                        .enabled(true)
                        .build())
                .serverIam(noServerIam)
                .build(),
            // client IAM
            ImmutableS3ClientIam.builder()
                .policy("default-policy")
                .externalId("default-id")
                .roleSessionName("default-rsn")
                .enabled(true)
                .build(),
            // server IAM
            null),
        //
        // #2 - enabled on default
        arguments(
            ImmutableS3Options.builder()
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .clientIam(
                            ImmutableS3ClientIam.builder()
                                .policy("default-policy")
                                .externalId("default-id")
                                .roleSessionName("default-rsn")
                                .enabled(true)
                                .build())
                        .build())
                .putBucket(
                    "bucket",
                    ImmutableS3NamedBucketOptions.builder().clientIam(noClientIam).build())
                .build(),
            // expected options
            ImmutableS3NamedBucketOptions.builder()
                .name("bucket")
                .clientIam(
                    ImmutableS3ClientIam.builder()
                        .policy("default-policy")
                        .externalId("default-id")
                        .roleSessionName("default-rsn")
                        .enabled(true)
                        .build())
                .serverIam(noServerIam)
                .build(),
            // client IAM
            ImmutableS3ClientIam.builder()
                .policy("default-policy")
                .externalId("default-id")
                .roleSessionName("default-rsn")
                .enabled(true)
                .build(),
            // server IAM
            null),
        //
        // #3 - enabled on bucket
        arguments(
            ImmutableS3Options.builder()
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .clientIam(
                            ImmutableS3ClientIam.builder()
                                .policy("default-policy")
                                .externalId("default-id")
                                .roleSessionName("default-rsn")
                                .build())
                        .build())
                .putBucket(
                    "bucket",
                    ImmutableS3NamedBucketOptions.builder()
                        .clientIam(ImmutableS3ClientIam.builder().enabled(true).build())
                        .build())
                .build(),
            // expected options
            ImmutableS3NamedBucketOptions.builder()
                .name("bucket")
                .clientIam(
                    ImmutableS3ClientIam.builder()
                        .policy("default-policy")
                        .externalId("default-id")
                        .roleSessionName("default-rsn")
                        .enabled(true)
                        .build())
                .serverIam(noServerIam)
                .build(),
            // client IAM
            ImmutableS3ClientIam.builder()
                .policy("default-policy")
                .externalId("default-id")
                .roleSessionName("default-rsn")
                .enabled(true)
                .build(),
            // server IAM
            null),

        //
        // #4 - server enabled on default AND bucket
        arguments(
            ImmutableS3Options.builder()
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .policy("default-policy")
                                .externalId("default-id")
                                .roleSessionName("default-rsn")
                                .enabled(true)
                                .build())
                        .build())
                .putBucket(
                    "bucket",
                    ImmutableS3NamedBucketOptions.builder().serverIam(noServerIam).build())
                .build(),
            // expected options
            ImmutableS3NamedBucketOptions.builder()
                .name("bucket")
                .clientIam(noClientIam)
                .serverIam(
                    ImmutableS3ServerIam.builder()
                        .policy("default-policy")
                        .externalId("default-id")
                        .roleSessionName("default-rsn")
                        .enabled(true)
                        .build())
                .build(),
            // client IAM
            null,
            // server IAM
            ImmutableS3ServerIam.builder()
                .policy("default-policy")
                .externalId("default-id")
                .roleSessionName("default-rsn")
                .enabled(true)
                .build()),
        //
        // #5 - server enabled on default
        arguments(
            ImmutableS3Options.builder()
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .policy("default-policy")
                                .externalId("default-id")
                                .roleSessionName("default-rsn")
                                .enabled(true)
                                .build())
                        .build())
                .putBucket(
                    "bucket",
                    ImmutableS3NamedBucketOptions.builder().serverIam(noServerIam).build())
                .build(),
            // expected options
            ImmutableS3NamedBucketOptions.builder()
                .name("bucket")
                .clientIam(noClientIam)
                .serverIam(
                    ImmutableS3ServerIam.builder()
                        .policy("default-policy")
                        .externalId("default-id")
                        .roleSessionName("default-rsn")
                        .enabled(true)
                        .build())
                .build(),
            // client IAM
            null,
            // server IAM
            ImmutableS3ServerIam.builder()
                .policy("default-policy")
                .externalId("default-id")
                .roleSessionName("default-rsn")
                .enabled(true)
                .build()),
        //
        // #6 - server enabled on bucket
        arguments(
            ImmutableS3Options.builder()
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .policy("default-policy")
                                .externalId("default-id")
                                .roleSessionName("default-rsn")
                                .build())
                        .build())
                .putBucket(
                    "bucket",
                    ImmutableS3NamedBucketOptions.builder()
                        .serverIam(ImmutableS3ServerIam.builder().enabled(true).build())
                        .build())
                .build(),
            // expected options
            ImmutableS3NamedBucketOptions.builder()
                .name("bucket")
                .clientIam(noClientIam)
                .serverIam(
                    ImmutableS3ServerIam.builder()
                        .policy("default-policy")
                        .externalId("default-id")
                        .roleSessionName("default-rsn")
                        .enabled(true)
                        .build())
                .build(),
            // client IAM
            null,
            // server IAM
            ImmutableS3ServerIam.builder()
                .policy("default-policy")
                .externalId("default-id")
                .roleSessionName("default-rsn")
                .enabled(true)
                .build()),

        //
        // #7 - client + server
        arguments(
            ImmutableS3Options.builder()
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .clientIam(
                            ImmutableS3ClientIam.builder()
                                .policy("default-client-policy")
                                .externalId("default-client-id")
                                .roleSessionName("default-client-rsn")
                                .build())
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .policy("default-server-policy")
                                .externalId("default-server-id")
                                .roleSessionName("default-server-rsn")
                                .build())
                        .build())
                .putBucket(
                    "bucket",
                    ImmutableS3NamedBucketOptions.builder()
                        .clientIam(
                            ImmutableS3ClientIam.builder()
                                .externalId("bucket-client-id")
                                .enabled(true)
                                .build())
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .externalId("bucket-server-id")
                                .enabled(true)
                                .build())
                        .build())
                .build(),
            // expected options
            ImmutableS3NamedBucketOptions.builder()
                .name("bucket")
                .clientIam(
                    ImmutableS3ClientIam.builder()
                        .policy("default-client-policy")
                        .externalId("default-client-id")
                        .roleSessionName("default-client-rsn")
                        .externalId("bucket-client-id")
                        .enabled(true)
                        .build())
                .serverIam(
                    ImmutableS3ServerIam.builder()
                        .policy("default-server-policy")
                        .externalId("default-server-id")
                        .roleSessionName("default-server-rsn")
                        .externalId("bucket-server-id")
                        .enabled(true)
                        .build())
                .build(),
            // client IAM
            ImmutableS3ClientIam.builder()
                .policy("default-client-policy")
                .externalId("default-client-id")
                .roleSessionName("default-client-rsn")
                .externalId("bucket-client-id")
                .enabled(true)
                .build(),
            // server IAM
            ImmutableS3ServerIam.builder()
                .policy("default-server-policy")
                .externalId("default-server-id")
                .roleSessionName("default-server-rsn")
                .externalId("bucket-server-id")
                .enabled(true)
                .build()),

        //
        // #8
        arguments(
            ImmutableS3Options.builder().build(),
            // expected options
            ImmutableS3NamedBucketOptions.builder().build(),
            // client IAM
            null,
            // server IAM
            null)
        //
        );
  }

  @ParameterizedTest
  @MethodSource
  void normalize(S3Options input, S3Options expected) {
    S3Options actual = S3Options.normalize(input);
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> normalize() {
    return Stream.of(
        //
        arguments(
            ImmutableS3Options.builder()
                .sessionGracePeriod(Duration.ofSeconds(1))
                .sessionCacheMaxSize(2)
                .clientsCacheMaxSize(3)
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .endpoint(URI.create("https://host"))
                        .externalEndpoint(URI.create("https://externalHost"))
                        .pathStyleAccess(true)
                        .region("region")
                        .accessPoint("accessPoint")
                        .allowCrossRegionAccessPoint(true)
                        .authType(S3AuthType.STATIC)
                        .stsEndpoint(URI.create("https://stsEndpoint"))
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .enabled(true)
                                .assumeRole("assumeRole")
                                .sessionDuration(Duration.ofSeconds(4))
                                .externalId("externalId")
                                .roleSessionName("roleSessionName")
                                .policy("sessionIamPolicy")
                                .build())
                        .build())
                .putBucket(
                    "bucket1",
                    ImmutableS3NamedBucketOptions.builder()
                        .endpoint(URI.create("https://host1"))
                        .externalEndpoint(URI.create("https://externalHost1"))
                        .pathStyleAccess(false)
                        .region("region1")
                        .accessPoint("accessPoint1")
                        .allowCrossRegionAccessPoint(false)
                        .authType(S3AuthType.APPLICATION_GLOBAL)
                        .stsEndpoint(URI.create("https://stsEndpoint1"))
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .enabled(true)
                                .assumeRole("assumeRole1")
                                .sessionDuration(Duration.ofSeconds(5))
                                .externalId("externalId1")
                                .roleSessionName("roleSessionName1")
                                .policy("sessionIamPolicy1")
                                .build())
                        .build())
                .putBucket(
                    "bucket2",
                    ImmutableS3NamedBucketOptions.builder()
                        .name("my-bucket-2")
                        .endpoint(URI.create("https://host2"))
                        .externalEndpoint(URI.create("https://externalHost2"))
                        .pathStyleAccess(true)
                        .region("region2")
                        .accessPoint("accessPoint2")
                        .allowCrossRegionAccessPoint(true)
                        .authType(S3AuthType.STATIC)
                        .stsEndpoint(URI.create("https://stsEndpoint2"))
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .enabled(true)
                                .assumeRole("assumeRole2")
                                .sessionDuration(Duration.ofSeconds(6))
                                .externalId("externalId2")
                                .roleSessionName("roleSessionName2")
                                .policy("sessionIamPolicy2")
                                .build())
                        .build())
                .build(),
            ImmutableS3Options.builder()
                .sessionGracePeriod(Duration.ofSeconds(1))
                .sessionCacheMaxSize(2)
                .clientsCacheMaxSize(3)
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .endpoint(URI.create("https://host"))
                        .externalEndpoint(URI.create("https://externalHost"))
                        .pathStyleAccess(true)
                        .region("region")
                        .accessPoint("accessPoint")
                        .allowCrossRegionAccessPoint(true)
                        .authType(S3AuthType.STATIC)
                        .stsEndpoint(URI.create("https://stsEndpoint"))
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .enabled(true)
                                .assumeRole("assumeRole")
                                .sessionDuration(Duration.ofSeconds(4))
                                .externalId("externalId")
                                .roleSessionName("roleSessionName")
                                .policy("sessionIamPolicy")
                                .build())
                        .build())
                .putBucket(
                    "bucket1",
                    ImmutableS3NamedBucketOptions.builder()
                        .name("bucket1")
                        .endpoint(URI.create("https://host1"))
                        .externalEndpoint(URI.create("https://externalHost1"))
                        .pathStyleAccess(false)
                        .region("region1")
                        .accessPoint("accessPoint1")
                        .allowCrossRegionAccessPoint(false)
                        .authType(S3AuthType.APPLICATION_GLOBAL)
                        .stsEndpoint(URI.create("https://stsEndpoint1"))
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .enabled(true)
                                .assumeRole("assumeRole1")
                                .sessionDuration(Duration.ofSeconds(5))
                                .externalId("externalId1")
                                .roleSessionName("roleSessionName1")
                                .policy("sessionIamPolicy1")
                                .build())
                        .build())
                .putBucket(
                    "my-bucket-2",
                    ImmutableS3NamedBucketOptions.builder()
                        .name("my-bucket-2")
                        .endpoint(URI.create("https://host2"))
                        .externalEndpoint(URI.create("https://externalHost2"))
                        .pathStyleAccess(true)
                        .region("region2")
                        .accessPoint("accessPoint2")
                        .allowCrossRegionAccessPoint(true)
                        .authType(S3AuthType.STATIC)
                        .stsEndpoint(URI.create("https://stsEndpoint2"))
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .enabled(true)
                                .assumeRole("assumeRole2")
                                .sessionDuration(Duration.ofSeconds(6))
                                .externalId("externalId2")
                                .roleSessionName("roleSessionName2")
                                .policy("sessionIamPolicy2")
                                .build())
                        .build())
                .build()),
        //
        arguments(
            ImmutableS3Options.builder()
                .sessionGracePeriod(Duration.ofSeconds(1))
                .sessionCacheMaxSize(2)
                .clientsCacheMaxSize(3)
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .endpoint(URI.create("https://host"))
                        .externalEndpoint(URI.create("https://externalHost"))
                        .pathStyleAccess(true)
                        .region("region")
                        .accessPoint("accessPoint")
                        .allowCrossRegionAccessPoint(true)
                        .authType(S3AuthType.STATIC)
                        .stsEndpoint(URI.create("https://stsEndpoint"))
                        .build())
                .putBucket(
                    "bucket1",
                    ImmutableS3NamedBucketOptions.builder()
                        .endpoint(URI.create("https://host1"))
                        .externalEndpoint(URI.create("https://externalHost1"))
                        .pathStyleAccess(false)
                        .region("region1")
                        .accessPoint("accessPoint1")
                        .allowCrossRegionAccessPoint(false)
                        .authType(S3AuthType.APPLICATION_GLOBAL)
                        .stsEndpoint(URI.create("https://stsEndpoint1"))
                        .build())
                .putBucket(
                    "bucket2",
                    ImmutableS3NamedBucketOptions.builder()
                        .name("my-bucket-2")
                        .endpoint(URI.create("https://host2"))
                        .externalEndpoint(URI.create("https://externalHost2"))
                        .pathStyleAccess(true)
                        .region("region2")
                        .accessPoint("accessPoint2")
                        .allowCrossRegionAccessPoint(true)
                        .authType(S3AuthType.STATIC)
                        .stsEndpoint(URI.create("https://stsEndpoint2"))
                        .build())
                .build(),
            ImmutableS3Options.builder()
                .sessionGracePeriod(Duration.ofSeconds(1))
                .sessionCacheMaxSize(2)
                .clientsCacheMaxSize(3)
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .endpoint(URI.create("https://host"))
                        .externalEndpoint(URI.create("https://externalHost"))
                        .pathStyleAccess(true)
                        .region("region")
                        .accessPoint("accessPoint")
                        .allowCrossRegionAccessPoint(true)
                        .authType(S3AuthType.STATIC)
                        .stsEndpoint(URI.create("https://stsEndpoint"))
                        .build())
                .putBucket(
                    "bucket1",
                    ImmutableS3NamedBucketOptions.builder()
                        .name("bucket1")
                        .endpoint(URI.create("https://host1"))
                        .externalEndpoint(URI.create("https://externalHost1"))
                        .pathStyleAccess(false)
                        .region("region1")
                        .accessPoint("accessPoint1")
                        .allowCrossRegionAccessPoint(false)
                        .authType(S3AuthType.APPLICATION_GLOBAL)
                        .stsEndpoint(URI.create("https://stsEndpoint1"))
                        .build())
                .putBucket(
                    "my-bucket-2",
                    ImmutableS3NamedBucketOptions.builder()
                        .name("my-bucket-2")
                        .endpoint(URI.create("https://host2"))
                        .externalEndpoint(URI.create("https://externalHost2"))
                        .pathStyleAccess(true)
                        .region("region2")
                        .accessPoint("accessPoint2")
                        .allowCrossRegionAccessPoint(true)
                        .authType(S3AuthType.STATIC)
                        .stsEndpoint(URI.create("https://stsEndpoint2"))
                        .build())
                .build())
        //
        );
  }

  @Test
  void normalizeBuckets() {
    assertThat(
            ImmutableS3Options.builder()
                .putBucket("fs1", ImmutableS3NamedBucketOptions.builder().build())
                .putBucket("fs2", ImmutableS3NamedBucketOptions.builder().name("my-bucket").build())
                .build()
                .buckets())
        .containsOnlyKeys("fs1", "my-bucket");
    assertThatThrownBy(
            () ->
                ImmutableS3Options.builder()
                    .putBucket("bucket1", ImmutableS3NamedBucketOptions.builder().build())
                    .putBucket(
                        "bucket2", ImmutableS3NamedBucketOptions.builder().name("bucket1").build())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Duplicate S3 bucket name 'bucket1', check your S3 bucket configurations");
  }
}
