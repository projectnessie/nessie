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
package org.projectnessie.catalog.files.s3;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.secrets.SecretsProvider;

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
    S3BucketOptions actual =
        options.effectiveOptionsForBucket(
            Optional.of("bucket"), new SecretsProvider(names -> Map.of()));
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
            ImmutableS3ProgrammaticOptions.builder()
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
                .putBuckets(
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
            ImmutableS3ProgrammaticOptions.builder()
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
                .putBuckets(
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
            ImmutableS3ProgrammaticOptions.builder()
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .clientIam(
                            ImmutableS3ClientIam.builder()
                                .policy("default-policy")
                                .externalId("default-id")
                                .roleSessionName("default-rsn")
                                .build())
                        .build())
                .putBuckets(
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
            ImmutableS3ProgrammaticOptions.builder()
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
                .putBuckets(
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
            ImmutableS3ProgrammaticOptions.builder()
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
                .putBuckets(
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
            ImmutableS3ProgrammaticOptions.builder()
                .defaultOptions(
                    ImmutableS3NamedBucketOptions.builder()
                        .serverIam(
                            ImmutableS3ServerIam.builder()
                                .policy("default-policy")
                                .externalId("default-id")
                                .roleSessionName("default-rsn")
                                .build())
                        .build())
                .putBuckets(
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
            ImmutableS3ProgrammaticOptions.builder()
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
                .putBuckets(
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
            ImmutableS3ProgrammaticOptions.builder().build(),
            // expected options
            ImmutableS3NamedBucketOptions.builder()
                .clientIam(noClientIam)
                .serverIam(noServerIam)
                .build(),
            // client IAM
            null,
            // server IAM
            null)
        //
        );
  }
}
