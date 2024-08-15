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

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestS3NamedBucketOptions {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  void validIamPolicies(S3NamedBucketOptions options) {
    soft.assertThatCode(() -> options.validate(options.name().orElseThrow()))
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> validIamPolicies() {
    return Stream.of(
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .clientIam(
                    ImmutableS3ClientIam.builder()
                        .enabled(true)
                        .policy(
                            "{ \"Version\":\"2012-10-17\",\n"
                                + "  \"Statement\": [\n"
                                + "    {\"Effect\":\"Allow\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*\"},\n"
                                + "    {\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blockedNamespace/*\"}\n"
                                + "   ]\n"
                                + "}\n")
                        .build())
                .build(),
            List.of("arn:aws:s3:::*", "arn:aws:s3:::*/blockedNamespace/*")),
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .clientIam(
                    ImmutableS3ClientIam.builder()
                        .enabled(true)
                        .statements(
                            List.of(
                                "{\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blocked\\\"Namespace/*\"}\n"))
                        .build())
                .build(),
            List.of(
                "arn:aws:s3:::foo/b\"ar/*",
                "arn:aws:s3:::foo/b\"ar",
                "arn:aws:s3:::*/blocked\"Namespace/*")));
  }

  @ParameterizedTest
  @MethodSource
  void invalidIamPolicies(S3NamedBucketOptions options, String message) {
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> options.validate(options.name().orElseThrow()))
        .withMessage(message);
  }

  static Stream<Arguments> invalidIamPolicies() {
    return Stream.of(
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .clientIam(
                    ImmutableS3ClientIam.builder()
                        .enabled(true)
                        .policy(
                            "{ \"Version\":\"2012-10-17\",\n"
                                + "  \"Statement\": [\n"
                                + "    {\"Effect\":\"Allow\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*},\n")
                        .build())
                .build(),
            "The client-iam.policy option for the bucketName bucket contains an invalid policy"),
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .serverIam(
                    ImmutableS3ServerIam.builder()
                        .enabled(true)
                        .policy(
                            "{ \"Version\":\"2012-10-17\",\n"
                                + "  \"Statement\": [\n"
                                + "    {\"Effect\":\"Allow\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*},\n")
                        .build())
                .build(),
            "The server-iam.policy option for the bucketName bucket contains an invalid policy"),
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .clientIam(
                    ImmutableS3ClientIam.builder()
                        .enabled(true)
                        .statements(
                            List.of(
                                "\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blockedNamespace/*\"}\n"))
                        .build())
                .build(),
            "The dynamically constructed iam-policy for the bucketName bucket results in an invalid policy, check the client-iam.statements option"));
  }
}
