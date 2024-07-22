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
package org.projectnessie.server.catalog;

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
import org.projectnessie.catalog.files.s3.ImmutableS3Iam;
import org.projectnessie.catalog.files.s3.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.s3.S3NamedBucketOptions;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCatalogProducers {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  void validIamPolicies(S3NamedBucketOptions options) {
    soft.assertThatCode(
            () -> CatalogProducers.validateS3BucketConfig(options.name().orElseThrow(), options))
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> validIamPolicies() {
    return Stream.of(
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .clientIam(
                    ImmutableS3Iam.builder()
                        .enabled(true)
                        .policy(
                            """
                            { "Version":"2012-10-17",
                              "Statement": [
                                {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*"},
                                {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blockedNamespace/*"}
                               ]
                            }
                            """)
                        .build())
                .build(),
            List.of("arn:aws:s3:::*", "arn:aws:s3:::*/blockedNamespace/*")),
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .clientIam(ImmutableS3Iam.builder().enabled(true).build())
                .clientIamStatements(
                    List.of(
                        """
                        {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blocked\\"Namespace/*"}
                        """))
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
        .isThrownBy(
            () -> CatalogProducers.validateS3BucketConfig(options.name().orElseThrow(), options))
        .withMessage(message);
  }

  static Stream<Arguments> invalidIamPolicies() {
    return Stream.of(
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .clientIam(
                    ImmutableS3Iam.builder()
                        .enabled(true)
                        .policy(
                            """
                            { "Version":"2012-10-17",
                              "Statement": [
                                {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*},
                            """)
                        .build())
                .build(),
            "The client-iam-policy option for the bucketName bucket contains an invalid policy"),
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .serverIam(
                    ImmutableS3Iam.builder()
                        .enabled(true)
                        .policy(
                            """
                            { "Version":"2012-10-17",
                              "Statement": [
                                {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*},
                            """)
                        .build())
                .build(),
            "The server-iam-policy option for the bucketName bucket contains an invalid policy"),
        arguments(
            ImmutableS3NamedBucketOptions.builder()
                .name("bucketName")
                .clientIam(ImmutableS3Iam.builder().enabled(true).build())
                .clientIamStatements(
                    List.of(
                        """
                        "Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blockedNamespace/*"}
                        """))
                .build(),
            "The dynamically constructed iam-policy for the bucketName bucket results in an invalid policy, check the client-iam-statements"));
  }
}
