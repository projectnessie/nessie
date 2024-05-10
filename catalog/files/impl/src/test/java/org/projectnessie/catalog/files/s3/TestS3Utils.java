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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestS3Utils {

  @ParameterizedTest
  @MethodSource
  void extractBucket(S3BucketOptions bucketOptions, URI uri, Optional<String> expectedBucket) {
    Optional<String> actualBucket = bucketOptions.extractBucket(uri);
    assertThat(actualBucket).isEqualTo(expectedBucket);
  }

  static Stream<Arguments> extractBucket() {
    return Stream.of(
        // AMAZON
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.AMAZON)).build(),
            URI.create("s3://mybucket/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.AMAZON)).build(),
            URI.create("https://mybucket.s3.us-east-1.amazonaws.com/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.AMAZON)).build(),
            URI.create("https://mybucket.s3-us-east-1.amazonaws.com/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.AMAZON)).build(),
            URI.create("https://mybucket.s3.amazonaws.com/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.AMAZON)).build(),
            URI.create("https://s3.us-east-1.amazonaws.com/mybucket/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.AMAZON)).build(),
            URI.create("https://s3-us-east-1.amazonaws.com/mybucket/myfile"),
            Optional.of("mybucket")),
        // PRIVATE
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.PRIVATE)).build(),
            URI.create("https://user@mybucket.s3.region1.private.com:9000/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.PRIVATE)).build(),
            URI.create("https://user@mybucket.s3-region1.private.com:9000/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.PRIVATE)).build(),
            URI.create("https://user@mybucket.s3.private.com:9000/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.PRIVATE)).build(),
            URI.create("https://user@s3.region1.private.amazonaws.com:9000/mybucket/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.PRIVATE)).build(),
            URI.create("https://user@s3-region1.private.amazonaws.com:9000/mybucket/myfile"),
            Optional.of("mybucket")),
        // non-standard ones for testing
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder()
                .cloud(Optional.of(Cloud.PRIVATE))
                .pathStyleAccess(true)
                .build(),
            URI.create("http://127.0.0.1:9000/mybucket/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder()
                .cloud(Optional.of(Cloud.PRIVATE))
                .pathStyleAccess(true)
                .build(),
            URI.create("http://127.0.0.1:9000/mybucket/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder()
                .cloud(Optional.of(Cloud.PRIVATE))
                .pathStyleAccess(true)
                .build(),
            URI.create("http://s3.foo:9000/mybucket/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder()
                .cloud(Optional.of(Cloud.PRIVATE))
                .pathStyleAccess(true)
                .build(),
            URI.create("http://s3.localhost:9000/mybucket/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            ImmutableS3ProgrammaticOptions.builder().cloud(Optional.of(Cloud.PRIVATE)).build(),
            URI.create("http://s3.localhost.localdomain:9000/mybucket/myfile"),
            Optional.of("mybucket")));
  }
}
