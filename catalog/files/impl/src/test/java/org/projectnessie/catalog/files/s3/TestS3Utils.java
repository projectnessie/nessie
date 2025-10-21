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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.net.URI;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestS3Utils {

  @ParameterizedTest
  @MethodSource
  void iamEscapeString(String input, String expected) {
    assertThat(S3Utils.iamEscapeString(input)).isEqualTo(expected);
  }

  static Stream<Arguments> iamEscapeString() {
    return Stream.of(
        arguments("Hello*world$anybody?there{here}", "Hello${*}world${$}anybody${?}there{here}"));
  }

  @ParameterizedTest
  @MethodSource
  void extractBucketName(URI uri, Optional<String> expectedBucket) {
    Optional<String> actualBucket = S3Utils.extractBucketName(uri);
    assertThat(actualBucket).isEqualTo(expectedBucket);
  }

  static Stream<Arguments> extractBucketName() {
    return Stream.of(
        // AMAZON
        Arguments.of(URI.create("s3://mybucket/myfile"), Optional.of("mybucket")),
        Arguments.of(
            URI.create("https://mybucket.s3.us-east-1.amazonaws.com/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            URI.create("https://mybucket.s3-us-east-1.amazonaws.com/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            URI.create("https://mybucket.s3.amazonaws.com/myfile"), Optional.of("mybucket")),
        Arguments.of(
            URI.create("https://s3.us-east-1.amazonaws.com/mybucket/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            URI.create("https://s3-us-east-1.amazonaws.com/mybucket/myfile"),
            Optional.of("mybucket")),
        // PRIVATE
        Arguments.of(
            URI.create("https://user@mybucket.s3.region1.private.com:9000/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            URI.create("https://user@mybucket.s3-region1.private.com:9000/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            URI.create("https://user@mybucket.s3.private.com:9000/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            URI.create("https://user@s3.region1.private.amazonaws.com:9000/mybucket/myfile"),
            Optional.of("mybucket")),
        Arguments.of(
            URI.create("https://user@s3-region1.private.amazonaws.com:9000/mybucket/myfile"),
            Optional.of("mybucket")),
        // non-standard ones for testing
        Arguments.of(URI.create("http://127.0.0.1:9000/mybucket/myfile"), Optional.of("mybucket")),
        Arguments.of(URI.create("http://127.0.0.1:9000/mybucket/myfile"), Optional.of("mybucket")),
        Arguments.of(URI.create("http://s3.foo:9000/mybucket/myfile"), Optional.of("mybucket")),
        Arguments.of(
            URI.create("http://s3.localhost:9000/mybucket/myfile"), Optional.of("mybucket")),
        Arguments.of(
            URI.create("http://s3.localhost.localdomain:9000/mybucket/myfile"),
            Optional.of("mybucket")));
  }

  @ParameterizedTest
  @CsvSource({
    "https://mybucket.s3.us-east-1.amazonaws.com/mydir/myfile      , s3://mybucket/mydir/myfile",
    "https://mybucket.s3-us-east-1.amazonaws.com/mydir/myfile      , s3://mybucket/mydir/myfile",
    "https://s3.us-east-1.amazonaws.com/mybucket/mydir/myfile      , s3://mybucket/mydir/myfile",
    "https://s3-us-east-1.amazonaws.com/mybucket/mydir/myfile      , s3://mybucket/mydir/myfile",
    "https://user@mybucket.s3.private.com:9000/mydir/myfile        , s3://mybucket/mydir/myfile",
    "https://user@s3.region1.private.com:9000/mybucket/mydir/myfile, s3://mybucket/mydir/myfile",
    "http://127.0.0.1:9000/mybucket/mydir/myfile                   , s3://mybucket/mydir/myfile",
    "http://s3.localhost.localdomain:9000/mybucket/mydir/myfile    , s3://mybucket/mydir/myfile",
    "http://mybucket.s3.us-east-1.amazonaws.com/                   , s3://mybucket/",
    "http://s3.localhost.localdomain:9000/mybucket                 , s3://mybucket",
    "http://127.0.0.1:9000/mybucket                                , s3://mybucket",
  })
  void asS3Location(String location, String expected) {
    assertThat(S3Utils.asS3Location(location)).isEqualTo(expected);
  }

  @Test
  void asS3LocationErrors() {
    assertThatThrownBy(() -> S3Utils.asS3Location("ftp://localhost:9000/mybucket/mydir/myfile"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported URI scheme: 'ftp'");
    assertThatThrownBy(() -> S3Utils.asS3Location("/mybucket/mydir/myfile"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No scheme in URI: '/mybucket/mydir/myfile'");
    assertThatThrownBy(() -> S3Utils.asS3Location("http:///mybucket/mydir/myfile"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No host in URI: 'http:///mybucket/mydir/myfile'");
    assertThatThrownBy(() -> S3Utils.asS3Location("https://s3.us-east-1.amazonaws.com"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid S3 URI: 'https://s3.us-east-1.amazonaws.com'");
  }
}
