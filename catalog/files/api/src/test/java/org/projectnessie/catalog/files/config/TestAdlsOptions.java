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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions.AdlsRetryStrategy;
import org.projectnessie.storage.uri.StorageUri;

public class TestAdlsOptions {

  @ParameterizedTest
  @MethodSource
  public void resolveOptionsForUri(AdlsOptions options, StorageUri uri, String expectedName) {
    AdlsNamedFileSystemOptions resolved = options.resolveOptionsForUri(uri);
    assertThat(resolved.name()).isEqualTo(Optional.ofNullable(expectedName));
  }

  static Stream<Arguments> resolveOptionsForUri() {
    return Stream.of(
        arguments(
            ImmutableAdlsOptions.builder()
                .putFileSystem(
                    "foo",
                    ImmutableAdlsNamedFileSystemOptions.builder()
                        .name("my-bucket")
                        .authority("my-authority")
                        .pathPrefix("foop")
                        .build())
                .putFileSystem(
                    "bar",
                    ImmutableAdlsNamedFileSystemOptions.builder()
                        .name("other-bucket")
                        .authority("my-authority")
                        .pathPrefix("baaz")
                        .build())
                .build(),
            StorageUri.of("s3://my-authority/foop"),
            "my-bucket"),
        //
        arguments(
            ImmutableAdlsOptions.builder()
                .putFileSystem(
                    "foo",
                    ImmutableAdlsNamedFileSystemOptions.builder()
                        .name("my-bucket")
                        .authority("my-authority")
                        .pathPrefix("foop")
                        .build())
                .putFileSystem(
                    "baz",
                    ImmutableAdlsNamedFileSystemOptions.builder()
                        .name("too-short-path-prefix")
                        .authority("my-authority")
                        .pathPrefix("fo")
                        .build())
                .putFileSystem(
                    "bar",
                    ImmutableAdlsNamedFileSystemOptions.builder()
                        .name("other-bucket")
                        .authority("my-authority")
                        .pathPrefix("baaz")
                        .build())
                .build(),
            StorageUri.of("s3://my-authority/foop"),
            "my-bucket"),
        //
        arguments(
            ImmutableAdlsOptions.builder()
                .putFileSystem(
                    "foo",
                    ImmutableAdlsNamedFileSystemOptions.builder()
                        .name("my-bucket")
                        .authority("my-authority")
                        .pathPrefix("foop")
                        .build())
                .putFileSystem(
                    "bar",
                    ImmutableAdlsNamedFileSystemOptions.builder()
                        .name("other-bucket")
                        .authority("my-authority")
                        .pathPrefix("baaz")
                        .build())
                .build(),
            StorageUri.of("s3://my-authority/baaz/blah/moo"),
            "other-bucket"),
        //
        arguments(
            ImmutableAdlsOptions.builder()
                .putFileSystem(
                    "foo",
                    ImmutableAdlsNamedFileSystemOptions.builder()
                        .name("my-bucket")
                        .authority("my-authority")
                        .pathPrefix("foop")
                        .build())
                .putFileSystem(
                    "bar",
                    ImmutableAdlsNamedFileSystemOptions.builder()
                        .name("other-bucket")
                        .authority("my-authority")
                        .pathPrefix("baaz")
                        .build())
                .build(),
            StorageUri.of("s3://my-authority/"),
            null));
  }

  @ParameterizedTest
  @MethodSource
  public void missingEndpoint(AdlsOptions options, String keys) {
    assertThatThrownBy(options::validate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Mandatory ADLS endpoint is not configured for file system '")
        .hasMessageEndingWith("'.")
        .hasMessageContaining(keys);
  }

  @ParameterizedTest
  @MethodSource
  public void goodEndpoint(AdlsOptions options) {
    assertThatCode(options::validate).doesNotThrowAnyException();
  }

  static Stream<Arguments> missingEndpoint() {
    return Stream.of(
        arguments(
            ImmutableAdlsOptions.builder()
                .defaultOptions(ImmutableAdlsNamedFileSystemOptions.builder().build())
                .putFileSystem("fs1", ImmutableAdlsNamedFileSystemOptions.builder().build())
                .putFileSystem(
                    "fs2", ImmutableAdlsNamedFileSystemOptions.builder().endpoint("ep").build())
                .build(),
            "'fs1'"),
        arguments(
            ImmutableAdlsOptions.builder()
                .defaultOptions(ImmutableAdlsNamedFileSystemOptions.builder().build())
                .putFileSystem("fs1", ImmutableAdlsNamedFileSystemOptions.builder().build())
                .putFileSystem("fs2", ImmutableAdlsNamedFileSystemOptions.builder().build())
                .build(),
            "'fs1', 'fs2'"),
        arguments(
            ImmutableAdlsOptions.builder()
                .putFileSystem("fs1", ImmutableAdlsNamedFileSystemOptions.builder().build())
                .putFileSystem(
                    "fs2", ImmutableAdlsNamedFileSystemOptions.builder().endpoint("ep").build())
                .build(),
            "'fs1'"),
        arguments(
            ImmutableAdlsOptions.builder()
                .putFileSystem("fs1", ImmutableAdlsNamedFileSystemOptions.builder().build())
                .putFileSystem("fs2", ImmutableAdlsNamedFileSystemOptions.builder().build())
                .build(),
            "'fs1', 'fs2'"));
  }

  static Stream<AdlsOptions> goodEndpoint() {
    return Stream.of(
        ImmutableAdlsOptions.builder().build(),
        ImmutableAdlsOptions.builder()
            .defaultOptions(ImmutableAdlsNamedFileSystemOptions.builder().build())
            .build(),
        ImmutableAdlsOptions.builder()
            .defaultOptions(
                ImmutableAdlsNamedFileSystemOptions.builder().endpoint("endpoint").build())
            .putFileSystem("fs1", ImmutableAdlsNamedFileSystemOptions.builder().build())
            .putFileSystem(
                "fs2", ImmutableAdlsNamedFileSystemOptions.builder().endpoint("ep").build())
            .build(),
        ImmutableAdlsOptions.builder()
            .defaultOptions(ImmutableAdlsNamedFileSystemOptions.builder().build())
            .putFileSystem(
                "fs1", ImmutableAdlsNamedFileSystemOptions.builder().endpoint("ep1").build())
            .putFileSystem(
                "fs2", ImmutableAdlsNamedFileSystemOptions.builder().endpoint("ep2").build())
            .build(),
        ImmutableAdlsOptions.builder()
            .putFileSystem(
                "fs1", ImmutableAdlsNamedFileSystemOptions.builder().endpoint("ep1").build())
            .putFileSystem(
                "fs2", ImmutableAdlsNamedFileSystemOptions.builder().endpoint("ep2").build())
            .build());
  }

  @Test
  void deepClone() {
    AdlsOptions input =
        ImmutableAdlsOptions.builder()
            .readBlockSize(1)
            .writeBlockSize(2)
            .defaultOptions(
                ImmutableAdlsNamedFileSystemOptions.builder()
                    .endpoint("endpoint")
                    .externalEndpoint("externalEndpoint")
                    .retryPolicy(AdlsRetryStrategy.EXPONENTIAL_BACKOFF)
                    .maxRetries(3)
                    .tryTimeout(Duration.ofSeconds(1))
                    .retryDelay(Duration.ofSeconds(2))
                    .maxRetryDelay(Duration.ofSeconds(3))
                    .build())
            .putFileSystem(
                "fs1",
                ImmutableAdlsNamedFileSystemOptions.builder()
                    .endpoint("endpoint1")
                    .externalEndpoint("externalEndpoint1")
                    .retryPolicy(AdlsRetryStrategy.FIXED_DELAY)
                    .maxRetries(4)
                    .tryTimeout(Duration.ofSeconds(4))
                    .retryDelay(Duration.ofSeconds(5))
                    .maxRetryDelay(Duration.ofSeconds(6))
                    .build())
            .putFileSystem(
                "fs2",
                ImmutableAdlsNamedFileSystemOptions.builder()
                    .name("my-bucket-2")
                    .endpoint("endpoint2")
                    .externalEndpoint("externalEndpoint2")
                    .retryPolicy(AdlsRetryStrategy.NONE)
                    .maxRetries(5)
                    .tryTimeout(Duration.ofSeconds(7))
                    .retryDelay(Duration.ofSeconds(8))
                    .maxRetryDelay(Duration.ofSeconds(9))
                    .build())
            .build();
    AdlsOptions expected =
        ImmutableAdlsOptions.builder()
            .readBlockSize(1)
            .writeBlockSize(2)
            .defaultOptions(
                ImmutableAdlsNamedFileSystemOptions.builder()
                    .endpoint("endpoint")
                    .externalEndpoint("externalEndpoint")
                    .retryPolicy(AdlsRetryStrategy.EXPONENTIAL_BACKOFF)
                    .maxRetries(3)
                    .tryTimeout(Duration.ofSeconds(1))
                    .retryDelay(Duration.ofSeconds(2))
                    .maxRetryDelay(Duration.ofSeconds(3))
                    .build())
            .putFileSystem(
                "fs1",
                ImmutableAdlsNamedFileSystemOptions.builder()
                    .endpoint("endpoint1")
                    .externalEndpoint("externalEndpoint1")
                    .retryPolicy(AdlsRetryStrategy.FIXED_DELAY)
                    .maxRetries(4)
                    .tryTimeout(Duration.ofSeconds(4))
                    .retryDelay(Duration.ofSeconds(5))
                    .maxRetryDelay(Duration.ofSeconds(6))
                    .build())
            .putFileSystem(
                "fs2",
                ImmutableAdlsNamedFileSystemOptions.builder()
                    .name("my-bucket-2")
                    .endpoint("endpoint2")
                    .externalEndpoint("externalEndpoint2")
                    .retryPolicy(AdlsRetryStrategy.NONE)
                    .maxRetries(5)
                    .tryTimeout(Duration.ofSeconds(7))
                    .retryDelay(Duration.ofSeconds(8))
                    .maxRetryDelay(Duration.ofSeconds(9))
                    .build())
            .build();
    AdlsOptions actual = input.deepClone();
    assertThat(actual).isEqualTo(expected);
  }
}
