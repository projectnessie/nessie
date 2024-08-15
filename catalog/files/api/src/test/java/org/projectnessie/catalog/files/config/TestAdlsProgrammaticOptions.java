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

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions.AdlsRetryStrategy;

class TestAdlsProgrammaticOptions {

  @Test
  void normalize() {
    AdlsOptions input =
        ImmutableAdlsProgrammaticOptions.builder()
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
            .putFileSystems(
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
            .putFileSystems(
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
        ImmutableAdlsProgrammaticOptions.builder()
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
            .putFileSystems(
                "fs1",
                ImmutableAdlsNamedFileSystemOptions.builder()
                    .name("fs1")
                    .endpoint("endpoint1")
                    .externalEndpoint("externalEndpoint1")
                    .retryPolicy(AdlsRetryStrategy.FIXED_DELAY)
                    .maxRetries(4)
                    .tryTimeout(Duration.ofSeconds(4))
                    .retryDelay(Duration.ofSeconds(5))
                    .maxRetryDelay(Duration.ofSeconds(6))
                    .build())
            .putFileSystems(
                "my-bucket-2",
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
    AdlsOptions actual = AdlsProgrammaticOptions.normalize(input);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void normalizeBuckets() {
    assertThat(
            ImmutableAdlsProgrammaticOptions.builder()
                .putFileSystems("fs1", ImmutableAdlsNamedFileSystemOptions.builder().build())
                .putFileSystems(
                    "fs2", ImmutableAdlsNamedFileSystemOptions.builder().name("my-bucket").build())
                .build()
                .fileSystems())
        .containsOnlyKeys("fs1", "my-bucket");
    assertThatThrownBy(
            () ->
                ImmutableAdlsProgrammaticOptions.builder()
                    .putFileSystems("fs1", ImmutableAdlsNamedFileSystemOptions.builder().build())
                    .putFileSystems(
                        "fs2", ImmutableAdlsNamedFileSystemOptions.builder().name("fs1").build())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Duplicate ADLS filesystem name 'fs1', check your ADLS file system configurations");
  }
}
