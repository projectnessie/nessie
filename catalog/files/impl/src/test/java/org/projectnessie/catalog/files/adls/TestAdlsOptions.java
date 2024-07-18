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
package org.projectnessie.catalog.files.adls;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.adls.AdlsProgrammaticOptions.AdlsPerFileSystemOptions;

public class TestAdlsOptions {
  @ParameterizedTest
  @MethodSource
  public void missingEndpoint(AdlsOptions<?> options, String keys) {
    assertThatThrownBy(options::checkEndpoint)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Mandatory ADLS endpoint is not configured for file system '")
        .hasMessageEndingWith("'.")
        .hasMessageContaining(keys);
  }

  @ParameterizedTest
  @MethodSource
  public void goodEndpoint(AdlsOptions<?> options) {
    assertThatCode(options::checkEndpoint).doesNotThrowAnyException();
  }

  static Stream<Arguments> missingEndpoint() {
    return Stream.of(
        arguments(
            AdlsProgrammaticOptions.builder()
                .defaultOptions(AdlsPerFileSystemOptions.builder().build())
                .putFileSystems("fs1", AdlsPerFileSystemOptions.builder().build())
                .putFileSystems("fs2", AdlsPerFileSystemOptions.builder().endpoint("ep").build())
                .build(),
            "'fs1'"),
        arguments(
            AdlsProgrammaticOptions.builder()
                .defaultOptions(AdlsPerFileSystemOptions.builder().build())
                .putFileSystems("fs1", AdlsPerFileSystemOptions.builder().build())
                .putFileSystems("fs2", AdlsPerFileSystemOptions.builder().build())
                .build(),
            "'fs1', 'fs2'"),
        arguments(
            AdlsProgrammaticOptions.builder()
                .putFileSystems("fs1", AdlsPerFileSystemOptions.builder().build())
                .putFileSystems("fs2", AdlsPerFileSystemOptions.builder().endpoint("ep").build())
                .build(),
            "'fs1'"),
        arguments(
            AdlsProgrammaticOptions.builder()
                .putFileSystems("fs1", AdlsPerFileSystemOptions.builder().build())
                .putFileSystems("fs2", AdlsPerFileSystemOptions.builder().build())
                .build(),
            "'fs1', 'fs2'"));
  }

  static Stream<AdlsOptions<?>> goodEndpoint() {
    return Stream.of(
        AdlsProgrammaticOptions.builder().build(),
        AdlsProgrammaticOptions.builder()
            .defaultOptions(AdlsPerFileSystemOptions.builder().build())
            .build(),
        AdlsProgrammaticOptions.builder()
            .defaultOptions(AdlsPerFileSystemOptions.builder().endpoint("endpoint").build())
            .putFileSystems("fs1", AdlsPerFileSystemOptions.builder().build())
            .putFileSystems("fs2", AdlsPerFileSystemOptions.builder().endpoint("ep").build())
            .build(),
        AdlsProgrammaticOptions.builder()
            .defaultOptions(AdlsPerFileSystemOptions.builder().build())
            .putFileSystems("fs1", AdlsPerFileSystemOptions.builder().endpoint("ep1").build())
            .putFileSystems("fs2", AdlsPerFileSystemOptions.builder().endpoint("ep2").build())
            .build(),
        AdlsProgrammaticOptions.builder()
            .putFileSystems("fs1", AdlsPerFileSystemOptions.builder().endpoint("ep1").build())
            .putFileSystems("fs2", AdlsPerFileSystemOptions.builder().endpoint("ep2").build())
            .build());
  }
}
