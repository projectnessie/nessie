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
package org.projectnessie.catalog.files.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.files.gcs.GcsLocation.gcsLocation;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.storage.uri.StorageUri;

public class TestGcsLocation {

  @ParameterizedTest
  @MethodSource
  void location(StorageUri storageUri, String bucket, String path) {
    assertThat(gcsLocation(storageUri))
        .extracting(GcsLocation::bucket, GcsLocation::path)
        .containsExactly(bucket, path);
  }

  static Stream<Arguments> location() {
    return Stream.of(
        arguments(StorageUri.of("gs://mybucket"), "mybucket", ""),
        arguments(StorageUri.of("gs://mybucket/"), "mybucket", ""),
        arguments(StorageUri.of("gs://mybucket/"), "mybucket", ""),
        arguments(StorageUri.of("gs://mybucket/foo"), "mybucket", "foo"),
        arguments(StorageUri.of("gs://mybucket/foo/bar"), "mybucket", "foo/bar"),
        arguments(StorageUri.of("gs://mybucket/foo/bar/"), "mybucket", "foo/bar/"));
  }
}
